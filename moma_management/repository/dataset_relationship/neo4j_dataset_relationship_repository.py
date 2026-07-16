from logging import getLogger
from typing import Any, Dict, List, Optional
from uuid import UUID

from neo4j import AsyncSession

from moma_management.domain.dataset_relationship import DatasetRelationship
from moma_management.domain.generated.edges.edge_schema import Edge
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.repository.dataset_relationship.dataset_relationship_repository import (
    DatasetRelationshipRepository,
)
from moma_management.repository.neo4j_pgson_mixin import Neo4jPgJsonMixin

logger = getLogger(__name__)


class Neo4jDatasetRelationshipRepository(Neo4jPgJsonMixin, DatasetRelationshipRepository):
    """Neo4j-backed implementation of ``DatasetRelationshipRepository``."""

    # HAS_TARGET links relationship nodes to external sc:Dataset nodes and must
    # NOT be traversed when reading/deleting a relationship subgraph in isolation.
    FORBIDDEN_EDGES: list[str] = ["HAS_TARGET"]

    _INDEX_STATEMENTS: list[str] = [
        "CREATE CONSTRAINT basic_dl_element_id_unique IF NOT EXISTS "
        "FOR (n:BasicDLElement) REQUIRE n.id IS UNIQUE",
        "CREATE INDEX basic_dl_element_id IF NOT EXISTS "
        "FOR (n:BasicDLElement) ON (n.id)",
        "CREATE CONSTRAINT property_comparison_id_unique IF NOT EXISTS "
        "FOR (n:PropertyComparison) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT text_evidence_id_unique IF NOT EXISTS "
        "FOR (n:TextEvidence) REQUIRE n.id IS UNIQUE",
    ]
    _indexes_ensured: bool = False

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @classmethod
    async def create_with_indexes(cls, session: AsyncSession) -> "Neo4jDatasetRelationshipRepository":
        repo = cls(session)
        if not cls._indexes_ensured:
            for stmt in cls._INDEX_STATEMENTS:
                await session.run(stmt)
            cls._indexes_ensured = True
            logger.info("Neo4jDatasetRelationshipRepository indexes ensured")
        return repo

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    async def create(self, relationship: DatasetRelationship) -> None:
        """Store the full DatasetRelationship subgraph using the mixin's MERGE/SET helpers."""
        await self._session.execute_write(self.create_pgson, relationship)

    async def delete(self, relationship_id: str) -> None:
        """Delete the relationship and its connected subgraph; leaves dataset nodes intact."""
        await self._session.run(
            """//cypher
            MATCH (root:BasicDLElement {id: $relationshipId})
            OPTIONAL MATCH path=(root)-[*1..4]-(m)
            WHERE NONE(r IN relationships(path) WHERE type(r) IN $forbiddenEdges)
            WITH root, collect(DISTINCT m) AS related
            FOREACH (n IN related | DETACH DELETE n)
            DETACH DELETE root
            """,
            relationshipId=str(relationship_id),
            forbiddenEdges=self.FORBIDDEN_EDGES,
        )

    async def delete_referencing(self, dataset_id: str) -> None:
        """Delete every relationship subgraph that targets *dataset_id* (cascade on dataset deletion).

        Only follows the relationship's own internal edges (HAS_COMPARISON /
        HAS_EVIDENCE) from the entry point(s) found via HAS_TARGET, so a
        relationship linking dataset A and dataset B is fully removed when A
        is deleted, without ever touching B.
        """
        await self._session.run(
            """//cypher
            MATCH (target:`sc:Dataset` {id: $datasetId})<-[:HAS_TARGET]-(seed)
            WHERE NOT seed:`sc:Dataset`
            OPTIONAL MATCH path=(seed)-[:HAS_COMPARISON|HAS_EVIDENCE*0..10]-(m)
            WHERE NONE(x IN nodes(path) WHERE x:`sc:Dataset`)
            WITH collect(DISTINCT seed) + collect(DISTINCT m) AS toDelete
            UNWIND toDelete AS n
            WITH DISTINCT n
            WHERE n IS NOT NULL
            DETACH DELETE n
            """,
            datasetId=str(dataset_id),
        )

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def list_for_dataset(self, dataset_id: str) -> List[DatasetRelationship]:
        """Return every DatasetRelationship whose root directly targets *dataset_id*."""
        result = await self._session.run(
            """//cypher
            MATCH (root:BasicDLElement)-[:HAS_TARGET]->(:`sc:Dataset` {id: $datasetId})
            RETURN root.id AS relationshipId
            """,
            datasetId=str(dataset_id),
        )
        relationship_ids = [record["relationshipId"] async for record in result]
        relationships = [await self.get(rid) for rid in relationship_ids]
        return [r for r in relationships if r is not None]

    async def find_id_for_dataset_pair(self, dataset_id_a: str, dataset_id_b: str) -> Optional[str]:
        """Return the root ID of an existing relationship between the two datasets, if any."""
        result = await self._session.run(
            """//cypher
            MATCH (root:BasicDLElement)-[:HAS_TARGET]->(d1:`sc:Dataset` {id: $idA})
            MATCH (root)-[:HAS_TARGET]->(d2:`sc:Dataset` {id: $idB})
            RETURN root.id AS existingId
            LIMIT 1
            """,
            idA=str(dataset_id_a),
            idB=str(dataset_id_b),
        )
        record = await result.single()
        return record["existingId"] if record else None

    async def get(self, relationship_id: str) -> Optional[DatasetRelationship]:
        """Shallow retrieval: root + connected subgraph (excluding HAS_TARGET)."""
        query = """//cypher
            MATCH (root:BasicDLElement {id: $relationshipId})
            OPTIONAL MATCH path=(root)-[*1..4]-(m)
            WHERE NONE(r IN relationships(path) WHERE type(r) IN $forbiddenEdges)
            RETURN root, m, relationships(path) AS rels
        """
        result = await self._session.run(
            query, relationshipId=str(relationship_id), forbiddenEdges=self.FORBIDDEN_EDGES)
        rows = [record async for record in result]
        if not rows:
            return None

        nodes: Dict[str, Any] = {}
        edges: Dict[Any, Any] = {}

        root = rows[0]["root"]
        if root is None:
            return None
        nodes[root["id"]] = self._deserialize_node(root)

        for record in rows:
            m = record["m"]
            rels = record["rels"] or []
            if m:
                mid = m["id"]
                if mid not in nodes:
                    nodes[mid] = self._deserialize_node(m)
            for rel in rels:
                key = (rel.start_node["id"], rel.end_node["id"], rel.type)
                if key not in edges:
                    edges[key] = self._deserialize_edge(rel)

        valid_edges = [
            e for e in edges.values()
            if e["from"] in nodes and e["to"] in nodes
        ]

        node_objects = [
            Node.model_construct(id=UUID(n["id"]), labels=n["labels"], properties=n["properties"])
            for n in nodes.values()
        ]
        edge_objects: Optional[List[Edge]] = (
            [
                Edge.model_construct(
                    from_=UUID(e["from"]), to=UUID(e["to"]),
                    labels=e["labels"], properties=e.get("properties"),
                )
                for e in valid_edges
            ]
            if valid_edges else None
        )
        return DatasetRelationship.model_construct(nodes=node_objects, edges=edge_objects)
