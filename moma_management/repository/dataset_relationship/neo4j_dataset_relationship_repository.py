from logging import getLogger
from typing import Any, Dict, List, Optional
from uuid import UUID

from neo4j import AsyncSession

from moma_management.domain.dataset_relationship import DatasetRelationship
from moma_management.domain.generated.edges.edge_schema import Edge, EdgeLabel
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.repository.dataset_relationship.dataset_relationship_repository import (
    DatasetRelationshipRepository,
)
from moma_management.repository.neo4j_pgson_mixin import Neo4jPgJsonMixin

logger = getLogger(__name__)


class Neo4jDatasetRelationshipRepository(Neo4jPgJsonMixin, DatasetRelationshipRepository):
    """Neo4j-backed implementation of ``DatasetRelationshipRepository``."""

    # HAS_TARGET links relationship nodes to external sc:Dataset / cr:FileObject
    # nodes and must NOT be traversed when deleting a relationship subgraph in
    # isolation. (Reads stop *at* those nodes instead — see ``get``.)
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
        "CREATE CONSTRAINT file_object_linking_element_id_unique IF NOT EXISTS "
        "FOR (n:FileObjectLinkingElement) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT file_object_comparison_id_unique IF NOT EXISTS "
        "FOR (n:FileObjectComparison) REQUIRE n.id IS UNIQUE",
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
        """Store the DatasetRelationship subgraph using the mixin's MERGE/SET helpers.

        The targeted ``sc:Dataset`` and ``cr:FileObject`` nodes are references,
        not payload: they are dropped before writing so their stored properties
        and labels are never overwritten by whatever the caller put in the
        request body. The service has already checked they exist, and
        ``create_pgson_edge`` binds them with ``MATCH``. Edges starting from a
        reference (the optional ``distribution`` edges) belong to the dataset:
        the service checked they match what is stored, and they are dropped too.
        """
        reference_labels = DatasetRelationship.REFERENCE_LABELS
        reference_ids = {str(n.id) for n in relationship.nodes if reference_labels & set(n.labels)}
        await self._session.execute_write(
            self.create_pgson,
            relationship.model_copy(update={
                "nodes": [n for n in relationship.nodes if str(n.id) not in reference_ids],
                "edges": [e for e in relationship.edges or [] if str(e.from_) not in reference_ids],
            }),
        )

    async def delete(self, relationship_id: str) -> None:
        """Delete the relationship and its connected subgraph; leaves dataset nodes intact."""
        result = await self._session.run(
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
        await result.consume()

    async def delete_referencing(self, dataset_id: str) -> None:
        """Delete every relationship subgraph that targets *dataset_id* (cascade on dataset deletion).

        Only follows the relationship's own internal edges (HAS_COMPARISON /
        HAS_EVIDENCE / HAS_DETAIL) from the entry point(s) found via HAS_TARGET, so a
        relationship linking dataset A and dataset B is fully removed when A
        is deleted, without ever touching B.
        """
        result = await self._session.run(
            """//cypher
            MATCH (target:`sc:Dataset` {id: $datasetId})<-[:HAS_TARGET]-(seed)
            WHERE NOT seed:`sc:Dataset`
            OPTIONAL MATCH path=(seed)-[:HAS_COMPARISON|HAS_EVIDENCE|HAS_DETAIL*0..10]-(m)
            WHERE NONE(x IN nodes(path) WHERE x:`sc:Dataset`)
            WITH collect(DISTINCT seed) + collect(DISTINCT m) AS toDelete
            UNWIND toDelete AS n
            WITH DISTINCT n
            WHERE n IS NOT NULL
            DETACH DELETE n
            """,
            datasetId=str(dataset_id),
        )
        await result.consume()

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
        """
        Shallow retrieval: Return relationship subgraph + refs to the datasets
        and file objects it targets. A ref has its id and its reference label
        (sc:Dataset / cr:FileObject) only: no properties, no other labels.
        The stored ``distribution`` edges of the linked files are added, so a
        reader knows which of the two datasets each file belongs to.
        """
        query = """//cypher
            MATCH (root:BasicDLElement {id: $relationshipId})
            OPTIONAL MATCH path=(root)-[*1..4]-(m)
            WHERE NONE(n IN nodes(path)[0..-1] WHERE n:`sc:Dataset` OR n:`cr:FileObject`)
            RETURN root, m, relationships(path) AS rels
        """
        result = await self._session.run(query, relationshipId=str(relationship_id))
        rows = [record async for record in result]
        if not rows:
            return None

        nodes: Dict[str, Any] = {}
        edges: Dict[Any, Any] = {}

        root = rows[0]["root"]
        if root is None:
            return None
        reference_labels = DatasetRelationship.REFERENCE_LABELS
        nodes[root["id"]] = self._deserialize_node(root)

        for record in rows:
            m = record["m"]
            rels = record["rels"] or []
            if m:
                mid = m["id"]
                if mid not in nodes:
                    node = self._deserialize_node(m)
                    # NOTE: References come back as stubs. A stored file object is
                    # also e.g. PDF/Data, and a Data node without properties is invalid.
                    if reference_labels & set(node["labels"]):
                        node["labels"] = [l for l in node["labels"] if l in reference_labels]
                        node["properties"] = {}
                    nodes[mid] = node
            for rel in rels:
                key = (rel.start_node["id"], rel.end_node["id"], rel.type)
                if key not in edges:
                    edges[key] = self._deserialize_edge(rel)

        # The traversal stops at references, so it never crosses a dataset's own
        # `distribution` edge: fetch the ones of the linked files separately.
        file_ids = [i for i, n in nodes.items() if "cr:FileObject" in n["labels"]]
        if file_ids:
            dataset_ids = [i for i, n in nodes.items() if "sc:Dataset" in n["labels"]]
            membership = await self._session.run(
                """//cypher
                MATCH (d:`sc:Dataset`)-[:distribution]->(f:`cr:FileObject`)
                WHERE d.id IN $datasetIds AND f.id IN $fileIds
                RETURN d.id AS datasetId, f.id AS fileId
                """,
                datasetIds=dataset_ids, fileIds=file_ids,
            )
            async for record in membership:
                edges[(record["datasetId"], record["fileId"], "distribution")] = {
                    "from": record["datasetId"], "to": record["fileId"],
                    "labels": [EdgeLabel.distribution], "properties": {},
                }

        valid_edges = [
            e for e in edges.values()
            if e["from"] in nodes and e["to"] in nodes
        ]

        node_objects = [
            Node.model_construct(
                id=UUID(n["id"]), labels=n["labels"], properties=n["properties"])
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
