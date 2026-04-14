from logging import getLogger
from typing import Any, Dict, List, Optional, Tuple

from neo4j import AsyncSession

from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.repository.analytical_pattern.analytical_pattern_repository import (
    AnalyticalPatternRepository,
)
from moma_management.repository.neo4j_pgson_mixin import Neo4jPgJsonMixin

logger = getLogger(__name__)

_VECTOR_INDEX_NAME = "ap_description_embedding"


class Neo4jAnalyticalPatternRepository(Neo4jPgJsonMixin, AnalyticalPatternRepository):
    """Synchronous Neo4j-backed implementation of ``AnalyticalPatternRepository``."""

    # Edges that link APs/Operators to external entities (Data, User, …) and
    # must NOT be traversed when manipulating an AP subgraph in isolation.
    FORBIDDEN_EDGES: list[str] = ["input", "output",
                                  "perform", "perform_inference", "uses"]

    _index_ensured: bool = False

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    async def create(self, ap: AnalyticalPattern, embedding: Optional[List[float]] = None) -> None:
        """Store the full AP subgraph using the mixin's MERGE/SET helpers."""
        await self._session.execute_write(self.create_pgson, ap)
        if embedding is not None:
            await self._ensure_index(len(embedding))
            await self._session.run(
                """//cypher
                MATCH (n:Analytical_Pattern {id: $id})
                CALL db.create.setNodeVectorProperty(n, 'description_embedding', $embedding)
                """,
                id=str(ap.root.id),
                embedding=embedding,
            )

    async def delete(self, ap_id: str) -> None:
        """Delete the AP and its connected subgraph; leaves data nodes intact."""
        await self._session.run(
            """//cypher
            MATCH (root:Analytical_Pattern {id: $ap_id})
            OPTIONAL MATCH path=(root)-[*1..10]-(m)
            WHERE NONE(r IN relationships(path) WHERE type(r) IN $forbiddenEdges)
            WITH root, collect(DISTINCT m) AS related
            FOREACH (n IN related | DETACH DELETE n)
            DETACH DELETE root
            """,
            ap_id=str(ap_id),
            forbiddenEdges=self.FORBIDDEN_EDGES,
        )

    # ------------------------------------------------------------------
    # Vector index (lazy)
    # ------------------------------------------------------------------

    async def _ensure_index(self, dimensions: int) -> None:
        """Create the vector index on first use (idempotent, class-level flag)."""
        if Neo4jAnalyticalPatternRepository._index_ensured:
            return
        await self._session.run(
            f"CREATE VECTOR INDEX `{_VECTOR_INDEX_NAME}` IF NOT EXISTS "
            "FOR (n:Analytical_Pattern) ON (n.description_embedding) "
            "OPTIONS {indexConfig: {"
            f"  `vector.dimensions`: {dimensions},"
            "  `vector.similarity_function`: 'cosine'"
            "}}",
        )
        Neo4jAnalyticalPatternRepository._index_ensured = True
        logger.info("Vector index '%s' ensured (%d dimensions)",
                    _VECTOR_INDEX_NAME, dimensions)

    # ------------------------------------------------------------------
    # Vector search
    # ------------------------------------------------------------------

    async def search(
        self,
        query_vector: List[float],
        top_k: int = 10,
        accessible_dataset_ids: Optional[List[str]] = None,
    ) -> List[Tuple[AnalyticalPattern, float]]:
        """Return APs ranked by cosine similarity to *query_vector*."""
        await self._ensure_index(len(query_vector))
        filter_clause, params = self._access_filter(accessible_dataset_ids)
        query = f"""//cypher
            CALL db.index.vector.queryNodes($index_name, $top_k, $query_vector)
            YIELD node AS root, score
            {filter_clause}
            OPTIONAL MATCH path=(root)-[*1..4]-(m)
            WHERE NONE(r IN relationships(path) WHERE type(r) IN $forbiddenEdges)
            RETURN root, score, m, relationships(path) AS rels
        """
        params.update(
            index_name=_VECTOR_INDEX_NAME,
            top_k=top_k,
            query_vector=query_vector,
            forbiddenEdges=self.FORBIDDEN_EDGES,
        )
        result = await self._session.run(query, **params)
        records = [record async for record in result]
        return self._group_ap_records(records, with_score=True)

    # ------------------------------------------------------------------
    # Read (shallow)
    # ------------------------------------------------------------------

    async def get(self, ap_id: str) -> Optional[AnalyticalPattern]:
        """Shallow retrieval: root + connected subgraph (excluding forbidden edges)."""
        query = """//cypher
            MATCH (root:Analytical_Pattern {id: $ap_id})
            OPTIONAL MATCH path=(root)-[*1..4]-(m)
            WHERE NONE(r IN relationships(path) WHERE type(r) IN $forbiddenEdges)
            RETURN root, m, relationships(path) AS rels
        """
        result = await self._session.run(
            query, ap_id=str(ap_id), forbiddenEdges=self.FORBIDDEN_EDGES)
        rows = [record async for record in result]
        if not rows:
            return None

        nodes: Dict[str, Any] = {}
        edges: Dict[str, Any] = {}

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

        return AnalyticalPattern(
            nodes=list(nodes.values()),
            edges=valid_edges or None,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _access_filter(accessible_dataset_ids: Optional[List[str]]) -> Tuple[str, dict]:
        """Return a WHERE clause + params dict that restricts APs by accessible datasets.

        When *accessible_dataset_ids* is ``None`` (auth disabled), no filter is
        applied.  Otherwise only APs whose ``input`` edges **all** lead to
        ``sc:Dataset`` nodes whose ``id`` is in the allowed list — or APs with
        no ``input`` edges at all — are kept.

        An AP that references datasets D1 and D2 is only visible to a user
        who can browse **both** D1 and D2.
        """
        if accessible_dataset_ids is None:
            return "", {}
        clause = """
            WHERE (
                NOT EXISTS { MATCH (root)-[:consist_of]->(:Operator)-[:input]->() }
                OR NOT EXISTS {
                    MATCH (root)-[:consist_of]->(:Operator)-[:input]->(d)
                          -[*0..4]-(ds:`sc:Dataset`)
                    WHERE NOT ds.id IN $accessible_ids
                }
            )
        """
        return clause, {"accessible_ids": accessible_dataset_ids}

    async def list(self, accessible_dataset_ids: Optional[List[str]] = None) -> List[AnalyticalPattern]:
        """Shallow retrieval of all AnalyticalPattern subgraphs."""
        filter_clause, params = self._access_filter(accessible_dataset_ids)
        query = f"""//cypher
            MATCH (root:Analytical_Pattern)
            {filter_clause}
            OPTIONAL MATCH path=(root)-[*1..4]-(m)
            WHERE NONE(r IN relationships(path) WHERE type(r) IN $forbiddenEdges)
            RETURN root, m, relationships(path) AS rels
        """
        params["forbiddenEdges"] = self.FORBIDDEN_EDGES
        result = await self._session.run(query, **params)
        records = [record async for record in result]
        return self._group_ap_records(records, with_score=False)

    async def get_ids_by_task_id(self, task_id: str) -> List[str]:
        """Return AP IDs accomplished by the given Task."""
        query = """//cypher
            MATCH (t:Task {id: $task_id})-[:is_accomplished_by]->(ap:Analytical_Pattern)
            RETURN ap.id AS ap_id
        """
        result = await self._session.run(query, task_id=str(task_id))
        records = await result.data()
        return [r["ap_id"] for r in records]

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _group_ap_records(self, records: list, *, with_score: bool) -> list:
        """Group flat (root, m, rels) rows into per-AP results.

        When *with_score* is ``True`` each result is a ``(AnalyticalPattern, float)``
        tuple; otherwise a plain ``AnalyticalPattern``.
        """
        ap_map: Dict[str, Dict[str, Any]] = {
        }  # root_id -> {nodes, edges, score}

        for record in records:
            root = record["root"]
            if root is None:
                continue
            root_id = root["id"]

            if root_id not in ap_map:
                ap_map[root_id] = {
                    "nodes": {root_id: self._deserialize_node(root)},
                    "edges": {},
                }
                if with_score:
                    ap_map[root_id]["score"] = record["score"]

            entry = ap_map[root_id]
            m = record["m"]
            rels = record["rels"] or []

            if m:
                mid = m["id"]
                if mid not in entry["nodes"]:
                    entry["nodes"][mid] = self._deserialize_node(m)

            for rel in rels:
                key = (rel.start_node["id"], rel.end_node["id"], rel.type)
                if key not in entry["edges"]:
                    entry["edges"][key] = self._deserialize_edge(rel)

        results = []
        for entry in ap_map.values():
            nodes = entry["nodes"]
            valid_edges = [
                e for e in entry["edges"].values()
                if e["from"] in nodes and e["to"] in nodes
            ]
            try:
                ap = AnalyticalPattern(
                    nodes=list(nodes.values()),
                    edges=valid_edges or None,
                )
                if with_score:
                    results.append((ap, entry["score"]))
                else:
                    results.append(ap)
            except Exception:
                root_id = next(iter(nodes), "unknown")
                logger.exception(
                    "Failed to deserialize AP with id=%s", root_id)

        return results
