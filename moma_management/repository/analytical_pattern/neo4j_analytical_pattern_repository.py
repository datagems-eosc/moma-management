
import json
from logging import getLogger
from typing import Any, Dict, List, Optional, Tuple

from neo4j import AsyncSession

from moma_management.domain import EDGE_CONSTRAINTS_PATH
from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.domain.filters import AnalyticalPatternFilter
from moma_management.repository.analytical_pattern.analytical_pattern_repository import (
    AnalyticalPatternRepository,
)
from moma_management.repository.neo4j_pgson_mixin import Neo4jPgJsonMixin

logger = getLogger(__name__)

_VECTOR_INDEX_NAME = "ap_description_embedding"

_edge_constraints: list[dict] = json.loads(EDGE_CONSTRAINTS_PATH.read_text())
_EVALUATION_EDGE: str = next(
    c["label"]
    for c in _edge_constraints
    if c["fromLabel"] == "Analytical_Pattern" and c["toLabel"] == "Evaluation"
)


class Neo4jAnalyticalPatternRepository(Neo4jPgJsonMixin, AnalyticalPatternRepository):
    """Synchronous Neo4j-backed implementation of ``AnalyticalPatternRepository``."""

    # Edges that link APs/Operators to external entities (Data, User, …) and
    # must NOT be traversed when manipulating an AP subgraph in isolation.
    FORBIDDEN_EDGES: list[str] = ["input", "output", "perform",
                                  "perform_inference", "uses"]

    _INDEX_STATEMENTS: list[str] = [
        "CREATE CONSTRAINT ap_id_unique IF NOT EXISTS "
        "FOR (n:Analytical_Pattern) REQUIRE n.id IS UNIQUE",
        "CREATE INDEX ap_id IF NOT EXISTS "
        "FOR (n:Analytical_Pattern) ON (n.id)",
        "CREATE CONSTRAINT evaluation_id_unique IF NOT EXISTS "
        "FOR (n:Evaluation) REQUIRE n.id IS UNIQUE",
        "CREATE INDEX evaluation_ap_id IF NOT EXISTS "
        "FOR (n:Evaluation) ON (n.ap_id)",
    ]
    _indexes_ensured: bool = False
    _vector_index_ensured: bool = False
    # When deserializing nodes, ignore these properties
    _NODE_IGNORE_PROPS: frozenset[str] = frozenset({"description_embedding"})

    def _effective_forbidden_edges(self, include_evaluations: bool) -> list[str]:
        """Return the forbidden-edge list for subgraph traversal.

        When *include_evaluations* is ``False`` (the default), the
        ``is_measured_by`` edge is added to the forbidden list so Evaluation
        nodes are excluded from the traversal entirely.
        """
        if include_evaluations:
            return self.FORBIDDEN_EDGES
        return self.FORBIDDEN_EDGES + [_EVALUATION_EDGE]

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @classmethod
    async def create_with_indexes(cls, session: AsyncSession) -> "Neo4jAnalyticalPatternRepository":
        repo = cls(session)
        if not cls._indexes_ensured:
            for stmt in cls._INDEX_STATEMENTS:
                await session.run(stmt)
            cls._indexes_ensured = True
            logger.info("Neo4jAnalyticalPatternRepository indexes ensured")
        return repo

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
        """Delete the AP and its connected subgraph; leaves data nodes intact.

        ResultType nodes are internal to the AP subgraph (connected via
        input/output edges which are in FORBIDDEN_EDGES).  A second query
        explicitly deletes them so no orphaned ResultType nodes remain.
        """
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
        # Remove internal ResultType nodes attached to Operators in this AP.
        # Data nodes (which are also ResultType) are persistent and must NOT be deleted.
        await self._session.run(
            """//cypher
            MATCH (root:Analytical_Pattern {id: $ap_id})
            OPTIONAL MATCH (root)-[:consist_of]->(:Operator)-[:input|output]-(rt:ResultType)
            WHERE NOT rt:Data
            DETACH DELETE rt
            """,
            ap_id=str(ap_id),
        )

    # ------------------------------------------------------------------
    # Vector index (lazy)
    # ------------------------------------------------------------------

    async def _ensure_index(self, dimensions: int) -> None:
        """Create the vector index on first use (idempotent, class-level flag)."""
        if Neo4jAnalyticalPatternRepository._vector_index_ensured:
            return
        await self._session.run(
            f"CREATE VECTOR INDEX `{_VECTOR_INDEX_NAME}` IF NOT EXISTS "
            "FOR (n:Analytical_Pattern) ON (n.description_embedding) "
            "OPTIONS {indexConfig: {"
            f"  `vector.dimensions`: {dimensions},"
            "  `vector.similarity_function`: 'cosine'"
            "}}",
        )
        Neo4jAnalyticalPatternRepository._vector_index_ensured = True
        logger.info("Vector index '%s' ensured (%d dimensions)",
                    _VECTOR_INDEX_NAME, dimensions)

    # ------------------------------------------------------------------
    # Read (shallow)
    # ------------------------------------------------------------------

    async def get(self, ap_id: str, include_evaluations: bool = False) -> Optional[AnalyticalPattern]:
        """Shallow retrieval: root + connected subgraph (excluding forbidden edges).

        When *include_evaluations* is ``False`` (default) Evaluation nodes are
        excluded from the traversal.  Pass ``True`` to include them.

        ResultType nodes are internal to the AP but connected via input/output
        edges (which are in FORBIDDEN_EDGES).  A second query fetches them
        explicitly and merges them into the subgraph.
        """
        forbidden = self._effective_forbidden_edges(include_evaluations)
        query = """//cypher
            MATCH (root:Analytical_Pattern {id: $ap_id})
            OPTIONAL MATCH path=(root)-[*1..4]-(m)
            WHERE NONE(r IN relationships(path) WHERE type(r) IN $forbiddenEdges)
            RETURN root, m, relationships(path) AS rels
        """
        result = await self._session.run(
            query, ap_id=str(ap_id), forbiddenEdges=forbidden)
        rows = [record async for record in result]
        if not rows:
            return None

        nodes: Dict[str, Any] = {}
        edges: Dict[str, Any] = {}

        root = rows[0]["root"]
        if root is None:
            return None
        nodes[root["id"]] = self._deserialize_node(
            root, ignore_props=self._NODE_IGNORE_PROPS)

        for record in rows:
            m = record["m"]
            rels = record["rels"] or []
            if m:
                mid = m["id"]
                if mid not in nodes:
                    nodes[mid] = self._deserialize_node(
                        m, ignore_props=self._NODE_IGNORE_PROPS)
            for rel in rels:
                key = (rel.start_node["id"], rel.end_node["id"], rel.type)
                if key not in edges:
                    edges[key] = self._deserialize_edge(rel)

        # Second pass: collect internal ResultType nodes connected to Operators.
        # Data nodes (which are also ResultType) are excluded — they are external
        # persistent nodes not owned by the AP subgraph.
        rt_query = """//cypher
            MATCH (root:Analytical_Pattern {id: $ap_id})-[:consist_of]->(op:Operator)
            OPTIONAL MATCH (op)-[r:input|output]-(rt:ResultType)
            WHERE NOT rt:Data
            RETURN rt, r
        """
        rt_result = await self._session.run(rt_query, ap_id=str(ap_id))
        async for record in rt_result:
            rt = record["rt"]
            r = record["r"]
            if rt is not None:
                rt_id = rt["id"]
                if rt_id not in nodes:
                    nodes[rt_id] = self._deserialize_node(rt)
            if r is not None:
                key = (r.start_node["id"], r.end_node["id"], r.type)
                if key not in edges:
                    edges[key] = self._deserialize_edge(r)

        valid_edges = [
            e for e in edges.values()
            if e["from"] in nodes and e["to"] in nodes
        ]

        return AnalyticalPattern(
            nodes=list(nodes.values()),
            edges=valid_edges or None,
        )

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
                    WHERE NOT (d:ResultType AND NOT d:Data)
                    MATCH (d)-[*0..4]-(ds:`sc:Dataset`)
                    WHERE NOT ds.id IN $accessible_ids
                }
            )
        """
        return clause, {"accessible_ids": accessible_dataset_ids}

    async def list(
        self,
        filter: AnalyticalPatternFilter,
        accessible_dataset_ids: Optional[List[str]] = None,
        query_vector: Optional[List[float]] = None,
    ) -> dict:
        """Paginated retrieval of AnalyticalPattern subgraphs.

        When *query_vector* is provided a vector-similarity search is performed;
        threshold and top_k are read from ``filter.search``.  Both paths share
        the same access filter and DB-side SKIP/LIMIT pagination.

        Returns ``{"aps": [AnalyticalPattern, ...], "total": int}``.
        """
        skip = (filter.page - 1) * filter.pageSize
        limit = filter.pageSize
        access_clause, access_params = self._access_filter(
            accessible_dataset_ids)

        if query_vector is not None:
            search = filter.search
            top_k = search.top_k if search else 10
            threshold = search.threshold if search else 0.0
            await self._ensure_index(len(query_vector))

            access_clause_clean = access_clause.strip().removeprefix("WHERE").strip()
            access_and = f"AND ({access_clause_clean})" if access_clause_clean else ""

            count_query = f"""//cypher
                CALL db.index.vector.queryNodes($index_name, $top_k, $query_vector)
                YIELD node AS root, score
                WHERE score >= $threshold
                {access_and}
                RETURN count(DISTINCT root) AS total
            """
            id_query = f"""//cypher
                CALL db.index.vector.queryNodes($index_name, $top_k, $query_vector)
                YIELD node AS root, score
                WHERE score >= $threshold
                {access_and}
                RETURN root.id AS id
                ORDER BY score DESC
                SKIP $skip
                LIMIT $limit
            """
            params = {
                "index_name": _VECTOR_INDEX_NAME,
                "top_k": top_k,
                "query_vector": query_vector,
                "threshold": threshold,
                **access_params,
            }
        else:
            count_query = f"""//cypher
                MATCH (root:Analytical_Pattern)
                {access_clause}
                RETURN count(DISTINCT root) AS total
            """
            id_query = f"""//cypher
                MATCH (root:Analytical_Pattern)
                {access_clause}
                WITH root
                ORDER BY root.id ASC
                SKIP $skip
                LIMIT $limit
                RETURN root.id AS id
            """
            params = dict(access_params)

        count_result = await self._session.run(count_query, **params)
        count_record = await count_result.single()
        total = count_record["total"] if count_record else 0

        if total == 0:
            return {"aps": [], "total": 0}

        id_result = await self._session.run(id_query, **{**params, "skip": skip, "limit": limit})
        page_ids = [record["id"] async for record in id_result]

        aps = []
        for ap_id in page_ids:
            ap = await self.get(ap_id, include_evaluations=filter.include_evaluations)
            if ap is not None:
                aps.append(ap)

        return {"aps": aps, "total": total}

    async def get_ids_by_task_id(self, task_id: str) -> List[str]:
        """Return AP IDs accomplished by the given Task."""
        query = """//cypher
            MATCH (t:Task {id: $task_id})-[:is_accomplished_by]->(ap:Analytical_Pattern)
            RETURN ap.id AS ap_id
        """
        result = await self._session.run(query, task_id=str(task_id))
        records = await result.data()
        return [r["ap_id"] for r in records]
