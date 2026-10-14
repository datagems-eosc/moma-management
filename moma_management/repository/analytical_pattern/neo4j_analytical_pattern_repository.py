
import json
from logging import getLogger
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

from neo4j import AsyncManagedTransaction, AsyncSession

from moma_management.domain import EDGE_CONSTRAINTS_PATH
from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.domain.exceptions import ConflictError
from moma_management.domain.filters import AnalyticalPatternFilter
from moma_management.domain.generated.edges.edge_schema import Edge
from moma_management.domain.generated.nodes.node_schema import Node
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
        """Store the full AP subgraph and its embedding, in one transaction.

        Raises:
            ConflictError: if one of its operators, or one of their internal ResultType
                nodes, already belongs to another AP. Nodes are stored by id, so the two
                APs would share it, and deleting one would delete part of the other.
        """
        await self._session.execute_write(self._create, ap, embedding)

    async def _create(self, tx: AsyncManagedTransaction, ap: AnalyticalPattern,
                      embedding: Optional[List[float]]) -> None:
        owned = [str(n.id) for n in ap.nodes
                 if "Operator" in n.labels or ("ResultType" in n.labels and "Data" not in n.labels)]
        result = await tx.run(
            """//cypher
            MATCH (n) WHERE (n:Operator OR (n:ResultType AND NOT n:Data)) AND n.id IN $owned
            MATCH (n)-[:input|output*0..1]-(:Operator)<-[:consist_of]-(other:Analytical_Pattern)
            WHERE other.id <> $root_id
            RETURN DISTINCT n.id AS id, other.id AS other_id
            """,
            root_id=str(ap.root.id), owned=owned,
        )
        clashes = [(record["id"], record["other_id"]) async for record in result]
        if clashes:
            raise ConflictError(
                "These nodes already belong to another analytical pattern; give them new ids: "
                + ", ".join(f"{id_} (in {other})" for id_, other in clashes))

        await self.create_pgson(tx, ap)
        if embedding is not None:
            await tx.run(
                """//cypher
                MATCH (n:Analytical_Pattern {id: $id})
                CALL db.create.setNodeVectorProperty(n, 'description_embedding', $embedding)
                """,
                id=str(ap.root.id),
                embedding=embedding,
            )

    async def delete(self, ap_id: str) -> None:
        """Delete what the AP owns: its root, its evaluations, its operators and their
        internal ResultType nodes. Data nodes, and operators another AP also consists of
        (stored before ids were checked), are left in place."""
        await self._session.run(
            """//cypher
            MATCH (root:Analytical_Pattern {id: $ap_id})
            OPTIONAL MATCH (root)-[:consist_of]->(op:Operator)
            WHERE NOT EXISTS { MATCH (op)<-[:consist_of]-(other:Analytical_Pattern) WHERE other <> root }
            OPTIONAL MATCH (op)-[:input|output]-(rt:ResultType) WHERE NOT rt:Data
            OPTIONAL MATCH (root)-[:is_measured_by]->(evaluation:Evaluation)
            WITH root, collect(DISTINCT op) + collect(DISTINCT rt) + collect(DISTINCT evaluation) AS owned
            FOREACH (n IN owned | DETACH DELETE n)
            DETACH DELETE root
            """,
            ap_id=str(ap_id),
        )

    # ------------------------------------------------------------------
    # Vector index
    # ------------------------------------------------------------------

    @staticmethod
    async def ensure_vector_index(session: AsyncSession, dimensions: int) -> None:
        """Create the vector index AP search uses, for vectors of `dimensions`.

        Called once at startup: requests creating it concurrently race, and the loser
        fails. Raises when an existing index holds vectors of another size, which is what
        is left after the embedder changes and the stored APs were not re-embedded.
        """
        await session.run(
            f"CREATE VECTOR INDEX `{_VECTOR_INDEX_NAME}` IF NOT EXISTS "
            "FOR (n:Analytical_Pattern) ON (n.description_embedding) "
            "OPTIONS {indexConfig: {"
            f"  `vector.dimensions`: {dimensions},"
            "  `vector.similarity_function`: 'cosine'"
            "}}",
        )
        result = await session.run(
            "SHOW VECTOR INDEXES YIELD name, options WHERE name = $name RETURN options",
            name=_VECTOR_INDEX_NAME,
        )
        record = await result.single()
        existing = record["options"]["indexConfig"]["vector.dimensions"]
        if existing != dimensions:
            raise RuntimeError(
                f"The vector index '{_VECTOR_INDEX_NAME}' holds {existing}-dimension vectors, "
                f"but the embedder makes {dimensions}-dimension ones. Drop the index and "
                "re-embed the stored analytical patterns before changing the embedder.")
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
              // Another AP's root ends the walk: what lies past it is that AP's.
              AND NONE(n IN nodes(path)[1..] WHERE n:Analytical_Pattern)
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

        node_objects = [
            Node.model_construct(id=UUID(n["id"]), labels=n["labels"], properties=n["properties"])
            for n in nodes.values()
        ]
        edge_objects = (
            [
                Edge.model_construct(
                    from_=UUID(e["from"]), to=UUID(e["to"]),
                    labels=e["labels"], properties=e.get("properties"),
                )
                for e in valid_edges
            ]
            if valid_edges else None
        )
        return AnalyticalPattern.model_construct(
            nodes=node_objects,
            edges=edge_objects,
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
                NOT EXISTS { MATCH (root)-[:consist_of]->(:Operator)<-[:input]-() }
                OR NOT EXISTS {
                    MATCH (root)-[:consist_of]->(:Operator)<-[:input]-(d)
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
