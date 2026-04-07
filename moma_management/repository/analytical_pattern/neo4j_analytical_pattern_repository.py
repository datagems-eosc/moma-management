from logging import getLogger
from typing import Any, Dict, List, Optional, Tuple

from neo4j import Session

from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.repository.analytical_pattern.analytical_pattern_repository import (
    AnalyticalPatternRepository,
)
from moma_management.repository.neo4j_pgson_mixin import Neo4jPgJsonMixin

logger = getLogger(__name__)

# Edges used to traverse from Operators to their connected data/user nodes
_OP_EDGES = "input|output|uses|follows"

_VECTOR_INDEX_NAME = "ap_description_embedding"


class Neo4jAnalyticalPatternRepository(Neo4jPgJsonMixin, AnalyticalPatternRepository):
    """Synchronous Neo4j-backed implementation of ``AnalyticalPatternRepository``."""

    _index_ensured: bool = False

    def __init__(self, session: Session) -> None:
        self._session = session

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def create(self, ap: AnalyticalPattern, embedding: Optional[List[float]] = None) -> None:
        """Store the full AP subgraph using the mixin's MERGE/SET helpers."""
        self._session.execute_write(self.create_pgson, ap)
        if embedding is not None:
            self._ensure_index(len(embedding))
            self._session.run(
                """//cypher
                MATCH (n:Analytical_Pattern {id: $id})
                CALL db.create.setNodeVectorProperty(n, 'description_embedding', $embedding)
                """,
                id=str(ap.root.id),
                embedding=embedding,
            )

    # ------------------------------------------------------------------
    # Vector index (lazy)
    # ------------------------------------------------------------------

    def _ensure_index(self, dimensions: int) -> None:
        """Create the vector index on first use (idempotent, class-level flag)."""
        if Neo4jAnalyticalPatternRepository._index_ensured:
            return
        self._session.run(
            f"CREATE VECTOR INDEX `{_VECTOR_INDEX_NAME}` IF NOT EXISTS "
            "FOR (n:Analytical_Pattern) ON (n.description_embedding) "
            "OPTIONS {indexConfig: {"
            f"  `vector.dimensions`: {dimensions},"
            "  `vector.similarity_function`: 'cosine'"
            "}}",
        )
        Neo4jAnalyticalPatternRepository._index_ensured = True
        logger.info("Vector index '%s' ensured (%d dimensions)", _VECTOR_INDEX_NAME, dimensions)

    # ------------------------------------------------------------------
    # Vector search
    # ------------------------------------------------------------------

    def search(
        self,
        query_vector: List[float],
        top_k: int = 10,
        accessible_dataset_ids: Optional[List[str]] = None,
    ) -> List[Tuple[AnalyticalPattern, float]]:
        """Return APs ranked by cosine similarity to *query_vector*."""
        self._ensure_index(len(query_vector))
        filter_clause, params = self._access_filter(accessible_dataset_ids)
        query = f"""//cypher
            CALL db.index.vector.queryNodes($index_name, $top_k, $query_vector)
            YIELD node AS root, score
            {filter_clause}
            OPTIONAL MATCH (root)-[r1:consist_of]->(op:Operator)
            OPTIONAL MATCH (op)-[r2:input|output|uses|follows]->(connected)
            OPTIONAL MATCH (connected2)-[r3:input|output|uses|follows]->(op)
            WITH
                root, score,
                collect(DISTINCT op)         AS operators,
                collect(DISTINCT r1)         AS r1s,
                collect(DISTINCT connected)  AS connected_out,
                collect(DISTINCT r2)         AS r2s,
                collect(DISTINCT connected2) AS connected_in,
                collect(DISTINCT r3)         AS r3s
            RETURN
                root, score,
                operators, r1s,
                connected_out, r2s,
                connected_in, r3s
        """
        params.update(
            index_name=_VECTOR_INDEX_NAME,
            top_k=top_k,
            query_vector=query_vector,
        )
        records = list(self._session.run(query, **params))
        results: List[Tuple[AnalyticalPattern, float]] = []
        for record in records:
            if record.get("root") is not None:
                try:
                    ap = self._record_to_ap(record)
                    results.append((ap, record["score"]))
                except Exception:
                    root = record.get("root")
                    ap_id = root["id"] if root is not None else "unknown"
                    logger.exception("Failed to deserialize AP with id=%s", ap_id)
        return results

    # ------------------------------------------------------------------
    # Read (shallow)
    # ------------------------------------------------------------------

    def get(self, ap_id: str) -> Optional[AnalyticalPattern]:
        """Shallow retrieval: root + operators + first-level connected nodes."""
        query = """//cypher
            MATCH (root:Analytical_Pattern {id: $ap_id})
            OPTIONAL MATCH (root)-[r1:consist_of]->(op:Operator)
            OPTIONAL MATCH (op)-[r2:input|output|uses|follows]->(connected)
            OPTIONAL MATCH (connected2)-[r3:input|output|uses|follows]->(op)
            WITH
                root,
                collect(DISTINCT op)         AS operators,
                collect(DISTINCT r1)         AS r1s,
                collect(DISTINCT connected)  AS connected_out,
                collect(DISTINCT r2)         AS r2s,
                collect(DISTINCT connected2) AS connected_in,
                collect(DISTINCT r3)         AS r3s
            RETURN
                root,
                operators,
                r1s,
                connected_out,
                r2s,
                connected_in,
                r3s
        """
        record = self._session.run(query, ap_id=str(ap_id)).single()
        if record is None or record["root"] is None:
            return None
        return self._record_to_ap(record)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _access_filter(accessible_dataset_ids: Optional[List[str]]) -> Tuple[str, dict]:
        """Return a WHERE clause + params dict that restricts APs by accessible datasets.

        When *accessible_dataset_ids* is ``None`` (auth disabled), no filter is
        applied.  Otherwise only APs whose ``input`` edges lead to a
        ``sc:Dataset`` node whose ``id`` is in the allowed list — or APs with
        no ``input`` edges at all — are kept.
        """
        if accessible_dataset_ids is None:
            return "", {}
        clause = """
            WHERE (
                NOT EXISTS { MATCH (root)-[:consist_of]->(:Operator)-[:input]->() }
                OR EXISTS {
                    MATCH (root)-[:consist_of]->(:Operator)-[:input]->(d)
                          -[*0..4]-(ds:`sc:Dataset`)
                    WHERE ds.id IN $accessible_ids
                }
            )
        """
        return clause, {"accessible_ids": accessible_dataset_ids}

    def list(self, accessible_dataset_ids: Optional[List[str]] = None) -> List[AnalyticalPattern]:
        """Shallow retrieval of all AnalyticalPattern subgraphs."""
        filter_clause, params = self._access_filter(accessible_dataset_ids)
        query = f"""//cypher
            MATCH (root:Analytical_Pattern)
            {filter_clause}
            OPTIONAL MATCH (root)-[r1:consist_of]->(op:Operator)
            OPTIONAL MATCH (op)-[r2:input|output|uses|follows]->(connected)
            OPTIONAL MATCH (connected2)-[r3:input|output|uses|follows]->(op)
            WITH
                root,
                collect(DISTINCT op)         AS operators,
                collect(DISTINCT r1)         AS r1s,
                collect(DISTINCT connected)  AS connected_out,
                collect(DISTINCT r2)         AS r2s,
                collect(DISTINCT connected2) AS connected_in,
                collect(DISTINCT r3)         AS r3s
            RETURN
                root,
                operators,
                r1s,
                connected_out,
                r2s,
                connected_in,
                r3s
        """
        records = list(self._session.run(query, **params))
        results = []
        for record in records:
            if record.get("root") is not None:
                try:
                    results.append(self._record_to_ap(record))
                except Exception:
                    root = record.get("root")
                    ap_id = root["id"] if root is not None else "unknown"
                    logger.exception(
                        "Failed to deserialize AP with id=%s", ap_id)
        return results

    def get_ids_by_task_id(self, task_id: str) -> List[str]:
        """Return AP IDs accomplished by the given Task."""
        query = """//cypher
            MATCH (t:Task {id: $task_id})-[:is_accomplished_by]->(ap:Analytical_Pattern)
            RETURN ap.id AS ap_id
        """
        records = self._session.run(query, task_id=str(task_id)).data()
        return [r["ap_id"] for r in records]

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _record_to_ap(self, record: Any) -> AnalyticalPattern:
        """Convert a Cypher record (from get/list) into an ``AnalyticalPattern``."""
        nodes_dict: Dict[str, Any] = {}
        edges_list: List[Dict[str, Any]] = []

        # Root node
        root_data = self._deserialize_node(record["root"])
        nodes_dict[root_data["id"]] = root_data

        # Operators
        for op in record.get("operators") or []:
            if op is not None:
                d = self._deserialize_node(op)
                nodes_dict[d["id"]] = d

        # Outgoing operator edges (r1: consist_of)
        for rel in record.get("r1s") or []:
            if rel is not None:
                edges_list.append(self._deserialize_edge(rel))

        # Nodes connected OUT from operators
        for node in record.get("connected_out") or []:
            if node is not None:
                d = self._deserialize_node(node)
                nodes_dict[d["id"]] = d

        # Edges from operators to connected nodes (r2: input/output/uses/follows)
        for rel in record.get("r2s") or []:
            if rel is not None:
                edges_list.append(self._deserialize_edge(rel))

        # Nodes connected INTO operators from other side (e.g. User -uses-> Op)
        for node in record.get("connected_in") or []:
            if node is not None:
                d = self._deserialize_node(node)
                nodes_dict[d["id"]] = d

        # Edges r3
        for rel in record.get("r3s") or []:
            if rel is not None:
                edges_list.append(self._deserialize_edge(rel))

        # Deduplicate edges by (from, to, label)
        seen_edges: set = set()
        deduped_edges = []
        for e in edges_list:
            key = (e["from"], e["to"], tuple(e["labels"]))
            if key not in seen_edges:
                seen_edges.add(key)
                deduped_edges.append(e)

        return AnalyticalPattern.model_validate(
            {
                "nodes": list(nodes_dict.values()),
                "edges": deduped_edges if deduped_edges else None,
            }
        )
