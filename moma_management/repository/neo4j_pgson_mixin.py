from datetime import date as date_type
from logging import getLogger
from typing import Any, Dict, List, LiteralString, Optional, cast

import arrow
from neo4j import Transaction

from moma_management.domain.generated.edges.edge_schema import Edge
from moma_management.domain.generated.moma_schema import MoMaGraphModel
from moma_management.domain.generated.nodes.node_schema import Node

logger = getLogger(__name__)

# Properties whose values must be stored in ISO-8601 (YYYY-MM-DD) so that
# Neo4j's date() function and lexicographic ORDER BY both work correctly.
_DATE_PROPS = {"datePublished"}


# Formats tried in order.  ISO-8601 is first so the common case is one
# attempt.  Dash and slash European variants follow.  The order matters for
# ambiguous values (e.g. 01/02/2024): DD comes before MM so we treat unknown
# separators as DD-first (European convention).
_DATE_FORMATS = [
    "YYYY-MM-DD",   # ISO-8601          2024-06-01  ← canonical storage format
    "DD-MM-YYYY",   # European dash     01-06-2024
    "DD/MM/YYYY",   # European slash    01/06/2024
    "YYYY/MM/DD",   # ISO slash         2024/06/01
    "DD.MM.YYYY",   # European dot      01.06.2024
    # US slash          06/01/2024  (tried last to prefer DD/MM)
    "MM/DD/YYYY",
]


def _to_iso_date(value: Any) -> str | Any:
    if isinstance(value, date_type):
        return value.isoformat()
    if not isinstance(value, str) or not value.strip():
        return value
    try:
        return arrow.get(value.strip(), _DATE_FORMATS).date().isoformat()
    except Exception:
        logger.warning("Could not parse date value %r – storing as-is", value)
        return value


class Neo4jPgJsonMixin:
    """
    Mixin class providing common Neo4j operations for PG-JSON nodes and edges.

    This mixin provides methods to:
    - Store individual PG-JSON nodes and edges in Neo4j
    - Handle property sanitization and label escaping

    Follows the same structure as the ap-management Neo4jPgJsonMixin but
    targets the synchronous Neo4j driver and MoMa-specific conventions
    (colon namespace separators encoded as double-underscore).
    """

    @staticmethod
    def _sanitize_properties(props: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Sanitize property keys so they are valid Neo4j identifiers.

        Transformations applied:
        - Leading/trailing whitespace stripped
        - Spaces replaced with ``_``
        - ``:`` namespace separators replaced with ``__``

        Args:
            props: Dictionary of raw properties.

        Returns:
            Sanitized properties dictionary (empty dict if *props* is falsy).
        """
        if not props:
            return {}
        cleaned: Dict[str, Any] = {}
        for k, v in props.items():
            new_key = k.strip().replace(" ", "_").replace(":", "__")
            if isinstance(v, list):
                cleaned[new_key] = v if len(v) > 0 else None
            elif new_key in _DATE_PROPS:
                cleaned[new_key] = _to_iso_date(v)
            else:
                cleaned[new_key] = v
        return cleaned

    @staticmethod
    def _escape_labels(labels: List[str]) -> List[str]:
        """
        Wrap each label in backticks so that namespace separators (``:``) and
        other special characters are accepted by Cypher.

        Args:
            labels: List of raw label strings.

        Returns:
            List of backtick-escaped label strings.
        """
        return [f"`{label}`" for label in labels]

    def create_pgson_node(self, tx: Transaction, node: Node) -> None:
        """
        Store a single PG-JSON node in Neo4j using MERGE/SET.

        Creates or updates a node identified by its ``id`` property,
        assigning all provided labels and properties.

        Args:
            tx:   Neo4j write transaction.
            node: A :class:`Node` model with ``id``, ``labels``, and ``properties``.

        Raises:
            Exception: If the Neo4j operation fails.
        """
        node_id = str(node.id)
        labels = ":".join(self._escape_labels(node.labels))
        props = self._sanitize_properties(node.properties)
        props["id"] = node_id

        prop_assignments = ", ".join(f"{k}: ${k}" for k in props.keys())
        query = f"MERGE (n:{labels} {{id: $id}}) SET n += {{{prop_assignments}}}"

        tx.run(cast(LiteralString, query), props)

    def create_pgson_edge(self, tx: Transaction, edge: Edge) -> None:
        """
        Store a single PG-JSON edge in Neo4j using MERGE/SET.

        Creates or updates a relationship between two existing nodes.

        .. warning::
            Both source and target nodes must already exist in the database.

        Args:
            tx:   Neo4j write transaction.
            edge: An :class:`Edge` model with ``from_``, ``to``, ``labels``, and
                  optionally ``properties``.

        Raises:
            Exception: If the Neo4j operation fails.
        """
        from_id = str(edge.from_)
        to_id = str(edge.to)
        # Edge type labels: encode "/" as "___" to keep Cypher valid
        labels = ":".join(
            f"`{lbl.replace('/', '___')}`" for lbl in edge.labels
        )
        props = self._sanitize_properties(edge.properties or {})

        query = f"""
        MATCH (a {{id: $from_id}})
        MATCH (b {{id: $to_id}})
        MERGE (a)-[r:{labels}]->(b)
        """

        if props:
            prop_assignments = ", ".join(f"{k}: ${k}" for k in props.keys())
            query += f"\nSET r += {{{prop_assignments}}}"

        parameters: Dict[str, Any] = {
            "from_id": from_id, "to_id": to_id, **props}
        tx.run(cast(LiteralString, query), parameters)

    @staticmethod
    def _deserialize_node(neo4j_node: Any) -> Dict[str, Any]:
        """
        Convert a raw Neo4j node object into a PG-JSON node dict.

        Reverses the key sanitisation applied at write time:
        - ``__`` in property keys is restored to ``:``
        - Labels are returned as-is (Neo4j stores them without backticks)
        - ``None``-valued properties are excluded (Neo4j never persists them)

        Args:
            neo4j_node: A Neo4j ``Node`` object as returned by the driver.

        Returns:
            A PG-JSON node dict with keys ``id``, ``labels``, ``properties``.
        """
        properties = {
            k.replace("__", ":"): v
            for k, v in dict(neo4j_node).items()
            if v is not None and k != "id"
        }
        return {
            "id": neo4j_node["id"],
            "labels": list(neo4j_node.labels),
            "properties": properties,
        }

    @staticmethod
    def _deserialize_edge(neo4j_rel: Any) -> Dict[str, Any]:
        """
        Convert a raw Neo4j relationship object into a PG-JSON edge dict.

        Reverses the encoding applied at write time:
        - ``___`` in the relationship type is restored to ``/``
        - ``__`` in property keys is restored to ``:``
        - ``None``-valued properties are excluded

        Args:
            neo4j_rel: A Neo4j ``Relationship`` object as returned by the driver.

        Returns:
            A PG-JSON edge dict with keys ``from``, ``to``, ``labels``,
            ``properties``.
        """
        label = neo4j_rel.type.replace("___", "/")
        properties = {
            k.replace("__", ":"): v
            for k, v in dict(neo4j_rel).items()
            if v is not None
        }
        return {
            "from": neo4j_rel.start_node["id"],
            "to": neo4j_rel.end_node["id"],
            "labels": [label],
            "properties": properties,
        }

    def create_pgson(self, tx: Transaction, pg_json: MoMaGraphModel) -> None:
        """
        Store an entire PG-JSON structure (nodes then edges) in Neo4j.

        All nodes are written first so that edge MATCH clauses can resolve
        both endpoints.

        Args:
            tx:      Neo4j write transaction.
            pg_json: A :class:`MoMaGraphModel` with ``nodes`` and optional ``edges``.

        Raises:
            Exception: If any Neo4j operation fails.
        """
        for node in pg_json.nodes:
            self.create_pgson_node(tx, node)
        for edge in pg_json.edges or []:
            self.create_pgson_edge(tx, edge)

    def _build_dataset(self, root: Any, node_lists: List, rel_lists: List) -> MoMaGraphModel:
        """
        Build a :class:`MoMaGraphModel` from a root Neo4j node and the
        collected subgraph lists returned by a Cypher query.

        Nodes are deduplicated by id; edges whose endpoints are not both
        present in the collected node set are discarded.

        Args:
            root:       The root Neo4j node (``sc:Dataset``).
            node_lists: List of node-lists from ``collect(nodes(p))``.
            rel_lists:  List of rel-lists from ``collect(relationships(p))``.

        Returns:
            A :class:`MoMaGraphModel` (``Dataset``) with ``nodes`` and ``edges``.
        """
        nodes_dict: Dict[str, Any] = {root["id"]: self._deserialize_node(root)}

        for node_list in node_lists:
            for node in node_list:
                if node and node["id"] not in nodes_dict:
                    nodes_dict[node["id"]] = self._deserialize_node(node)

        edges_dict: Dict[tuple, Any] = {}
        for rel_list in rel_lists:
            for rel in rel_list:
                if rel is None:
                    continue
                key = (rel.start_node["id"], rel.end_node["id"], rel.type)
                if key not in edges_dict:
                    edges_dict[key] = self._deserialize_edge(rel)

        valid_edges = [
            e for e in edges_dict.values()
            if e["from"] in nodes_dict and e["to"] in nodes_dict
        ]

        return MoMaGraphModel(
            nodes=list(nodes_dict.values()),
            edges=valid_edges,
        )
