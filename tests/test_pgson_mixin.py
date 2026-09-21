"""Unit tests for Neo4jPgJsonMixin deserialisation invariants (no Neo4j container)."""

from unittest.mock import MagicMock

from moma_management.domain.generated.edges.edge_schema import EdgeLabel
from moma_management.repository.neo4j_pgson_mixin import Neo4jPgJsonMixin


def _fake_rel(rel_type: str):
    rel = MagicMock()
    rel.type = rel_type
    rel.start_node = {"id": "a"}
    rel.end_node = {"id": "b"}
    rel.__iter__ = lambda self: iter({})
    rel.keys.return_value = []
    return rel


def test_deserialized_edge_label_is_an_enum_member():
    """Labels must come back as EdgeLabel, not str.

    EdgeLabel is a plain Enum, so ``EdgeLabel.input in ["input"]`` is False.
    Callers in the auth middleware select edges exactly that way, and a bare
    string made every such test silently match nothing.
    """
    edge = Neo4jPgJsonMixin._deserialize_edge(_fake_rel("input"))

    assert edge["labels"] == [EdgeLabel.input]
    assert EdgeLabel.input in edge["labels"]


def test_deserialized_edge_label_decodes_slash_encoding():
    """``___`` is restored to ``/`` before the enum lookup."""
    edge = Neo4jPgJsonMixin._deserialize_edge(_fake_rel("source___fileObject"))

    assert edge["labels"] == [EdgeLabel.source_file_object]
