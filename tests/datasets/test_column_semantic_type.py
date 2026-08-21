"""
Unit tests for the Column `semanticType` property (issue #25).

No database is required — schema validation happens at construction time
(Pydantic `@model_validator`, backed by the JSON Schema files).
"""

from uuid import uuid4

import pytest
from pydantic import ValidationError

from moma_management.domain.dataset import Dataset
from moma_management.domain.generated.edges.edge_schema import Edge
from moma_management.domain.generated.nodes.node_schema import Node


def _make_dataset_with_column(semantic_type) -> Dataset:
    """Build a minimal sc:Dataset -> cr:RecordSet -> Column graph."""
    root_id = str(uuid4())
    record_set_id = str(uuid4())
    column_id = str(uuid4())
    return Dataset(
        nodes=[
            Node(id=root_id, labels=["sc:Dataset"], properties={"name": "ds"}),
            Node(id=record_set_id, labels=["cr:RecordSet"], properties={}),
            Node(
                id=column_id,
                labels=["Column"],
                properties={"name": "country", "semanticType": semantic_type},
            ),
        ],
        edges=[
            Edge(**{"from": root_id, "to": record_set_id, "labels": ["recordSet"]}),
            Edge(**{"from": record_set_id, "to": column_id, "labels": ["field"]}),
        ],
    )


def test_column_semantic_type_valid_string_accepted():
    """A Column with a string semanticType parses without error."""
    ds = _make_dataset_with_column("country name")
    assert ds is not None


def test_column_semantic_type_wrong_type_rejected():
    """A non-string semanticType must be rejected by the Column JSON schema."""
    with pytest.raises(ValidationError):
        _make_dataset_with_column(123)
