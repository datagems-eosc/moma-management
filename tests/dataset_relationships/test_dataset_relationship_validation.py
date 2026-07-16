"""
Unit tests for DatasetRelationship validation (pure Pydantic/JSON-Schema, no DB).
"""

from uuid import uuid4

import pytest
from pydantic import ValidationError as PydanticValidationError

from moma_management.domain.dataset_relationship import DatasetRelationship
from moma_management.domain.generated.edges.edge_schema import Edge
from moma_management.domain.generated.nodes.node_schema import Node

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_full_relationship() -> tuple[DatasetRelationship, str, str, str]:
    """Build the full shape from the real dataset-linking example:
    BasicDLElement -HAS_COMPARISON-> PropertyComparison -HAS_EVIDENCE-> TextEvidence,
    with HAS_TARGET edges from all three to the same two datasets.

    Returns (relationship, root_id, dataset_id_a, dataset_id_b).
    """
    root_id = str(uuid4())
    ds_a = str(uuid4())
    ds_b = str(uuid4())
    pc_id = str(uuid4())
    te_id = str(uuid4())
    rel = DatasetRelationship(
        nodes=[
            Node(id=root_id, labels=["BasicDLElement"], properties={
                 "similarityScore": 85.07, "usesMetrics": "avg"}),
            Node(id=ds_a, labels=["sc:Dataset"], properties={"name": "ds-a"}),
            Node(id=ds_b, labels=["sc:Dataset"], properties={"name": "ds-b"}),
            Node(id=pc_id, labels=["PropertyComparison"], properties={
                 "targetProperty": "keywords", "similarityScore": 100.0}),
            Node(id=te_id, labels=["TextEvidence"],
                 properties={"similarityScore": 100.0}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": ds_a, "labels": ["HAS_TARGET"]}),
            Edge(**{"from": root_id, "to": ds_b, "labels": ["HAS_TARGET"]}),
            Edge(**{"from": root_id, "to": pc_id,
                 "labels": ["HAS_COMPARISON"], "properties": {"weight": 0.6}}),
            Edge(**{"from": pc_id, "to": ds_a, "labels": ["HAS_TARGET"]}),
            Edge(**{"from": pc_id, "to": ds_b, "labels": ["HAS_TARGET"]}),
            Edge(**{"from": pc_id, "to": te_id,
                 "labels": ["HAS_EVIDENCE"], "properties": {"rank": 1}}),
            Edge(**{"from": te_id, "to": ds_a, "labels": [
                 "HAS_TARGET"], "properties": {"chunk": "some chunk"}}),
            Edge(**{"from": te_id, "to": ds_b, "labels": [
                 "HAS_TARGET"], "properties": {"chunk": "some chunk"}}),
        ],
    )
    return rel, root_id, ds_a, ds_b


# ---------------------------------------------------------------------------
# Valid graph
# ---------------------------------------------------------------------------

def test_full_relationship_from_real_example_validates():
    """The full BasicDLElement/PropertyComparison/TextEvidence shape validates."""
    rel, root_id, ds_a, ds_b = _make_full_relationship()
    assert rel is not None
    assert set(rel.target_dataset_ids) == {ds_a, ds_b}
    assert str(rel.root.id) == root_id


# ---------------------------------------------------------------------------
# Structural validation
# ---------------------------------------------------------------------------

def test_relationship_rejects_missing_root_label():
    """A graph with no BasicDLElement root node must be rejected."""
    ds_a = str(uuid4())
    ds_b = str(uuid4())
    with pytest.raises(PydanticValidationError):
        DatasetRelationship(
            nodes=[
                Node(id=ds_a, labels=["sc:Dataset"], properties={}),
                Node(id=ds_b, labels=["sc:Dataset"], properties={}),
            ],
            edges=[],
        )


def test_relationship_rejects_disconnected_node():
    """A node not reachable from the root must be rejected by StructureStep."""
    root_id = str(uuid4())
    ds_a = str(uuid4())
    ds_b = str(uuid4())
    orphan_id = str(uuid4())
    with pytest.raises(PydanticValidationError):
        DatasetRelationship(
            nodes=[
                Node(id=root_id, labels=["BasicDLElement"], properties={}),
                Node(id=ds_a, labels=["sc:Dataset"], properties={}),
                Node(id=ds_b, labels=["sc:Dataset"], properties={}),
                Node(id=orphan_id, labels=["PropertyComparison"], properties={}),
            ],
            edges=[
                Edge(**{"from": root_id, "to": ds_a, "labels": ["HAS_TARGET"]}),
                Edge(**{"from": root_id, "to": ds_b, "labels": ["HAS_TARGET"]}),
            ],
        )


# ---------------------------------------------------------------------------
# Business invariant: exactly two datasets
# ---------------------------------------------------------------------------

def test_relationship_rejects_single_dataset_target():
    """A root with only one HAS_TARGET edge must be rejected."""
    root_id = str(uuid4())
    ds_a = str(uuid4())
    with pytest.raises(PydanticValidationError, match="exactly two"):
        DatasetRelationship(
            nodes=[
                Node(id=root_id, labels=["BasicDLElement"], properties={}),
                Node(id=ds_a, labels=["sc:Dataset"], properties={}),
            ],
            edges=[
                Edge(**{"from": root_id, "to": ds_a, "labels": ["HAS_TARGET"]}),
            ],
        )


def test_relationship_rejects_three_dataset_targets():
    """A root with three HAS_TARGET edges must be rejected (always exactly two datasets)."""
    root_id = str(uuid4())
    ds_a, ds_b, ds_c = str(uuid4()), str(uuid4()), str(uuid4())
    with pytest.raises(PydanticValidationError, match="exactly two"):
        DatasetRelationship(
            nodes=[
                Node(id=root_id, labels=["BasicDLElement"], properties={}),
                Node(id=ds_a, labels=["sc:Dataset"], properties={}),
                Node(id=ds_b, labels=["sc:Dataset"], properties={}),
                Node(id=ds_c, labels=["sc:Dataset"], properties={}),
            ],
            edges=[
                Edge(**{"from": root_id, "to": ds_a, "labels": ["HAS_TARGET"]}),
                Edge(**{"from": root_id, "to": ds_b, "labels": ["HAS_TARGET"]}),
                Edge(**{"from": root_id, "to": ds_c, "labels": ["HAS_TARGET"]}),
            ],
        )


def test_relationship_rejects_target_outside_root_pair():
    """A PropertyComparison HAS_TARGET-ing a third dataset (not one of the root's two) must be rejected."""
    root_id = str(uuid4())
    ds_a, ds_b, ds_c = str(uuid4()), str(uuid4()), str(uuid4())
    pc_id = str(uuid4())
    with pytest.raises(PydanticValidationError, match="other than the two"):
        DatasetRelationship(
            nodes=[
                Node(id=root_id, labels=["BasicDLElement"], properties={}),
                Node(id=ds_a, labels=["sc:Dataset"], properties={}),
                Node(id=ds_b, labels=["sc:Dataset"], properties={}),
                Node(id=ds_c, labels=["sc:Dataset"], properties={}),
                Node(id=pc_id, labels=["PropertyComparison"], properties={}),
            ],
            edges=[
                Edge(**{"from": root_id, "to": ds_a, "labels": ["HAS_TARGET"]}),
                Edge(**{"from": root_id, "to": ds_b, "labels": ["HAS_TARGET"]}),
                Edge(**{"from": root_id, "to": pc_id,
                     "labels": ["HAS_COMPARISON"]}),
                Edge(**{"from": pc_id, "to": ds_c, "labels": ["HAS_TARGET"]}),
            ],
        )
