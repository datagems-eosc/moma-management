"""
Unit tests for DatasetRelationship validation (pure Pydantic/JSON-Schema, no DB).
"""

from uuid import uuid4

import pytest
from pydantic import ValidationError as PydanticValidationError

from moma_management.domain.dataset_relationship import DatasetRelationship
from moma_management.domain.generated.edges.edge_schema import Edge
from moma_management.domain.generated.nodes.node_schema import Node
from tests.utils import make_file_object_relationship

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


# ---------------------------------------------------------------------------
# FileObject linking (issue #30)
# ---------------------------------------------------------------------------

def test_file_object_linking_validates():
    """BasicDLElement -HAS_DETAIL-> FileObjectLinkingElement -HAS_COMPARISON->
    FileObjectComparison -HAS_TARGET-> cr:FileObject is accepted, and the files
    are not mistaken for extra target datasets."""
    ds_a, ds_b, file_a, file_b = (str(uuid4()) for _ in range(4))
    rel = make_file_object_relationship(ds_a, ds_b, file_a, file_b)
    assert set(rel.target_dataset_ids) == {ds_a, ds_b}
    assert rel.file_object_ids == {file_a, file_b}


def test_file_object_linking_accepts_distribution_edges():
    """The optional ds -distribution-> file edges (issue #30 payload) validate."""
    ds_a, ds_b, file_a, file_b = (str(uuid4()) for _ in range(4))
    rel = make_file_object_relationship(ds_a, ds_b, file_a, file_b, with_distribution=True)
    assert rel.file_memberships == {(ds_a, file_a), (ds_b, file_b)}


@pytest.mark.parametrize("edge", [
    pytest.param(("file_a", "ds_a", "containedIn"), id="from-a-file"),
    pytest.param(("ds_a", "file_a", "linkTo"), id="dataset-to-file-not-distribution"),
    pytest.param(("ds_a", "ds_b", "distribution"), id="distribution-to-a-dataset"),
])
def test_relationship_rejects_edge_from_a_reference(edge):
    """sc:Dataset / cr:FileObject are references: apart from a dataset's
    `distribution` edge to a linked file, any edge starting from one must be
    rejected with a message saying a relationship cannot modify datasets."""
    ids = {k: str(uuid4()) for k in ("ds_a", "ds_b", "file_a", "file_b")}
    raw = make_file_object_relationship(*ids.values()).model_dump(by_alias=True, mode="json")
    src, dst, label = edge
    raw["edges"].append({"from": ids[src], "to": ids[dst], "labels": [label]})
    with pytest.raises(PydanticValidationError, match="cannot modify datasets") as exc:
        DatasetRelationship.model_validate(raw)
    assert f"{ids[src]} -{label}-> {ids[dst]}" in str(exc.value)


def test_relationship_rejects_distribution_from_a_third_dataset():
    """A distribution edge may only start from one of the two linked datasets."""
    ds_a, ds_b, ds_c, file_a, file_b = (str(uuid4()) for _ in range(5))
    raw = make_file_object_relationship(ds_a, ds_b, file_a, file_b).model_dump(by_alias=True, mode="json")
    raw["nodes"].append({"id": ds_c, "labels": ["sc:Dataset"], "properties": {}})
    raw["edges"].append({"from": ds_c, "to": file_a, "labels": ["distribution"]})
    with pytest.raises(PydanticValidationError, match="distribution edges starting from dataset"):
        DatasetRelationship.model_validate(raw)


def test_relationship_rejects_file_no_comparison_targets():
    """A file reached only through a distribution edge is not part of any
    comparison: it must be rejected rather than silently ignored."""
    ds_a, ds_b, file_a, file_b, lone_file = (str(uuid4()) for _ in range(5))
    raw = make_file_object_relationship(ds_a, ds_b, file_a, file_b).model_dump(by_alias=True, mode="json")
    raw["nodes"].append({"id": lone_file, "labels": ["cr:FileObject"], "properties": {}})
    raw["edges"].append({"from": ds_a, "to": lone_file, "labels": ["distribution"]})
    with pytest.raises(PydanticValidationError, match="no FileObjectComparison targets"):
        DatasetRelationship.model_validate(raw)


def test_file_object_comparison_cannot_target_a_dataset():
    """FileObjectComparison -HAS_TARGET-> sc:Dataset is not an allowed edge."""
    ds_a, ds_b, file_a, file_b = (str(uuid4()) for _ in range(4))
    raw = make_file_object_relationship(ds_a, ds_b, file_a, file_b).model_dump(by_alias=True, mode="json")
    foc_id = next(n["id"] for n in raw["nodes"] if "FileObjectComparison" in n["labels"])
    raw["edges"].append({"from": foc_id, "to": ds_a, "labels": ["HAS_TARGET"]})
    with pytest.raises(PydanticValidationError, match="Invalid edge 'HAS_TARGET'"):
        DatasetRelationship.model_validate(raw)
