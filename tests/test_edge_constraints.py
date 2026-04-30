"""
Unit tests for edge-constraint enforcement in Dataset and AnalyticalPattern.

Both models share the same edge_constraints.json file via PgJsonGraph.
No database is required — validation happens at construction time (Pydantic).
"""

from uuid import uuid4

import pytest
from pydantic import ValidationError

from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.domain.dataset import Dataset
from moma_management.domain.generated.edges.edge_schema import Edge
from moma_management.domain.generated.nodes.node_schema import Node

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_dataset(edge_label: str, from_labels: list[str], to_labels: list[str]) -> Dataset:
    """Build a minimal Dataset with a single edge using the supplied labels."""
    root_id = str(uuid4())
    child_id = str(uuid4())
    return Dataset(
        nodes=[
            Node(id=root_id, labels=["sc:Dataset"], properties={}),
            Node(id=child_id, labels=to_labels, properties={}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": child_id, "labels": [edge_label]}),
        ],
    )


def _make_ap(edge_label: str, from_labels: list[str], to_labels: list[str]) -> AnalyticalPattern:
    """
    Build a minimal AnalyticalPattern with two nodes: the AP root + one child.
    The single edge uses the supplied labels.
    """
    root_id = str(uuid4())
    child_id = str(uuid4())
    return AnalyticalPattern(
        nodes=[
            Node(id=root_id, labels=["Analytical_Pattern"], properties={}),
            Node(id=child_id, labels=to_labels, properties={}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": child_id, "labels": [edge_label]}),
        ],
    )


# ---------------------------------------------------------------------------
# Dataset — valid edges
# ---------------------------------------------------------------------------

def test_dataset_valid_distribution_edge():
    """sc:Dataset --distribution--> Data is a permitted edge."""
    ds = _make_dataset("distribution", ["sc:Dataset"], ["Data"])
    assert ds is not None


def test_dataset_valid_recordset_edge():
    """sc:Dataset --recordSet--> cr:RecordSet is a permitted edge."""
    ds = _make_dataset("recordSet", ["sc:Dataset"], ["cr:RecordSet"])
    assert ds is not None


# ---------------------------------------------------------------------------
# Dataset — invalid edges
# ---------------------------------------------------------------------------

def test_dataset_rejects_ap_edge_label():
    """An AP-specific edge label (consist_of) must be rejected inside a Dataset."""
    with pytest.raises(ValidationError, match="Edges violate graph constraints"):
        _make_dataset("consist_of", ["sc:Dataset"], ["Data"])


def test_dataset_rejects_unknown_edge_label():
    """A completely unknown edge label must be rejected."""
    with pytest.raises(ValidationError, match="Edges violate graph constraints"):
        _make_dataset("unknown_edge", ["sc:Dataset"], ["Data"])


def test_dataset_rejects_wrong_target_label():
    """distribution from sc:Dataset to Operator (not Data) must be rejected."""
    with pytest.raises(ValidationError, match="Edges violate graph constraints"):
        _make_dataset("distribution", ["sc:Dataset"], ["Operator"])


# ---------------------------------------------------------------------------
# AnalyticalPattern — valid edges
# ---------------------------------------------------------------------------

def test_ap_valid_consist_of_edge():
    """Analytical_Pattern --consist_of--> Operator is a permitted edge."""
    ap = _make_ap("consist_of", ["Analytical_Pattern"], ["Operator"])
    assert ap is not None


# ---------------------------------------------------------------------------
# AnalyticalPattern — invalid edges
# ---------------------------------------------------------------------------

def test_ap_rejects_dataset_edge_label():
    """A Dataset-specific edge label (distribution) must be rejected inside an AP."""
    with pytest.raises(ValidationError, match="Edges violate graph constraints"):
        _make_ap("distribution", ["Analytical_Pattern"], ["Operator"])


def test_ap_rejects_consist_of_to_wrong_target():
    """consist_of from Analytical_Pattern to Data (not Operator) must be rejected."""
    with pytest.raises(ValidationError, match="Edges violate graph constraints"):
        _make_ap("consist_of", ["Analytical_Pattern"], ["Data"])


def test_ap_rejects_unknown_edge_label():
    """A completely unknown edge label must be rejected."""
    with pytest.raises(ValidationError, match="Edges violate graph constraints"):
        _make_ap("unknown_edge", ["Analytical_Pattern"], ["Operator"])


# ---------------------------------------------------------------------------
# AnalyticalPattern — ResultType edges (new)
# ---------------------------------------------------------------------------

def _make_ap_with_result_type(
    edge_label: str,
    op_labels: list[str],
    rt_labels: list[str],
) -> AnalyticalPattern:
    """Build a minimal AP: root → consist_of → Operator -(edge_label)→ ResultType."""
    root_id = str(uuid4())
    op_id = str(uuid4())
    rt_id = str(uuid4())
    return AnalyticalPattern(
        nodes=[
            Node(id=root_id, labels=["Analytical_Pattern"], properties={}),
            Node(id=op_id, labels=op_labels, properties={}),
            Node(id=rt_id, labels=rt_labels, properties={}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": op_id, "labels": ["consist_of"]}),
            Edge(**{"from": op_id, "to": rt_id, "labels": [edge_label]}),
        ],
    )


def test_ap_valid_operator_output_to_result_type():
    """Operator --output--> ResultType is a permitted edge."""
    ap = _make_ap_with_result_type("output", ["Operator"], [
                                   "ResultType", "string"])
    assert ap is not None


def test_ap_valid_operator_input_from_result_type():
    """Operator --input--> ResultType is a permitted edge."""
    ap = _make_ap_with_result_type("input", ["Operator"], [
                                   "ResultType", "boolean"])
    assert ap is not None


def test_ap_valid_operator_output_boolean_result_type():
    """Operator --output--> ResultType (boolean subtype) is a permitted edge."""
    ap = _make_ap_with_result_type("output", ["Operator"], [
                                   "ResultType", "boolean"])
    assert ap is not None


def test_ap_valid_operator_output_number_result_type():
    """Operator --output--> ResultType (number subtype) is a permitted edge."""
    ap = _make_ap_with_result_type("output", ["Operator"], [
                                   "ResultType", "number"])
    assert ap is not None


def test_ap_valid_operator_output_array_result_type():
    """Operator --output--> ResultType (array subtype) is a permitted edge."""
    ap = _make_ap_with_result_type(
        "output", ["Operator"], ["ResultType", "array"])
    assert ap is not None


def test_ap_valid_operator_output_object_result_type():
    """Operator --output--> ResultType (object subtype) is a permitted edge."""
    ap = _make_ap_with_result_type("output", ["Operator"], [
                                   "ResultType", "object"])
    assert ap is not None


def test_ap_rejects_operator_input_sc_dataset():
    """Operator --input--> sc:Dataset is NOT permitted; Data nodes (not sc:Dataset) are the correct target."""
    root_id = str(uuid4())
    op_id = str(uuid4())
    ds_id = str(uuid4())
    with pytest.raises(ValidationError, match="Edges violate graph constraints"):
        AnalyticalPattern(
            nodes=[
                Node(id=root_id, labels=["Analytical_Pattern"], properties={}),
                Node(id=op_id, labels=["Operator"], properties={}),
                Node(id=ds_id, labels=["sc:Dataset"], properties={}),
            ],
            edges=[
                Edge(**{"from": root_id, "to": op_id,
                     "labels": ["consist_of"]}),
                Edge(**{"from": op_id, "to": ds_id, "labels": ["input"]}),
            ],
        )


def test_ap_rejects_operator_output_sc_dataset():
    """Operator --output--> sc:Dataset is NOT permitted; use Data nodes instead."""
    root_id = str(uuid4())
    op_id = str(uuid4())
    ds_id = str(uuid4())
    with pytest.raises(ValidationError, match="Edges violate graph constraints"):
        AnalyticalPattern(
            nodes=[
                Node(id=root_id, labels=["Analytical_Pattern"], properties={}),
                Node(id=op_id, labels=["Operator"], properties={}),
                Node(id=ds_id, labels=["sc:Dataset"], properties={}),
            ],
            edges=[
                Edge(**{"from": root_id, "to": op_id,
                     "labels": ["consist_of"]}),
                Edge(**{"from": op_id, "to": ds_id, "labels": ["output"]}),
            ],
        )


def test_ap_valid_operator_input_data_node():
    """Operator --input--> Data is valid; Data is-a ResultType (persistent typed value)."""
    root_id = str(uuid4())
    op_id = str(uuid4())
    data_id = str(uuid4())
    ap = AnalyticalPattern(
        nodes=[
            Node(id=root_id, labels=["Analytical_Pattern"], properties={}),
            Node(id=op_id, labels=["Operator"], properties={}),
            Node(id=data_id, labels=["Data"], properties={}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": op_id, "labels": ["consist_of"]}),
            Edge(**{"from": op_id, "to": data_id, "labels": ["input"]}),
        ],
    )
    assert ap is not None


def test_ap_valid_operator_output_data_node():
    """Operator --output--> Data is valid; Data is-a ResultType (persistent typed value)."""
    root_id = str(uuid4())
    op_id = str(uuid4())
    data_id = str(uuid4())
    ap = AnalyticalPattern(
        nodes=[
            Node(id=root_id, labels=["Analytical_Pattern"], properties={}),
            Node(id=op_id, labels=["Operator"], properties={}),
            Node(id=data_id, labels=["Data"], properties={}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": op_id, "labels": ["consist_of"]}),
            Edge(**{"from": op_id, "to": data_id, "labels": ["output"]}),
        ],
    )
    assert ap is not None
