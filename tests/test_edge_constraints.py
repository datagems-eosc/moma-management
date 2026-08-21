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
from moma_management.domain.dataset_relationship import DatasetRelationship
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
            Node(id=root_id, labels=[
                 "Analytical_Pattern"], properties={"name": "test"}),
            Node(id=child_id, labels=to_labels, properties={"name": "test"}),
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
    """A completely unknown edge label must be rejected.

    Since EdgeLabel is an enum, Pydantic rejects unrecognised values at
    model-construction time (before domain constraint validation runs).
    """
    with pytest.raises(ValidationError):
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
    """A completely unknown edge label must be rejected.

    Since EdgeLabel is an enum, Pydantic rejects unrecognised values at
    model-construction time (before domain constraint validation runs).
    """
    with pytest.raises(ValidationError):
        _make_ap("unknown_edge", ["Analytical_Pattern"], ["Operator"])


# ---------------------------------------------------------------------------
# AnalyticalPattern — ResultType edges (new)
# ---------------------------------------------------------------------------

def _make_ap_with_result_type(
    edge_label: str,
    op_labels: list[str],
    rt_labels: list[str],
) -> AnalyticalPattern:
    """Build a minimal AP with an edge between an Operator and a ResultType/Data node.

    For ``input`` edges the direction is RT/Data → Operator.
    For ``output`` edges the direction is Operator → RT/Data.
    """
    root_id = str(uuid4())
    op_id = str(uuid4())
    rt_id = str(uuid4())
    if edge_label == "input":
        edge_from, edge_to = rt_id, op_id
    else:
        edge_from, edge_to = op_id, rt_id
    return AnalyticalPattern(
        nodes=[
            Node(id=root_id, labels=[
                 "Analytical_Pattern"], properties={"name": "test"}),
            Node(id=op_id, labels=op_labels, properties={"name": "test"}),
            Node(id=rt_id, labels=rt_labels, properties={"name": "test"}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": op_id, "labels": ["consist_of"]}),
            Edge(**{"from": edge_from, "to": edge_to, "labels": [edge_label]}),
        ],
    )


def test_ap_valid_operator_output_to_result_type():
    """Operator --output--> ResultType is a permitted edge."""
    ap = _make_ap_with_result_type("output", ["Operator"], [
                                   "ResultType", "string"])
    assert ap is not None


def test_ap_valid_operator_input_from_result_type():
    """ResultType --input--> Operator is a permitted edge."""
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


def test_ap_valid_operator_input_sc_dataset():
    """sc:Dataset --input--> Operator is permitted (whole-dataset reference, mapping is Any)."""
    root_id = str(uuid4())
    op_id = str(uuid4())
    ds_id = str(uuid4())
    ap = AnalyticalPattern(
        nodes=[
            Node(id=root_id, labels=[
                 "Analytical_Pattern"], properties={"name": "ap"}),
            Node(id=op_id, labels=["Operator"], properties={"name": "op"}),
            Node(id=ds_id, labels=["sc:Dataset"], properties={}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": op_id, "labels": ["consist_of"]}),
            Edge(**{"from": ds_id, "to": op_id, "labels": ["input"]}),
        ],
    )
    assert ap is not None


def test_ap_valid_operator_output_sc_dataset():
    """Operator --output--> sc:Dataset is permitted (whole-dataset reference, mapping is Any)."""
    root_id = str(uuid4())
    op_id = str(uuid4())
    ds_id = str(uuid4())
    ap = AnalyticalPattern(
        nodes=[
            Node(id=root_id, labels=[
                 "Analytical_Pattern"], properties={"name": "ap"}),
            Node(id=op_id, labels=["Operator"], properties={"name": "op"}),
            Node(id=ds_id, labels=["sc:Dataset"], properties={}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": op_id, "labels": ["consist_of"]}),
            Edge(**{"from": op_id, "to": ds_id, "labels": ["output"]}),
        ],
    )
    assert ap is not None


def test_ap_valid_operator_input_data_node():
    """Data --input--> Operator is valid (persistent Data node feeds the Operator)."""
    root_id = str(uuid4())
    op_id = str(uuid4())
    data_id = str(uuid4())
    ap = AnalyticalPattern(
        nodes=[
            Node(id=root_id, labels=[
                 "Analytical_Pattern"], properties={"name": "test"}),
            Node(id=op_id, labels=["Operator"], properties={"name": "test"}),
            Node(id=data_id, labels=["Data"], properties={}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": op_id, "labels": ["consist_of"]}),
            Edge(**{"from": data_id, "to": op_id, "labels": ["input"]}),
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
            Node(id=root_id, labels=[
                 "Analytical_Pattern"], properties={"name": "test"}),
            Node(id=op_id, labels=["Operator"], properties={"name": "test"}),
            Node(id=data_id, labels=["Data"], properties={}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": op_id, "labels": ["consist_of"]}),
            Edge(**{"from": op_id, "to": data_id, "labels": ["output"]}),
        ],
    )
    assert ap is not None


# ---------------------------------------------------------------------------
# Dataset — Column → intervalStatistics → IntervalColumnStatistics edges
# ---------------------------------------------------------------------------

def _make_dataset_with_interval_stats(
    edge_label: str,
    from_labels: list[str],
    to_labels: list[str],
    stats_properties: dict | None = None,
) -> Dataset:
    """Build a connected Dataset:
      sc:Dataset -distribution-> Data <-source/fileObject- Column -(edge_label)-> to_labels
    All four nodes are reachable from the root via undirected DFS.
    """
    root_id = str(uuid4())
    data_id = str(uuid4())
    col_id = str(uuid4())
    stats_id = str(uuid4())
    return Dataset(
        nodes=[
            Node(id=root_id, labels=["sc:Dataset"], properties={}),
            Node(id=data_id, labels=["Data"], properties={}),
            Node(id=col_id, labels=from_labels, properties={}),
            Node(id=stats_id, labels=to_labels,
                 properties=stats_properties or {}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": data_id,
                 "labels": ["distribution"]}),
            Edge(**{"from": col_id, "to": data_id,
                 "labels": ["source/fileObject"]}),
            Edge(**{"from": col_id, "to": stats_id, "labels": [edge_label]}),
        ],
    )


def test_dataset_valid_column_interval_statistics_edge():
    """Column --intervalStatistics--> IntervalColumnStatistics is a permitted edge."""
    ds = _make_dataset_with_interval_stats(
        "intervalStatistics",
        ["Column"],
        ["IntervalColumnStatistics"],
        {"windowStart": "2024-01-01T00:00:00Z",
            "windowEnd": "2024-01-01T01:00:00Z", "scopeType": "global"},
    )
    assert ds is not None


def test_dataset_rejects_interval_statistics_wrong_source():
    """sc:Dataset --intervalStatistics--> IntervalColumnStatistics must be rejected."""
    with pytest.raises(ValidationError, match="Edges violate graph constraints"):
        _make_dataset("intervalStatistics", ["sc:Dataset"], [
                      "IntervalColumnStatistics"])


def test_dataset_rejects_interval_statistics_wrong_target():
    """Column --intervalStatistics--> Operator must be rejected."""
    with pytest.raises(ValidationError, match="Edges violate graph constraints"):
        _make_dataset_with_interval_stats(
            "intervalStatistics", ["Column"], ["Operator"])


# ---------------------------------------------------------------------------
# Dataset — RecordSet → dataQuality → DataQuality → error → DataQualityError
# ---------------------------------------------------------------------------

_VALID_DATA_QUALITY_PROPS = {"type": "dg:DataQuality",
                             "summary": "1 issue detected"}
_VALID_DATA_QUALITY_ERROR_PROPS = {
    "type": "dg:DataQualityError",
    "column": "country",
    "errorType": "value_error",
    "description": "Negative or invalid values detected.",
    "totalAffectedRows": 12,
}


def _make_dataset_with_data_quality(
    edge_label: str,
    from_labels: list[str],
    to_labels: list[str],
    to_properties: dict | None = None,
) -> Dataset:
    """Build a connected Dataset:
      sc:Dataset -recordSet-> cr:RecordSet -(edge_label)-> to_labels
    """
    root_id = str(uuid4())
    record_set_id = str(uuid4())
    target_id = str(uuid4())
    return Dataset(
        nodes=[
            Node(id=root_id, labels=["sc:Dataset"], properties={}),
            Node(id=record_set_id, labels=from_labels, properties={}),
            Node(id=target_id, labels=to_labels,
                 properties=to_properties or {}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": record_set_id,
                 "labels": ["recordSet"]}),
            Edge(**{"from": record_set_id, "to": target_id,
                 "labels": [edge_label]}),
        ],
    )


def test_dataset_valid_recordset_data_quality_edge():
    """cr:RecordSet --dataQuality--> DataQuality is a permitted edge."""
    ds = _make_dataset_with_data_quality(
        "dataQuality", ["cr:RecordSet"], ["DataQuality"],
        to_properties=_VALID_DATA_QUALITY_PROPS)
    assert ds is not None


def test_dataset_valid_full_data_quality_chain():
    """The full chain sc:Dataset -recordSet-> RecordSet -dataQuality->
    DataQuality -error-> DataQualityError validates end to end."""
    root_id = str(uuid4())
    record_set_id = str(uuid4())
    dq_id = str(uuid4())
    dqe_id = str(uuid4())
    ds = Dataset(
        nodes=[
            Node(id=root_id, labels=["sc:Dataset"], properties={}),
            Node(id=record_set_id, labels=["cr:RecordSet"], properties={}),
            Node(id=dq_id, labels=["DataQuality"],
                 properties=_VALID_DATA_QUALITY_PROPS),
            Node(id=dqe_id, labels=["DataQualityError"],
                 properties=_VALID_DATA_QUALITY_ERROR_PROPS),
        ],
        edges=[
            Edge(**{"from": root_id, "to": record_set_id,
                 "labels": ["recordSet"]}),
            Edge(**{"from": record_set_id, "to": dq_id,
                 "labels": ["dataQuality"]}),
            Edge(**{"from": dq_id, "to": dqe_id, "labels": ["error"]}),
        ],
    )
    assert ds is not None


def test_dataset_rejects_data_quality_wrong_source():
    """sc:Dataset --dataQuality--> DataQuality must be rejected: only a RecordSet may carry a dataQuality edge."""
    with pytest.raises(ValidationError, match="Edges violate graph constraints"):
        _make_dataset("dataQuality", ["sc:Dataset"], ["DataQuality"])


def test_dataset_rejects_data_quality_error_skipping_hop():
    """cr:RecordSet --error--> DataQualityError must be rejected: DataQualityError only attaches under DataQuality, not directly under RecordSet."""
    with pytest.raises(ValidationError, match="Edges violate graph constraints"):
        _make_dataset_with_data_quality(
            "error", ["cr:RecordSet"], ["DataQualityError"])


def test_data_quality_error_rejects_unknown_error_type():
    """A DataQualityError with an out-of-enum errorType must fail schema
    validation, even when every other required property is present."""
    root_id = str(uuid4())
    record_set_id = str(uuid4())
    dq_id = str(uuid4())
    dqe_id = str(uuid4())
    with pytest.raises(ValidationError):
        Dataset(
            nodes=[
                Node(id=root_id, labels=["sc:Dataset"], properties={}),
                Node(id=record_set_id, labels=[
                     "cr:RecordSet"], properties={}),
                Node(id=dq_id, labels=["DataQuality"],
                     properties=_VALID_DATA_QUALITY_PROPS),
                Node(id=dqe_id, labels=["DataQualityError"], properties={
                    **_VALID_DATA_QUALITY_ERROR_PROPS,
                    "errorType": "typo_error",
                }),
            ],
            edges=[
                Edge(**{"from": root_id, "to": record_set_id,
                     "labels": ["recordSet"]}),
                Edge(**{"from": record_set_id, "to": dq_id,
                     "labels": ["dataQuality"]}),
                Edge(**{"from": dq_id, "to": dqe_id, "labels": ["error"]}),
            ],
        )


# ---------------------------------------------------------------------------
# DatasetRelationship — helpers
# ---------------------------------------------------------------------------

def _make_relationship(edge_label: str, from_labels: list[str], to_labels: list[str]) -> DatasetRelationship:
    """Build a minimal two-node/one-edge DatasetRelationship for edge-constraint tests.

    Not a structurally complete relationship (it does not necessarily target
    two datasets) — only used for cases where an edge-constraint violation
    is expected to be raised before the "exactly two datasets" check runs.
    """
    root_id = str(uuid4())
    child_id = str(uuid4())
    return DatasetRelationship(
        nodes=[
            Node(id=root_id, labels=["BasicDLElement"], properties={}),
            Node(id=child_id, labels=to_labels, properties={}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": child_id, "labels": [edge_label]}),
        ],
    )


def _make_valid_relationship_base() -> tuple[DatasetRelationship, str, str, str, str]:
    """Build a minimal *valid* DatasetRelationship: root -HAS_TARGET-> two datasets.

    Returns (relationship, root_id, dataset_id_a, dataset_id_b).
    """
    root_id = str(uuid4())
    ds_a = str(uuid4())
    ds_b = str(uuid4())
    rel = DatasetRelationship(
        nodes=[
            Node(id=root_id, labels=["BasicDLElement"], properties={}),
            Node(id=ds_a, labels=["sc:Dataset"], properties={}),
            Node(id=ds_b, labels=["sc:Dataset"], properties={}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": ds_a, "labels": ["HAS_TARGET"]}),
            Edge(**{"from": root_id, "to": ds_b, "labels": ["HAS_TARGET"]}),
        ],
    )
    return rel, root_id, ds_a, ds_b


# ---------------------------------------------------------------------------
# DatasetRelationship — valid edges
# ---------------------------------------------------------------------------

def test_relationship_valid_has_target_from_root():
    """BasicDLElement --HAS_TARGET--> sc:Dataset (x2) is a permitted, complete relationship."""
    rel, *_ = _make_valid_relationship_base()
    assert rel is not None


def test_relationship_valid_has_comparison_edge():
    """BasicDLElement --HAS_COMPARISON--> PropertyComparison is a permitted edge."""
    base, root_id, ds_a, ds_b = _make_valid_relationship_base()
    pc_id = str(uuid4())
    rel = DatasetRelationship(
        nodes=base.nodes + [Node(id=pc_id, labels=["PropertyComparison"], properties={})],
        edges=base.edges + [Edge(**{"from": root_id, "to": pc_id, "labels": ["HAS_COMPARISON"]})],
    )
    assert rel is not None


def test_relationship_valid_has_evidence_edge():
    """PropertyComparison --HAS_EVIDENCE--> TextEvidence is a permitted edge."""
    base, root_id, ds_a, ds_b = _make_valid_relationship_base()
    pc_id = str(uuid4())
    te_id = str(uuid4())
    rel = DatasetRelationship(
        nodes=base.nodes + [
            Node(id=pc_id, labels=["PropertyComparison"], properties={}),
            Node(id=te_id, labels=["TextEvidence"], properties={}),
        ],
        edges=base.edges + [
            Edge(**{"from": root_id, "to": pc_id, "labels": ["HAS_COMPARISON"]}),
            Edge(**{"from": pc_id, "to": te_id, "labels": ["HAS_EVIDENCE"]}),
        ],
    )
    assert rel is not None


def test_relationship_valid_has_target_from_property_comparison():
    """PropertyComparison --HAS_TARGET--> sc:Dataset is a permitted edge."""
    base, root_id, ds_a, ds_b = _make_valid_relationship_base()
    pc_id = str(uuid4())
    rel = DatasetRelationship(
        nodes=base.nodes + [Node(id=pc_id, labels=["PropertyComparison"], properties={})],
        edges=base.edges + [
            Edge(**{"from": root_id, "to": pc_id, "labels": ["HAS_COMPARISON"]}),
            Edge(**{"from": pc_id, "to": ds_a, "labels": ["HAS_TARGET"]}),
        ],
    )
    assert rel is not None


def test_relationship_valid_has_target_from_text_evidence():
    """TextEvidence --HAS_TARGET--> sc:Dataset is a permitted edge."""
    base, root_id, ds_a, ds_b = _make_valid_relationship_base()
    pc_id = str(uuid4())
    te_id = str(uuid4())
    rel = DatasetRelationship(
        nodes=base.nodes + [
            Node(id=pc_id, labels=["PropertyComparison"], properties={}),
            Node(id=te_id, labels=["TextEvidence"], properties={}),
        ],
        edges=base.edges + [
            Edge(**{"from": root_id, "to": pc_id, "labels": ["HAS_COMPARISON"]}),
            Edge(**{"from": pc_id, "to": te_id, "labels": ["HAS_EVIDENCE"]}),
            Edge(**{"from": te_id, "to": ds_b, "labels": ["HAS_TARGET"]}),
        ],
    )
    assert rel is not None


# ---------------------------------------------------------------------------
# DatasetRelationship — invalid edges
# ---------------------------------------------------------------------------

def test_relationship_rejects_ap_edge_label():
    """An AP-specific edge label (consist_of) must be rejected inside a DatasetRelationship."""
    with pytest.raises(ValidationError, match="Edges violate graph constraints"):
        _make_relationship("consist_of", ["BasicDLElement"], ["Operator"])


def test_relationship_rejects_unknown_edge_label():
    """A completely unknown edge label must be rejected.

    Since EdgeLabel is an enum, Pydantic rejects unrecognised values at
    model-construction time (before domain constraint validation runs).
    """
    with pytest.raises(ValidationError):
        _make_relationship("unknown_edge", ["BasicDLElement"], ["PropertyComparison"])


def test_relationship_rejects_has_comparison_to_wrong_target():
    """HAS_COMPARISON from BasicDLElement to TextEvidence (not PropertyComparison) must be rejected."""
    with pytest.raises(ValidationError, match="Edges violate graph constraints"):
        _make_relationship("HAS_COMPARISON", ["BasicDLElement"], ["TextEvidence"])


def test_relationship_rejects_has_target_to_wrong_target():
    """HAS_TARGET from BasicDLElement to Operator (not sc:Dataset) must be rejected."""
    with pytest.raises(ValidationError, match="Edges violate graph constraints"):
        _make_relationship("HAS_TARGET", ["BasicDLElement"], ["Operator"])
