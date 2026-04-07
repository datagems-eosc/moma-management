"""
Unit tests for AnalyticalPatternService (MagicMock — no Neo4j container).
"""

from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.domain.exceptions import NotFoundError, ValidationError
from moma_management.domain.generated.edges.edge_schema import Edge
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.services.analytical_pattern import AnalyticalPatternService

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ap_with_input(data_node_id: str) -> AnalyticalPattern:
    """Build a minimal AP with one 'input' edge targeting *data_node_id*."""
    root_id = str(uuid4())
    op_id = str(uuid4())
    return AnalyticalPattern(
        nodes=[
            Node(id=root_id, labels=[
                 "Analytical_Pattern"], properties={"name": "ap"}),
            Node(id=op_id, labels=["Operator"], properties={"name": "op"}),
            Node(id=data_node_id, labels=["Data"],
                 properties={"name": "data"}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": op_id, "labels": ["consist_of"]}),
            Edge(**{"from": op_id, "to": data_node_id, "labels": ["input"]}),
        ],
    )


def _make_ap_no_input() -> AnalyticalPattern:
    """Build a minimal AP with no input edges."""
    root_id = str(uuid4())
    op_id = str(uuid4())
    return AnalyticalPattern(
        nodes=[
            Node(id=root_id, labels=[
                 "Analytical_Pattern"], properties={"name": "ap"}),
            Node(id=op_id, labels=["Operator"], properties={"name": "op"}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": op_id, "labels": ["consist_of"]}),
        ],
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_create_fails_if_input_dataset_not_found():
    """create() must raise ValidationError when an input node has no parent dataset."""
    data_id = str(uuid4())
    ap = _make_ap_with_input(data_id)

    repo = MagicMock()
    dataset_svc = MagicMock()
    # Simulate: no datasets contain the input node
    dataset_svc.list.return_value = {"datasets": []}

    svc = AnalyticalPatternService(repo, dataset_svc)

    with pytest.raises(ValidationError):
        svc.create(ap)

    repo.create.assert_not_called()


def test_create_success_when_input_node_found():
    """create() must persist the AP when all input nodes belong to a known dataset."""
    data_id = str(uuid4())
    ap = _make_ap_with_input(data_id)

    repo = MagicMock()
    dataset_svc = MagicMock()

    # Simulate: input node found in a dataset
    mock_node = MagicMock()
    mock_node.id = data_id
    mock_dataset = MagicMock()
    mock_dataset.nodes = [mock_node]
    dataset_svc.list.return_value = {"datasets": [mock_dataset]}

    svc = AnalyticalPatternService(repo, dataset_svc)
    returned_id = svc.create(ap)

    repo.create.assert_called_once_with(ap)
    assert returned_id == str(ap.root.id)


def test_create_success_with_no_input_edges():
    """create() must not query datasets when the AP has no input edges."""
    ap = _make_ap_no_input()

    repo = MagicMock()
    dataset_svc = MagicMock()

    svc = AnalyticalPatternService(repo, dataset_svc)
    returned_id = svc.create(ap)

    dataset_svc.list.assert_not_called()
    repo.create.assert_called_once_with(ap)
    assert returned_id == str(ap.root.id)


def test_get_raises_not_found():
    """get() must raise NotFoundError when the repo returns None."""
    repo = MagicMock()
    repo.get.return_value = None

    svc = AnalyticalPatternService(repo, MagicMock())

    with pytest.raises(NotFoundError):
        svc.get(str(uuid4()))


# ---------------------------------------------------------------------------
# list() — accessible_dataset_ids filtering
# ---------------------------------------------------------------------------

def _make_ap_with_input_and_dataset(data_node_id: str, dataset_id: str) -> tuple[AnalyticalPattern, MagicMock]:
    """Return (AP, mock_dataset) where the AP has one input edge to *data_node_id*."""
    root_id = str(uuid4())
    op_id = str(uuid4())
    ap = AnalyticalPattern(
        nodes=[
            Node(id=root_id, labels=[
                 "Analytical_Pattern"], properties={"name": "ap"}),
            Node(id=op_id, labels=["Operator"], properties={"name": "op"}),
            Node(id=data_node_id, labels=["Data"],
                 properties={"name": "data"}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": op_id, "labels": ["consist_of"]}),
            Edge(**{"from": op_id, "to": data_node_id, "labels": ["input"]}),
        ],
    )
    mock_ds = MagicMock()
    ds_node = MagicMock()
    ds_node.id = dataset_id
    ds_node.labels = ["sc:Dataset"]
    mock_ds.nodes = [ds_node]
    return ap, mock_ds


def test_list_no_filter_returns_all():
    """list() with accessible_dataset_ids=None returns everything."""
    data_id = str(uuid4())
    ap, _ = _make_ap_with_input_and_dataset(data_id, str(uuid4()))

    repo = MagicMock()
    repo.list.return_value = [ap]
    svc = AnalyticalPatternService(repo, MagicMock())

    result = svc.list(accessible_dataset_ids=None)
    assert result == [ap]


def test_list_filter_includes_accessible_ap():
    """list() returns an AP whose input dataset is in accessible_dataset_ids."""
    data_id = str(uuid4())
    ds_id = str(uuid4())
    ap, mock_ds = _make_ap_with_input_and_dataset(data_id, ds_id)

    repo = MagicMock()
    repo.list.return_value = [ap]
    dataset_svc = MagicMock()
    dataset_svc.list.return_value = {"datasets": [mock_ds]}

    svc = AnalyticalPatternService(repo, dataset_svc)
    result = svc.list(accessible_dataset_ids=[ds_id])

    assert result == [ap]


def test_list_filter_excludes_inaccessible_ap():
    """list() excludes an AP whose input dataset is not in accessible_dataset_ids."""
    data_id = str(uuid4())
    ds_id = str(uuid4())
    ap, mock_ds = _make_ap_with_input_and_dataset(data_id, ds_id)

    repo = MagicMock()
    repo.list.return_value = [ap]
    dataset_svc = MagicMock()
    dataset_svc.list.return_value = {"datasets": [mock_ds]}

    svc = AnalyticalPatternService(repo, dataset_svc)
    result = svc.list(accessible_dataset_ids=["other-dataset-id"])

    assert result == []


def test_list_filter_includes_ap_with_no_inputs():
    """list() always includes APs with no input edges, regardless of accessible_dataset_ids."""
    root_id = str(uuid4())
    op_id = str(uuid4())
    ap = AnalyticalPattern(
        nodes=[
            Node(id=root_id, labels=[
                 "Analytical_Pattern"], properties={"name": "ap"}),
            Node(id=op_id, labels=["Operator"], properties={"name": "op"}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": op_id, "labels": ["consist_of"]}),
        ],
    )

    repo = MagicMock()
    repo.list.return_value = [ap]
    dataset_svc = MagicMock()

    svc = AnalyticalPatternService(repo, dataset_svc)
    result = svc.list(accessible_dataset_ids=[])

    dataset_svc.list.assert_not_called()
    assert result == [ap]
