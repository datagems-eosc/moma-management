"""
Unit tests for AnalyticalPatternService (MagicMock — no Neo4j container).
"""

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from moma_management.domain.evaluation import (
    Evaluation,
    EvaluationDimension,
)
from pydantic import ValidationError as PydanticValidationError

from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.domain.exceptions import NotFoundError, ValidationError
from moma_management.domain.filters import AnalyticalPatternFilter
from moma_management.domain.generated.edges.edge_schema import Edge
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.services.analytical_pattern import AnalyticalPatternService

_EVALUATIONS_DIR = Path(__file__).parent.parent.parent / \
    "assets" / "aps" / "evaluations"

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

@pytest.mark.asyncio
async def test_create_fails_if_input_dataset_not_found():
    """create() must raise ValidationError when an input node has no parent dataset."""
    data_id = str(uuid4())
    ap = _make_ap_with_input(data_id)

    repo = AsyncMock()
    dataset_svc = AsyncMock()
    # Simulate: no datasets contain the input node
    dataset_svc.list.return_value = {"datasets": []}

    svc = AnalyticalPatternService(repo, dataset_svc)

    with pytest.raises(ValidationError):
        await svc.create(ap)

    repo.create.assert_not_called()


@pytest.mark.asyncio
async def test_create_success_when_input_node_found():
    """create() must persist the AP when all input nodes belong to a known dataset."""
    data_id = str(uuid4())
    ap = _make_ap_with_input(data_id)

    repo = AsyncMock()
    dataset_svc = AsyncMock()

    # Simulate: input node found in a dataset
    mock_node = MagicMock()
    mock_node.id = data_id
    mock_dataset = MagicMock()
    mock_dataset.nodes = [mock_node]
    dataset_svc.list.return_value = {"datasets": [mock_dataset]}

    svc = AnalyticalPatternService(repo, dataset_svc)
    returned_id = await svc.create(ap)

    repo.create.assert_called_once_with(ap, embedding=None)
    assert returned_id == str(ap.root.id)


@pytest.mark.asyncio
async def test_create_success_with_no_input_edges():
    """create() must not query datasets when the AP has no input edges."""
    ap = _make_ap_no_input()

    repo = AsyncMock()
    dataset_svc = AsyncMock()

    svc = AnalyticalPatternService(repo, dataset_svc)
    returned_id = await svc.create(ap)

    dataset_svc.list.assert_not_called()
    repo.create.assert_called_once_with(ap, embedding=None)
    assert returned_id == str(ap.root.id)


@pytest.mark.asyncio
async def test_get_raises_not_found():
    """get() must raise NotFoundError when the repo returns None."""
    repo = AsyncMock()
    repo.get.return_value = None

    svc = AnalyticalPatternService(repo, AsyncMock())

    with pytest.raises(NotFoundError):
        await svc.get(str(uuid4()))


# ---------------------------------------------------------------------------
# list() — accessible_dataset_ids passed to repo
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_no_filter_returns_all():
    """list_with_filter() with accessible_dataset_ids=None passes None to repo."""
    ap = _make_ap_no_input()
    filter_ = AnalyticalPatternFilter()

    repo = AsyncMock()
    repo.list.return_value = {"aps": [ap], "total": 1}
    svc = AnalyticalPatternService(repo, AsyncMock())

    result = await svc.list(filter_, accessible_dataset_ids=None)
    repo.list.assert_called_once_with(filter_, accessible_dataset_ids=None)
    assert [entry["ap"] for entry in result["aps"]] == [ap]


@pytest.mark.asyncio
async def test_list_passes_accessible_ids_to_repo():
    """list_with_filter() forwards accessible_dataset_ids to the repository."""
    ap = _make_ap_no_input()
    ds_ids = ["ds-1", "ds-2"]
    filter_ = AnalyticalPatternFilter()

    repo = AsyncMock()
    repo.list.return_value = {"aps": [ap], "total": 1}
    svc = AnalyticalPatternService(repo, AsyncMock())

    result = await svc.list(filter_, accessible_dataset_ids=ds_ids)
    repo.list.assert_called_once_with(filter_, accessible_dataset_ids=ds_ids)
    assert [entry["ap"] for entry in result["aps"]] == [ap]


# ---------------------------------------------------------------------------
# create() — embedding
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_embeds_description_when_embedder_set():
    """create() calls embedder.embed() and passes the vector to repo."""
    data_id = str(uuid4())
    ap = _make_ap_with_input(data_id)
    # Add a description to the root node
    ap.root.properties["description"] = "weather data analysis"

    repo = AsyncMock()
    dataset_svc = AsyncMock()
    mock_node = MagicMock()
    mock_node.id = data_id
    mock_dataset = MagicMock()
    mock_dataset.nodes = [mock_node]
    dataset_svc.list.return_value = {"datasets": [mock_dataset]}

    embedder = MagicMock()
    embedder.embed.return_value = [0.1, 0.2, 0.3]

    svc = AnalyticalPatternService(repo, dataset_svc, embedder=embedder)
    await svc.create(ap)

    embedder.embed.assert_called_once_with("weather data analysis")
    repo.create.assert_called_once_with(ap, embedding=[0.1, 0.2, 0.3])


@pytest.mark.asyncio
async def test_create_no_embedding_when_no_embedder():
    """create() passes embedding=None when no embedder is configured."""
    ap = _make_ap_no_input()
    ap.root.properties["description"] = "some description"

    repo = AsyncMock()
    svc = AnalyticalPatternService(repo, AsyncMock())
    await svc.create(ap)

    repo.create.assert_called_once_with(ap, embedding=None)


# ---------------------------------------------------------------------------
# list_with_filter() — search branch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_with_filter_search_raises_when_no_embedder():
    """list_with_filter() raises ValidationError when search is set but no embedder."""
    from moma_management.domain.filters import APSearchParams
    repo = AsyncMock()
    svc = AnalyticalPatternService(repo, AsyncMock())
    f = AnalyticalPatternFilter(search=APSearchParams(
        q="weather", top_k=10, threshold=0.0))

    with pytest.raises(ValidationError, match="no embedder configured"):
        await svc.list(f)


@pytest.mark.asyncio
async def test_list_with_filter_search_embeds_and_calls_repo_list():
    """list_with_filter() embeds the query and forwards query_vector to repo.list."""
    from moma_management.domain.filters import APSearchParams
    ap = _make_ap_no_input()
    embedding = [0.1, 0.2, 0.3]

    embedder = MagicMock()
    embedder.embed.return_value = embedding

    repo = AsyncMock()
    repo.list.return_value = {"aps": [ap], "total": 1}

    svc = AnalyticalPatternService(repo, MagicMock(), embedder=embedder)
    f = AnalyticalPatternFilter(search=APSearchParams(
        q="some query", top_k=10, threshold=0.0))
    result = await svc.list(f)

    embedder.embed.assert_called_once_with("some query")
    repo.list.assert_called_once_with(
        f, accessible_dataset_ids=None, query_vector=embedding)
    assert result["total"] == 1
    assert result["aps"] == [ap]


@pytest.mark.asyncio
async def test_list_with_filter_search_passes_accessible_ids():
    """list_with_filter() forwards accessible_dataset_ids to repo.list."""
    from moma_management.domain.filters import APSearchParams
    ap = _make_ap_no_input()
    embedding = [0.1, 0.2, 0.3]
    ds_ids = ["ds-1"]

    embedder = MagicMock()
    embedder.embed.return_value = embedding

    repo = AsyncMock()
    repo.list.return_value = {"aps": [ap], "total": 1}

    svc = AnalyticalPatternService(repo, MagicMock(), embedder=embedder)
    f = AnalyticalPatternFilter(search=APSearchParams(
        q="query", top_k=10, threshold=0.0))
    result = await svc.list(f, accessible_dataset_ids=ds_ids)

    repo.list.assert_called_once_with(
        f, accessible_dataset_ids=ds_ids, query_vector=embedding)
    assert result["aps"] == [ap]


# ---------------------------------------------------------------------------
# add_evaluation() / delete_evaluation()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_add_evaluation_persists_via_ap_repo():
    ap_repo = AsyncMock()
    ap_repo.get.return_value = MagicMock()  # AP exists
    ap_repo.add_evaluation = AsyncMock()

    svc = AnalyticalPatternService(ap_repo, AsyncMock())
    body = Evaluation(
        type=EvaluationDimension.system,
        evaluation=json.dumps({"latency": 12}),
        execution_id=uuid4(),
    )
    result = await svc.add_evaluation(ap_id="ap-123", eval=body)

    ap_repo.get.assert_called_once_with("ap-123")
    ap_repo.add_evaluation.assert_called_once_with(
        ap_id="ap-123",
        execution_id=str(body.execution_id),
        evaluation=Evaluation({EvaluationDimension.system: {"latency": 12}}),
    )
    assert result["execution_id"] == str(body.execution_id)


@pytest.mark.asyncio
async def test_add_evaluation_uses_provided_execution_id():
    ap_repo = AsyncMock()
    ap_repo.get.return_value = MagicMock()
    ap_repo.add_evaluation = AsyncMock()
    from uuid import UUID
    execution_id = uuid4()

    svc = AnalyticalPatternService(ap_repo, AsyncMock())
    body = Evaluation(
        type=EvaluationDimension.data,
        evaluation=json.dumps({"completeness": 0.9}),
        execution_id=execution_id,
    )
    result = await svc.add_evaluation(ap_id="ap-123", eval=body)

    assert result["execution_id"] == str(execution_id)
    assert ap_repo.add_evaluation.call_args.kwargs["execution_id"] == str(
        execution_id)


@pytest.mark.asyncio
async def test_add_evaluation_raises_not_found_when_ap_missing():
    ap_repo = AsyncMock()
    ap_repo.get.return_value = None

    svc = AnalyticalPatternService(ap_repo, AsyncMock())
    with pytest.raises(NotFoundError):
        await svc.add_evaluation(
            ap_id="missing-ap",
            eval=Evaluation(
                type=EvaluationDimension.system,
                evaluation=json.dumps({"value": 1}),
                execution_id=uuid4(),
            ),
        )


def test_evaluation_create_fails_without_dimensions():
    with pytest.raises(PydanticValidationError, match="(?i)at least one"):
        Evaluation({})


def test_evaluation_create_fails_with_unknown_dimension():
    with pytest.raises(PydanticValidationError):
        Evaluation({"unknown_dim": {"value": 1}})


@pytest.mark.asyncio
async def test_delete_evaluation_raises_not_found_when_repo_deletes_nothing():
    ap_repo = AsyncMock()
    ap_repo.delete_evaluation = AsyncMock(return_value=0)

    svc = AnalyticalPatternService(ap_repo, AsyncMock())
    with pytest.raises(NotFoundError):
        await svc.delete_evaluation(ap_id="ap-123", execution_id="missing")


@pytest.mark.asyncio
async def test_delete_evaluation_calls_repo():
    ap_repo = AsyncMock()
    ap_repo.delete_evaluation = AsyncMock(return_value=1)

    svc = AnalyticalPatternService(ap_repo, AsyncMock())
    await svc.delete_evaluation(ap_id="ap-123", execution_id="exec-1")
    ap_repo.delete_evaluation.assert_called_once_with(
        ap_id="ap-123", execution_id="exec-1")


@pytest.mark.parametrize("filename", ["MCQGen.json", "OfferRecom.json", "QuizComp.json"])
@pytest.mark.asyncio
async def test_add_evaluation_from_real_payload(filename: str):
    payload = json.loads((_EVALUATIONS_DIR / filename).read_text())

    ap_repo = AsyncMock()
    ap_repo.get.return_value = MagicMock()
    ap_repo.add_evaluation = AsyncMock()

    svc = AnalyticalPatternService(ap_repo, AsyncMock())
    # Submit one dimension at a time (as per EvaluationCreateRequest contract)
    for dim_str, metrics in payload.items():
        body = Evaluation(
            type=EvaluationDimension(dim_str),
            evaluation=json.dumps(metrics),
            execution_id=uuid4(),
        )
        result = await svc.add_evaluation(ap_id="ap-test", eval=body)
        assert result["execution_id"] == str(body.execution_id)
        ap_repo.add_evaluation.assert_called_with(
            ap_id="ap-test",
            execution_id=str(body.execution_id),
            evaluation=Evaluation({EvaluationDimension(dim_str): metrics}),
        )
