"""
Unit tests for AnalyticalPatternService (MagicMock — no Neo4j container).
"""

from unittest.mock import AsyncMock, MagicMock
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
    """list() with accessible_dataset_ids=None passes None to repo."""
    ap = _make_ap_no_input()

    repo = AsyncMock()
    repo.list.return_value = [ap]
    svc = AnalyticalPatternService(repo, AsyncMock())

    result = await svc.list(accessible_dataset_ids=None)
    repo.list.assert_called_once_with(accessible_dataset_ids=None)
    assert result == [ap]


@pytest.mark.asyncio
async def test_list_passes_accessible_ids_to_repo():
    """list() forwards accessible_dataset_ids to the repository."""
    ap = _make_ap_no_input()
    ds_ids = ["ds-1", "ds-2"]

    repo = AsyncMock()
    repo.list.return_value = [ap]
    svc = AnalyticalPatternService(repo, AsyncMock())

    result = await svc.list(accessible_dataset_ids=ds_ids)
    repo.list.assert_called_once_with(accessible_dataset_ids=ds_ids)
    assert result == [ap]


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
# search()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_search_raises_when_no_embedder():
    """search() raises ValidationError when embedder is not configured."""
    repo = AsyncMock()
    svc = AnalyticalPatternService(repo, AsyncMock())

    with pytest.raises(ValidationError, match="no embedder configured"):
        await svc.search("weather")


@pytest.mark.asyncio
async def test_search_returns_results():
    """search() delegates to embedder.embed and repo.search."""
    ap = _make_ap_no_input()
    embedding = [0.1, 0.2, 0.3]

    embedder = MagicMock()
    embedder.embed.return_value = embedding

    repo = AsyncMock()
    repo.search.return_value = [(ap, 0.95)]

    svc = AnalyticalPatternService(repo, MagicMock(), embedder=embedder)
    results = await svc.search("some query")

    embedder.embed.assert_called_once_with("some query")
    repo.search.assert_called_once_with(
        embedding, 10, accessible_dataset_ids=None)
    assert len(results) == 1
    assert results[0] == (ap, 0.95)


@pytest.mark.asyncio
async def test_search_passes_accessible_ids_to_repo():
    """search() forwards accessible_dataset_ids to repo.search."""
    ap = _make_ap_no_input()
    embedding = [0.1, 0.2, 0.3]
    ds_ids = ["ds-1"]

    embedder = MagicMock()
    embedder.embed.return_value = embedding

    repo = AsyncMock()
    repo.search.return_value = [(ap, 0.9)]

    svc = AnalyticalPatternService(repo, MagicMock(), embedder=embedder)
    results = await svc.search("query", accessible_dataset_ids=ds_ids)

    repo.search.assert_called_once_with(
        embedding, 10, accessible_dataset_ids=ds_ids)
    assert results == [(ap, 0.9)]
