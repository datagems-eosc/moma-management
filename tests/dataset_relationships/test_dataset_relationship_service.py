"""
Unit tests for DatasetRelationshipService (MagicMock/AsyncMock — no Neo4j container).
"""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from moma_management.domain.dataset_relationship import DatasetRelationship
from moma_management.domain.exceptions import ConflictError, NotFoundError, ValidationError
from moma_management.domain.generated.edges.edge_schema import Edge
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.services.dataset_relationship import DatasetRelationshipService

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_relationship(ds_id_a: str, ds_id_b: str) -> DatasetRelationship:
    """Build a minimal DatasetRelationship whose root directly targets the two given datasets."""
    root_id = str(uuid4())
    return DatasetRelationship(
        nodes=[
            Node(id=root_id, labels=["BasicDLElement"], properties={}),
            Node(id=ds_id_a, labels=["sc:Dataset"], properties={}),
            Node(id=ds_id_b, labels=["sc:Dataset"], properties={}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": ds_id_a, "labels": ["HAS_TARGET"]}),
            Edge(**{"from": root_id, "to": ds_id_b, "labels": ["HAS_TARGET"]}),
        ],
    )


def _found_datasets(*ids: str) -> dict:
    """Build a DatasetService.list()-shaped return value where *ids* all resolve."""
    datasets = []
    for i in ids:
        node = MagicMock()
        node.id = i
        ds = MagicMock()
        ds.nodes = [node]
        datasets.append(ds)
    return {"datasets": datasets}


# ---------------------------------------------------------------------------
# create()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_fails_if_dataset_not_found():
    """create() must raise ValidationError when a target dataset does not exist."""
    ds_a, ds_b = str(uuid4()), str(uuid4())
    rel = _make_relationship(ds_a, ds_b)

    repo = AsyncMock()
    dataset_svc = AsyncMock()
    dataset_svc.list.return_value = {"datasets": []}

    svc = DatasetRelationshipService(repo, dataset_svc)

    with pytest.raises(ValidationError):
        await svc.create(rel)

    repo.create.assert_not_called()


@pytest.mark.asyncio
async def test_create_fails_if_pair_already_linked():
    """create() must raise ConflictError when the dataset pair is already linked."""
    ds_a, ds_b = str(uuid4()), str(uuid4())
    rel = _make_relationship(ds_a, ds_b)

    repo = AsyncMock()
    repo.find_id_for_dataset_pair.return_value = str(uuid4())
    dataset_svc = AsyncMock()
    dataset_svc.list.return_value = _found_datasets(ds_a, ds_b)

    svc = DatasetRelationshipService(repo, dataset_svc)

    with pytest.raises(ConflictError):
        await svc.create(rel)

    repo.create.assert_not_called()


@pytest.mark.asyncio
async def test_create_success_when_both_datasets_found_and_pair_unique():
    """create() persists the relationship when both datasets exist and no duplicate pair exists."""
    ds_a, ds_b = str(uuid4()), str(uuid4())
    rel = _make_relationship(ds_a, ds_b)

    repo = AsyncMock()
    repo.find_id_for_dataset_pair.return_value = None
    dataset_svc = AsyncMock()
    dataset_svc.list.return_value = _found_datasets(ds_a, ds_b)

    svc = DatasetRelationshipService(repo, dataset_svc)
    returned_id = await svc.create(rel)

    repo.create.assert_called_once_with(rel)
    assert returned_id == str(rel.root.id)


# ---------------------------------------------------------------------------
# get() / delete()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_raises_not_found():
    """get() must raise NotFoundError when the repo returns None."""
    repo = AsyncMock()
    repo.get.return_value = None

    svc = DatasetRelationshipService(repo, AsyncMock())

    with pytest.raises(NotFoundError):
        await svc.get(str(uuid4()))


@pytest.mark.asyncio
async def test_delete_raises_not_found_when_missing():
    """delete() must raise NotFoundError when the repo returns None for get()."""
    repo = AsyncMock()
    repo.get.return_value = None

    svc = DatasetRelationshipService(repo, AsyncMock())

    with pytest.raises(NotFoundError):
        await svc.delete(str(uuid4()))

    repo.delete.assert_not_called()


@pytest.mark.asyncio
async def test_delete_succeeds_when_found():
    """delete() calls repo.delete() when the relationship exists."""
    ds_a, ds_b = str(uuid4()), str(uuid4())
    rel = _make_relationship(ds_a, ds_b)

    repo = AsyncMock()
    repo.get.return_value = rel

    svc = DatasetRelationshipService(repo, AsyncMock())
    await svc.delete(str(rel.root.id))

    repo.delete.assert_called_once_with(str(rel.root.id))


# ---------------------------------------------------------------------------
# list_for_dataset()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_for_dataset_raises_not_found_when_dataset_missing():
    """list_for_dataset() must raise NotFoundError when the dataset does not exist."""
    repo = AsyncMock()
    dataset_svc = AsyncMock()
    dataset_svc.get.side_effect = NotFoundError("no such dataset")

    svc = DatasetRelationshipService(repo, dataset_svc)
    with pytest.raises(NotFoundError):
        await svc.list_for_dataset(str(uuid4()))

    repo.list_for_dataset.assert_not_called()


@pytest.mark.asyncio
async def test_list_for_dataset_returns_all_when_no_access_filter():
    """list_for_dataset() returns every result unfiltered when accessible_dataset_ids is None."""
    ds_a, ds_b, ds_c = str(uuid4()), str(uuid4()), str(uuid4())
    rel_ab = _make_relationship(ds_a, ds_b)
    rel_ac = _make_relationship(ds_a, ds_c)

    repo = AsyncMock()
    repo.list_for_dataset.return_value = [rel_ab, rel_ac]
    dataset_svc = AsyncMock()

    svc = DatasetRelationshipService(repo, dataset_svc)
    result = await svc.list_for_dataset(ds_a)

    assert result == [rel_ab, rel_ac]


@pytest.mark.asyncio
async def test_list_for_dataset_filters_out_relationships_to_inaccessible_datasets():
    """list_for_dataset() excludes relationships whose OTHER dataset is not accessible."""
    ds_a, ds_b, ds_c = str(uuid4()), str(uuid4()), str(uuid4())
    rel_ab = _make_relationship(ds_a, ds_b)
    rel_ac = _make_relationship(ds_a, ds_c)

    repo = AsyncMock()
    repo.list_for_dataset.return_value = [rel_ab, rel_ac]
    dataset_svc = AsyncMock()

    svc = DatasetRelationshipService(repo, dataset_svc)
    # Caller can only browse ds_a and ds_b, not ds_c -> rel_ac must be excluded
    result = await svc.list_for_dataset(ds_a, accessible_dataset_ids=[ds_a, ds_b])

    assert result == [rel_ab]
