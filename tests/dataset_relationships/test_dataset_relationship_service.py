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
from tests.utils import make_file_object_relationship

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


def _found_datasets(*ids: str, files: dict[str, str] | None = None) -> dict:
    """Build a DatasetService.list()-shaped return value where *ids* all resolve,
    *files* mapping each stored cr:FileObject id to the dataset id that owns it."""
    datasets = {}
    for i in ids:
        node = MagicMock()
        node.id = i
        node.labels = ["sc:Dataset"]
        ds = MagicMock()
        ds.nodes = [node]
        datasets[i] = ds
    for f, owner in (files or {}).items():
        node = MagicMock()
        node.id = f
        node.labels = ["PDF", "Data", "cr:FileObject"]
        datasets[owner].nodes.append(node)
    return {"datasets": list(datasets.values())}


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
async def test_create_fails_if_target_is_a_node_inside_a_dataset():
    """A child node id (e.g. a file) posing as a dataset must not pass the
    existence check just because list() returns its parent dataset."""
    ds_a, ds_c, file_of_c = str(uuid4()), str(uuid4()), str(uuid4())
    rel = _make_relationship(ds_a, file_of_c)

    repo = AsyncMock()
    repo.find_id_for_dataset_pair.return_value = None
    dataset_svc = AsyncMock()
    dataset_svc.list.return_value = _found_datasets(ds_c, ds_a, files={file_of_c: ds_c})

    svc = DatasetRelationshipService(repo, dataset_svc)

    with pytest.raises(ValidationError, match=file_of_c):
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


@pytest.mark.asyncio
async def test_create_fails_if_file_object_not_in_linked_datasets():
    """create() must reject a cr:FileObject that belongs to neither linked dataset."""
    ds_a, ds_b, file_a, foreign_file = (str(uuid4()) for _ in range(4))
    rel = make_file_object_relationship(ds_a, ds_b, file_a, foreign_file)

    repo = AsyncMock()
    repo.find_id_for_dataset_pair.return_value = None
    dataset_svc = AsyncMock()
    dataset_svc.list.return_value = _found_datasets(ds_a, ds_b, files={file_a: ds_a})

    svc = DatasetRelationshipService(repo, dataset_svc)

    with pytest.raises(ValidationError, match=foreign_file):
        await svc.create(rel)

    repo.create.assert_not_called()


@pytest.mark.asyncio
async def test_create_accepts_file_objects_of_linked_datasets():
    """create() persists a FileObject linking whose files all belong to the linked datasets."""
    ds_a, ds_b, file_a, file_b = (str(uuid4()) for _ in range(4))
    rel = make_file_object_relationship(ds_a, ds_b, file_a, file_b)

    repo = AsyncMock()
    repo.find_id_for_dataset_pair.return_value = None
    dataset_svc = AsyncMock()
    dataset_svc.list.return_value = _found_datasets(ds_a, ds_b, files={file_a: ds_a, file_b: ds_b})

    svc = DatasetRelationshipService(repo, dataset_svc)
    await svc.create(rel)

    repo.create.assert_called_once_with(rel)


@pytest.mark.asyncio
@pytest.mark.parametrize("with_distribution", [False, True])
async def test_create_accepts_matching_distribution_edges(with_distribution):
    """distribution edges are optional; when sent, matching ones are accepted."""
    ds_a, ds_b, file_a, file_b = (str(uuid4()) for _ in range(4))
    rel = make_file_object_relationship(ds_a, ds_b, file_a, file_b, with_distribution=with_distribution)

    repo = AsyncMock()
    repo.find_id_for_dataset_pair.return_value = None
    dataset_svc = AsyncMock()
    dataset_svc.list.return_value = _found_datasets(ds_a, ds_b, files={file_a: ds_a, file_b: ds_b})

    await DatasetRelationshipService(repo, dataset_svc).create(rel)
    repo.create.assert_called_once_with(rel)


@pytest.mark.asyncio
async def test_create_fails_if_distribution_edge_contradicts_storage():
    """A distribution edge putting a file in the wrong dataset must be rejected."""
    ds_a, ds_b, file_a, file_b = (str(uuid4()) for _ in range(4))
    rel = make_file_object_relationship(ds_a, ds_b, file_a, file_b, with_distribution=True)

    repo = AsyncMock()
    repo.find_id_for_dataset_pair.return_value = None
    dataset_svc = AsyncMock()
    # MoMa stores the two files the other way round
    dataset_svc.list.return_value = _found_datasets(ds_a, ds_b, files={file_a: ds_b, file_b: ds_a})

    with pytest.raises(ValidationError, match=f"{file_a} is in {ds_b}, not {ds_a}"):
        await DatasetRelationshipService(repo, dataset_svc).create(rel)
    repo.create.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("without_files", [False, True], ids=["same-dataset", "no-file"])
async def test_create_fails_if_comparison_only_covers_one_dataset(without_files):
    """A FileObjectComparison of two files of the same dataset, or of no file
    at all, must be rejected."""
    ds_a, ds_b, file_a, file_a2 = (str(uuid4()) for _ in range(4))
    rel = make_file_object_relationship(ds_a, ds_b, file_a, file_a2)
    if without_files:
        empty_id = str(uuid4())
        fole_id = next(str(n.id) for n in rel.nodes if "FileObjectLinkingElement" in n.labels)
        raw = rel.model_dump(by_alias=True, mode="json")
        raw["nodes"].append({"id": empty_id, "labels": ["FileObjectComparison"], "properties": {}})
        raw["edges"].append({"from": fole_id, "to": empty_id, "labels": ["HAS_COMPARISON"]})
        rel = DatasetRelationship.model_validate(raw)  # the graph itself is valid

    repo = AsyncMock()
    repo.find_id_for_dataset_pair.return_value = None
    dataset_svc = AsyncMock()
    dataset_svc.list.return_value = _found_datasets(ds_a, ds_b, files={file_a: ds_a, file_a2: ds_a})

    with pytest.raises(ValidationError, match="compare files from both linked datasets") as exc:
        await DatasetRelationshipService(repo, dataset_svc).create(rel)
    if without_files:
        assert empty_id in str(exc.value)
    repo.create.assert_not_called()


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
