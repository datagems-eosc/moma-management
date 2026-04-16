"""
Performance tests for Dataset repository get() and list() operations.

Every query must complete within MAX_SECONDS (60 s). The heavy_dataset_repository
fixture (conftest.py) loads all heavy datasets into a shared Neo4j container.
"""

import time
from pathlib import Path

import pytest

from moma_management.domain.dataset import Dataset
from moma_management.domain.filters import (
    DatasetFilter,
    DatasetSortField,
    MimeType,
    NodeLabel,
)
from moma_management.domain.generated.nodes.dataset.dataset_schema import Status

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
HEAVY_DIR = PROJECT_ROOT / "assets" / "datasets" / "heavy"

MAX_SECONDS = 60

# ---------------------------------------------------------------------------
# Pre-compute dataset metadata at collection time
# ---------------------------------------------------------------------------


def _dataset_ids() -> dict[str, str]:
    """Return {filename_stem: root_id} for every heavy dataset."""
    ids: dict[str, str] = {}
    for path in sorted(HEAVY_DIR.glob("*.json")):
        ds = Dataset.model_validate_json(path.read_text())
        ids[path.stem] = ds.root_id
    return ids


def _child_node_id(stem: str) -> str:
    """Return the id of the first non-root node in a heavy dataset."""
    path = HEAVY_DIR / f"{stem}.json"
    ds = Dataset.model_validate_json(path.read_text())
    for n in ds.nodes:
        if "sc:Dataset" not in n.labels:
            return str(n.id)
    raise ValueError(f"No child node found in {stem}")


_IDS = _dataset_ids()
_ALL_ROOT_IDS = list(_IDS.values())
_FIRST_STEM = list(_IDS.keys())[0]
_FIRST_CHILD_ID = _child_node_id(_FIRST_STEM)
_NONEXISTENT_ID = "00000000-0000-0000-0000-ffffffffffff"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _timed_get(repo, ds_id: str):
    t0 = time.monotonic()
    result = await repo.get(ds_id)
    return result, time.monotonic() - t0


async def _timed_list(repo, **kwargs):
    t0 = time.monotonic()
    result = await repo.list(DatasetFilter(**kwargs))
    return result, time.monotonic() - t0


# ---------------------------------------------------------------------------
# get() performance
# ---------------------------------------------------------------------------


class TestGetPerformance:

    @pytest.mark.asyncio
    @pytest.mark.parametrize("stem,ds_id", list(_IDS.items()), ids=list(_IDS.keys()))
    async def test_get_dataset(self, heavy_dataset_repository, stem, ds_id):
        ds, elapsed = await _timed_get(heavy_dataset_repository, ds_id)
        assert ds is not None, f"get({ds_id}) returned None"
        assert len(ds.nodes) > 0
        assert elapsed < MAX_SECONDS, f"get({stem}) took {elapsed:.2f}s"

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, heavy_dataset_repository):
        ds, elapsed = await _timed_get(heavy_dataset_repository, _NONEXISTENT_ID)
        assert ds is None
        assert elapsed < MAX_SECONDS


# ---------------------------------------------------------------------------
# list() performance – parametrized filter combinations
# ---------------------------------------------------------------------------

# Each entry: (test_id, filter_kwargs, optional_assertions)
# optional_assertions is a callable(result) or None.
_LIST_CASES = [
    # -- baseline --
    ("no_filter", {}, lambda r: r["total"] == len(_IDS)),

    # -- nodeIds --
    ("single_root_id", {"nodeIds": [
     _ALL_ROOT_IDS[0]]}, lambda r: r["total"] == 1),
    ("all_root_ids", {"nodeIds": _ALL_ROOT_IDS},
     lambda r: r["total"] == len(_IDS)),
    ("unknown_node_id", {"nodeIds": [
     _NONEXISTENT_ID]}, lambda r: r["total"] == 0),
    ("mixed_root_and_child", {
        "nodeIds": [_FIRST_CHILD_ID, _ALL_ROOT_IDS[-1]],
    }, lambda r: r["total"] >= 1),

    # -- pagination --
    ("page_size_1", {"pageSize": 1}, lambda r: len(r["datasets"]) == 1),
    ("page_2", {"pageSize": 1, "page": 2}, lambda r: len(r["datasets"]) == 1),
    ("page_beyond_total", {"page": 999}, lambda r: len(r["datasets"]) == 0),

    # -- sorting (desc, single field as smoke check) --
    ("order_desc_by_id", {
        "orderBy": [DatasetSortField.ID], "direction": "desc",
    }, lambda r: r["total"] == len(_IDS)),

    # -- combined filters --
    ("nodeid_plus_status", {
        "nodeIds": [_ALL_ROOT_IDS[0]], "status": Status.published,
    }, None),
    ("nodeid_plus_type", {
        "nodeIds": [_FIRST_CHILD_ID], "types": [NodeLabel.FILE_OBJECT],
    }, None),
    ("nodeid_sort_pagination", {
        "nodeIds": _ALL_ROOT_IDS,
        "orderBy": [DatasetSortField.ID], "direction": "desc",
        "pageSize": 2, "page": 1,
    }, lambda r: r["total"] == len(_IDS) and len(r["datasets"]) <= 2),
    ("all_filters", {
        "nodeIds": [_FIRST_CHILD_ID],
        "types": [NodeLabel.FILE_OBJECT], "mimeTypes": [MimeType.CSV],
        "status": Status.published,
        "orderBy": [DatasetSortField.DATE_PUBLISHED], "direction": "desc",
        "pageSize": 5, "page": 1,
    }, None),
]


class TestListPerformance:

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "filter_kwargs,extra_assert",
        [(kw, ea) for _, kw, ea in _LIST_CASES],
        ids=[tid for tid, _, _ in _LIST_CASES],
    )
    async def test_list_filter(self, heavy_dataset_repository, filter_kwargs, extra_assert):
        result, elapsed = await _timed_list(heavy_dataset_repository, **filter_kwargs)
        assert elapsed < MAX_SECONDS, (
            f"list({filter_kwargs}) took {elapsed:.2f}s (limit {MAX_SECONDS}s)"
        )
        if extra_assert is not None:
            assert extra_assert(
                result), f"Assertion failed for {filter_kwargs}"

    # -- nodeIds: child node per dataset (VIRTUAL_BELONGS_TO) ----------------

    @pytest.mark.asyncio
    @pytest.mark.parametrize("stem", list(_IDS.keys()))
    async def test_list_by_child_node_id(self, heavy_dataset_repository, stem):
        child_id = _child_node_id(stem)
        result, elapsed = await _timed_list(
            heavy_dataset_repository, nodeIds=[child_id]
        )
        assert result["total"] >= 1, f"Expected >=1 dataset for child of {stem}"
        assert elapsed < MAX_SECONDS, (
            f"list(child of {stem}) took {elapsed:.2f}s"
        )

    # -- sort by every field -------------------------------------------------

    @pytest.mark.asyncio
    @pytest.mark.parametrize("field", list(DatasetSortField), ids=lambda f: f.value)
    async def test_list_order_by(self, heavy_dataset_repository, field):
        result, elapsed = await _timed_list(
            heavy_dataset_repository, orderBy=[field], direction="asc"
        )
        assert result["total"] == len(_IDS)
        assert elapsed < MAX_SECONDS, f"list(orderBy={field.value}) took {elapsed:.2f}s"

    # -- type filter ---------------------------------------------------------
    @pytest.mark.asyncio
    @pytest.mark.parametrize("label", [NodeLabel.FILE_OBJECT, NodeLabel.CSV, NodeLabel.COLUMN])
    async def test_list_by_type(self, heavy_dataset_repository, label):
        result, elapsed = await _timed_list(
            heavy_dataset_repository, types=[label]
        )
        assert elapsed < MAX_SECONDS, f"list(types=[{label.value}]) took {elapsed:.2f}s"

    # -- mime type filter ----------------------------------------------------

    @pytest.mark.asyncio
    @pytest.mark.parametrize("mime", [MimeType.CSV, MimeType.PDF, MimeType.EXCEL])
    async def test_list_by_mime_type(self, heavy_dataset_repository, mime):
        result, elapsed = await _timed_list(
            heavy_dataset_repository, mimeTypes=[mime]
        )
        assert elapsed < MAX_SECONDS, f"list(mimeTypes=[{mime.value}]) took {elapsed:.2f}s"

    # -- status filter -------------------------------------------------------

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", list(Status), ids=lambda s: s.value)
    async def test_list_by_status(self, heavy_dataset_repository, status):
        result, elapsed = await _timed_list(
            heavy_dataset_repository, status=status
        )
        assert elapsed < MAX_SECONDS, f"list(status={status.value}) took {elapsed:.2f}s"
