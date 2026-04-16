"""
Storage testing for Dataset repository implementations.
Basically, we need to kow if manipulating the dataset in neo4j works
"""

import copy
from datetime import date
from pathlib import Path
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from neo4j import AsyncGraphDatabase
from pydantic import ValidationError
from testcontainers.neo4j import Neo4jContainer

from moma_management.domain.dataset import Dataset
from moma_management.domain.filters import (
    DatasetFilter,
    DatasetSortField,
    MimeType,
    NodeLabel,
    SortDirection,
)
from moma_management.domain.generated.nodes.dataset.dataset_schema import Status
from moma_management.repository.dataset import Neo4jDatasetRepository
from moma_management.repository.neo4j_pgson_mixin import _DATE_PROPS, _to_iso_date
from tests.utils import (
    BLUE_FILE_NODE_ID,
    DS_ALPHA_FILE_ID,
    DS_ALPHA_ID,
    DS_BETA_FILE_ID,
    DS_BETA_ID,
    DS_CSV_ONLY_ID,
    DS_DATE_A_ID,
    DS_DATE_B_ID,
    DS_DATE_C_ID,
    DS_FORBIDDEN_TEST_ID,
    DS_GAMMA_ID,
    DS_MIXED_ID,
    DS_PDF_ONLY_ID,
    ORANGE_NODE_BASE,
)

# ---------------------------------------------------------------------------
# Local fixture override
# ---------------------------------------------------------------------------
# test_round_trip, test_deletion, and test_prevent_ap_or_ml_traversal all
# create and/or delete nodes. They need isolated containers so that:
#  1. Deletions in one test don't affect another test's data.
#  2. Many parameterised variants don't all start containers simultaneously
#     (which causes Docker timeout failures when running with xdist).
# The conftest module-scoped dataset_repository is intentionally overridden
# here with a function-scoped one. test_dataset_service.py still uses the
# module-scoped conftest fixture unchanged.


@pytest_asyncio.fixture(scope="function")
async def dataset_repository(neo4j_container: Neo4jContainer) -> AsyncGenerator[Neo4jDatasetRepository, None]:
    """Function-scoped repository providing full test isolation for mutation tests."""
    uri = neo4j_container.get_connection_url()
    auth = (neo4j_container.username, neo4j_container.password)
    driver = AsyncGraphDatabase.driver(uri, auth=auth)
    async with driver.session() as session:
        yield Neo4jDatasetRepository(session)
    await driver.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _list(repo: Neo4jDatasetRepository, **kwargs) -> dict:
    """Shorthand: build a DatasetFilter from keyword overrides and call list()."""
    return await repo.list(DatasetFilter(**kwargs))


def _with_normalised_dates(dataset: Dataset) -> Dataset:
    """
    Return a copy of *dataset* with every ``datePublished`` / ``archivedAt``
    property on every node normalised to ISO-8601 (YYYY-MM-DD).

    Storage applies the same normalisation via ``_sanitize_properties``, so
    ``stored == _with_normalised_dates(original)`` is the correct round-trip
    invariant when the ingested profile may contain non-ISO date strings.
    """
    cloned = copy.deepcopy(dataset)
    for node in cloned.nodes:
        if node.properties:
            for key in _DATE_PROPS:
                if key in node.properties:
                    node.properties[key] = _to_iso_date(node.properties[key])
    return cloned

# ---------------------------------------------------------------------------
# Round-trip / deletion (existing tests, unchanged)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_round_trip(
    dataset_file: Path,
    dataset_repository: Neo4jDatasetRepository,
):
    """
    Store / Retrieve round-trip test for a single dataset JSON file.

    Dates are normalised to ISO-8601 on write, so the comparison target is
    the normalised form of the original rather than the raw file content.
    """
    original = Dataset.model_validate_json(dataset_file.read_text())
    await dataset_repository.create(original)

    stored = await dataset_repository.get(original.root_id)
    expected = _with_normalised_dates(original)

    assert len(stored.nodes) > 0 and len(stored.edges) > 0, (
        f"get() did not return a PG-JSON structure for {dataset_file.name!r}"
    )

    assert len(stored.nodes) == len(expected.nodes), (
        f"Node count mismatch for {dataset_file.name!r}:\n"
        f"  expected: {len(expected.nodes)}\n"
        f"  got     : {len(stored.nodes)}"
    )
    assert len(stored.edges) == len(expected.edges), (
        f"Edge count mismatch for {dataset_file.name!r}:\n"
        f"  expected: {len(expected.edges)}\n"
        f"  got     : {len(stored.edges)}"
    )

    assert stored == expected, (
        f"Round-trip mismatch for {dataset_file.name!r}"
    )


@pytest.mark.asyncio
async def test_deletion(
    dataset_file: Path,
    dataset_repository: Neo4jDatasetRepository,
):
    """
    Store / Retrieve round-trip test for a single dataset JSON file.
    """
    original = Dataset.model_validate_json(dataset_file.read_text())
    await dataset_repository.create(original)

    stored = await dataset_repository.get(original.root_id)

    assert len(stored.nodes) > 0 and len(stored.edges) > 0, (
        f"get() did not return a PG-JSON structure for {dataset_file.name!r}"
    )

    number_deleted = await dataset_repository.delete(original.root_id)
    assert number_deleted == 1, (
        f"Expected to delete exactly 1 dataset for {dataset_file.name!r}, "
        f"but delete() reported {number_deleted} deleted"
    )

    deleted = await dataset_repository.get(original.root_id)
    assert deleted is None, (
        f"Dataset with id {original.root_id!r} was not deleted properly, "
        f"expected None but got {deleted!r}"
    )


# NOTE: The listing test are autogenerated and are subject to change as the listing functionality evolves.
# ---------------------------------------------------------------------------
# list() – all filter/sort/pagination tests share ONE Neo4j container via
# the class-scoped populated_repository fixture.
#
# Seed data (from conftest.populated_repository):
#   ds-alpha  datePublished=2024-01-15  status=published  cr__FileObject + CSV
#   ds-beta   datePublished=2024-06-01  status=draft      cr__FileObject + CSV
#   ds-gamma  datePublished=2025-03-01  status=published  cr__FileObject (no CSV)
# ---------------------------------------------------------------------------


class TestListMethod:
    # -- no filter -----------------------------------------------------------

    @pytest.mark.asyncio
    async def test_returns_all_datasets(self, populated_repository):
        result = await _list(populated_repository)
        assert "datasets" in result
        assert result["total"] == 3
        assert len(result["datasets"]) == 3

    @pytest.mark.asyncio
    async def test_response_shape(self, populated_repository):
        result = await _list(populated_repository)
        for ds in result["datasets"]:
            assert hasattr(ds, "nodes")
            assert hasattr(ds, "edges")
            assert any("sc:Dataset" in n.labels for n in ds.nodes)

    @pytest.mark.asyncio
    async def test_includes_connected_nodes(self, populated_repository):
        result = await _list(populated_repository)
        for ds in result["datasets"]:
            assert len(ds.nodes) > 1, "Expected root + connected file node"

    @pytest.mark.asyncio
    async def test_includes_edges(self, populated_repository):
        result = await _list(populated_repository)
        for ds in result["datasets"]:
            assert ds.edges and len(ds.edges) > 0

    # -- filter by nodeId ----------------------------------------------------

    @pytest.mark.asyncio
    async def test_single_id(self, populated_repository):
        result = await _list(populated_repository, nodeIds=[DS_ALPHA_ID])
        assert result["total"] == 1
        ids = [
            str(n.id) for ds in result["datasets"]
            for n in ds.nodes if "sc:Dataset" in n.labels
        ]
        assert ids == [DS_ALPHA_ID]

    @pytest.mark.asyncio
    async def test_multiple_ids(self, populated_repository):
        result = await _list(populated_repository, nodeIds=[
            DS_ALPHA_ID, DS_GAMMA_ID])
        assert result["total"] == 2

    @pytest.mark.asyncio
    async def test_unknown_id_returns_empty(self, populated_repository):
        result = await _list(populated_repository, nodeIds=["no-such-id"])
        assert result["total"] == 0
        assert result["datasets"] == []

    @pytest.mark.asyncio
    async def test_filter_by_nodeid_excludes_other_datasets(self, populated_repository):
        """Verify that filtering by nodeId returns only the correct dataset and excludes others."""
        result = await _list(populated_repository, nodeIds=[DS_BETA_ID])

        # Verify only one dataset is returned
        assert result["total"] == 1
        assert len(result["datasets"]) == 1

        # Extract all dataset node IDs from the result
        returned_ids = {
            str(n.id) for ds in result["datasets"]
            for n in ds.nodes if "sc:Dataset" in n.labels
        }

        # Verify that only ds-beta is returned
        assert returned_ids == {DS_BETA_ID}

        # Explicitly verify that other datasets are NOT in the results
        assert DS_ALPHA_ID not in returned_ids
        assert DS_GAMMA_ID not in returned_ids

    @pytest.mark.asyncio
    async def test_filter_by_subgraph_child_node_id(self, populated_repository):
        """Filtering by a child node ID (not the root sc:Dataset) must return the parent dataset.

        Each seeded dataset has a connected file node whose ID follows the pattern
        ``<dataset-id>-file``.  Passing that child ID via nodeIds must resolve the
        owning dataset, confirming that the filter inspects ALL nodes in the
        subgraph and not only the dataset root node.
        """
        # DS_ALPHA_FILE_ID is the cr:FileObject connected to DS_ALPHA_ID
        result = await _list(populated_repository, nodeIds=[DS_ALPHA_FILE_ID])

        assert result["total"] == 1, (
            "Expected the parent dataset to be returned when filtering by a child node ID"
        )
        returned_dataset_ids = {
            str(n.id) for ds in result["datasets"]
            for n in ds.nodes if "sc:Dataset" in n.labels
        }
        assert returned_dataset_ids == {DS_ALPHA_ID}

    # -- filter by status ----------------------------------------------------

    @pytest.mark.asyncio
    async def test_published_status(self, populated_repository):
        result = await _list(populated_repository, status=Status.published)
        assert result["total"] == 2
        for ds in result["datasets"]:
            root = next(n for n in ds.nodes if "sc:Dataset" in n.labels)
            assert root.properties.get("status") == "published"

    @pytest.mark.asyncio
    async def test_draft_status(self, populated_repository):
        result = await _list(populated_repository, status=Status.draft)
        assert result["total"] == 1

    @pytest.mark.asyncio
    async def test_nonexistent_status_returns_empty(self, populated_repository):
        result = await _list(populated_repository, status=Status.archived)
        assert result["total"] == 0
        assert result["datasets"] == []

    # -- filter by date range ------------------------------------------------

    @pytest.mark.asyncio
    async def test_from_date_excludes_older(self, populated_repository):
        result = await _list(populated_repository,
                             publishedFrom=date(2024, 6, 1))
        assert result["total"] == 2  # ds-beta and ds-gamma

    @pytest.mark.asyncio
    async def test_to_date_excludes_newer(self, populated_repository):
        result = await _list(populated_repository,
                             publishedTo=date(2024, 12, 31))
        assert result["total"] == 2  # ds-alpha and ds-beta

    @pytest.mark.asyncio
    async def test_exact_date_range(self, populated_repository):
        result = await _list(
            populated_repository,
            publishedFrom=date(2024, 6, 1),
            publishedTo=date(2024, 6, 1),
        )
        assert result["total"] == 1

    @pytest.mark.asyncio
    async def test_range_with_no_match(self, populated_repository):
        result = await _list(
            populated_repository,
            publishedFrom=date(2020, 1, 1),
            publishedTo=date(2020, 12, 31),
        )
        assert result["total"] == 0

    @pytest.mark.asyncio
    async def test_combined_date_and_status(self, populated_repository):
        result = await _list(
            populated_repository,
            publishedFrom=date(2024, 1, 1),
            publishedTo=date(2024, 12, 31),
            status=Status.published,
        )
        assert result["total"] == 1  # only ds-alpha

    # -- filter by type / mimeType -------------------------------------------

    @pytest.mark.asyncio
    async def test_fileobject_type_filter(self, populated_repository):
        result = await _list(populated_repository, types=[NodeLabel.FILE_OBJECT])
        assert result["total"] == 3  # all datasets have a cr:FileObject node

    @pytest.mark.asyncio
    async def test_csv_label_filter(self, populated_repository):
        result = await _list(populated_repository, types=[NodeLabel.CSV])
        assert result["total"] == 2  # ds-alpha and ds-beta only

    @pytest.mark.asyncio
    async def test_no_type_filter_returns_all(self, populated_repository):
        result = await _list(populated_repository, types=[])
        assert result["total"] == 3

    @pytest.mark.asyncio
    async def test_mimetype_csv_maps_to_csv_label(self, populated_repository):
        result = await _list(populated_repository, mimeTypes=[MimeType.CSV])
        # MimeType.CSV maps to the CSV node label via MIME_TYPE_TO_NODE_LABEL
        assert result["total"] == 2

    # -- sorting -------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_order_asc_by_id(self, populated_repository):
        result = await _list(populated_repository, orderBy=[
            "id"], direction=SortDirection.ASC)
        root_ids = [
            n.id for ds in result["datasets"]
            for n in ds.nodes if "sc:Dataset" in n.labels
        ]
        assert root_ids == sorted(root_ids)

    @pytest.mark.asyncio
    async def test_order_desc_by_id(self, populated_repository):
        result = await _list(populated_repository, orderBy=[
            "id"], direction=SortDirection.DESC)
        root_ids = [
            n.id for ds in result["datasets"]
            for n in ds.nodes if "sc:Dataset" in n.labels
        ]
        assert root_ids == sorted(root_ids, reverse=True)

    # -- pagination ----------------------------------------------------------

    @pytest.mark.asyncio
    async def test_page_size_limits_results(self, populated_repository):
        result = await _list(populated_repository, pageSize=2)
        assert len(result["datasets"]) == 2
        assert result["total"] == 3
        assert result["pageSize"] == 2
        assert result["page"] == 1

    @pytest.mark.asyncio
    async def test_page_two_skips_first_page(self, populated_repository):
        result = await _list(populated_repository, page=2,
                             pageSize=2, orderBy=["id"])
        assert len(result["datasets"]) == 1
        assert result["page"] == 2

    @pytest.mark.asyncio
    async def test_page_beyond_total_returns_empty(self, populated_repository):
        result = await _list(populated_repository, page=10, pageSize=25)
        assert len(result["datasets"]) == 0

    @pytest.mark.asyncio
    async def test_paginate_all_with_page_size_one(self, populated_repository):
        page1 = await _list(populated_repository, page=1, pageSize=1, orderBy=["id"])
        page2 = await _list(populated_repository, page=2, pageSize=1, orderBy=["id"])
        page3 = await _list(populated_repository, page=3, pageSize=1, orderBy=["id"])

        assert len(page1["datasets"]) == 1
        assert len(page2["datasets"]) == 1
        assert len(page3["datasets"]) == 1

        def root_id(page):
            return next(
                n.id for ds in page["datasets"]
                for n in ds.nodes if "sc:Dataset" in n.labels
            )

        assert len({root_id(page1), root_id(page2), root_id(page3)}) == 3

    @pytest.mark.asyncio
    async def test_pagination_totals_consistent(self, populated_repository):
        page1 = await _list(populated_repository, page=1, pageSize=2, orderBy=["id"])
        page2 = await _list(populated_repository, page=2, pageSize=2, orderBy=["id"])
        assert page1["total"] == page2["total"] == 3

    # -- filter by mimeType --------------------------------------------------
    # Seed data reminder:
    #   ds-alpha  cr:FileObject + CSV  →  matches text/csv
    #   ds-beta   cr:FileObject + CSV  →  matches text/csv
    #   ds-gamma  cr:FileObject only   →  no mimeType match

    @pytest.mark.asyncio
    async def test_mime_type_csv_returns_matching_datasets(self, populated_repository):
        result = await _list(populated_repository, mimeTypes=[MimeType.CSV])
        assert result["total"] == 2
        ids = {
            str(n.id) for ds in result["datasets"]
            for n in ds.nodes if "sc:Dataset" in n.labels
        }
        assert ids == {DS_ALPHA_ID, DS_BETA_ID}

    @pytest.mark.asyncio
    async def test_mime_type_unknown_returns_empty(self, populated_repository):
        result = await _list(populated_repository, mimeTypes=[MimeType.PDF])
        assert result["total"] == 0
        assert result["datasets"] == []

    @pytest.mark.asyncio
    async def test_multiple_mime_types_returns_union(self, populated_repository):
        # MimeType.CSV matches ds-alpha and ds-beta; MimeType.PDF matches none.
        # The union should still be just the two CSV datasets.
        result = await _list(populated_repository, mimeTypes=[
            MimeType.CSV, MimeType.PDF])
        assert result["total"] == 2

    @pytest.mark.asyncio
    async def test_mime_type_no_filter_returns_all(self, populated_repository):
        # An empty mimeTypes list must not restrict results.
        result = await _list(populated_repository, mimeTypes=[])
        assert result["total"] == 3


# ---------------------------------------------------------------------------
# Date normalisation and sort-by-date
#
# All tests in this class share a single Neo4j container via
# mixed_date_repository, which seeds four datasets with datePublished values
# written in three different input formats.
#
# Expected normalised ISO-8601 storage (chronological ASC order):
#   ds-date-a  "15-01-2023" (DD-MM-YYYY)  → 2023-01-15
#   ds-date-b  "2024-01-15" (ISO)          → 2024-01-15
#   ds-date-c  "01/06/2024" (DD/MM/YYYY)  → 2024-06-01
#   ds-date-d  "2025-03-01" (ISO)          → 2025-03-01
# ---------------------------------------------------------------------------

class TestOrderByDate:

    def _root(self, ds) -> object:
        return next(n for n in ds.nodes if "sc:Dataset" in n.labels)

    def _dates(self, result: dict) -> list[str]:
        return [self._root(ds).properties["datePublished"] for ds in result["datasets"]]

    @pytest.mark.asyncio
    async def test_dates_are_normalised_to_iso(self, mixed_date_repository):
        """All datePublished values must be stored as YYYY-MM-DD regardless of input format."""
        result = await _list(mixed_date_repository)
        for ds in result["datasets"]:
            date_val = self._root(ds).properties.get("datePublished", "")
            assert len(date_val) == 10 and date_val[4] == "-" and date_val[7] == "-", (
                f"datePublished {date_val!r} is not in YYYY-MM-DD format "
                f"for dataset {self._root(ds).id!r}"
            )

    @pytest.mark.asyncio
    async def test_dd_mm_yyyy_normalised_correctly(self, mixed_date_repository):
        """DD-MM-YYYY input '15-01-2023' must be stored as '2023-01-15'."""
        result = await _list(mixed_date_repository, nodeIds=[DS_DATE_A_ID])
        stored_date = self._root(
            result["datasets"][0]).properties["datePublished"]
        assert stored_date == "2023-01-15"

    @pytest.mark.asyncio
    async def test_dd_slash_mm_yyyy_normalised_correctly(self, mixed_date_repository):
        """DD/MM/YYYY input '01/06/2024' must be stored as '2024-06-01'."""
        result = await _list(mixed_date_repository, nodeIds=[DS_DATE_C_ID])
        stored_date = self._root(
            result["datasets"][0]).properties["datePublished"]
        assert stored_date == "2024-06-01"

    @pytest.mark.asyncio
    async def test_sort_asc_by_date_returns_chronological_order(self, mixed_date_repository):
        """Sorting ASC by datePublished must return datasets oldest-first."""
        result = await _list(
            mixed_date_repository,
            orderBy=[DatasetSortField.DATE_PUBLISHED],
            direction=SortDirection.ASC,
        )
        dates = self._dates(result)
        assert dates == sorted(dates), (
            f"ASC sort by datePublished not chronological: {dates}"
        )
        assert dates[0] == "2023-01-15", f"Oldest dataset should be first, got {dates[0]}"

    @pytest.mark.asyncio
    async def test_sort_desc_by_date_returns_reverse_chronological_order(self, mixed_date_repository):
        """Sorting DESC by datePublished must return datasets newest-first."""
        result = await _list(
            mixed_date_repository,
            orderBy=[DatasetSortField.DATE_PUBLISHED],
            direction=SortDirection.DESC,
        )
        dates = self._dates(result)
        assert dates == sorted(dates, reverse=True), (
            f"DESC sort by datePublished not reverse-chronological: {dates}"
        )
        assert dates[0] == "2025-03-01", f"Newest dataset should be first, got {dates[0]}"

    @pytest.mark.asyncio
    async def test_sort_order_matches_expected_sequence(self, mixed_date_repository):
        """Full ASC sequence must be exactly [2023-01-15, 2024-01-15, 2024-06-01, 2025-03-01]."""
        result = await _list(
            mixed_date_repository,
            orderBy=[DatasetSortField.DATE_PUBLISHED],
            direction=SortDirection.ASC,
        )
        assert self._dates(result) == [
            "2023-01-15", "2024-01-15", "2024-06-01", "2025-03-01"]

    @pytest.mark.asyncio
    async def test_date_range_filter_works_with_mixed_input_formats(self, mixed_date_repository):
        """publishedFrom/publishedTo must correctly bound datasets after normalisation."""
        # Only ds-date-b (2024-01-15) and ds-date-c (2024-06-01) fall in 2024
        result = await _list(
            mixed_date_repository,
            publishedFrom=date(2024, 1, 1),
            publishedTo=date(2024, 12, 31),
        )
        assert result["total"] == 2
        ids = {str(self._root(ds).id) for ds in result["datasets"]}
        assert ids == {DS_DATE_B_ID, DS_DATE_C_ID}


# ---------------------------------------------------------------------------
# mimeType filter: full subgraph must be returned
#
# Requirement: filtering by mimeType selects datasets that CONTAIN at least
# one FileObject of the given type, but the response must include ALL connected
# nodes for those datasets — not only the nodes matching the filter type.
#
# Seed (mixed_types_repository):
#   ds-mixed    cr:FileObject+CSV  +  cr:FileObject+PDFSet
#   ds-csv-only cr:FileObject+CSV
#   ds-pdf-only cr:FileObject+PDFSet
# ---------------------------------------------------------------------------

class TestMimeTypeFullSubgraph:

    def _file_labels(self, ds) -> set[frozenset]:
        """Return the set of label-sets for every cr:FileObject node in ds."""
        return {
            frozenset(n.labels)
            for n in ds.nodes
            if "cr:FileObject" in n.labels
        }

    def _root_id(self, ds) -> str:
        return str(next(n.id for n in ds.nodes if "sc:Dataset" in n.labels))

    # -- selection correctness -----------------------------------------------

    @pytest.mark.asyncio
    async def test_csv_filter_selects_csv_and_mixed_datasets(self, mixed_types_repository):
        result = await _list(mixed_types_repository, mimeTypes=[MimeType.CSV])
        assert result["total"] == 2
        ids = {self._root_id(ds) for ds in result["datasets"]}
        assert ids == {DS_MIXED_ID, DS_CSV_ONLY_ID}

    @pytest.mark.asyncio
    async def test_pdf_filter_selects_pdf_and_mixed_datasets(self, mixed_types_repository):
        result = await _list(mixed_types_repository, mimeTypes=[MimeType.PDF])
        assert result["total"] == 2
        ids = {self._root_id(ds) for ds in result["datasets"]}
        assert ids == {DS_MIXED_ID, DS_PDF_ONLY_ID}

    # -- full subgraph correctness -------------------------------------------

    @pytest.mark.asyncio
    async def test_csv_filter_returns_all_file_nodes_of_mixed_dataset(
        self, mixed_types_repository
    ):
        """
        A dataset that has both a CSV and a PDF file must return BOTH file nodes
        when the query filters by text/csv — not only the CSV node.
        """
        result = await _list(mixed_types_repository, mimeTypes=[MimeType.CSV])
        mixed = next(
            ds for ds in result["datasets"] if self._root_id(ds) == DS_MIXED_ID
        )
        label_sets = self._file_labels(mixed)
        assert frozenset(["cr:FileObject", "CSV", "Data"]) in label_sets, (
            "CSV file node missing from ds-mixed when filtered by text/csv"
        )
        assert frozenset(["cr:FileObject", "PDFSet", "Data"]) in label_sets, (
            "PDF file node incorrectly stripped from ds-mixed when filtered by text/csv"
        )

    @pytest.mark.asyncio
    async def test_pdf_filter_returns_all_file_nodes_of_mixed_dataset(
        self, mixed_types_repository
    ):
        """
        A dataset that has both a CSV and a PDF file must return BOTH file nodes
        when the query filters by application/pdf.
        """
        result = await _list(mixed_types_repository, mimeTypes=[MimeType.PDF])
        mixed = next(
            ds for ds in result["datasets"] if self._root_id(ds) == DS_MIXED_ID
        )
        label_sets = self._file_labels(mixed)
        assert frozenset(["cr:FileObject", "PDFSet", "Data"]) in label_sets, (
            "PDF file node missing from ds-mixed when filtered by application/pdf"
        )
        assert frozenset(["cr:FileObject", "CSV", "Data"]) in label_sets, (
            "CSV file node incorrectly stripped from ds-mixed when filtered by application/pdf"
        )

    @pytest.mark.asyncio
    async def test_no_filter_returns_all_nodes_of_all_datasets(self, mixed_types_repository):
        """Baseline: unfiltered list returns full subgraphs for all datasets."""
        result = await _list(mixed_types_repository)
        assert result["total"] == 3
        mixed = next(
            ds for ds in result["datasets"] if self._root_id(ds) == DS_MIXED_ID
        )
        assert len(self._file_labels(mixed)) == 2, (
            "ds-mixed should have 2 file nodes when no filter is applied"
        )


@pytest.mark.asyncio
async def test_prevent_ap_or_ml_traversal(
    dataset_repository: Neo4jDatasetRepository,
):
    """
    The dataset repository can't interact with non-Dataset nodes
    """
    from moma_management.domain.generated.edges.edge_schema import Edge
    from moma_management.domain.generated.nodes.node_schema import Node

    ds_id = DS_FORBIDDEN_TEST_ID
    blue_id = BLUE_FILE_NODE_ID

    # One node per forbidden edge type — simulating orange/green nodes
    orange_nodes = {
        # start at 3 to avoid collision with root (1) and blue (2)
        edge_type: ORANGE_NODE_BASE.format(index=idx + 3)
        for idx, edge_type in enumerate(Neo4jDatasetRepository.FORBIDDEN_EDGES)
    }

    dataset = Dataset.model_construct(
        nodes=[
            Node(id=ds_id, labels=["sc:Dataset"],
                 properties={"status": "published"}),
            Node(id=blue_id, labels=["cr:FileObject", "Data"], properties={}),
            *[
                Node(id=node_id, labels=["Operator"], properties={})
                for node_id in orange_nodes.values()
            ],
        ],
        edges=[
            # Normal edge — must be traversed
            Edge(**{"from": ds_id, "to": blue_id, "labels": ["distribution"]}),
            # Forbidden edges — must NOT be traversed
            *[
                Edge(**{"from": ds_id, "to": node_id, "labels": [edge_type]})
                for edge_type, node_id in orange_nodes.items()
            ],
        ],
    )

    await dataset_repository.create(dataset)
    result = await dataset_repository.get(ds_id)

    assert result is not None
    returned_ids = {str(n.id) for n in result.nodes}

    # The blue node must be present
    assert blue_id in returned_ids, "Blue node reachable via normal edge must be returned"

    # No orange/green node may appear in the result
    for edge_type, node_id in orange_nodes.items():
        assert node_id not in returned_ids, (
            f"Node connected via forbidden edge '{edge_type}' must not be returned"
        )
