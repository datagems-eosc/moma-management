from datetime import date
from typing import List

from fastapi import Depends, Query

from moma_management.di import get_allowed_datasets_ids, get_dataset_service
from moma_management.domain.filters import (
    DatasetFilter,
    DatasetProperty,
    DatasetSortField,
    MimeType,
    NodeLabel,
    SortDirection,
)
from moma_management.domain.generated.nodes.dataset_schema import Status
from moma_management.services.dataset import DatasetService


def _dataset_filters(
    nodeIds: List[str] = Query(
        default=[], description="Filter results to only datasets whose subgraph contains nodes with these IDs."),
    properties: List[DatasetProperty] = Query(
        default=[], description="Dataset root-node properties to include in each result item. Returns all properties if empty."),
    types: List[NodeLabel] = Query(
        default=[], description="Filter datasets by the label types of their connected file nodes."),
    mimeTypes: List[MimeType] = Query(
        default=[], description="Filter datasets by the MIME types of their file objects."),
    orderBy: List[DatasetSortField] = Query(
        default=[], description="One or more dataset properties to sort results by. Applied left-to-right."),
    direction: SortDirection = Query(
        default=SortDirection.ASC, description="Sort direction applied to all `orderBy` fields."),
    publishedFrom: date = Query(
        default=None, description="Inclusive lower bound on `datePublished` (ISO 8601 date, e.g. `2024-01-01`)."),
    publishedTo: date = Query(
        default=None, description="Inclusive upper bound on `datePublished` (ISO 8601 date, e.g. `2024-12-31`)."),
    status: Status = Query(
        default=None, description="Filter datasets by publication status."),
    page: int = Query(default=1, ge=1, description="Page number (1-indexed)."),
    pageSize: int = Query(default=25, ge=1, le=100,
                          description="Number of results per page (1–100)."),
) -> DatasetFilter:
    return DatasetFilter(
        nodeIds=nodeIds,
        properties=properties,
        types=types,
        mimeTypes=mimeTypes,
        orderBy=orderBy,
        direction=direction,
        publishedFrom=publishedFrom,
        publishedTo=publishedTo,
        status=status,
        page=page,
        pageSize=pageSize,
    )


async def list_datasets(
    filters: DatasetFilter = Depends(_dataset_filters),
    svc: DatasetService = Depends(get_dataset_service),
    accessible_ids: list[str] | None = Depends(get_allowed_datasets_ids()),
) -> dict:
    """
    List datasets with optional filtering, sorting, and pagination criteria.

    Only datasets the authenticated user holds the `dg_ds-browse` grant for are returned.
    Realm roles `dg_admin` and `dg_dataset-curator` bypass individual dataset
    grants and allow browsing all datasets. When authentication is disabled, all datasets
    are returned regardless of grants.
    """
    if accessible_ids is not None:
        # Intersect caller-visible IDs with any existing nodeIds filter.
        if filters.nodeIds:
            merged = [id_ for id_ in filters.nodeIds if id_ in accessible_ids]
        else:
            merged = accessible_ids
        filters = filters.model_copy(update={"nodeIds": merged})

        # An empty merged list means zero accessible datasets
        if not filters.nodeIds:
            return {"datasets": [], "page": filters.page, "pageSize": filters.pageSize, "total": 0}

    return svc.list(filters)
