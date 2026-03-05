from datetime import date
from typing import List

from fastapi import Depends, HTTPException, Query

from moma_management.di import get_dataset_service
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
    nodeIds: List[str] = Query(default=[]),
    properties: List[DatasetProperty] = Query(default=[]),
    types: List[NodeLabel] = Query(default=[]),
    mimeTypes: List[MimeType] = Query(default=[]),
    orderBy: List[DatasetSortField] = Query(default=[]),
    direction: SortDirection = Query(default=SortDirection.ASC),
    publishedFrom: date = Query(default=None),
    publishedTo: date = Query(default=None),
    status: Status = Query(default=None),
    page: int = Query(default=1, ge=1),
    pageSize: int = Query(default=25, ge=1, le=100),
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
) -> dict:
    """
    List datasets with optional filtering, sorting, and pagination criteria.
    """

    try:
        return svc.list(filters)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}")
