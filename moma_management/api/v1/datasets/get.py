from fastapi import Depends, HTTPException

from moma_management.di import get_dataset_service
from moma_management.domain.dataset import Dataset
from moma_management.services.dataset import DatasetService


async def get_dataset(
    id: str,
    svc: DatasetService = Depends(get_dataset_service),
) -> Dataset:
    """
    Retrieve the full dataset subgraph (nodes + edges) by dataset ID.
    """

    try:
        result = svc.get(id)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}")

    if result is None:
        raise HTTPException(
            status_code=404, detail=f"Dataset '{id}' not found.")

    return result
