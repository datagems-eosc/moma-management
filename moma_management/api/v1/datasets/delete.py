from fastapi import Depends, HTTPException

from moma_management.di import get_dataset_service
from moma_management.services.dataset import DatasetService


async def delete_dataset(
    id: str,
    svc: DatasetService = Depends(get_dataset_service),
) -> None:
    """
    Delete a dataset and its connected subgraph by dataset ID.
    """

    try:
        svc.delete(id)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}")
