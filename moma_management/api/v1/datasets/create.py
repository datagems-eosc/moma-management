from typing import Any, Dict, Never

from fastapi import Depends, HTTPException

from moma_management.di import get_dataset_service, require_permission
from moma_management.domain.dataset import Dataset
from moma_management.services.authorization import DatasetAction
from moma_management.services.dataset import DatasetService


async def create_dataset(
    candidate: Dataset,
    svc: DatasetService = Depends(get_dataset_service),
    _auth: Never = Depends(require_permission(DatasetAction.manage)),
) -> None:
    """
        Create a new dataset
    """

    try:
        return svc.create(candidate)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}")
