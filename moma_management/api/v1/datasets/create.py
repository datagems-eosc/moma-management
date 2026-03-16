from typing import Any, Dict, Never

from fastapi import Depends

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
    Create a new dataset.
    """
    return svc.create(candidate)
