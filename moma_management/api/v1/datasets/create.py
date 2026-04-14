from typing import Any, Dict, Never

from fastapi import Depends

from moma_management.di import get_dataset_service, require_permission
from moma_management.domain.dataset import Dataset
from moma_management.services.authorization import DatasetRole
from moma_management.services.dataset import DatasetService


async def create_dataset(
    candidate: Dataset,
    svc: DatasetService = Depends(get_dataset_service),
    _auth: Never = Depends(require_permission(DatasetRole.CREATE)),
) -> None:
    """
    Create a new dataset in the MoMa graph repository.

    **Required permission:** realm role `dg_admin` or `dg_dataset-uploader`.
    """
    return await svc.create(candidate)
