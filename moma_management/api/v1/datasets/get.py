from typing import Never

from fastapi import Depends

from moma_management.di import get_dataset_service
from moma_management.domain.dataset import Dataset
from moma_management.middlewares.auth import require_permission
from moma_management.services.authorization import DatasetRole
from moma_management.services.dataset import DatasetService


async def get_dataset(
    id: str,
    svc: DatasetService = Depends(get_dataset_service),
    _auth: Never = Depends(require_permission(DatasetRole.BROWSE)),
) -> Dataset:
    """
    Retrieve the full dataset subgraph (nodes + edges) by dataset ID.

    **Required permission:** dataset grant `dg_ds-browse`, or realm role
    `dg_admin` / `dg_dataset-curator`.
    """
    return await svc.get(id)
