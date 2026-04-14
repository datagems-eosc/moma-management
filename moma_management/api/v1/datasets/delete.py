from typing import Never

from fastapi import Depends

from moma_management.di import get_dataset_service, require_permission
from moma_management.services.authorization import DatasetRole
from moma_management.services.dataset import DatasetService


async def delete_dataset(
    id: str,
    svc: DatasetService = Depends(get_dataset_service),
    _auth: Never = Depends(require_permission(DatasetRole.DELETE)),
) -> None:
    """
    Delete a dataset and its connected subgraph by dataset ID.

    **Required permission:** dataset grant `dg_ds-delete`, or realm role
    `dg_admin` / `dg_dataset-curator`.
    """
    await svc.delete(id)
