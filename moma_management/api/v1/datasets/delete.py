from typing import Never

from fastapi import Depends

from moma_management.di import get_dataset_service, require_permission
from moma_management.services.authorization import DatasetAction
from moma_management.services.dataset import DatasetService


async def delete_dataset(
    id: str,
    svc: DatasetService = Depends(get_dataset_service),
    _auth: Never = Depends(require_permission(DatasetAction.delete)),
) -> None:
    """
    Delete a dataset and its connected subgraph by dataset ID.
    """
    svc.delete(id)
