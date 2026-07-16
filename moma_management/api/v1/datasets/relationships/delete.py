from typing import Never

from fastapi import Depends

from moma_management.di import get_dataset_relationship_service
from moma_management.middlewares.auth import require_admin
from moma_management.services.dataset_relationship import DatasetRelationshipService


async def delete_relationship(
    id: str,
    svc: DatasetRelationshipService = Depends(get_dataset_relationship_service),
    _auth: Never = Depends(require_admin()),
) -> None:
    """
    Delete a DatasetRelationship by its root node ID.

    Only the relationship's internal nodes are removed; the two linked
    ``sc:Dataset`` nodes are left intact.

    **Required role:** ``dg_admin`` / ``dg_dataset-curator`` / system.
    """
    await svc.delete(id)
