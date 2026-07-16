from typing import Never

from fastapi import Depends

from moma_management.di import get_dataset_relationship_service
from moma_management.domain.dataset_relationship import DatasetRelationship
from moma_management.middlewares.auth import (
    IdType,
    get_allowed_datasets_ids,
    require_permission,
)
from moma_management.services.authorization import DatasetRole
from moma_management.services.dataset_relationship import DatasetRelationshipService


async def list_relationships_for_dataset(
    id: str,
    svc: DatasetRelationshipService = Depends(get_dataset_relationship_service),
    accessible_ids: list[str] | None = Depends(get_allowed_datasets_ids()),
    _auth: Never = Depends(require_permission(
        DatasetRole.BROWSE, id_type=IdType.Dataset)),
) -> list[DatasetRelationship]:
    """
    List every DatasetRelationship that targets the dataset identified by ``id``.

    A relationship is only included if the caller can also **browse** the
    other dataset it links; relationships linking to a dataset the caller
    cannot see are silently omitted rather than causing the request to fail.

    **Required permission:** ``dg_ds-browse`` on the path dataset, or realm
    role ``dg_admin`` / ``dg_dataset-curator``.
    """
    return await svc.list_for_dataset(id, accessible_dataset_ids=accessible_ids)
