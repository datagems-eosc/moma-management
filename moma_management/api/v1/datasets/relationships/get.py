from typing import Never

from fastapi import Depends

from moma_management.di import get_dataset_relationship_service
from moma_management.domain.dataset_relationship import DatasetRelationship
from moma_management.middlewares.auth import IdType, require_permission
from moma_management.services.authorization import DatasetRole
from moma_management.services.dataset_relationship import DatasetRelationshipService


async def get_relationship(
    id: str,
    svc: DatasetRelationshipService = Depends(get_dataset_relationship_service),
    _auth: Never = Depends(require_permission(
        DatasetRole.BROWSE, id_type=IdType.Relationship, require_all=True)),
) -> DatasetRelationship:
    """
    Retrieve a DatasetRelationship (shallow) by its root node ID.

    Only the root ``BasicDLElement`` node and its internal
    ``PropertyComparison``/``TextEvidence`` subgraph are returned; the
    referenced ``sc:Dataset`` nodes are not recursed into.

    **Required permission:** ``dg_ds-browse`` on **both** datasets linked by
    the relationship, or realm role ``dg_admin`` / ``dg_dataset-curator``.
    """
    return await svc.get(id)
