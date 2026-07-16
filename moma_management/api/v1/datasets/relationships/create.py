from typing import Never

from fastapi import Depends

from moma_management.di import get_dataset_relationship_service
from moma_management.domain.dataset_relationship import DatasetRelationship
from moma_management.middlewares.auth import require_admin
from moma_management.services.dataset_relationship import DatasetRelationshipService


async def create_relationship(
    candidate: DatasetRelationship,
    svc: DatasetRelationshipService = Depends(get_dataset_relationship_service),
    _auth: Never = Depends(require_admin()),
) -> dict:
    """
    Create a new DatasetRelationship ("dataset linking" subgraph) in the
    MoMa graph repository.

    The relationship's root ``BasicDLElement`` must directly reference,
    via ``HAS_TARGET`` edges, exactly two existing ``sc:Dataset`` nodes.
    At most one relationship may exist for a given dataset pair.

    **Required role:** ``dg_admin`` / ``dg_dataset-curator`` / system.
    """
    relationship_id = await svc.create(candidate)
    return {"id": relationship_id}
