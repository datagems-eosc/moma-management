from typing import Never

from fastapi import Depends

from moma_management.di import get_node_service
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.middlewares.auth import IdType, require_permission
from moma_management.services.authorization import DatasetRole
from moma_management.services.node import NodeService


async def get_node(
    id: str,
    svc: NodeService = Depends(get_node_service),
    _auth: Never = Depends(require_permission(
        DatasetRole.BROWSE, id_type=IdType.Node)),
) -> Node:
    """
    Retrieve a single node by its ID.

    **Required permission:** dataset grant `dg_ds-browse` on the parent dataset, or realm
    role `GLOBAL_dg_admin` / `GLOBAL_dg_dataset-curator`.
    Returns 404 (not 403) on permission denial to prevent dataset enumeration.
    """
    return await svc.get(id)
