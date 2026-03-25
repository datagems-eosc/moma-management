from typing import Never

from fastapi import Depends

from moma_management.di import IdType, get_node_service, require_permission
from moma_management.domain.generated.nodes.node_schema import Node
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
    """
    return svc.get(id)
