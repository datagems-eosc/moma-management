from typing import Any, Dict, Never

from fastapi import Depends

from moma_management.di import get_node_service, require_permission
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.services.authorization import DatasetRole
from moma_management.services.node import NodeService


async def update_node(
    id: str,
    properties: Dict[str, Any],
    svc: NodeService = Depends(get_node_service),
    _auth: Never = Depends(require_permission(DatasetRole.EDIT)),
) -> dict:
    """
    Merge the supplied properties onto the existing node identified by *id*.
    Only the provided keys are updated; all other properties are left unchanged.
    Returns 404 if the node does not exist.
    """
    return svc.update(Node(id=id, labels=[], properties=properties))
