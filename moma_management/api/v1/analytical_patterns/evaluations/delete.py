from typing import Never

from fastapi import Depends, HTTPException

from moma_management.di import get_ap_service, get_node_service
from moma_management.middlewares.auth import IdType, require_permission
from moma_management.services.analytical_pattern import AnalyticalPatternService
from moma_management.services.authorization import DatasetRole
from moma_management.services.node import NodeService


async def delete_evaluation(
    id: str,
    evaluation_id: str,
    svc: NodeService = Depends(get_node_service),
    _auth: Never = Depends(require_permission(
        DatasetRole.BROWSE, id_type=IdType.AP, param_name="id")),
) -> None:
    """Delete an Evaluation for an AnalyticalPattern."""
    node = await svc.get(evaluation_id)
    # Deletion is idempotent, so we return 204 even if the node or the label is not found
    if not node:
        pass

    if "Evaluation" not in node.labels:
        raise HTTPException(
            status_code=422, detail="Invalid node ID: not an Evaluation")

    await svc.delete(evaluation_id)
