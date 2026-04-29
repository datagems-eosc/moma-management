from typing import Never
from uuid import UUID

from fastapi import Depends
from pydantic import BaseModel

from moma_management.di import get_ap_service
from moma_management.domain.generated.nodes.ap.evaluation_schema import (
    Type as EvaluationType,
)
from moma_management.middlewares.auth import (
    IdType,
    require_permission,
)
from moma_management.services.analytical_pattern import AnalyticalPatternService
from moma_management.services.authorization import DatasetRole


class EvaluationRequest(BaseModel):
    dimension: EvaluationType
    evaluation: str  # JSON-encoded metrics for the given dimension
    execution_id: UUID


class EvaluationResponse(BaseModel):
    # Node ID of the created Evaluation node
    id: UUID


async def create_evaluation(
    id: str, rq: EvaluationRequest,
    svc: AnalyticalPatternService = Depends(get_ap_service),
    _auth: Never = Depends(require_permission(
        DatasetRole.BROWSE, id_type=IdType.AP)),
) -> EvaluationResponse:
    """Create an immutable Evaluation snapshot for an Analytical Pattern."""
    eval_node_id = await svc.add_evaluation(ap_id=id, type=rq.dimension, eval=rq.evaluation, execution_id=rq.execution_id)
    return EvaluationResponse(id=eval_node_id)
