from typing import Never

from fastapi import Depends

from moma_management.api.v1.analytical_patterns.evaluations.models import (
    EvaluationCreatedResponse,
    EvaluationCreateRequest,
)
from moma_management.di import get_evaluation_service
from moma_management.middlewares.auth import require_authentication
from moma_management.services.evaluation import EvaluationService


async def create_evaluation(
    ap_id: str,
    body: EvaluationCreateRequest,
    svc: EvaluationService = Depends(get_evaluation_service),
    _auth: Never = Depends(require_authentication()),
) -> EvaluationCreatedResponse:
    """Create an immutable Evaluation snapshot for an Analytical Pattern."""
    created = await svc.create(
        ap_id=ap_id,
        execution_id=str(
            body.execution_id) if body.execution_id is not None else None,
        evaluation=body.evaluation,
    )
    return EvaluationCreatedResponse(
        execution_id=created["execution_id"],
        ap_id=created["ap_id"],
        dimensions=created["dimensions"],
    )
