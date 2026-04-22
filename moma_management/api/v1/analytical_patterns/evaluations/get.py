from typing import Never

from fastapi import Depends

from moma_management.api.v1.analytical_patterns.evaluations.models import (
    EvaluationDetail,
)
from moma_management.di import get_evaluation_service
from moma_management.middlewares.auth import require_authentication
from moma_management.services.evaluation import EvaluationService


async def get_evaluation(
    execution_id: str,
    svc: EvaluationService = Depends(get_evaluation_service),
    _auth: Never = Depends(require_authentication()),
) -> EvaluationDetail:
    """Retrieve the full Evaluation snapshot by execution ID."""
    record = await svc.get(execution_id)
    return EvaluationDetail(
        execution_id=record["execution_id"],
        ap_id=record["ap_id"],
        evaluation=record["evaluation"],
    )
