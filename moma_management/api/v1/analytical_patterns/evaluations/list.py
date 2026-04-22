from typing import Never

from fastapi import Depends

from moma_management.api.v1.analytical_patterns.evaluations.models import (
    EvaluationDetail,
)
from moma_management.di import get_evaluation_service
from moma_management.domain.evaluation import Evaluation, EvaluationDimension
from moma_management.middlewares.auth import require_authentication
from moma_management.services.evaluation import EvaluationService


async def list_evaluations(
    ap_id: str,
    execution_id: str | None = None,
    dimension: EvaluationDimension | None = None,
    svc: EvaluationService = Depends(get_evaluation_service),
    _auth: Never = Depends(require_authentication()),
) -> list[EvaluationDetail]:
    """List Evaluation summaries for an Analytical Pattern."""
    evaluations = await svc.list_by_ap(
        ap_id=ap_id,
        execution_id=execution_id,
        dimension=dimension.value if dimension is not None else None,
    )
    result = []
    for e in evaluations:
        eval_obj: Evaluation | None = e["evaluation"]
        if dimension is not None and eval_obj is not None:
            eval_obj = Evaluation.model_validate(
                {dimension.value: eval_obj.root.get(dimension)}
            )
        result.append(
            EvaluationDetail(
                execution_id=e["execution_id"],
                ap_id=e["ap_id"],
                evaluation=eval_obj,
            )
        )
    return result
