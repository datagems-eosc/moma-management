from typing import Never

from fastapi import Depends

from moma_management.di import get_evaluation_service
from moma_management.middlewares.auth import require_authentication
from moma_management.services.evaluation import EvaluationService


async def delete_evaluation(
    execution_id: str,
    svc: EvaluationService = Depends(get_evaluation_service),
    _auth: Never = Depends(require_authentication()),
) -> None:
    """Delete an Evaluation by execution ID."""
    await svc.delete(execution_id)
