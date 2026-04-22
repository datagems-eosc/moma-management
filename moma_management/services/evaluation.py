import logging
from uuid import uuid4

from moma_management.domain.evaluation import Evaluation, EvaluationRecord
from moma_management.domain.exceptions import NotFoundError
from moma_management.repository.evaluation.evaluation_repository import (
    EvaluationRepository,
)
from moma_management.services.analytical_pattern import AnalyticalPatternService

logger = logging.getLogger(__name__)


class EvaluationService:
    """Business-logic layer for Evaluation nodes."""

    def __init__(
        self,
        repo: EvaluationRepository,
        ap_service: AnalyticalPatternService,
    ) -> None:
        self._repo = repo
        self._ap_service = ap_service

    async def create(
        self,
        ap_id: str,
        evaluation: Evaluation,
        execution_id: str | None = None,
    ) -> EvaluationRecord:
        """Persist an immutable Evaluation snapshot for an AP execution."""
        await self._ap_service.get(ap_id)

        resolved_execution_id = execution_id or str(uuid4())
        await self._repo.create(
            execution_id=resolved_execution_id,
            ap_id=ap_id,
            evaluation=evaluation,
        )
        return EvaluationRecord(
            execution_id=resolved_execution_id,
            ap_id=ap_id,
            evaluation=evaluation,
            dimensions=[dim.value for dim in evaluation.root.keys()],
            created_at=None,
        )

    async def get(self, execution_id: str) -> EvaluationRecord:
        """Retrieve an Evaluation by execution ID."""
        evaluation = await self._repo.get(execution_id)
        if evaluation is None:
            raise NotFoundError(f"Evaluation '{execution_id}' not found.")
        return evaluation

    async def list_by_ap(
        self,
        ap_id: str,
        execution_id: str | None = None,
        dimension: str | None = None,
    ) -> list[EvaluationRecord]:
        """List Evaluations for an AP, optionally filtered."""
        await self._ap_service.get(ap_id)
        return await self._repo.list_by_ap(
            ap_id,
            execution_id=execution_id,
            dimension=dimension,
        )

    async def delete(self, execution_id: str) -> None:
        """Delete an Evaluation by execution ID."""
        deleted = await self._repo.delete(execution_id)
        if deleted == 0:
            raise NotFoundError(f"Evaluation '{execution_id}' not found.")
