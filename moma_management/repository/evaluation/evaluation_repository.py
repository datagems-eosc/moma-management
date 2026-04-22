from typing import Optional, Protocol, runtime_checkable

from moma_management.domain.evaluation import Evaluation, EvaluationRecord


@runtime_checkable
class EvaluationRepository(Protocol):
    """Facade to decouple Evaluation persistence from physical storage."""

    async def create(
        self,
        execution_id: str,
        ap_id: str,
        evaluation: Evaluation,
    ) -> None:
        """Store an Evaluation node linked to an Analytical Pattern."""
        ...

    async def get(self, execution_id: str) -> Optional[EvaluationRecord]:
        """Retrieve an Evaluation by its execution ID, or ``None`` if not found."""
        ...

    async def list_by_ap(
        self,
        ap_id: str,
        execution_id: str | None = None,
        dimension: str | None = None,
    ) -> list[EvaluationRecord]:
        """List Evaluations for an Analytical Pattern, optionally filtered."""
        ...

    async def delete(self, execution_id: str) -> int:
        """Delete an Evaluation by execution ID. Returns the number deleted."""
        ...
