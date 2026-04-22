from uuid import UUID

from pydantic import BaseModel

from moma_management.domain.evaluation import Evaluation, EvaluationDimension

__all__ = ["Evaluation", "EvaluationDimension"]


class EvaluationCreateRequest(BaseModel):
    execution_id: UUID | None = None
    evaluation: Evaluation


class EvaluationCreatedResponse(BaseModel):
    execution_id: str
    ap_id: str
    dimensions: list[str]


class EvaluationSummary(BaseModel):
    execution_id: str
    dimensions: list[str]


class EvaluationDetail(BaseModel):
    execution_id: str
    ap_id: str
    evaluation: Evaluation
