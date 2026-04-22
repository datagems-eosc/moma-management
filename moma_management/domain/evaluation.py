from enum import Enum
from typing import Any, TypedDict

from pydantic import RootModel, model_validator


class EvaluationDimension(str, Enum):
    system = "system"
    data = "data"
    human = "human"
    ecological = "ecological"


class Evaluation(RootModel[dict[EvaluationDimension, Any]]):
    @model_validator(mode="after")
    def at_least_one_dimension(self) -> "Evaluation":
        if not self.root:
            raise ValueError(
                "At least one evaluation dimension must be provided.")
        return self


class EvaluationRecord(TypedDict):
    execution_id: str
    ap_id: str
    evaluation: Evaluation | None
    dimensions: list[str]
    created_at: str | None
