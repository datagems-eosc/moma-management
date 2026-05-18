from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Self

if TYPE_CHECKING:
    from moma_management.domain.pg_json_graph import MomaEntity

from ..schema_error import SchemaError


class ValidationStep(ABC):
    """Abstract base for a single step in the graph validation chain."""

    def __init__(self) -> None:
        self._next: Self | None = None

    def set_next(self, step: Self) -> Self:
        self._next = step
        return step

    def __and__(self, other: Self) -> Self:
        if not isinstance(other, ValidationStep):
            raise TypeError(f"Cannot chain {type(other)}")
        # Traverse to the current tail and attach, then return self (the head)
        tail = self
        while tail._next is not None:
            tail = tail._next
        tail._next = other
        return self

    @abstractmethod
    def handle(self, data: MomaEntity) -> List[SchemaError]: ...

    def _chain(self, data: MomaEntity) -> List[SchemaError]:
        return self._next.handle(data) if self._next is not None else []
