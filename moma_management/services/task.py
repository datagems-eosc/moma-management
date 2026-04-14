import logging
from typing import List
from uuid import uuid4

from moma_management.domain.exceptions import NotFoundError
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.repository.analytical_pattern.analytical_pattern_repository import (
    AnalyticalPatternRepository,
)
from moma_management.repository.task.task_repository import TaskRepository

logger = logging.getLogger(__name__)


class TaskService:
    """Business-logic layer for Task nodes."""

    def __init__(
        self,
        repo: TaskRepository,
        ap_repo: AnalyticalPatternRepository,
    ) -> None:
        self._repo = repo
        self._ap_repo = ap_repo

    async def create(self, name: str, description: str) -> Node:
        """
        Create a new Task node and persist it.

        Returns the created ``Node``.
        """
        task = Node(
            id=uuid4(),
            labels=["Task"],
            properties={
                "name": name,
                "description": description,
            },
        )
        return await self._repo.create(task)

    async def get_ap_ids(self, task_id: str) -> List[str]:
        """
        Return the IDs of all AnalyticalPattern nodes accomplished by the task.

        Raises:
            NotFoundError: if no Task with *task_id* exists.
        """
        task = await self._repo.get(task_id)
        if task is None:
            raise NotFoundError(f"Task '{task_id}' not found.")
        return await self._ap_repo.get_ids_by_task_id(task_id)
