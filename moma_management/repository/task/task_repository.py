from typing import Optional, Protocol, runtime_checkable

from moma_management.domain.generated.nodes.node_schema import Node


@runtime_checkable
class TaskRepository(Protocol):
    """Facade to decouple Task node operations from physical storage."""

    async def create(self, task: Node) -> Node:
        """Store a Task node. Returns the created node."""
        ...

    async def get(self, task_id: str) -> Optional[Node]:
        """Retrieve a Task node by ID, or ``None`` if not found."""
        ...
