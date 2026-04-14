from typing import Any, List, Optional, Protocol, runtime_checkable


@runtime_checkable
class Repository[T, F= Any](Protocol):
    """
    Facade to decouple single-node graph operations from their physical storage.
    """

    async def create(self, item: T) -> str:
        """Store a single node. Returns 'success' or an error string."""
        ...

    async def get(self, id: str) -> Optional[T]:
        """Retrieve a single node by its ID, or None if not found."""
        ...

    async def update(self, item: T) -> dict:
        """Update properties of an existing node. Returns a status dict."""
        ...

    async def delete(self, id: str) -> int:
        """Delete a node (and detach its relationships) by ID. Returns 1 on success, 0 if not found."""
        ...

    async def list(self, criteria: F) -> List[T]:
        """List with optional filters and pagination."""
        ...
