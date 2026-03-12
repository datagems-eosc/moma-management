from typing import Optional, Protocol

from moma_management.domain.generated.nodes.node_schema import Node


class NodeRepository(Protocol):
    """
    Facade to decouple single-node graph operations from their physical storage.
    """

    def create(self, node: Node) -> str:
        """Store a single node. Returns 'success' or an error string."""
        ...

    def get(self, node_id: str) -> Optional[Node]:
        """Retrieve a single node by its ID, or None if not found."""
        ...

    def update(self, node: Node) -> dict:
        """Update properties of an existing node. Returns a status dict."""
        ...

    def delete(self, node_id: str) -> int:
        """Delete a node (and detach its relationships) by ID. Returns 1 on success, 0 if not found."""
        ...
