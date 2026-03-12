from typing import Optional

from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.repository.node.node_repository import NodeRepository


class NodeService:

    _repo: NodeRepository

    def __init__(self, repo: NodeRepository):
        self._repo = repo

    def create(self, node: Node) -> str:
        """Store a single node. Returns 'success' or an error string."""
        return self._repo.create(node)

    def get(self, node_id: str) -> Optional[Node]:
        """Retrieve a single node by its ID, or None if not found."""
        return self._repo.get(node_id)

    def update(self, node: Node) -> dict:
        """Update properties of an existing node. Returns a status dict."""
        return self._repo.update(node)

    def delete(self, node_id: str) -> int:
        """Delete a node (detaching its relationships) by ID. Returns 1 on success, 0 if not found."""
        return self._repo.delete(node_id)
