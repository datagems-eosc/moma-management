import logging
from typing import Optional

from moma_management.domain.exceptions import NotFoundError
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.repository.node.node_repository import NodeRepository

logger = logging.getLogger(__name__)


class NodeService:

    _repo: NodeRepository

    def __init__(self, repo: NodeRepository):
        self._repo = repo

    async def create(self, node: Node) -> str:
        """Store a single node. Returns 'success' or an error string."""
        return await self._repo.create(node)

    async def get(self, node_id: str) -> Node:
        """
        Retrieve a single node by its ID.

        Raises:
            NotFoundError: if no node with *node_id* exists.
        """
        result = await self._repo.get(node_id)
        if result is None:
            raise NotFoundError(f"Node '{node_id}' not found.")
        return result

    async def update(self, node: Node) -> dict:
        """
        Update properties of an existing node.

        Raises:
            NotFoundError: if no node with *node.id* exists.
        """
        result = await self._repo.update(node)
        if result.get("updated", 0) == 0:
            raise NotFoundError(f"Node '{node.id}' not found.")
        return result

    async def delete(self, node_id: str) -> int:
        """Delete a node (detaching its relationships) by ID. Returns 1 on success, 0 if not found."""
        return await self._repo.delete(node_id)
