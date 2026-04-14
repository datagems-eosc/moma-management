import logging
from typing import List, Optional
from uuid import uuid4

from moma_management.domain.exceptions import ConflictError, NotFoundError
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.repository.ml_model.ml_model_repository import MlModelRepository

logger = logging.getLogger(__name__)


class MlModelService:
    """Business-logic layer for ML_Model nodes."""

    def __init__(self, repo: MlModelRepository) -> None:
        self._repo = repo

    async def create(self, name: str, type: str) -> Node:
        """Create a new ML_Model node and persist it."""
        node = Node(
            id=uuid4(),
            labels=["ML_Model"],
            properties={"name": name, "type": type},
        )
        return await self._repo.create(node)

    async def get(self, ml_model_id: str) -> Node:
        """Retrieve an ML_Model node by ID.

        Raises:
            NotFoundError: if no ML_Model with *ml_model_id* exists.
        """
        result = await self._repo.get(ml_model_id)
        if result is None:
            raise NotFoundError(f"ML_Model '{ml_model_id}' not found.")
        return result

    async def list(self) -> List[Node]:
        """Return all ML_Model nodes."""
        return await self._repo.list()

    async def update(self, ml_model_id: str, name: Optional[str] = None, type: Optional[str] = None) -> dict:
        """Update properties of an existing ML_Model node.

        Raises:
            NotFoundError: if no ML_Model with *ml_model_id* exists.
        """
        existing = await self._repo.get(ml_model_id)
        if existing is None:
            raise NotFoundError(f"ML_Model '{ml_model_id}' not found.")

        props = dict(existing.properties)
        if name is not None:
            props["name"] = name
        if type is not None:
            props["type"] = type

        updated_node = Node(
            id=existing.id, labels=existing.labels, properties=props)
        result = await self._repo.update(updated_node)
        if result.get("updated", 0) == 0:
            raise NotFoundError(f"ML_Model '{ml_model_id}' not found.")
        return result

    async def delete(self, ml_model_id: str) -> None:
        """Delete an ML_Model node by ID.

        Raises:
            NotFoundError: if no ML_Model with *ml_model_id* exists.
            ConflictError: if the ML_Model is referenced by an analytical pattern.
        """
        existing = await self._repo.get(ml_model_id)
        if existing is None:
            raise NotFoundError(f"ML_Model '{ml_model_id}' not found.")

        if await self._repo.has_referencing_aps(ml_model_id):
            raise ConflictError(
                f"Cannot delete ML_Model '{ml_model_id}': "
                "it is referenced by at least one analytical pattern."
            )

        await self._repo.delete(ml_model_id)
