from typing import List, Optional, Protocol, runtime_checkable

from moma_management.domain.generated.nodes.node_schema import Node


@runtime_checkable
class MlModelRepository(Protocol):
    """Facade to decouple ML_Model node operations from physical storage."""

    def create(self, node: Node) -> Node:
        """Store an ML_Model node. Returns the created node."""
        ...

    def get(self, ml_model_id: str) -> Optional[Node]:
        """Retrieve an ML_Model node by ID, or ``None`` if not found."""
        ...

    def update(self, node: Node) -> dict:
        """Update properties of an existing ML_Model node. Returns a status dict."""
        ...

    def delete(self, ml_model_id: str) -> int:
        """Delete an ML_Model node by ID. Returns 1 on success, 0 if not found."""
        ...

    def list(self) -> List[Node]:
        """Return all ML_Model nodes."""
        ...

    def has_referencing_aps(self, ml_model_id: str) -> bool:
        """Return True if at least one AP has an Operator that uses this ML_Model."""
        ...
