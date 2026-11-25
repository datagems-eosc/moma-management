from typing import List, Optional, Protocol, runtime_checkable

from moma_management.domain.dataset_relationship import DatasetRelationship


@runtime_checkable
class DatasetRelationshipRepository(Protocol):
    """Facade to decouple DatasetRelationship graph operations from physical storage."""

    async def create(self, relationship: DatasetRelationship) -> None:
        """Store the full DatasetRelationship subgraph (nodes + edges)."""
        ...

    async def get(self, relationship_id: str) -> Optional[DatasetRelationship]:
        """Retrieve a DatasetRelationship by its root node ID.

        Returns the relationship subgraph (shallow retrieval): the linked
        sc:Dataset / cr:FileObject nodes as id-and-label stubs, plus the stored
        ``distribution`` edge of each linked file. Returns ``None`` if not found.
        """
        ...

    async def delete(self, relationship_id: str) -> None:
        """Delete a DatasetRelationship and its internal nodes (not the referenced datasets)."""
        ...

    async def find_id_for_dataset_pair(self, dataset_id_a: str, dataset_id_b: str) -> Optional[str]:
        """Return the root ID of an existing relationship between the two datasets, if any."""
        ...

    async def list_for_dataset(self, dataset_id: str) -> List[DatasetRelationship]:
        """Return every DatasetRelationship whose root directly targets *dataset_id*."""
        ...

    async def delete_referencing(self, dataset_id: str) -> None:
        """Delete every DatasetRelationship that targets *dataset_id* (cascade on dataset deletion)."""
        ...
