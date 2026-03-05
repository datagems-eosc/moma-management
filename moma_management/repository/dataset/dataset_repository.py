from typing import List, Optional, Protocol

from moma_management.domain.dataset import Dataset
from moma_management.domain.filters import DatasetFilter


class DatasetRepository(Protocol):
    """
    Facade to decouple Dataset graph operations from their physical storage.
    """

    def create(self, pg_json: Dataset) -> str:
        """Store a full PG-JSON graph (nodes + edges)."""
        ...

    def create_nodes(self, pg_json: Dataset) -> str:
        """Store only the nodes of a PG-JSON graph."""
        ...

    def create_edges(self, pg_json: Dataset) -> str:
        """Store only the edges of a PG-JSON graph."""
        ...

    def delete(self, id: str) -> int:
        """Delete datasets and their connected subgraph by ID."""
        ...

    def get(self, dataset_id: str) -> Optional[Dataset]:
        """Retrieve the full dataset subgraph (nodes + edges) by dataset ID."""
        ...

    def list(self, criteria: DatasetFilter) -> List[Dataset]:
        """List datasets with optional filters and pagination."""
        ...

    def update(self, pg_json: Dataset) -> dict:
        """Update properties of existing nodes."""
        ...
