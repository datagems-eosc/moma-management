from typing import List, Optional, Protocol, Tuple, runtime_checkable

from moma_management.domain.analytical_pattern import AnalyticalPattern


@runtime_checkable
class AnalyticalPatternRepository(Protocol):
    """Facade to decouple AnalyticalPattern graph operations from physical storage."""

    def create(self, ap: AnalyticalPattern, embedding: Optional[List[float]] = None) -> None:
        """Store the full AnalyticalPattern subgraph (nodes + edges).

        When *embedding* is provided the vector is stored on the root node
        as ``description_embedding`` for later similarity search.
        """
        ...

    def get(self, ap_id: str) -> Optional[AnalyticalPattern]:
        """
        Retrieve an AnalyticalPattern by its root node ID.

        Returns the root node, its Operators, and the first-level Data/User
        nodes reachable from the operators (shallow retrieval — does NOT
        recurse into the full dataset subgraph).

        Returns ``None`` if not found.
        """
        ...

    def list(self, accessible_dataset_ids: Optional[List[str]] = None) -> List[AnalyticalPattern]:
        """Return all AnalyticalPattern subgraphs (shallow retrieval).

        When *accessible_dataset_ids* is provided, only APs whose input data
        nodes belong to one of those datasets are returned.
        """
        ...

    def get_ids_by_task_id(self, task_id: str) -> List[str]:
        """
        Return the IDs of all AnalyticalPattern nodes accomplished by the
        given Task.
        """
        ...

    def search(
        self,
        query_vector: List[float],
        top_k: int = 10,
        accessible_dataset_ids: Optional[List[str]] = None,
    ) -> List[Tuple[AnalyticalPattern, float]]:
        """Return APs ranked by cosine similarity to *query_vector*."""
        ...
