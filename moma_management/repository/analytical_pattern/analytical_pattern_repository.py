from typing import List, Optional, Protocol, runtime_checkable

from moma_management.domain.analytical_pattern import AnalyticalPattern


@runtime_checkable
class AnalyticalPatternRepository(Protocol):
    """Facade to decouple AnalyticalPattern graph operations from physical storage."""

    def create(self, ap: AnalyticalPattern) -> None:
        """Store the full AnalyticalPattern subgraph (nodes + edges)."""
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

    def list(self) -> List[AnalyticalPattern]:
        """Return all AnalyticalPattern subgraphs (shallow retrieval)."""
        ...

    def get_ids_by_task_id(self, task_id: str) -> List[str]:
        """
        Return the IDs of all AnalyticalPattern nodes accomplished by the
        given Task.
        """
        ...
