from typing import List, Optional, Protocol, runtime_checkable

from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.domain.filters import AnalyticalPatternFilter


@runtime_checkable
class AnalyticalPatternRepository(Protocol):
    """Facade to decouple AnalyticalPattern graph operations from physical storage."""

    async def create(self, ap: AnalyticalPattern, embedding: Optional[List[float]] = None) -> None:
        """Store the full AnalyticalPattern subgraph (nodes + edges).

        When *embedding* is provided the vector is stored on the root node
        as ``description_embedding`` for later similarity search.
        """
        ...

    async def get(self, ap_id: str, include_evaluations: bool = False) -> Optional[AnalyticalPattern]:
        """
        Retrieve an AnalyticalPattern by its root node ID.

        Returns the root node, its Operators, and the first-level Data/User
        nodes reachable from the operators (shallow retrieval — does NOT
        recurse into the full dataset subgraph).

        When *include_evaluations* is ``True``, Evaluation nodes linked via
        ``is_measured_by`` are included in the subgraph.

        Returns ``None`` if not found.
        """
        ...

    async def list(
        self,
        filter: AnalyticalPatternFilter,
        accessible_dataset_ids: Optional[List[str]] = None,
        query_vector: Optional[List[float]] = None,
    ) -> dict:
        """Return a paginated dict ``{"aps": [...], "total": int}`` (shallow retrieval).

        When *query_vector* is provided, a vector-similarity search is performed
        instead of a full scan; ``filter.search.threshold`` and
        ``filter.search.top_k`` are used to control the search.
        When *accessible_dataset_ids* is provided, only APs whose input data
        nodes belong to one of those datasets are returned.
        """
        ...

    async def get_ids_by_task_id(self, task_id: str) -> List[str]:
        """
        Return the IDs of all AnalyticalPattern nodes accomplished by the
        given Task.
        """
        ...

    async def delete(self, ap_id: str) -> None:
        """Delete an AP and its Operator nodes (not the referenced data nodes)."""
        ...
