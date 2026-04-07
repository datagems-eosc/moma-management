import logging
from typing import List, Optional, Tuple

from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.domain.exceptions import NotFoundError, ValidationError
from moma_management.domain.filters import DatasetFilter
from moma_management.repository.analytical_pattern.analytical_pattern_repository import (
    AnalyticalPatternRepository,
)
from moma_management.services.dataset import DatasetService
from moma_management.services.embeddings.embedder import Embedder

logger = logging.getLogger(__name__)


class AnalyticalPatternService:
    """Business-logic layer for AnalyticalPattern CRUD."""

    def __init__(
        self,
        repo: AnalyticalPatternRepository,
        dataset_service: DatasetService,
        embedder: Optional[Embedder] = None,
    ) -> None:
        self._repo = repo
        self._dataset_service = dataset_service
        self._embedder = embedder

    def create(self, ap: AnalyticalPattern) -> str:
        """
        Validate that every ``input`` edge in the AP references a Data node
        that belongs to an existing dataset, then persist the AP.

        Returns the AP root node ID.

        Raises:
            ValidationError: if any input target does not belong to a known dataset.
        """
        input_node_ids = [
            str(e.to)
            for e in (ap.edges or [])
            if "input" in e.labels
        ]

        if input_node_ids:
            result = self._dataset_service.list(
                DatasetFilter(nodeIds=input_node_ids)
            )
            found_ids: set[str] = set()
            for ds in result.get("datasets", []):
                for node in ds.nodes:
                    found_ids.add(str(node.id))

            missing = [nid for nid in input_node_ids if nid not in found_ids]
            if missing:
                raise ValidationError(
                    f"AnalyticalPattern references input node(s) that do not "
                    f"belong to any known dataset: {', '.join(missing)}"
                )

        self._repo.create(ap, embedding=self._embed_ap(ap))
        return str(ap.root.id)

    def _embed_ap(self, ap: AnalyticalPattern) -> Optional[List[float]]:
        """Extract description text and embed it, or return ``None`` if no embedder."""
        if self._embedder is None:
            return None
        text = ap.root.properties.get("description") or ap.root.properties.get("name", "")
        if not text:
            return None
        return self._embedder.embed(text)

    def search(
        self,
        q: str,
        top_k: int = 10,
        accessible_dataset_ids: list[str] | None = None,
    ) -> List[Tuple[AnalyticalPattern, float]]:
        """Semantic search over APs by natural-language query.

        Raises ``ValidationError`` when no embedder is configured.
        """
        if self._embedder is None:
            raise ValidationError("Semantic search is not available: no embedder configured.")
        query_vector = self._embedder.embed(q)
        return self._repo.search(query_vector, top_k, accessible_dataset_ids=accessible_dataset_ids)

    def get(self, ap_id: str) -> AnalyticalPattern:
        """
        Retrieve an AnalyticalPattern by its root node ID (shallow).

        Raises:
            NotFoundError: if no AP with *ap_id* exists.
        """
        result = self._repo.get(ap_id)
        if result is None:
            raise NotFoundError(f"AnalyticalPattern '{ap_id}' not found.")
        return result

    def list(self, accessible_dataset_ids: list[str] | None = None) -> List[AnalyticalPattern]:
        """
        Return all AnalyticalPattern subgraphs (shallow retrieval).

        When *accessible_dataset_ids* is provided, only APs whose ``input``
        edges reference a Data node that belongs to one of those datasets are
        returned.  APs with no ``input`` edges are always included.
        """
        return self._repo.list(accessible_dataset_ids=accessible_dataset_ids)
