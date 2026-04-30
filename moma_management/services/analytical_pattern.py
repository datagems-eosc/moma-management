import logging
from typing import List, Optional
from uuid import UUID
from uuid import uuid4 as uuidv4

from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.domain.exceptions import NotFoundError, ValidationError
from moma_management.domain.filters import AnalyticalPatternFilter, DatasetFilter
from moma_management.domain.generated.edges.edge_schema import Edge
from moma_management.domain.generated.nodes.ap.evaluation_schema import (
    Type as EvaluationType,
)
from moma_management.domain.schema_validator import LocalSchemaValidator, SchemaError
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

    async def create(self, ap: AnalyticalPattern) -> str:
        """
        Validate that every ``input`` edge in the AP that targets a non-ResultType
        node references a Data node belonging to an existing dataset, then persist
        the AP.

        ResultType nodes are internal to the AP (they carry transient values
        between Operators) and are excluded from the dataset-existence check.

        Returns the AP root node ID.

        Raises:
            ValidationError: if any input target does not belong to a known dataset.
        """
        # Build a set of node IDs that are internal ResultType nodes (transient
        # values between Operators).  Data nodes are also ResultType subtypes but
        # they are persistent and DO need dataset-existence validation.
        result_type_ids: set[str] = {
            str(n.id)
            for n in ap.nodes
            if "ResultType" in (n.labels or []) and "Data" not in (n.labels or [])
        }

        input_node_ids = [
            str(e.to)
            for e in (ap.edges or [])
            if "input" in e.labels and str(e.to) not in result_type_ids
        ]

        if input_node_ids:
            result = await self._dataset_service.list(
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

        await self._repo.create(ap, embedding=self._embed_ap(ap))
        return str(ap.root.id)

    def _embed_ap(self, ap: AnalyticalPattern) -> Optional[List[float]]:
        """Extract description text and embed it, or return ``None`` if no embedder."""
        if self._embedder is None:
            return None
        text = ap.root.properties.get(
            "description") or ap.root.properties.get("name", "")
        if not text:
            return None
        return self._embedder.embed(text)

    async def get(self, ap_id: str, include_evaluations: bool = False) -> AnalyticalPattern:
        """
        Retrieve an AnalyticalPattern by its root node ID (shallow).

        Raises:
            NotFoundError: if no AP with *ap_id* exists.
        """
        result = await self._repo.get(ap_id, include_evaluations=include_evaluations)
        if result is None:
            raise NotFoundError(f"AnalyticalPattern '{ap_id}' not found.")
        return result

    async def delete(self, ap_id: str) -> None:
        """Delete an AnalyticalPattern by its root node ID.

        Raises:
            NotFoundError: if no AP with *ap_id* exists.
        """
        if await self._repo.get(ap_id) is None:
            raise NotFoundError(f"AnalyticalPattern '{ap_id}' not found.")
        await self._repo.delete(ap_id)

    async def list(self, filter: AnalyticalPatternFilter, accessible_dataset_ids: list[str] | None = None) -> dict:
        """Unified list/search entry-point for the AP list endpoint.

        Returns a dict with shape::

            {
                "aps": [{"ap": AnalyticalPattern}, ...],
                "page": int,
                "pageSize": int,
                "total": int,
            }

        When ``filter.search`` is set a vector-similarity search is performed;
        ``top_k`` caps the number of candidates fetched from the DB, then
        threshold filtering and standard pagination are applied to the result.
        Otherwise a standard paginated list is returned.

        When ``filter.include_evaluations`` is ``True``, Evaluation nodes are
        included in each AP's subgraph (``ap.nodes``).  When ``False``
        (default), Evaluation nodes are excluded from the traversal entirely.
        """
        query_vector = None
        if filter.search is not None:
            if self._embedder is None:
                raise ValidationError(
                    "Semantic search is not available: no embedder configured.")
            query_vector = self._embedder.embed(filter.search.q)

        repo_result = await self._repo.list(filter, accessible_dataset_ids=accessible_dataset_ids, query_vector=query_vector)

        return {
            "aps": repo_result["aps"],
            "page": filter.page,
            "pageSize": filter.pageSize,
            "total": repo_result["total"],
        }

    async def add_evaluation(self, ap_id: str, type: EvaluationType, eval: str, execution_id: UUID | None = uuidv4()) -> str:
        """Create and persist an Evaluation node linked to the AP.

        Raises:
            NotFoundError: if the AP does not exist.
        """
        from moma_management.domain.generated.nodes.ap.evaluation_schema import (
            PgProperties as EvaluationProperties,
        )
        from moma_management.domain.generated.nodes.node_schema import Node as GraphNode

        eval_id = uuidv4()
        # Validate properties against the Evaluation schema before building the node.
        props = EvaluationProperties.model_validate({
            "executionId": str(execution_id or uuidv4()),
            "evaluation": eval,
            "type": type.value,
        })
        eval_node = GraphNode.model_validate({
            "id": eval_id,
            "labels": ["Evaluation", type.value],
            "properties": props.model_dump(by_alias=True, exclude_none=True),
        })
        ap = await self.get(ap_id)  # raises NotFoundError if AP is missing
        ap.nodes.append(eval_node)
        ap.edges.append(Edge.model_validate({
            "from": ap.root.id,
            "to": eval_id,
            "labels": ["is_measured_by"],
        }))
        AnalyticalPattern.model_validate(ap)

        await self._repo.create(ap)
        return str(eval_id)

    def validate(self, candidate: dict) -> list[SchemaError]:
        """Validate a raw PG-JSON dict as an AnalyticalPattern.

        Returns a list of AJV-style :class:`SchemaError` objects (empty when
        the candidate is valid).
        """
        validator = LocalSchemaValidator()
        return validator.validate_graph(candidate, graph_type="ap")
