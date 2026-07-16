import logging

from moma_management.domain.dataset_relationship import DatasetRelationship
from moma_management.domain.exceptions import ConflictError, NotFoundError, ValidationError
from moma_management.domain.filters import DatasetFilter
from moma_management.repository.dataset_relationship.dataset_relationship_repository import (
    DatasetRelationshipRepository,
)
from moma_management.services.dataset import DatasetService

logger = logging.getLogger(__name__)


class DatasetRelationshipService:
    """Business-logic layer for DatasetRelationship CRUD."""

    def __init__(
        self,
        repo: DatasetRelationshipRepository,
        dataset_service: DatasetService,
    ) -> None:
        self._repo = repo
        self._dataset_service = dataset_service

    async def create(self, relationship: DatasetRelationship) -> str:
        """
        Validate that both datasets targeted by the relationship exist and
        that no other relationship already links the same pair, then persist
        the relationship.

        Returns the relationship root node ID.

        Raises:
            ValidationError: if either target dataset does not exist.
            ConflictError: if a relationship already exists for this dataset pair.
        """
        target_ids = list(relationship.target_dataset_ids)
        result = await self._dataset_service.list(DatasetFilter(nodeIds=target_ids))
        found_ids: set[str] = set()
        for ds in result.get("datasets", []):
            for node in ds.nodes:
                found_ids.add(str(node.id))

        missing = [nid for nid in target_ids if nid not in found_ids]
        if missing:
            raise ValidationError(
                f"DatasetRelationship references dataset(s) that do not exist: "
                f"{', '.join(missing)}"
            )

        existing_id = await self._repo.find_id_for_dataset_pair(*relationship.target_dataset_ids)
        if existing_id is not None:
            raise ConflictError(
                f"A relationship already exists between datasets "
                f"{target_ids[0]} and {target_ids[1]} (id: {existing_id}). "
                f"Delete it first."
            )

        await self._repo.create(relationship)
        return str(relationship.root.id)

    async def get(self, relationship_id: str) -> DatasetRelationship:
        """
        Retrieve a DatasetRelationship by its root node ID.

        Raises:
            NotFoundError: if no relationship with *relationship_id* exists.
        """
        result = await self._repo.get(relationship_id)
        if result is None:
            raise NotFoundError(f"DatasetRelationship '{relationship_id}' not found.")
        return result

    async def delete(self, relationship_id: str) -> None:
        """Delete a DatasetRelationship by its root node ID.

        Raises:
            NotFoundError: if no relationship with *relationship_id* exists.
        """
        if await self._repo.get(relationship_id) is None:
            raise NotFoundError(f"DatasetRelationship '{relationship_id}' not found.")
        await self._repo.delete(relationship_id)

    async def list_for_dataset(
        self,
        dataset_id: str,
        accessible_dataset_ids: list[str] | None = None,
    ) -> list[DatasetRelationship]:
        """
        List every DatasetRelationship that targets *dataset_id*.

        When *accessible_dataset_ids* is provided, relationships whose OTHER
        linked dataset is not in that set are silently excluded (the caller
        must be able to browse both datasets to see a relationship between
        them) rather than causing the whole request to fail — the same
        convention used by AnalyticalPatternService.list().

        Raises:
            NotFoundError: if no dataset with *dataset_id* exists.
        """
        await self._dataset_service.get(dataset_id)  # raises NotFoundError if missing

        relationships = await self._repo.list_for_dataset(dataset_id)
        if accessible_dataset_ids is None:
            return relationships

        accessible = set(accessible_dataset_ids)
        return [
            rel for rel in relationships
            if all(did in accessible for did in rel.target_dataset_ids)
        ]
