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
            ValidationError: if either target dataset does not exist, a
                referenced cr:FileObject is not part of one of them, a
                ``distribution`` edge contradicts what MoMa stores, or a
                FileObjectComparison does not compare files of both datasets.
            ConflictError: if a relationship already exists for this dataset pair.
        """
        target_ids = list(relationship.target_dataset_ids)
        result = await self._dataset_service.list(DatasetFilter(nodeIds=target_ids))
        found_ids: set[str] = set()
        file_owner: dict[str, str] = {}  # stored file id -> id of the dataset it belongs to
        for ds in result.get("datasets", []):
            # list() also returns a dataset when one of its *children* matches
            # an id: only the dataset nodes themselves prove a target exists.
            ds_id = next((str(n.id) for n in ds.nodes if "sc:Dataset" in n.labels), None)
            if ds_id is None:
                continue
            found_ids.add(ds_id)
            for node in ds.nodes:
                if "cr:FileObject" in node.labels:
                    file_owner[str(node.id)] = ds_id

        missing = [nid for nid in target_ids if nid not in found_ids]
        if missing:
            raise ValidationError(
                f"DatasetRelationship references dataset(s) that do not exist: "
                f"{', '.join(missing)}"
            )

        # Linking a file from any other dataset would leak it into this relationship.
        foreign_files = sorted(relationship.file_object_ids - file_owner.keys())
        if foreign_files:
            raise ValidationError(
                f"DatasetRelationship references file object(s) that are not "
                f"part of the two linked datasets: {', '.join(foreign_files)}"
            )

        # Optional distribution edges are never written: they must match what MoMa stores.
        wrong_memberships = sorted(
            f"{f} is in {file_owner[f]}, not {d}"
            for d, f in relationship.file_memberships if file_owner[f] != d
        )
        if wrong_memberships:
            raise ValidationError(
                f"DatasetRelationship distribution edge(s) contradict MoMa: "
                f"{'; '.join(wrong_memberships)}"
            )

        # A comparison is *between* the two datasets: it needs a file from each.
        compared: dict[str, set[str]] = {}
        comparison_ids = {str(n.id) for n in relationship.nodes if "FileObjectComparison" in n.labels}
        for e in relationship.edges or []:
            if str(e.from_) in comparison_ids and str(e.to) in file_owner:
                compared.setdefault(str(e.from_), set()).add(file_owner[str(e.to)])
        one_sided = sorted(c for c in comparison_ids if len(compared.get(c, ())) < 2)
        if one_sided:
            raise ValidationError(
                f"FileObjectComparison(s) must compare files from both linked "
                f"datasets, but these do not: {', '.join(one_sided)}"
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
