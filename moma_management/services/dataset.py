import logging
from pathlib import Path
from typing import Any, Dict, List

from pydantic import ValidationError as PydanticValidationError
from yaml import safe_load

from moma_management.domain.dataset import Dataset
from moma_management.domain.exceptions import (
    ConflictError,
    ConversionError,
    NotFoundError,
    ValidationError,
)
from moma_management.domain.filters import DatasetFilter
from moma_management.domain.mapping_engine import croissant_to_pgjson
from moma_management.domain.schema_validator import LocalSchemaValidator, SchemaError
from moma_management.repository.dataset.dataset_repository import DatasetRepository

logger = logging.getLogger(__name__)


class DatasetService:

    _repo: DatasetRepository
    _mapping_file: Path

    def __init__(self, repo: DatasetRepository, mapping_file: Path):
        self._repo = repo
        assert mapping_file.exists(
        ), f"Mapping file not found at {mapping_file}"
        self._mapping_file = mapping_file

    def create(self, candidate: Dataset) -> Dataset:
        """
        Create a new dataset from a validated Dataset object.
        """
        self._repo.create(candidate)
        return candidate

    def convert(self, candidate: Dict[str, Any]) -> Dataset:
        """
        Convert a Croissant-format JSON body to PG-JSON according to the MoMa graph schema.
        Does not persist the result to Neo4j.

        Raises:
            ConversionError: if the Croissant profile cannot be mapped to PG-JSON.
            ValidationError: if the resulting PG-JSON does not conform to the MoMa schema.
        """
        try:
            mapping = safe_load(self._mapping_file.open("r"))
            dataset = croissant_to_pgjson(candidate, mapping)
        except Exception as e:
            logger.exception("Croissant conversion failed")
            raise ConversionError(
                f"Failed to convert Croissant profile: {e}") from e

        return self._parse(dataset)

    def _parse(self, pg_json: Dict[str, Any]) -> Dataset:
        """Parse and validate a raw PG-JSON dict into a :class:`Dataset`.

        Used internally by :meth:`convert` and :meth:`ingest`.

        Raises:
            ValidationError: if *pg_json* does not conform to the MoMa Dataset schema.
        """
        try:
            return Dataset.model_validate(pg_json)
        except PydanticValidationError as e:
            raise ValidationError(
                f"PG-JSON failed schema validation: {e}") from e

    def validate(self, candidate: dict) -> list[SchemaError]:
        """Validate a raw PG-JSON dict as a Dataset.

        Returns a list of AJV-style :class:`SchemaError` objects (empty when
        the candidate is valid).
        """
        validator = LocalSchemaValidator()
        return validator.validate_graph(candidate, graph_type="dataset")

    def ingest(self, candidate: Dict[str, Any]) -> Dataset:
        """
        Ingest a dataset profile into the MoMa repository.
        Accepts a Croissant-format JSON body, converts it to PG-JSON according to
        the MoMa graph schema, and persists the result to Neo4j.

        Raises:
            ConversionError: if the Croissant profile cannot be converted.
            ValidationError: if the converted PG-JSON fails schema validation.
        """
        dataset = self.convert(candidate)
        self._repo.create(dataset)
        return dataset

    def get(self, dataset_id: str) -> Dataset:
        """
        Retrieve the full dataset subgraph (nodes + edges) by dataset ID.

        Raises:
            NotFoundError: if no dataset with *dataset_id* exists.
        """
        result = self._repo.get(dataset_id)
        if result is None:
            raise NotFoundError(f"Dataset '{dataset_id}' not found.")
        return result

    def list(self, filters: DatasetFilter) -> List[Dataset]:
        """
        List datasets with optional filtering, sorting, and pagination criteria.
        """
        return self._repo.list(filters)

    def delete(self, id: str) -> int:
        """
        Delete a dataset and its connected subgraph by dataset ID.

        Raises:
            NotFoundError: if no dataset with *id* exists.
            ConflictError: if at least one AnalyticalPattern references this dataset.
        """
        if self._repo.get(id) is None:
            raise NotFoundError(f"Dataset '{id}' not found.")
        if self._repo.has_referencing_aps(id):
            raise ConflictError(
                f"Dataset '{id}' cannot be deleted: it is referenced by at "
                f"least one AnalyticalPattern."
            )
        return self._repo.delete(id)
