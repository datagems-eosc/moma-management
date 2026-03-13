from pathlib import Path
from typing import Any, Dict, List, Optional

from yaml import safe_load

from moma_management.domain.dataset import Dataset
from moma_management.domain.filters import DatasetFilter
from moma_management.domain.mapping_engine import croissant_to_pgjson
from moma_management.repository.dataset.dataset_repository import DatasetRepository


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
        """
        mapping = safe_load(self._mapping_file.open("r"))

        # TODO: This should throw if the croissant isn't convertible
        dataset = croissant_to_pgjson(candidate, mapping)

        # TODO: This throws an error if the dataset isn't valid
        valid_dataset = self.validate(dataset)

        return valid_dataset

    def validate(self, pg_json: Dict[str, Any]) -> Dataset:
        """
        Validate a PG-JSON dataset against the MoMa graph schema.
        """
        return Dataset.model_validate(pg_json)

    def ingest(self, candidate: Dict[str, Any]) -> Dataset:
        """
        Ingest a dataset profile into the MoMa repository.  
        Accepts a Croissant-format JSON body, converts it to PG-JSON according to
        the MoMa graph schema, and persists the result to Neo4j.
        """
        dataset = self.convert(candidate)
        self._repo.create(dataset)
        return dataset

    def get(self, dataset_id: str) -> Optional[Dataset]:
        """
        Retrieve the full dataset subgraph (nodes + edges) by dataset ID.
        """
        return self._repo.get(dataset_id)

    def list(self, filters: DatasetFilter) -> List[Dataset]:
        """
        List datasets with optional filtering, sorting, and pagination criteria.
        """
        return self._repo.list(filters)

    def delete(self, id: str) -> int:
        """
        Delete a dataset and its connected subgraph by dataset ID.
        """
        return self._repo.delete(id)
