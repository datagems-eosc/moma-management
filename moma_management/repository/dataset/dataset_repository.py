from moma_management.domain.dataset import Dataset
from moma_management.domain.filters import DatasetFilter
from moma_management.repository.repository import Repository


class DatasetRepository(Repository[Dataset, DatasetFilter]):

    def has_referencing_aps(self, dataset_id: str) -> bool:
        """Return True if at least one AP references a node in this dataset."""
        ...
