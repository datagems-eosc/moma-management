from moma_management.domain.dataset import Dataset
from moma_management.domain.filters import DatasetFilter
from moma_management.repository.repository import Repository


class DatasetRepository(Repository[Dataset, DatasetFilter]):
    pass
