from typing import Any, Dict

from fastapi import Depends

from moma_management.di import get_dataset_service
from moma_management.domain.dataset import Dataset
from moma_management.services.dataset import DatasetService


async def validate_dataset(
    input_data: Dict[str, Any],
    svc: DatasetService = Depends(get_dataset_service),
) -> Dataset:
    """
    Validate a PG-JSON dataset against the MoMa graph schema without persisting it.
    Returns the validated Dataset if valid, or a 422 error with details if validation fails.
    """
    return svc.validate(input_data)
