from typing import Any, Dict

from fastapi import Depends, HTTPException

from moma_management.di import get_dataset_service
from moma_management.domain.dataset import Dataset
from moma_management.services.dataset import DatasetService


async def validate_dataset(
    input_data: Dict[str, Any],
    svc: DatasetService = Depends(get_dataset_service),
) -> Dataset:
    """
        Validate a dataset profile against the MoMa graph schema.
    """
    try:
        return svc.validate(input_data)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}")
