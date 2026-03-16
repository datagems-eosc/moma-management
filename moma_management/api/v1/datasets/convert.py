from typing import Any, Dict

from fastapi import Depends, HTTPException

from moma_management.di import get_dataset_service
from moma_management.domain.dataset import Dataset
from moma_management.services.dataset import DatasetService


async def convert_profile(
    input_data: Dict[str, Any],
    svc: DatasetService = Depends(get_dataset_service),
) -> Dataset:
    """
    Convert a Croissant-format profile to a PG-JSON MoMa graph without persisting it.
    Accepts a Croissant-format JSON body, converts it to PG-JSON according to
    the MoMa graph schema, and returns the result without storing it in Neo4j.
    """

    try:
        return svc.convert(input_data)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}")
