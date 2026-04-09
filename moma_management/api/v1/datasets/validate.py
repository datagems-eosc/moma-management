from typing import Any, Dict, List

from fastapi import Depends, Response
from pydantic import BaseModel

from moma_management.di import get_dataset_service
from moma_management.domain.schema_validator import SchemaError
from moma_management.services.dataset import DatasetService


class ValidationPayload(BaseModel):
    valid: bool
    errors: List[SchemaError]


async def validate_dataset(
    input_data: Dict[str, Any],
    res: Response,
    svc: DatasetService = Depends(get_dataset_service),
) -> ValidationPayload:
    """
    Validate a PG-JSON dataset against the MoMa graph schema without persisting it.

    Returns a ``ValidationPayload`` with ``valid=true`` and an empty ``errors``
    list when the payload conforms to the MoMa Dataset schema, or ``valid=false``
    with AJV-style errors otherwise.

    This endpoint requires no authentication.
    """
    errors = svc.validate(input_data)
    if errors:
        res.status_code = 422
    return ValidationPayload(valid=len(errors) == 0, errors=errors)
