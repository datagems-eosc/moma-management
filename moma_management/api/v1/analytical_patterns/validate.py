from typing import Any, Dict, List

from fastapi import Depends, Response
from pydantic import BaseModel

from moma_management.di import get_ap_service
from moma_management.domain.schema_validator import SchemaError
from moma_management.services.analytical_pattern import AnalyticalPatternService


class ValidationPayload(BaseModel):
    valid: bool
    errors: List[SchemaError]


async def validate_ap(
    candidate: Dict[str, Any],
    res: Response,
    svc: AnalyticalPatternService = Depends(get_ap_service),
) -> ValidationPayload:
    """
    Validate a raw PG-JSON payload as an AnalyticalPattern without persisting it.

    Returns a ``ValidationPayload`` with ``valid=true`` and an empty ``errors``
    list when the payload conforms to the MoMa AP schema, or ``valid=false``
    with AJV-style errors otherwise.

    This endpoint requires no authentication.
    """
    errors = svc.validate(candidate)
    if errors:
        res.status_code = 422
    return ValidationPayload(valid=len(errors) == 0, errors=errors)
