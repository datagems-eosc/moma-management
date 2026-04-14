from typing import Never

from fastapi import Depends

from moma_management.di import get_ap_service
from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.middlewares.auth import require_browse_for_ap_creation
from moma_management.services.analytical_pattern import AnalyticalPatternService


async def create_ap(
    candidate: AnalyticalPattern,
    svc: AnalyticalPatternService = Depends(get_ap_service),
    _auth: Never = Depends(require_browse_for_ap_creation()),
) -> dict:
    """
    Create a new AnalyticalPattern in the MoMa graph repository.

    The ``input`` edges of the AP **must** reference Data nodes that belong
    to an existing dataset, and the caller must be able to **browse** those
    datasets.  The AP cannot create Dataset nodes itself.
    """
    ap_id = await svc.create(candidate)
    return {"id": ap_id}
