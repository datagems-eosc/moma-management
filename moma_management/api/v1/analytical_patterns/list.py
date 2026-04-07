from typing import List

from fastapi import Depends

from moma_management.di import get_allowed_datasets_ids, get_ap_service
from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.services.analytical_pattern import AnalyticalPatternService


async def list_aps(
    svc: AnalyticalPatternService = Depends(get_ap_service),
    accessible_ids: list[str] | None = Depends(get_allowed_datasets_ids()),
) -> List[AnalyticalPattern]:
    """
    List all AnalyticalPatterns (shallow retrieval).

    Only APs whose ``input`` edges reference datasets the authenticated user
    can browse are returned.  APs with no ``input`` edges are always included.
    When authentication is disabled all APs are returned.
    """
    return svc.list(accessible_dataset_ids=accessible_ids)
