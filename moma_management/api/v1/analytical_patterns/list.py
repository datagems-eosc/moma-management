from typing import List, Optional, Union

from fastapi import Depends, Query
from pydantic import BaseModel

from moma_management.di import get_allowed_datasets_ids, get_ap_service
from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.services.analytical_pattern import AnalyticalPatternService


class SearchResult(BaseModel):
    ap: AnalyticalPattern
    score: float


async def list_aps(
    q: Optional[str] = Query(default=None),
    svc: AnalyticalPatternService = Depends(get_ap_service),
    accessible_ids: list[str] | None = Depends(get_allowed_datasets_ids()),
) -> Union[List[AnalyticalPattern], List[SearchResult]]:
    """
    List all AnalyticalPatterns (shallow retrieval).

    When the ``q`` query parameter is provided, a semantic search is performed
    and results are returned as ``SearchResult`` objects sorted by relevance.

    Only APs whose ``input`` edges reference datasets the authenticated user
    can browse are returned.  APs with no ``input`` edges are always included.
    When authentication is disabled all APs are returned.
    """
    if q is not None:
        results = svc.search(q, accessible_dataset_ids=accessible_ids)
        return [SearchResult(ap=ap, score=score) for ap, score in results]

    return svc.list(accessible_dataset_ids=accessible_ids)
