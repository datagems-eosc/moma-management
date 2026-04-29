from fastapi import Depends, Query
from fastapi.responses import JSONResponse

from moma_management.di import get_ap_service
from moma_management.domain.filters import AnalyticalPatternFilter, APSearchParams
from moma_management.middlewares.auth import get_allowed_datasets_ids
from moma_management.services.analytical_pattern import AnalyticalPatternService


def _ap_filters(
    search_q: str | None = Query(
        default=None,
        alias="search.q",
        description="Semantic search query. When provided, results are ordered by relevance.",
    ),
    search_top_k: int = Query(
        default=10, ge=1, le=100,
        alias="search.top_k",
        description="Maximum number of results returned by semantic search (requires `search.q`).",
    ),
    search_threshold: float = Query(
        default=0.75, ge=0.0, le=1.0,
        alias="search.threshold",
        description="Minimum similarity score, 0–1 (requires `search.q`).",
    ),
    page: int = Query(default=1, ge=1, description="Page number (1-indexed)."),
    pageSize: int = Query(default=25, ge=1, le=100,
                          description="Number of results per page (1–100)."),
    include_evaluations: bool = Query(
        default=False,
        description="Include evaluations for each returned AnalyticalPattern.",
    ),
) -> AnalyticalPatternFilter:
    return AnalyticalPatternFilter(
        search=APSearchParams(q=search_q, top_k=search_top_k,
                              threshold=search_threshold)
        if search_q is not None else None,
        page=page,
        pageSize=pageSize,
        include_evaluations=include_evaluations,
    )


async def list_aps(
    filters: AnalyticalPatternFilter = Depends(_ap_filters),
    svc: AnalyticalPatternService = Depends(get_ap_service),
    accessible_ids: list[str] | None = Depends(get_allowed_datasets_ids()),
) -> JSONResponse:
    """
    List all AnalyticalPatterns with optional filtering, pagination and evaluation enrichment.

    When ``search.q`` is provided, a semantic search is performed and results are
    ordered by relevance.  Pagination (``page``/``pageSize``) applies to both the
    list and search paths.

    Only APs whose ``input`` edges reference datasets the authenticated user can browse
    are returned.  APs with no ``input`` edges are always included.
    When authentication is disabled all APs are returned.

    When ``include_evaluations=true``, Evaluation nodes are included in each
    AP's ``nodes`` list (with labels such as ``Evaluation`` + ``SystemEvaluation``).
    By default they are excluded.
    """
    if accessible_ids is not None:
        if not accessible_ids:
            return JSONResponse({"aps": [], "page": filters.page, "pageSize": filters.pageSize, "total": 0})

    result = await svc.list(filters, accessible_dataset_ids=accessible_ids)

    return JSONResponse({
        "aps": [ap.model_dump(mode="json", by_alias=True) for ap in result["aps"]],
        "page": result["page"],
        "pageSize": result["pageSize"],
        "total": result["total"],
    })
