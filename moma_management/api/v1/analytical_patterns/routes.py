from fastapi import APIRouter

from .create import create_ap
from .delete import delete_ap
from .get import get_ap
from .list import list_aps

router = APIRouter(tags=["analytical patterns"])

router.add_api_route(
    "/",
    create_ap,
    methods=["POST"],
    status_code=201,
    summary="Create a new AnalyticalPattern",
    responses={
        201: {"description": "AnalyticalPattern created", "content": {"application/json": {"schema": {"type": "object", "properties": {"id": {"type": "string"}}}}}},
        401: {"description": "Unauthorized"},
        403: {"description": "Forbidden"},
        422: {"description": "Validation error (e.g. input dataset not found)"},
        500: {"description": "Internal server error"},
    },
)
router.add_api_route(
    "/",
    list_aps,
    methods=["GET"],
    summary="List all AnalyticalPatterns",
    responses={
        401: {"description": "Unauthorized"},
        500: {"description": "Internal server error"},
    },
)
router.add_api_route(
    "/{id}",
    get_ap,
    methods=["GET"],
    summary="Get an AnalyticalPattern by ID",
    responses={
        404: {"description": "AnalyticalPattern not found"},
        500: {"description": "Internal server error"},
    },
)
router.add_api_route(
    "/{id}",
    delete_ap,
    methods=["DELETE"],
    status_code=204,
    summary="Delete an AnalyticalPattern by ID",
    responses={
        204: {"description": "AnalyticalPattern deleted"},
        401: {"description": "Unauthorized"},
        403: {"description": "Forbidden"},
        404: {"description": "AnalyticalPattern not found"},
        500: {"description": "Internal server error"},
    },
)
