from fastapi import APIRouter

from .create import create_evaluation
from .delete import delete_evaluation
from .get import get_evaluation
from .list import list_evaluations

router = APIRouter(tags=["analytical patterns"])

router.add_api_route(
    "/{ap_id}/evaluations",
    create_evaluation,
    methods=["POST"],
    status_code=201,
    summary="Create an Evaluation for an AnalyticalPattern",
    responses={
        201: {"description": "Evaluation created"},
        401: {"description": "Not authenticated"},
        404: {"description": "AnalyticalPattern not found"},
        422: {"description": "Validation error"},
    },
)

router.add_api_route(
    "/{ap_id}/evaluations",
    list_evaluations,
    methods=["GET"],
    summary="List Evaluations for an AnalyticalPattern",
    responses={
        401: {"description": "Not authenticated"},
        404: {"description": "AnalyticalPattern not found"},
    },
)

router.add_api_route(
    "/evaluations/{execution_id}",
    get_evaluation,
    methods=["GET"],
    summary="Get an Evaluation by execution ID",
    responses={
        401: {"description": "Not authenticated"},
        404: {"description": "Evaluation not found"},
    },
)

router.add_api_route(
    "/evaluations/{execution_id}",
    delete_evaluation,
    methods=["DELETE"],
    status_code=204,
    summary="Delete an Evaluation by execution ID",
    responses={
        204: {"description": "Evaluation deleted"},
        401: {"description": "Not authenticated"},
        404: {"description": "Evaluation not found"},
    },
)
