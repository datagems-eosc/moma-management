from fastapi import APIRouter

from .create import create_evaluation
from .delete import delete_evaluation

router = APIRouter(tags=["analytical patterns"])

router.add_api_route(
    "/{id}/evaluations",
    create_evaluation,
    methods=["POST"],
    status_code=201,
    summary="Create an Evaluation for an AnalyticalPattern",
    responses={
        201: {"description": "Evaluation created"},
        401: {"description": "Not authenticated"},
        403: {"description": "Forbidden"},
        404: {"description": "AnalyticalPattern not found"},
        422: {"description": "Validation error"},
    },
)

router.add_api_route(
    "/{id}/evaluations/{evaluation_id}",
    delete_evaluation,
    methods=["DELETE"],
    status_code=204,
    summary="Delete an Evaluation",
    responses={
        204: {"description": "Evaluation deleted"},
        401: {"description": "Not authenticated"},
        403: {"description": "Forbidden"},
        422: {"description": "Invalid node type: not an Evaluation"},
    },
)
