from fastapi import APIRouter

from .create import create_task
from .retrieve_aps import retrieve_ap_ids

router = APIRouter(tags=["tasks"])

router.add_api_route(
    "/",
    create_task,
    methods=["POST"],
    status_code=201,
    summary="Create a new Task",
    responses={
        201: {"description": "Task created"},
        422: {"description": "Validation error"},
        500: {"description": "Internal server error"},
    },
)
router.add_api_route(
    "/{id}/aps",
    retrieve_ap_ids,
    methods=["GET"],
    summary="Get AnalyticalPattern IDs for a Task",
    responses={
        404: {"description": "Task not found"},
        500: {"description": "Internal server error"},
    },
)
