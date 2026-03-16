from fastapi import APIRouter

from .get import get_node
from .update import update_node

router = APIRouter(
    tags=["nodes"],
)

router.add_api_route(
    "/{id}",
    get_node,
    methods=["GET"],
    summary="Get a node by ID",
    responses={
        401: {"description": "Unauthorized"},
        403: {"description": "Forbidden"},
        404: {"description": "Node not found"},
        500: {"description": "Internal server error"},
    },
)
router.add_api_route(
    "/{id}",
    update_node,
    methods=["PATCH"],
    status_code=200,
    summary="Partially update a node's properties",
    responses={
        401: {"description": "Unauthorized"},
        403: {"description": "Forbidden"},
        404: {"description": "Node not found"},
        500: {"description": "Internal server error"},
    },
)
