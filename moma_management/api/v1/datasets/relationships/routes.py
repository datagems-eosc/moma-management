from fastapi import APIRouter

from .create import create_relationship
from .delete import delete_relationship
from .get import get_relationship

router = APIRouter(tags=["dataset relationships"])

router.add_api_route(
    "/",
    create_relationship,
    methods=["POST"],
    status_code=201,
    summary="Create a new DatasetRelationship",
    responses={
        201: {"description": "DatasetRelationship created", "content": {"application/json": {"schema": {"type": "object", "properties": {"id": {"type": "string"}}}}}},
        401: {"description": "Unauthorized"},
        403: {"description": "Forbidden: admin role required"},
        409: {"description": "A relationship already exists for this dataset pair"},
        422: {"description": "Validation error (e.g. target dataset not found)"},
        500: {"description": "Internal server error"},
    },
)
router.add_api_route(
    "/{id}",
    get_relationship,
    methods=["GET"],
    summary="Get a DatasetRelationship by ID",
    responses={
        401: {"description": "Unauthorized"},
        403: {"description": "Forbidden"},
        404: {"description": "DatasetRelationship not found"},
        500: {"description": "Internal server error"},
    },
)
router.add_api_route(
    "/{id}",
    delete_relationship,
    methods=["DELETE"],
    status_code=204,
    summary="Delete a DatasetRelationship by ID",
    responses={
        204: {"description": "DatasetRelationship deleted"},
        401: {"description": "Unauthorized"},
        403: {"description": "Forbidden: admin role required"},
        404: {"description": "DatasetRelationship not found"},
        500: {"description": "Internal server error"},
    },
)
