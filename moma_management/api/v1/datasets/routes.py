
from fastapi import APIRouter

from .convert import convert_profile
from .delete import delete_dataset
from .get import get_dataset
from .ingest import ingest_profile
from .list import list_datasets
from .validate import validate_dataset

router = APIRouter(
    tags=["datasets"],
)

# CRUD
router.add_api_route(
    "/",
    ingest_profile,
    methods=["POST"],
    summary="Ingest a dataset profile",
    responses={
        401: {"description": "Unauthorized"},
        403: {"description": "Forbidden"},
        500: {"description": "Internal server error"},
    },
)
router.add_api_route(
    "/",
    list_datasets,
    methods=["GET"],
    summary="List datasets",
    responses={
        401: {"description": "Unauthorized"},
        403: {"description": "Forbidden"},
        500: {"description": "Internal server error"},
    },
)
router.add_api_route(
    "/{id}",
    get_dataset,
    methods=["GET"],
    summary="Get a dataset by ID",
    responses={
        401: {"description": "Unauthorized"},
        403: {"description": "Forbidden"},
        404: {"description": "Dataset not found"},
        500: {"description": "Internal server error"},
    },
)
router.add_api_route(
    "/{id}",
    delete_dataset,
    methods=["DELETE"],
    status_code=204,
    summary="Delete a dataset by ID",
    responses={
        204: {"description": "Dataset deleted successfully"},
        401: {"description": "Unauthorized"},
        403: {"description": "Forbidden"},
        500: {"description": "Internal server error"},
    },
)

# "Extension methods"
# NOTE: These should be limited as much as possible to avoid API bloat. Delete them if not used
router.add_api_route(
    "/convert",
    convert_profile,
    methods=["POST"],
    summary="Convert a Croissant profile to PG-JSON",
    responses={
        500: {"description": "Internal server error"},
    },
)
router.add_api_route(
    "/validate",
    validate_dataset,
    methods=["POST"],
    summary="Validate a Croissant profile",
    responses={
        500: {"description": "Validation or conversion error"},
    },
)
