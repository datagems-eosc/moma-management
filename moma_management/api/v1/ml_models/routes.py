from fastapi import APIRouter

from .create import create_ml_model
from .delete import delete_ml_model
from .get import get_ml_model
from .list import list_ml_models
from .update import update_ml_model

router = APIRouter(tags=["ml_models"])

router.add_api_route(
    "/",
    create_ml_model,
    methods=["POST"],
    status_code=201,
    summary="Create a new ML_Model",
    responses={
        201: {"description": "ML_Model created"},
        401: {"description": "Not authenticated"},
        403: {"description": "Admin role required"},
        422: {"description": "Validation error"},
    },
)

router.add_api_route(
    "/",
    list_ml_models,
    methods=["GET"],
    summary="List all ML_Models",
    responses={
        401: {"description": "Not authenticated"},
    },
)

router.add_api_route(
    "/{id}",
    get_ml_model,
    methods=["GET"],
    summary="Get an ML_Model by ID",
    responses={
        401: {"description": "Not authenticated"},
        404: {"description": "ML_Model not found"},
    },
)

router.add_api_route(
    "/{id}",
    update_ml_model,
    methods=["PATCH"],
    summary="Update an ML_Model",
    responses={
        401: {"description": "Not authenticated"},
        403: {"description": "Admin role required"},
        404: {"description": "ML_Model not found"},
    },
)

router.add_api_route(
    "/{id}",
    delete_ml_model,
    methods=["DELETE"],
    status_code=204,
    summary="Delete an ML_Model",
    responses={
        204: {"description": "ML_Model deleted"},
        401: {"description": "Not authenticated"},
        403: {"description": "Admin role required"},
        404: {"description": "ML_Model not found"},
        409: {"description": "ML_Model is referenced by an analytical pattern"},
    },
)
