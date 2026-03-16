from fastapi import APIRouter

from .datasets.routes import router as dataset_routes
from .health import health_check
from .nodes.routes import router as node_routes

router = APIRouter()

router.include_router(dataset_routes, prefix="/datasets")
router.include_router(node_routes, prefix="/nodes")
router.add_api_route(
    "/health",
    health_check,
    methods=["GET"],
    tags=["health"],
    summary="Health check",
    responses={
        200: {"description": "Service is healthy"},
    },
)
