from fastapi import APIRouter

from .analytical_patterns.routes import router as ap_routes
from .datasets.routes import router as dataset_routes
from .health import health_check
from .ml_models.routes import router as ml_model_routes
from .nodes.routes import router as node_routes
from .tasks.routes import router as task_routes

router = APIRouter()

router.include_router(dataset_routes, prefix="/datasets")
router.include_router(node_routes, prefix="/nodes")
router.include_router(ap_routes, prefix="/aps")
router.include_router(task_routes, prefix="/tasks")
router.include_router(ml_model_routes, prefix="/ml-models")
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
