from fastapi import APIRouter

from .datasets.routes import router as dataset_routes
from .health import health_check

router = APIRouter()

router.include_router(dataset_routes, prefix="/datasets")
router.add_api_route("/health", health_check, methods=["GET"])
