from fastapi import APIRouter

from .get import get_node
from .update import update_node

router = APIRouter(
    tags=["nodes"],
)

router.add_api_route("/{id}", get_node, methods=["GET"])
router.add_api_route("/{id}", update_node, methods=["PATCH"], status_code=200)
