
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
router.add_api_route("/", ingest_profile, methods=["POST"])
router.add_api_route("/", list_datasets, methods=["GET"])
router.add_api_route("/{id}", get_dataset, methods=["GET"])
router.add_api_route("/{id}", delete_dataset,
                     methods=["DELETE"], status_code=204)

# "Extension method"
# NOTE: These should be limited as much as possible to avoid API bloat. Delete them if not used
router.add_api_route("/convert", convert_profile, methods=["POST"])
router.add_api_route("/validate", validate_dataset, methods=["POST"])
