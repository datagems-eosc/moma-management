from typing import Any, Dict, Never

from fastapi import Depends

from moma_management.di import get_dataset_service
from moma_management.middlewares.auth import require_permission
from moma_management.services.authorization import DatasetRole
from moma_management.services.dataset import DatasetService


async def ingest_profile(
    input_data: Dict[str, Any],
    svc: DatasetService = Depends(get_dataset_service),
    _auth: Never = Depends(require_permission(DatasetRole.CREATE)),
) -> None:
    """
    Ingest entire profiling (basic, light, heavy) or only the basic part into the MoMa repository.
    Accepts a Croissant-format JSON body, converts it to PG-JSON according to
    the MoMa graph schema, and persists the result to Neo4j.

    **Required permission:** realm role `dg_admin` or `dg_dataset-uploader`.
    """
    return await svc.ingest(input_data)
