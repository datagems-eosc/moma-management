from typing import Never

from fastapi import Depends

from moma_management.di import get_ml_model_service, require_authentication
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.services.ml_model import MlModelService


async def get_ml_model(
    id: str,
    svc: MlModelService = Depends(get_ml_model_service),
    _auth: Never = Depends(require_authentication()),
) -> Node:
    """Retrieve a single ML_Model by its ID."""
    return svc.get(id)
