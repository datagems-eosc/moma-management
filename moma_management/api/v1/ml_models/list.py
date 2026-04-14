from typing import List, Never

from fastapi import Depends

from moma_management.di import get_ml_model_service, require_authentication
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.services.ml_model import MlModelService


async def list_ml_models(
    svc: MlModelService = Depends(get_ml_model_service),
    _auth: Never = Depends(require_authentication()),
) -> List[Node]:
    """Return all ML_Model nodes."""
    return await svc.list()
