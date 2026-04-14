from typing import Never, Optional

from fastapi import Depends
from pydantic import BaseModel

from moma_management.di import get_ml_model_service, require_admin
from moma_management.services.ml_model import MlModelService


class UpdateMlModelRequest(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None


async def update_ml_model(
    id: str,
    body: UpdateMlModelRequest,
    svc: MlModelService = Depends(get_ml_model_service),
    _auth: Never = Depends(require_admin()),
) -> dict:
    """Update properties of an existing ML_Model node."""
    return await svc.update(id, name=body.name, type=body.type)
