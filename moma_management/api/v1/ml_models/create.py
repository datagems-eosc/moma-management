from typing import Never

from fastapi import Depends
from pydantic import BaseModel

from moma_management.di import get_ml_model_service, require_admin
from moma_management.services.ml_model import MlModelService


class CreateMlModelRequest(BaseModel):
    name: str
    type: str


async def create_ml_model(
    body: CreateMlModelRequest,
    svc: MlModelService = Depends(get_ml_model_service),
    _auth: Never = Depends(require_admin()),
) -> dict:
    """Create a new ML_Model node. Returns the created model's ``id``."""
    node = await svc.create(name=body.name, type=body.type)
    return {"id": str(node.id)}
