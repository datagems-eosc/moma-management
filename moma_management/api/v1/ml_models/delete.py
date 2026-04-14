from typing import Never

from fastapi import Depends

from moma_management.di import get_ml_model_service, require_admin
from moma_management.services.ml_model import MlModelService


async def delete_ml_model(
    id: str,
    svc: MlModelService = Depends(get_ml_model_service),
    _auth: Never = Depends(require_admin()),
) -> None:
    """Delete an ML_Model by its ID.

    Returns 409 Conflict if the model is referenced by an analytical pattern.
    """
    await svc.delete(id)
