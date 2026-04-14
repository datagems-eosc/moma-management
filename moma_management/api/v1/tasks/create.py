from typing import Never

from fastapi import Depends
from pydantic import BaseModel

from moma_management.di import get_task_service, require_authentication
from moma_management.services.task import TaskService


class CreateTaskRequest(BaseModel):
    name: str
    description: str


async def create_task(
    body: CreateTaskRequest,
    svc: TaskService = Depends(get_task_service),
    _auth: Never = Depends(require_authentication()),
) -> dict:
    """
    Create a new Task node.

    Returns the created Task's ``id``.
    """
    task = await svc.create(name=body.name, description=body.description)
    return {"id": str(task.id)}
