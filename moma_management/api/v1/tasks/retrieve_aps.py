from typing import List, Never

from fastapi import Depends

from moma_management.di import get_task_service, require_authentication
from moma_management.services.task import TaskService


async def retrieve_ap_ids(
    id: str,
    svc: TaskService = Depends(get_task_service),
    _auth: Never = Depends(require_authentication()),
) -> List[str]:
    """
    Retrieve the IDs of all AnalyticalPatterns accomplished by the given Task.

    Returns a list of AnalyticalPattern root node IDs.
    """
    return await svc.get_ap_ids(id)
