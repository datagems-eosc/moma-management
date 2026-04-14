from typing import Never

from fastapi import Depends

from moma_management.di import get_ap_service
from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.middlewares.auth import IdType, require_permission
from moma_management.services.analytical_pattern import AnalyticalPatternService
from moma_management.services.authorization import DatasetRole


async def get_ap(
    id: str,
    svc: AnalyticalPatternService = Depends(get_ap_service),
    _auth: Never = Depends(require_permission(
        DatasetRole.BROWSE, id_type=IdType.AP)),
) -> AnalyticalPattern:
    """
    Retrieve an AnalyticalPattern (shallow) by its root node ID.

    Only the root node, its Operator nodes, and the first-level Data/User
    nodes reachable from the operators are returned.  The full dataset
    subgraph is **not** recursed into.

    **Required permission:** ``dg_ds-browse`` on the referenced input
    dataset, or realm role ``dg_admin`` / ``dg_dataset-curator``.
    Returns 404 on permission denial to prevent enumeration.
    """
    return await svc.get(id)
