from typing import Never

from fastapi import Depends

from moma_management.di import IdType, get_ap_service, require_permission
from moma_management.services.analytical_pattern import AnalyticalPatternService
from moma_management.services.authorization import DatasetRole


async def delete_ap(
    id: str,
    svc: AnalyticalPatternService = Depends(get_ap_service),
    _auth: Never = Depends(require_permission(
        DatasetRole.BROWSE, id_type=IdType.AP, require_all=True)),
) -> None:
    """
    Delete an AnalyticalPattern by its root node ID.

    Only the AP root and its Operator nodes are removed; referenced data
    nodes (belonging to datasets) are left intact.

    **Required permission:** ``dg_ds-browse`` on **all** datasets referenced
    by the AP's ``input`` edges, or realm role ``dg_admin`` /
    ``dg_dataset-curator``.
    """
    await svc.delete(id)
