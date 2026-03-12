import logging
from enum import Enum

import requests
from fastapi import HTTPException

logger = logging.getLogger(__name__)


class DatasetAction(str, Enum):
    """Action verbs that can be granted/denied on a specific dataset."""
    browse = "browse"
    delete = "delete"
    download = "download"
    edit = "edit"
    search = "search"
    manage = "manage"


class AuthorizationService:
    """
    This service checks dataset-level permissions by querying an external gateway.
    """

    def __init__(self, gateway_url: str = "") -> None:
        self._gateway_url = gateway_url

    def check(self, dataset_id: str, action: DatasetAction, token: str) -> None:
        """Raise ``HTTPException`` when *action* is not permitted.

        Parameters
        ----------
        dataset_id:
            The dataset identifier extracted from the request path.
        action:
            The action verb to verify.
        token:
            The raw Bearer token forwarded to the gateway.

        Raises
        ------
        HTTPException 403  Action not in the resolved permission set.
        HTTPException 404  Dataset not found (gateway returned 404).
        HTTPException 502  Gateway unreachable or returned an unexpected error.
        """

        try:
            resp = requests.get(
                f"{self._gateway_url}/datasets/{dataset_id}/permissions",
                headers={"Authorization": f"Bearer {token}"}
            )
        except requests.RequestException as exc:
            logger.error("Permission gateway unreachable: %s", exc)
            raise HTTPException(
                status_code=502, detail="Permission gateway unavailable")

        if resp.status_code == 404:
            raise HTTPException(
                status_code=404, detail=f"Dataset '{dataset_id}' not found")
        if not resp.ok:
            raise HTTPException(
                status_code=502, detail="Permission gateway error")

        allowed: list[str] = resp.json().get("allowed", [])
        if action not in allowed:
            raise HTTPException(
                status_code=403,
                detail=f"Action '{action}' not permitted on dataset '{dataset_id}'",
            )
