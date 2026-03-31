import logging
from enum import Enum
from typing import List

import requests

logger = logging.getLogger(__name__)


class DatasetRole(str, Enum):
    """Action verbs that can be granted/denied on a specific dataset."""
    #  Users with this access level can browse the dataset metadata
    BROWSE = "dg_ds-browse"
    #  Users with this access level can browse the dataset metadata
    DELETE = "dg_ds-delete"
    # Users with this access level can download the dataset
    DOWNLOAD = "dg_ds-download"
    # Users with this access level can edit the dataset metadata
    EDIT = "dg_ds-edit"
    # Users with this access level can manage the dataset and share it with others
    SEARCH = "dg_ds-search"
    # Users with this access level can perform content based search in the dataset
    MANAGE = "dg_ds-manage"
    # User can create new dataset.
    # This is a special role that is not assigned to any dataset but to users that can create datasets.
    # The check for this role is handled separately
    CREATE = "THIS-ROLE-DOES-NOT-EXISTS"


class RealmRole(str, Enum):
    """Realm-level roles that can be granted/denied to users."""
    # Users with this role are considered administrators and have full access to all datasets, regardless of dataset-specific grants.
    ADMIN = "GLOBAL_dg_admin"
    # User can upload new dataset
    UPLOADER = "GLOBAL_dg_dataset-uploader"
    # Reserved and assigned to users that should be able to process datasets available in the platform.
    CURATOR = "GLOBAL_dg_dataset-curator"


class GatewayError(Exception):
    """Raised when the permissions gateway is unreachable or returns an unexpected error."""
    pass


class UserError(Exception):
    def __init__(self, response: requests.Response):
        self.text = response.text
        self.status_code = response.status_code
        super().__init__(f"{self.status_code}: {self.text}")


class DatagemsAuthorizationService:

    def __init__(self, gateway_url: str) -> None:
        self._gateway_url = gateway_url.rstrip("/")

    def has_realm_roles(self, who_token: str, any_of: List[RealmRole]) -> bool:
        """
        Returns True if the user has any of the specified realm roles, False if not.
        Raises GatewayError if the Gateway is not available, UserError if the gateway returns an error
        """
        try:
            resp = requests.get(
                f"{self._gateway_url}/api/principal/me",
                headers={"Authorization": f"Bearer {who_token}"},
                timeout=10,
            )
        except requests.RequestException as exc:
            logger.error("Permission gateway unreachable: %s", exc)
            raise GatewayError(exc)

        if not resp.ok:
            raise UserError(resp)

        # The gateway returns a JSON object like {"dataset_id": ["role1", "role2", ...]}
        grants: dict[str, list[str]] = resp.json()
        return any(r == role.value for r in grants.get("roles", []) for role in any_of)

    def has_dataset_permission(self, who_token: str, what: DatasetRole, on_which_id: str) -> bool:
        """
        Check if *who* can perform *what* on *on_which_id* by querying the permissions gateway.
        Arguments:
        who_token: The access token of the user
        what: The action to check
        on_which_id: The ID of the dataset

        Returns:
            True if the user has the required grant, False if not.

        Raises:
            GatewayErrir if the Gateway is not available
        """
        if what == DatasetRole.CREATE:
            # The CREATE role is a special case, as it is not assigned to any dataset but to users that can create datasets.
            return self.has_realm_roles(who_token, [RealmRole.ADMIN, RealmRole.UPLOADER])

        return self.has_realm_roles(who_token, [RealmRole.ADMIN, RealmRole.CURATOR]) or self._has_dataset_grant(who_token, what, on_which_id)

    def _has_dataset_grant(self, who_token: str, what: DatasetRole, on_which_id: str) -> bool:
        """
        Check if *who* can perform *what* on *on_which_id* by querying the permissions gateway.
        Arguments:
        who_token: The access token of the user
        what: The action to check
        on_which_id: The ID of the dataset

        Returns:
            True if the user has the required grant, False if not.

        Raises:
            GatewayErrir if the Gateway is not available
        """

        try:
            resp = requests.get(
                f"{self._gateway_url}/api/principal/me/context-grants/dataset",
                params={"id": on_which_id},
                headers={"Authorization": f"Bearer {who_token}"},
                timeout=10,
            )
        except requests.RequestException as exc:
            logger.error("Permission gateway unreachable: %s", exc)
            raise GatewayError(exc)

        # A 404 means the dataset doesn't exists. This is not exposed to prevent enumeration of dataset ids
        if resp.status_code == 404:
            return False

        if not resp.ok:
            raise UserError(resp)

        # The gateway returns a JSON object like {"dataset_id": ["role1", "role2", ...]}
        grants: dict[str, list[str]] = resp.json()
        return any(role == what.value for role in grants.get(on_which_id, []))

    def get_browseable_dataset_ids(self, who_token: str) -> List[str] | None:
        """
        Returns all the dataset IDs the user has access to, or None if the
        user holds ADMIN / CURATOR realm roles (meaning they can see everything).
        """
        if self.has_realm_roles(who_token, [RealmRole.ADMIN, RealmRole.CURATOR]):
            return None

        try:
            resp = requests.get(
                f"{self._gateway_url}/api/principal/me/context-grants",
                headers={
                    "Authorization": f"Bearer {who_token}"
                }
            )
        except requests.RequestException as exc:
            logger.error("Permission gateway unreachable: %s", exc)
            raise GatewayError(exc)

        if not resp.ok:
            logger.error("Permission gateway returned %s: %s",
                         resp.status_code, resp.text)
            raise UserError(resp)

        permissions = resp.json()
        if not permissions or not isinstance(permissions, list):
            return []

        browseable = filter(
            lambda grant: grant.get("role") == DatasetRole.BROWSE.value,
            permissions
        )
        return [grant.get("targetId") for grant in browseable if grant.get("targetId")]
