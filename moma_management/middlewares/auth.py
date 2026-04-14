import logging
from enum import Enum
from typing import AsyncGenerator, List, Optional

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError

from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.domain.filters import DatasetFilter, DatasetProperty
from moma_management.services.analytical_pattern import AnalyticalPatternService
from moma_management.services.authentication import Authentication
from moma_management.services.authorization import (
    DatagemsAuthorizationService,
    DatasetRole,
    GatewayError,
    RealmRole,
    UserError,
)
from moma_management.services.dataset import DatasetService

logger = logging.getLogger(__name__)

bearer_scheme = HTTPBearer(auto_error=False)


def _authenticate(
    credentials: HTTPAuthorizationCredentials | None,
    authentication: Authentication,
) -> tuple[str, dict]:
    """Validate bearer credentials and return (raw_token, JWT claims). Raises 401 on failure."""
    if credentials is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    token = credentials.credentials
    try:
        return token, authentication.validate(token)
    except JWTError as exc:
        logger.warning("JWT validation failed: %s", exc)
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    except Exception:
        logger.exception("Unexpected error during token validation")
        raise HTTPException(status_code=401, detail="Token validation failed")


def _exchange(authentication: Authentication, token: str) -> str:
    """Exchange *token* for a dg-app-api scoped token. Raises 502 on failure."""
    try:
        return authentication.exchange_token(token)
    except Exception:
        logger.exception("Token exchange failed")
        raise HTTPException(status_code=502, detail="Token exchange failed")


def require_authentication():
    """Validate the caller's bearer token without enforcing any RBAC role.

    Returns the JWT claims dict when authentication is enabled, ``None`` when
    it is disabled (``OIDC_ISSUER`` not set).
    """
    from moma_management.di import get_authentication_service

    async def _check(
        credentials: HTTPAuthorizationCredentials | None = Depends(
            bearer_scheme),
        authentication: Optional[Authentication] = Depends(
            get_authentication_service),
    ) -> dict | None:
        if authentication is None:
            return None
        _, user = _authenticate(credentials, authentication)
        return user

    return _check


class IdType(str, Enum):
    """Describes what kind of resource the ``id`` path parameter refers to."""
    Dataset = "Dataset"
    Node = "Node"
    AP = "AP"


def require_permission(
    action: DatasetRole,
    *,
    id_type: IdType = IdType.Dataset,
    require_all: bool = False,
):
    """Validate the caller's token and enforce *action*.

    ``id_type=IdType.Dataset`` (default): the ``id`` path parameter is a
    dataset ID and is passed directly to the permissions gateway.  A missing ID
    is only acceptable for ``DatasetRole.CREATE``.

    ``id_type=IdType.Node``: the ``id`` path parameter is a node ID.  The
    parent dataset(s) are resolved first via a subgraph lookup, and the caller
    must hold *action* on at least one of them.  404 (rather than 403) is
    returned on denial to avoid leaking whether the node lives in an
    inaccessible dataset.

    ``id_type=IdType.AP``: the ``id`` path parameter is an AnalyticalPattern
    root node ID.  The parent dataset(s) are resolved via the AP's ``input``
    edges, and the caller must hold *action* on at least one of them.

    ``require_all``: when ``True``, the caller must hold *action* on **all**
    resolved datasets, not just one.
    """
    from moma_management.di import (
        get_ap_service,
        get_authorization_service,
        get_dataset_service,
    )

    async def _check(
        request: Request,
        credentials: HTTPAuthorizationCredentials | None = Depends(
            bearer_scheme),
        user: dict | None = Depends(require_authentication()),
        authorization: Optional[DatagemsAuthorizationService] = Depends(
            get_authorization_service
        ),
        dataset_svc: DatasetService = Depends(get_dataset_service),
        ap_svc: AnalyticalPatternService = Depends(get_ap_service),
    ) -> dict | None:

        # Auth disabled -> Skip
        if user is None:
            return None

        # Safe: require_authentication() already validated credentials, so
        # credentials is non-None here.
        token = credentials.credentials

        # Authorization disabled -> Skip grants
        if authorization is None:
            return user

        # Now, we have to know if the user can perform the requested action.
        # A permission can be either asked for datasets or nodes within datasets
        path_id = request.path_params.get("id")
        dataset_ids = []
        match id_type:
            # For nodes, we needs to check the dataset they belongs to
            case IdType.Node:
                result = await dataset_svc.list(DatasetFilter(
                    nodeIds=[path_id], properties=[DatasetProperty.ID]))
                dataset_ids = [
                    n.id
                    for d in result.get("datasets", [])
                    for n in d.nodes
                    if "sc:Dataset" in n.labels
                ]
                if not dataset_ids:
                    raise HTTPException(
                        status_code=404, detail=f"Node '{path_id}' not found."
                    )

            # For dataset, we can retrieve the ID from the request path.
            # The only exception is for dataset creation
            case IdType.Dataset:
                if not path_id and action != DatasetRole.CREATE:
                    raise ValueError("Dataset ID not found in path parameters")
                dataset_ids = [path_id]

            # For APs, resolve parent datasets via the AP's input edges.
            # Returns 404 to prevent enumeration of inaccessible APs.
            case IdType.AP:
                # NotFoundError -> 404 via app handler
                ap_result = await ap_svc.get(path_id)
                input_node_ids = [
                    str(e.to)
                    for e in (ap_result.edges or [])
                    if "input" in e.labels
                ]
                if not input_node_ids:
                    # AP has no input constraint → no dataset to check, grant access
                    return user
                result = await dataset_svc.list(DatasetFilter(
                    nodeIds=input_node_ids, properties=[DatasetProperty.ID]))
                dataset_ids = [
                    str(n.id)
                    for d in result.get("datasets", [])
                    for n in d.nodes
                    if "sc:Dataset" in n.labels
                ]
                if not dataset_ids:
                    raise HTTPException(
                        status_code=404,
                        detail=f"AnalyticalPattern '{path_id}' not found.",
                    )

            case _:
                raise ValueError("Wrong auth type")

        # Now, we query the authorization gateway to make sure the user has the correct permission
        # on the requested datasets

        # NOTE: This is kept but commented out in case we have to do the token exchange again
        # gw_token = _exchange(authentication, token)
        try:
            # Short-circuit: if user holds admin/curator/system realm role,
            # they have permission on all datasets — skip per-dataset checks.
            if await authorization.has_realm_roles(
                token, [RealmRole.ADMIN, RealmRole.CURATOR, RealmRole.SYSTEM]
            ):
                return user

            # NOTE: This consider that having acess to ONE dataset is enough to access the node, even if it belongs to other datasets.
            # For now I don't know if it's possible to have a node belonging to multiple datasets, but if it is the case,
            # we might want to enforce permissions on ALL parent datasets instead of just one, at least for non-idempotent actions
            #
            # Realm roles were already checked above, so call has_dataset_grant
            # directly to avoid a redundant HTTP round-trip.
            check = all if require_all else any
            results = [await authorization.has_dataset_grant(token, action, ds_id)
                       for ds_id in dataset_ids]
            allowed = check(results)
        except UserError as exc:
            logger.warning(
                "Authorization failed: %s %s: %s",
                exc.status_code,
                exc.text,
            )
            raise HTTPException(
                status_code=exc.status_code, detail=f"Gateway error. {exc.text}"
            )
        except GatewayError:
            raise HTTPException(
                status_code=502,
                detail="Permission gateway unavailable. Please see logs for details.",
            )

        if not allowed:
            raise HTTPException(
                status_code=403, detail="Forbidden: insufficient permissions or not a dataset id"
            )

        return user

    return _check


def require_browse_for_ap_creation():
    """Check that the caller can BROWSE each dataset referenced by the new AP's input edges.

    This differs from ``require_permission`` in that the dataset IDs are
    derived from the request *body* (the AP's ``input`` edges) rather than
    a path parameter.
    """
    from moma_management.di import (
        get_authentication_service,
        get_authorization_service,
        get_dataset_service,
    )

    async def _check(
        candidate: AnalyticalPattern,
        credentials: HTTPAuthorizationCredentials | None = Depends(
            bearer_scheme),
        authentication: Optional[Authentication] = Depends(
            get_authentication_service),
        authorization: Optional[DatagemsAuthorizationService] = Depends(
            get_authorization_service
        ),
        dataset_svc: DatasetService = Depends(get_dataset_service),
    ) -> dict | None:
        if authentication is None:
            return None

        token, user = _authenticate(credentials, authentication)

        if authorization is None:
            return user

        input_node_ids = [
            str(e.to)
            for e in (candidate.edges or [])
            if "input" in e.labels
        ]
        if not input_node_ids:
            # No input references → no dataset constraint, grant access
            return user

        result = await dataset_svc.list(DatasetFilter(
            nodeIds=input_node_ids, properties=[DatasetProperty.ID]))
        dataset_ids = [
            str(n.id)
            for d in result.get("datasets", [])
            for n in d.nodes
            if "sc:Dataset" in n.labels
        ]
        if not dataset_ids:
            raise HTTPException(
                status_code=404,
                detail="Referenced input dataset(s) not found.",
            )

        try:
            if await authorization.has_realm_roles(
                token, [RealmRole.ADMIN, RealmRole.CURATOR, RealmRole.SYSTEM]
            ):
                return user

            # Realm roles already checked above — use has_dataset_grant
            # directly to avoid a redundant HTTP round-trip.
            results = [await authorization.has_dataset_grant(
                token, DatasetRole.BROWSE, ds_id)
                for ds_id in dataset_ids]
            allowed = all(results)
        except UserError as exc:
            logger.warning("Authorization failed: %s %s",
                           exc.status_code, exc.text)
            raise HTTPException(
                status_code=exc.status_code, detail=f"Gateway error. {exc.text}"
            )
        except GatewayError:
            raise HTTPException(
                status_code=502,
                detail="Permission gateway unavailable. Please see logs for details.",
            )

        if not allowed:
            raise HTTPException(
                status_code=403,
                detail="Forbidden: cannot browse referenced dataset.",
            )
        return user

    return _check


def require_admin():
    """Validate the caller's token and enforce admin-level realm roles.

    Grants access to users with ``ADMIN``, ``CURATOR``, or ``SYSTEM``
    realm roles.  Returns the JWT claims dict when authorised, ``None``
    when authentication is disabled.

    Raises:
        HTTPException 401: missing or invalid token.
        HTTPException 403: authenticated but lacks admin realm role.
    """
    from moma_management.di import get_authentication_service, get_authorization_service

    async def _check(
        credentials: HTTPAuthorizationCredentials | None = Depends(
            bearer_scheme),
        authentication: Optional[Authentication] = Depends(
            get_authentication_service),
        authorization: Optional[DatagemsAuthorizationService] = Depends(
            get_authorization_service
        ),
    ) -> dict | None:
        if authentication is None:
            return None

        token, user = _authenticate(credentials, authentication)

        if authorization is None:
            return user

        try:
            if not await authorization.has_realm_roles(
                token, [RealmRole.ADMIN, RealmRole.CURATOR, RealmRole.SYSTEM]
            ):
                raise HTTPException(
                    status_code=403,
                    detail="Forbidden: admin role required.",
                )
        except GatewayError:
            raise HTTPException(
                status_code=502,
                detail="Permission gateway unavailable. Please see logs for details.",
            )
        except UserError as exc:
            raise HTTPException(
                status_code=exc.status_code, detail=f"Gateway error. {exc.text}"
            )

        return user

    return _check


def get_allowed_datasets_ids() -> AsyncGenerator[List[str]]:
    """Authenticate the caller and return the dataset IDs they are allowed to browse.

    Returns ``None`` when authentication is disabled (no filtering applied).
    Returns an empty list when authentication is enabled but the gateway reports
    no accessible datasets, effectively returning an empty listing.
    """
    from moma_management.di import get_authentication_service, get_authorization_service

    async def _check(
        credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
        authentication: Optional[Authentication] = Depends(
            get_authentication_service),
        authorization: Optional[DatagemsAuthorizationService] = Depends(
            get_authorization_service
        ),
    ) -> list[str] | None:

        if authentication is None:
            return None  # Auth disabled – return everything
        token, _ = _authenticate(credentials, authentication)

        # gateway_token = _exchange(authentication, token)
        if authorization is None:
            return None  # Authz disabled – RBAC disabled

        try:
            return await authorization.get_browseable_dataset_ids(token)
        except GatewayError:
            raise HTTPException(
                status_code=502, detail="Permission gateway unavailable"
            )
        except UserError as exc:
            logger.warning(
                "Authorization failed: %s %s: %s",
                exc.status_code,
                exc.text,
            )
            raise HTTPException(
                status_code=exc.status_code, detail=f"Gateway error. {exc.text}"
            )

    return _check
