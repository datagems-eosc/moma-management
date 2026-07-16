"""
This are unit tests for the reqire_permission function that has many branches.
This assumes remote services are functional, so we mock them to test the logic of the permission check itself.
"""
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from moma_management.domain.exceptions import NotFoundError
from moma_management.middlewares.auth import (
    IdType,
    require_admin,
    require_authentication,
    require_permission,
)
from moma_management.services.authorization import DatasetRole, GatewayError


def _make_request(path_id: str | None = "ds-123"):
    request = MagicMock()
    request.path_params = {"id": path_id} if path_id else {}
    return request


def _make_credentials(token: str = "tok"):
    return HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)


# ---------------------------------------------------------------------------
# require_authentication
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_require_authentication_disabled_returns_none():
    check = require_authentication()
    result = await check(
        credentials=_make_credentials(),
        authentication=None,
    )
    assert result is None


@pytest.mark.asyncio
async def test_require_authentication_valid_token_returns_user():
    auth_svc = MagicMock()
    auth_svc.validate.return_value = {"sub": "user1"}

    check = require_authentication()
    result = await check(
        credentials=_make_credentials(),
        authentication=auth_svc,
    )
    assert result == {"sub": "user1"}


@pytest.mark.asyncio
async def test_require_authentication_invalid_token_raises_401():
    from jose import JWTError
    auth_svc = MagicMock()
    auth_svc.validate.side_effect = JWTError("bad token")

    check = require_authentication()
    with pytest.raises(HTTPException) as exc_info:
        await check(
            credentials=_make_credentials(),
            authentication=auth_svc,
        )
    assert exc_info.value.status_code == 401


# ---------------------------------------------------------------------------
# require_permission
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_auth_disabled_returns_none():
    check = require_permission(DatasetRole.BROWSE)
    result = await check(
        request=_make_request(),
        credentials=_make_credentials(),
        user=None,            # auth disabled
        authorization=None,
        dataset_svc=MagicMock(),
    )
    assert result is None


@pytest.mark.asyncio
async def test_authz_disabled_returns_user():
    check = require_permission(DatasetRole.BROWSE)
    result = await check(
        request=_make_request(),
        credentials=_make_credentials(),
        user={"sub": "user1"},
        authorization=None,   # authz disabled
        dataset_svc=MagicMock(),
    )
    assert result == {"sub": "user1"}


@pytest.mark.asyncio
async def test_permission_granted():
    authz_svc = MagicMock()
    authz_svc.has_realm_roles = AsyncMock(return_value=False)
    authz_svc.has_dataset_grant = AsyncMock(return_value=True)

    check = require_permission(DatasetRole.BROWSE)
    result = await check(
        request=_make_request("ds-123"),
        credentials=_make_credentials(),
        user={"sub": "user1"},
        authorization=authz_svc,
        dataset_svc=MagicMock(),
    )
    assert result == {"sub": "user1"}
    authz_svc.has_dataset_grant.assert_called_once_with(
        "tok", DatasetRole.BROWSE, "ds-123")


@pytest.mark.asyncio
async def test_permission_denied_raises_403():
    authz_svc = MagicMock()
    authz_svc.has_realm_roles = AsyncMock(return_value=False)
    authz_svc.has_dataset_grant = AsyncMock(return_value=False)

    check = require_permission(DatasetRole.BROWSE)
    with pytest.raises(HTTPException) as exc_info:
        await check(
            request=_make_request("ds-123"),
            credentials=_make_credentials(),
            user={"sub": "user1"},
            authorization=authz_svc,
            dataset_svc=MagicMock(),
        )
    assert exc_info.value.status_code == 403


@pytest.mark.asyncio
async def test_gateway_error_raises_502():
    authz_svc = MagicMock()
    authz_svc.has_realm_roles = AsyncMock(return_value=False)
    authz_svc.has_dataset_grant = AsyncMock(side_effect=GatewayError("down"))

    check = require_permission(DatasetRole.BROWSE)
    with pytest.raises(HTTPException) as exc_info:
        await check(
            request=_make_request("ds-123"),
            credentials=_make_credentials(),
            user={"sub": "user1"},
            authorization=authz_svc,
            dataset_svc=MagicMock(),
        )
    assert exc_info.value.status_code == 502


@pytest.mark.asyncio
async def test_node_not_found_raises_404():
    authz_svc = MagicMock()
    dataset_svc = AsyncMock()
    dataset_svc.list.return_value = {
        "datasets": []}  # node has no parent datasets

    check = require_permission(DatasetRole.BROWSE, id_type=IdType.Node)
    with pytest.raises(HTTPException) as exc_info:
        await check(
            request=_make_request("node-xyz"),
            credentials=_make_credentials(),
            user={"sub": "user1"},
            authorization=authz_svc,
            dataset_svc=dataset_svc,
        )
    assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# require_permission — IdType.Relationship
# ---------------------------------------------------------------------------

def _make_relationship_result(ds_id_a: str, ds_id_b: str):
    result = MagicMock()
    result.target_dataset_ids = (ds_id_a, ds_id_b)
    return result


@pytest.mark.asyncio
async def test_relationship_not_found_raises_404():
    """A missing relationship (NotFoundError from the service) must surface as 404."""
    authz_svc = MagicMock()
    relationship_svc = AsyncMock()
    relationship_svc.get.side_effect = NotFoundError("not found")

    check = require_permission(
        DatasetRole.BROWSE, id_type=IdType.Relationship, require_all=True)
    with pytest.raises(NotFoundError):
        await check(
            request=_make_request("rel-xyz"),
            credentials=_make_credentials(),
            user={"sub": "user1"},
            authorization=authz_svc,
            dataset_svc=MagicMock(),
            relationship_svc=relationship_svc,
        )


@pytest.mark.asyncio
async def test_relationship_permission_granted_when_all_datasets_allowed():
    """require_all=True must grant access when the caller can BROWSE both linked datasets."""
    authz_svc = MagicMock()
    authz_svc.has_realm_roles = AsyncMock(return_value=False)
    authz_svc.has_dataset_grant = AsyncMock(return_value=True)

    relationship_svc = AsyncMock()
    relationship_svc.get.return_value = _make_relationship_result("ds-a", "ds-b")

    check = require_permission(
        DatasetRole.BROWSE, id_type=IdType.Relationship, require_all=True)
    result = await check(
        request=_make_request("rel-xyz"),
        credentials=_make_credentials(),
        user={"sub": "user1"},
        authorization=authz_svc,
        dataset_svc=MagicMock(),
        relationship_svc=relationship_svc,
    )
    assert result == {"sub": "user1"}
    assert authz_svc.has_dataset_grant.await_count == 2


@pytest.mark.asyncio
async def test_relationship_permission_denied_when_one_dataset_lacks_grant():
    """require_all=True must deny access when the caller lacks BROWSE on either dataset."""
    authz_svc = MagicMock()
    authz_svc.has_realm_roles = AsyncMock(return_value=False)
    # Grant on the first dataset only
    authz_svc.has_dataset_grant = AsyncMock(side_effect=[True, False])

    relationship_svc = AsyncMock()
    relationship_svc.get.return_value = _make_relationship_result("ds-a", "ds-b")

    check = require_permission(
        DatasetRole.BROWSE, id_type=IdType.Relationship, require_all=True)
    with pytest.raises(HTTPException) as exc_info:
        await check(
            request=_make_request("rel-xyz"),
            credentials=_make_credentials(),
            user={"sub": "user1"},
            authorization=authz_svc,
            dataset_svc=MagicMock(),
            relationship_svc=relationship_svc,
        )
    assert exc_info.value.status_code == 403


# ---------------------------------------------------------------------------
# require_admin
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_require_admin_disabled_returns_none():
    check = require_admin()
    result = await check(
        credentials=_make_credentials(),
        authentication=None,
        authorization=MagicMock(),
    )
    assert result is None


@pytest.mark.asyncio
async def test_require_admin_authz_disabled_returns_user():
    auth_svc = MagicMock()
    auth_svc.validate.return_value = {"sub": "user1"}

    check = require_admin()
    result = await check(
        credentials=_make_credentials(),
        authentication=auth_svc,
        authorization=None,
    )
    assert result == {"sub": "user1"}


@pytest.mark.asyncio
async def test_require_admin_granted_for_admin_role():
    auth_svc = MagicMock()
    auth_svc.validate.return_value = {"sub": "user1"}
    authz_svc = MagicMock()
    authz_svc.has_realm_roles = AsyncMock(return_value=True)

    check = require_admin()
    result = await check(
        credentials=_make_credentials(),
        authentication=auth_svc,
        authorization=authz_svc,
    )
    assert result == {"sub": "user1"}


@pytest.mark.asyncio
async def test_require_admin_denied_raises_403_for_non_admin():
    auth_svc = MagicMock()
    auth_svc.validate.return_value = {"sub": "user1"}
    authz_svc = MagicMock()
    authz_svc.has_realm_roles = AsyncMock(return_value=False)

    check = require_admin()
    with pytest.raises(HTTPException) as exc_info:
        await check(
            credentials=_make_credentials(),
            authentication=auth_svc,
            authorization=authz_svc,
        )
    assert exc_info.value.status_code == 403
