"""
This are test for the reqire_permission function that has many branches.
This assumes remote services are functional, so we mock them to test the logic of the permission check itself.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from moma_management.di import IdType, require_permission
from moma_management.services.authorization import DatasetRole, GatewayError, UserError


def _make_request(path_id: str | None = "ds-123"):
    request = MagicMock()
    request.path_params = {"id": path_id} if path_id else {}
    return request


def _make_credentials(token: str = "tok"):
    return HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)


@pytest.mark.asyncio
async def test_auth_disabled_returns_none():
    check = require_permission(DatasetRole.BROWSE)
    result = await check(
        request=_make_request(),
        credentials=_make_credentials(),
        authentication=None,      # auth disabled
        authorization=None,
        dataset_svc=MagicMock(),
    )
    assert result is None


@pytest.mark.asyncio
async def test_authz_disabled_returns_user():
    auth_svc = MagicMock()
    auth_svc.validate.return_value = {"sub": "user1"}

    check = require_permission(DatasetRole.BROWSE)
    result = await check(
        request=_make_request(),
        credentials=_make_credentials(),
        authentication=auth_svc,
        authorization=None,       # authz disabled
        dataset_svc=MagicMock(),
    )
    assert result == {"sub": "user1"}


@pytest.mark.asyncio
async def test_invalid_token_raises_401():
    from jose import JWTError
    auth_svc = MagicMock()
    auth_svc.validate.side_effect = JWTError("bad token")

    check = require_permission(DatasetRole.BROWSE)
    with pytest.raises(HTTPException) as exc_info:
        await check(
            request=_make_request(),
            credentials=_make_credentials(),
            authentication=auth_svc,
            authorization=MagicMock(),
            dataset_svc=MagicMock(),
        )
    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_permission_granted():
    auth_svc = MagicMock()
    auth_svc.validate.return_value = {"sub": "user1"}
    authz_svc = MagicMock()
    authz_svc.has_dataset_permission.return_value = True

    check = require_permission(DatasetRole.BROWSE)
    result = await check(
        request=_make_request("ds-123"),
        credentials=_make_credentials(),
        authentication=auth_svc,
        authorization=authz_svc,
        dataset_svc=MagicMock(),
    )
    assert result == {"sub": "user1"}
    authz_svc.has_dataset_permission.assert_called_once_with(
        "tok", DatasetRole.BROWSE, "ds-123")


@pytest.mark.asyncio
async def test_permission_denied_raises_403():
    auth_svc = MagicMock()
    auth_svc.validate.return_value = {"sub": "user1"}
    authz_svc = MagicMock()
    authz_svc.has_dataset_permission.return_value = False

    check = require_permission(DatasetRole.BROWSE)
    with pytest.raises(HTTPException) as exc_info:
        await check(
            request=_make_request("ds-123"),
            credentials=_make_credentials(),
            authentication=auth_svc,
            authorization=authz_svc,
            dataset_svc=MagicMock(),
        )
    assert exc_info.value.status_code == 403


@pytest.mark.asyncio
async def test_gateway_error_raises_502():
    auth_svc = MagicMock()
    auth_svc.validate.return_value = {"sub": "user1"}
    authz_svc = MagicMock()
    authz_svc.has_dataset_permission.side_effect = GatewayError("down")

    check = require_permission(DatasetRole.BROWSE)
    with pytest.raises(HTTPException) as exc_info:
        await check(
            request=_make_request("ds-123"),
            credentials=_make_credentials(),
            authentication=auth_svc,
            authorization=authz_svc,
            dataset_svc=MagicMock(),
        )
    assert exc_info.value.status_code == 502


@pytest.mark.asyncio
async def test_node_not_found_raises_404():
    auth_svc = MagicMock()
    auth_svc.validate.return_value = {"sub": "user1"}
    authz_svc = MagicMock()
    dataset_svc = MagicMock()
    dataset_svc.list.return_value = {
        "datasets": []}  # node has no parent datasets

    check = require_permission(DatasetRole.BROWSE, id_type=IdType.Node)
    with pytest.raises(HTTPException) as exc_info:
        await check(
            request=_make_request("node-xyz"),
            credentials=_make_credentials(),
            authentication=auth_svc,
            authorization=authz_svc,
            dataset_svc=dataset_svc,
        )
    assert exc_info.value.status_code == 404
