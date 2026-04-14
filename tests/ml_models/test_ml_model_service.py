"""
Unit tests for MlModelService (MagicMock — no Neo4j container).
"""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from moma_management.domain.exceptions import ConflictError, NotFoundError
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.services.ml_model import MlModelService

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ml_node(name: str = "test-model", type_: str = "classification") -> Node:
    return Node(id=uuid4(), labels=["ML_Model"], properties={"name": name, "type": type_})


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_returns_node():
    repo = AsyncMock()
    repo.create.side_effect = lambda n: n
    svc = MlModelService(repo)

    result = await svc.create(name="my-model", type="LLM")

    assert result.properties["name"] == "my-model"
    assert result.properties["type"] == "LLM"
    assert "ML_Model" in result.labels
    repo.create.assert_called_once()


# ---------------------------------------------------------------------------
# get
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_returns_node_when_found():
    node = _make_ml_node()
    repo = AsyncMock()
    repo.get.return_value = node
    svc = MlModelService(repo)

    result = await svc.get(str(node.id))
    assert str(result.id) == str(node.id)


@pytest.mark.asyncio
async def test_get_raises_not_found():
    repo = AsyncMock()
    repo.get.return_value = None
    svc = MlModelService(repo)

    with pytest.raises(NotFoundError):
        await svc.get(str(uuid4()))


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_returns_all_models():
    nodes = [_make_ml_node("a"), _make_ml_node("b")]
    repo = AsyncMock()
    repo.list.return_value = nodes
    svc = MlModelService(repo)

    result = await svc.list()
    assert len(result) == 2


# ---------------------------------------------------------------------------
# update
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_applies_partial_changes():
    existing = _make_ml_node("old-name", "old-type")
    repo = AsyncMock()
    repo.get.return_value = existing
    repo.update.return_value = {"status": "success", "updated": 1}
    svc = MlModelService(repo)

    result = await svc.update(str(existing.id), name="new-name")
    assert result["updated"] == 1
    # Verify that the node passed to update has the new name but keeps old type
    call_node = repo.update.call_args[0][0]
    assert call_node.properties["name"] == "new-name"
    assert call_node.properties["type"] == "old-type"


@pytest.mark.asyncio
async def test_update_raises_not_found_when_missing():
    repo = AsyncMock()
    repo.get.return_value = None
    svc = MlModelService(repo)

    with pytest.raises(NotFoundError):
        await svc.update(str(uuid4()), name="x")


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_succeeds_when_no_references():
    node = _make_ml_node()
    repo = AsyncMock()
    repo.get.return_value = node
    repo.has_referencing_aps.return_value = False
    repo.delete.return_value = 1
    svc = MlModelService(repo)

    await svc.delete(str(node.id))  # Should not raise
    repo.delete.assert_called_once_with(str(node.id))


@pytest.mark.asyncio
async def test_delete_raises_conflict_when_referenced_by_ap():
    node = _make_ml_node()
    repo = AsyncMock()
    repo.get.return_value = node
    repo.has_referencing_aps.return_value = True
    svc = MlModelService(repo)

    with pytest.raises(ConflictError, match="referenced by at least one analytical pattern"):
        await svc.delete(str(node.id))

    repo.delete.assert_not_called()


@pytest.mark.asyncio
async def test_delete_raises_not_found_when_missing():
    repo = AsyncMock()
    repo.get.return_value = None
    svc = MlModelService(repo)

    with pytest.raises(NotFoundError):
        await svc.delete(str(uuid4()))
