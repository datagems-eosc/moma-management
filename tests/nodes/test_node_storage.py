"""
Storage tests for the NodeRepository implementations.
Covers the CRUD lifecycle for a single node in Neo4j.
"""

import pytest

from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.repository.node import Neo4jNodeRepository

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SAMPLE_NODE = Node(
    id="00000000-0000-0000-0000-000000000001",
    labels=["cr:FileObject", "CSV"],
    properties={"name": "sample.csv", "encodingFormat": "text/csv"},
)


# ---------------------------------------------------------------------------
# create / get round-trip
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_and_get(node_repository: Neo4jNodeRepository):
    """Stored node must be retrievable with all labels and properties intact."""
    result = await node_repository.create(_SAMPLE_NODE)
    assert result == "success"

    retrieved = await node_repository.get(_SAMPLE_NODE.id)
    assert retrieved is not None
    assert retrieved.id == _SAMPLE_NODE.id
    assert set(retrieved.labels) == set(_SAMPLE_NODE.labels)
    assert retrieved.properties["name"] == _SAMPLE_NODE.properties["name"]
    assert retrieved.properties["encodingFormat"] == _SAMPLE_NODE.properties["encodingFormat"]


@pytest.mark.asyncio
async def test_get_nonexistent_returns_none(node_repository: Neo4jNodeRepository):
    """get() must return None when the node ID does not exist."""
    assert await node_repository.get("00000000-0000-0000-0000-000000000000") is None


@pytest.mark.asyncio
async def test_create_is_idempotent(node_repository: Neo4jNodeRepository):
    """Calling create() twice for the same node must not raise and must not duplicate it."""
    await node_repository.create(_SAMPLE_NODE)
    result = await node_repository.create(_SAMPLE_NODE)
    assert result == "success"

    # Only one node should exist for this id
    retrieved = await node_repository.get(_SAMPLE_NODE.id)
    assert retrieved is not None
    assert retrieved.id == _SAMPLE_NODE.id


# ---------------------------------------------------------------------------
# update
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_properties(node_repository: Neo4jNodeRepository):
    """update() must merge new properties onto the existing node."""
    await node_repository.create(_SAMPLE_NODE)

    updated_node = Node(
        id=_SAMPLE_NODE.id,
        labels=_SAMPLE_NODE.labels,
        properties={"name": "renamed.csv", "size": 1024},
    )
    outcome = await node_repository.update(updated_node)
    assert outcome["status"] == "success"
    assert outcome["updated"] == 1

    retrieved = await node_repository.get(_SAMPLE_NODE.id)
    assert retrieved is not None
    assert retrieved.properties["name"] == "renamed.csv"
    # Previously stored property should still be present (SET n += merges)
    assert retrieved.properties["encodingFormat"] == "text/csv"
    assert retrieved.properties["size"] == 1024


@pytest.mark.asyncio
async def test_update_nonexistent_returns_zero(node_repository: Neo4jNodeRepository):
    """update() on an unknown ID must report 0 updated nodes."""
    ghost = Node(id="00000000-0000-0000-0000-000000000099",
                 labels=["cr:FileObject"], properties={"x": 1})
    outcome = await node_repository.update(ghost)
    assert outcome["status"] == "success"
    assert outcome["updated"] == 0


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete(node_repository: Neo4jNodeRepository):
    """delete() must remove the node and return 1."""
    await node_repository.create(_SAMPLE_NODE)

    deleted = await node_repository.delete(_SAMPLE_NODE.id)
    assert deleted == 1

    assert await node_repository.get(_SAMPLE_NODE.id) is None


@pytest.mark.asyncio
async def test_delete_nonexistent_returns_zero(node_repository: Neo4jNodeRepository):
    """delete() on an unknown ID must return 0 without raising."""
    assert await node_repository.delete("00000000-0000-0000-0000-000000000000") == 0


@pytest.mark.asyncio
async def test_delete_detaches_relationships(node_repository: Neo4jNodeRepository):
    """Deleting a node that has relationships must succeed (DETACH DELETE)."""
    # We use the repository's underlying session to add a relationship manually
    # and then verify that delete does not raise a ConstraintError.
    node_a = Node(id="00000000-0000-0000-0000-0000000000aa",
                  labels=["cr:FileObject"], properties={})
    node_b = Node(id="00000000-0000-0000-0000-0000000000bb",
                  labels=["cr:RecordSet"], properties={})
    await node_repository.create(node_a)
    await node_repository.create(node_b)

    # Create a relationship directly via cypher
    await node_repository._session.run(
        "MATCH (a {id: $a}), (b {id: $b}) MERGE (a)-[:linksTo]->(b)",
        a=str(node_a.id), b=str(node_b.id),
    )

    deleted = await node_repository.delete(node_a.id)
    assert deleted == 1
    assert await node_repository.get(node_a.id) is None
    # node_b must still exist
    assert await node_repository.get(node_b.id) is not None
