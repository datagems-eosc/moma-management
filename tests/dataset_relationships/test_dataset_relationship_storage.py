"""
Storage (integration) tests for Neo4jDatasetRelationshipRepository.

All tests in this module share a single Neo4j container (module-scoped).
Tests use uuid4() for node IDs so they are fully independent of each other.
"""

from typing import AsyncGenerator
from uuid import uuid4

import pytest
import pytest_asyncio
from neo4j import AsyncGraphDatabase
from testcontainers.neo4j import Neo4jContainer

from moma_management.domain.dataset_relationship import DatasetRelationship
from moma_management.domain.generated.edges.edge_schema import Edge
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.repository.dataset_relationship import (
    Neo4jDatasetRelationshipRepository,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_relationship(
    ds_id_a: str,
    ds_id_b: str,
    root_id: str | None = None,
) -> DatasetRelationship:
    """Build a minimal valid DatasetRelationship linking the two given datasets."""
    root_id = root_id or str(uuid4())
    pc_id = str(uuid4())
    return DatasetRelationship(
        nodes=[
            Node(id=root_id, labels=["BasicDLElement"],
                 properties={"similarityScore": 42.0}),
            Node(id=ds_id_a, labels=["sc:Dataset"], properties={}),
            Node(id=ds_id_b, labels=["sc:Dataset"], properties={}),
            Node(id=pc_id, labels=["PropertyComparison"],
                 properties={"targetProperty": "keywords"}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": ds_id_a, "labels": ["HAS_TARGET"]}),
            Edge(**{"from": root_id, "to": ds_id_b, "labels": ["HAS_TARGET"]}),
            Edge(**{"from": root_id, "to": pc_id,
                 "labels": ["HAS_COMPARISON"], "properties": {"weight": 0.5}}),
        ],
    )


@pytest_asyncio.fixture(scope="module")
async def relationship_repository(
    neo4j_container_module: Neo4jContainer,
) -> AsyncGenerator[Neo4jDatasetRelationshipRepository, None]:
    """DatasetRelationship repository backed by a module-scoped Neo4j container."""
    uri = neo4j_container_module.get_connection_url()
    auth = (neo4j_container_module.username, neo4j_container_module.password)
    driver = AsyncGraphDatabase.driver(uri, auth=auth)
    async with driver.session() as session:
        yield await Neo4jDatasetRelationshipRepository.create_with_indexes(session)
    await driver.close()


# ---------------------------------------------------------------------------
# create() / get()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_returns_none_for_unknown_id(
    relationship_repository: Neo4jDatasetRelationshipRepository,
):
    """get() returns None when the relationship does not exist."""
    assert await relationship_repository.get(str(uuid4())) is None


@pytest.mark.asyncio
async def test_create_and_get_round_trip(
    relationship_repository: Neo4jDatasetRelationshipRepository,
):
    """A stored relationship must be retrievable with its root and internal nodes intact."""
    ds_a, ds_b = str(uuid4()), str(uuid4())
    rel = _make_relationship(ds_a, ds_b)

    await relationship_repository.create(rel)
    retrieved = await relationship_repository.get(str(rel.root.id))

    assert retrieved is not None
    labels = {frozenset(n.labels) for n in retrieved.nodes}
    assert frozenset(["BasicDLElement"]) in labels
    assert frozenset(["PropertyComparison"]) in labels
    # The referenced sc:Dataset nodes are NOT included (HAS_TARGET is forbidden)
    assert frozenset(["sc:Dataset"]) not in labels

    await relationship_repository.delete(str(rel.root.id))


@pytest.mark.asyncio
async def test_get_does_not_include_target_datasets(
    relationship_repository: Neo4jDatasetRelationshipRepository,
):
    """get() must not traverse into the referenced sc:Dataset nodes."""
    ds_a, ds_b = str(uuid4()), str(uuid4())
    rel = _make_relationship(ds_a, ds_b)

    await relationship_repository.create(rel)
    retrieved = await relationship_repository.get(str(rel.root.id))

    ids = {str(n.id) for n in retrieved.nodes}
    assert ds_a not in ids
    assert ds_b not in ids

    await relationship_repository.delete(str(rel.root.id))


# ---------------------------------------------------------------------------
# delete()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_removes_relationship(
    relationship_repository: Neo4jDatasetRelationshipRepository,
):
    """delete() removes the relationship subgraph entirely."""
    ds_a, ds_b = str(uuid4()), str(uuid4())
    rel = _make_relationship(ds_a, ds_b)

    await relationship_repository.create(rel)
    await relationship_repository.delete(str(rel.root.id))

    assert await relationship_repository.get(str(rel.root.id)) is None


# ---------------------------------------------------------------------------
# find_id_for_dataset_pair()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_find_id_for_dataset_pair_misses_when_none_exists(
    relationship_repository: Neo4jDatasetRelationshipRepository,
):
    """find_id_for_dataset_pair() returns None when no relationship links the pair."""
    found = await relationship_repository.find_id_for_dataset_pair(str(uuid4()), str(uuid4()))
    assert found is None


@pytest.mark.asyncio
async def test_find_id_for_dataset_pair_finds_existing_relationship(
    relationship_repository: Neo4jDatasetRelationshipRepository,
):
    """find_id_for_dataset_pair() finds the relationship regardless of pair order."""
    ds_a, ds_b = str(uuid4()), str(uuid4())
    rel = _make_relationship(ds_a, ds_b)
    await relationship_repository.create(rel)

    found_forward = await relationship_repository.find_id_for_dataset_pair(ds_a, ds_b)
    found_reversed = await relationship_repository.find_id_for_dataset_pair(ds_b, ds_a)

    assert found_forward == str(rel.root.id)
    assert found_reversed == str(rel.root.id)

    await relationship_repository.delete(str(rel.root.id))


# ---------------------------------------------------------------------------
# delete_referencing()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_referencing_removes_matching_relationship_only(
    relationship_repository: Neo4jDatasetRelationshipRepository,
):
    """delete_referencing() removes only relationships that target the given dataset."""
    ds_a, ds_b, ds_c = str(uuid4()), str(uuid4()), str(uuid4())
    rel_ab = _make_relationship(ds_a, ds_b)
    rel_bc = _make_relationship(ds_b, ds_c)
    await relationship_repository.create(rel_ab)
    await relationship_repository.create(rel_bc)

    await relationship_repository.delete_referencing(ds_a)

    assert await relationship_repository.get(str(rel_ab.root.id)) is None
    # The other relationship (not targeting ds_a) must be untouched
    assert await relationship_repository.get(str(rel_bc.root.id)) is not None

    await relationship_repository.delete(str(rel_bc.root.id))


# ---------------------------------------------------------------------------
# list_for_dataset()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_for_dataset_returns_empty_when_none_exist(
    relationship_repository: Neo4jDatasetRelationshipRepository,
):
    """list_for_dataset() returns an empty list when no relationship targets the dataset."""
    assert await relationship_repository.list_for_dataset(str(uuid4())) == []


@pytest.mark.asyncio
async def test_list_for_dataset_returns_only_matching_relationships(
    relationship_repository: Neo4jDatasetRelationshipRepository,
):
    """list_for_dataset() returns every relationship targeting the dataset, and no others."""
    ds_a, ds_b, ds_c = str(uuid4()), str(uuid4()), str(uuid4())
    rel_ab = _make_relationship(ds_a, ds_b)
    rel_ac = _make_relationship(ds_a, ds_c)
    rel_bc = _make_relationship(ds_b, ds_c)
    await relationship_repository.create(rel_ab)
    await relationship_repository.create(rel_ac)
    await relationship_repository.create(rel_bc)

    result = await relationship_repository.list_for_dataset(ds_a)

    result_ids = {str(r.root.id) for r in result}
    assert result_ids == {str(rel_ab.root.id), str(rel_ac.root.id)}

    await relationship_repository.delete(str(rel_ab.root.id))
    await relationship_repository.delete(str(rel_ac.root.id))
    await relationship_repository.delete(str(rel_bc.root.id))
