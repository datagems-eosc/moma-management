"""
Storage (integration) tests for Neo4jAnalyticalPatternRepository.

Each test spins up a fresh Neo4j container (function-scoped) so tests are
fully independent.
"""

from typing import Generator
from uuid import uuid4

import pytest
from neo4j import GraphDatabase
from testcontainers.neo4j import Neo4jContainer

from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.domain.generated.edges.edge_schema import Edge
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.repository.analytical_pattern import (
    Neo4jAnalyticalPatternRepository,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ap(
    root_id: str | None = None,
    operator_id: str | None = None,
    data_id: str | None = None,
) -> AnalyticalPattern:
    """Build a minimal valid AnalyticalPattern with one operator and one data node."""
    root_id = root_id or str(uuid4())
    operator_id = operator_id or str(uuid4())
    data_id = data_id or str(uuid4())
    return AnalyticalPattern(
        nodes=[
            Node(
                id=root_id,
                labels=["Analytical_Pattern"],
                properties={"name": "test-ap", "description": "A test AP"},
            ),
            Node(
                id=operator_id,
                labels=["Operator"],
                properties={"name": "test-operator"},
            ),
            Node(
                id=data_id,
                labels=["Data"],
                properties={"name": "input-data"},
            ),
        ],
        edges=[
            Edge(**{"from": root_id, "to": operator_id, "labels": ["consist_of"]}),
            Edge(**{"from": operator_id, "to": data_id, "labels": ["input"]}),
        ],
    )


@pytest.fixture
def ap_repository(neo4j_container: Neo4jContainer) -> Generator[Neo4jAnalyticalPatternRepository, None, None]:
    """AP repository backed by a fresh Neo4j container."""
    uri = neo4j_container.get_connection_url()
    auth = (neo4j_container.username, neo4j_container.password)
    driver = GraphDatabase.driver(uri, auth=auth)
    with driver.session() as session:
        yield Neo4jAnalyticalPatternRepository(session)
    driver.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_create_and_get(ap_repository: Neo4jAnalyticalPatternRepository):
    """Stored AP must be retrievable with root and operators intact.

    Data nodes connected via forbidden edges (``input``, ``output``, …) are
    NOT included in the shallow retrieval — they belong to the dataset
    subgraph.
    """
    ap = _make_ap()
    root_id = str(ap.root.id)

    ap_repository.create(ap)
    retrieved = ap_repository.get(root_id)

    assert retrieved is not None
    assert str(retrieved.root.id) == root_id
    retrieved_ids = {str(n.id) for n in retrieved.nodes}
    # Root and operator are returned
    for orig_node in ap.nodes:
        if "Data" not in orig_node.labels:
            assert str(orig_node.id) in retrieved_ids
    # Data node (linked via 'input') is excluded
    data_node = next(n for n in ap.nodes if "Data" in n.labels)
    assert str(data_node.id) not in retrieved_ids


def test_get_nonexistent_returns_none(ap_repository: Neo4jAnalyticalPatternRepository):
    """get() must return None when the AP ID does not exist."""
    result = ap_repository.get(str(uuid4()))
    assert result is None


def test_shallow_retrieval_does_not_include_deep_dataset_nodes(
    ap_repository: Neo4jAnalyticalPatternRepository,
    neo4j_container: Neo4jContainer,
):
    """
    When an AP's input node is also part of a dataset subgraph, the AP's
    shallow GET must NOT return nodes that are deeper in the dataset (e.g.
    a Table node reachable via containedIn from a Data node).
    """
    from moma_management.domain.dataset import Dataset
    from moma_management.repository.dataset import Neo4jDatasetRepository

    # Seed a dataset with a deep subgraph
    ds_id = str(uuid4())
    data_id = str(uuid4())
    deep_table_id = str(uuid4())

    dataset = Dataset(
        nodes=[
            Node(id=ds_id, labels=["sc:Dataset"], properties={"status": "published"}),
            Node(id=data_id, labels=["Data", "RelationalDatabase"], properties={"name": "db"}),
            Node(id=deep_table_id, labels=["Data", "Table"], properties={"name": "tbl"}),
        ],
        edges=[
            Edge(**{"from": ds_id, "to": data_id, "labels": ["distribution"]}),
            Edge(**{"from": data_id, "to": deep_table_id, "labels": ["containedIn"]}),
        ],
    )

    uri = neo4j_container.get_connection_url()
    auth = (neo4j_container.username, neo4j_container.password)
    driver = GraphDatabase.driver(uri, auth=auth)
    try:
        with driver.session() as ds_session:
            Neo4jDatasetRepository(ds_session).create(dataset)
    finally:
        driver.close()

    # Create AP that references the data node as input
    root_id = str(uuid4())
    op_id = str(uuid4())
    ap = AnalyticalPattern(
        nodes=[
            Node(id=root_id, labels=["Analytical_Pattern"], properties={"name": "ap"}),
            Node(id=op_id, labels=["Operator"], properties={"name": "op"}),
            Node(id=data_id, labels=["Data", "RelationalDatabase"], properties={"name": "db"}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": op_id, "labels": ["consist_of"]}),
            Edge(**{"from": op_id, "to": data_id, "labels": ["input"]}),
        ],
    )
    ap_repository.create(ap)

    retrieved = ap_repository.get(root_id)
    assert retrieved is not None

    retrieved_ids = {str(n.id) for n in retrieved.nodes}
    # data_id (via input) and deep_table_id must NOT be in the AP retrieval
    assert data_id not in retrieved_ids
    assert deep_table_id not in retrieved_ids


def test_lifecycle_create_get_delete(ap_repository: Neo4jAnalyticalPatternRepository):
    """Full AP lifecycle: create → get → delete → get returns None."""
    ap = _make_ap()
    root_id = str(ap.root.id)

    # Get before create returns None
    assert ap_repository.get(root_id) is None

    # Create and verify retrieval
    ap_repository.create(ap)
    retrieved = ap_repository.get(root_id)
    assert retrieved is not None
    assert str(retrieved.root.id) == root_id

    # Delete and verify it's gone
    ap_repository.delete(root_id)
    assert ap_repository.get(root_id) is None


def test_delete_nonexistent_is_noop(ap_repository: Neo4jAnalyticalPatternRepository):
    """Deleting an AP that doesn't exist must not raise."""
    ap_repository.delete(str(uuid4()))


def test_delete_preserves_data_nodes(
    ap_repository: Neo4jAnalyticalPatternRepository,
    neo4j_container: Neo4jContainer,
):
    """Deleting an AP must leave referenced Data nodes intact."""
    from moma_management.domain.dataset import Dataset
    from moma_management.repository.dataset import Neo4jDatasetRepository

    # Seed a dataset
    ds_id = str(uuid4())
    data_id = str(uuid4())
    dataset = Dataset(
        nodes=[
            Node(id=ds_id, labels=["sc:Dataset"], properties={"status": "published"}),
            Node(id=data_id, labels=["Data"], properties={"name": "data"}),
        ],
        edges=[
            Edge(**{"from": ds_id, "to": data_id, "labels": ["distribution"]}),
        ],
    )

    uri = neo4j_container.get_connection_url()
    auth = (neo4j_container.username, neo4j_container.password)
    driver = GraphDatabase.driver(uri, auth=auth)
    try:
        with driver.session() as ds_session:
            ds_repo = Neo4jDatasetRepository(ds_session)
            ds_repo.create(dataset)
    finally:
        driver.close()

    # Create an AP that references the data node via input
    ap = _make_ap(data_id=data_id)
    root_id = str(ap.root.id)
    ap_repository.create(ap)

    # Delete the AP
    ap_repository.delete(root_id)
    assert ap_repository.get(root_id) is None

    # Dataset and data node must still exist
    driver = GraphDatabase.driver(uri, auth=auth)
    try:
        with driver.session() as ds_session:
            ds_repo = Neo4jDatasetRepository(ds_session)
            ds = ds_repo.get(ds_id)
            assert ds is not None
            ds_ids = {str(n.id) for n in ds.nodes}
            assert data_id in ds_ids
    finally:
        driver.close()
