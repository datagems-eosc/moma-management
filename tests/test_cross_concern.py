"""
Cross-concern integration tests that exercise multiple domain layers together.

These tests use a real Neo4j container so they cover the full stack from
service down to repository.
"""

import json
from pathlib import Path
from typing import Generator
from uuid import uuid4

import pytest
from neo4j import GraphDatabase
from testcontainers.neo4j import Neo4jContainer

from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.domain.exceptions import ValidationError
from moma_management.domain.generated.edges.edge_schema import Edge
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.repository.analytical_pattern import (
    Neo4jAnalyticalPatternRepository,
)
from moma_management.repository.dataset import Neo4jDatasetRepository
from moma_management.services.analytical_pattern import AnalyticalPatternService
from moma_management.services.dataset import DatasetService

# ---------------------------------------------------------------------------
# Fixtures — shared Neo4j container for the whole module
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def neo4j_container_module() -> Generator[Neo4jContainer, None, None]:
    container = Neo4jContainer(image="neo4j:latest")
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="module")
def services(
    neo4j_container_module: Neo4jContainer,
    mapping_file: Path,
) -> Generator[tuple[DatasetService, AnalyticalPatternService], None, None]:
    """Return (DatasetService, AnalyticalPatternService) sharing one Neo4j session."""
    uri = neo4j_container_module.get_connection_url()
    auth = (neo4j_container_module.username, neo4j_container_module.password)
    driver = GraphDatabase.driver(uri, auth=auth)
    with driver.session() as session:
        ds_repo = Neo4jDatasetRepository(session)
        ap_repo = Neo4jAnalyticalPatternRepository(session)
        ds_svc = DatasetService(ds_repo, mapping_file)
        ap_svc = AnalyticalPatternService(ap_repo, ds_svc)
        yield ds_svc, ap_svc
    driver.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_LIGHT_PROFILE_PATH = (
    Path(__file__).parent.parent / "assets" /
    "profiles" / "light" / "esco_light.json"
)


def _make_ap(data_node_id: str) -> AnalyticalPattern:
    """Build a minimal AP whose operator reads from *data_node_id*."""
    root_id = str(uuid4())
    op_id = str(uuid4())
    return AnalyticalPattern(
        nodes=[
            Node(id=root_id, labels=["Analytical_Pattern"],
                 properties={"name": "cross-test-ap"}),
            Node(id=op_id, labels=["Operator"], properties={"name": "op"}),
            Node(id=data_node_id, labels=["Data"],
                 properties={"name": "input-data"}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": op_id, "labels": ["consist_of"]}),
            Edge(**{"from": op_id, "to": data_node_id, "labels": ["input"]}),
        ],
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_create_ap_referencing_nonexistent_dataset_fails(
    services: tuple[DatasetService, AnalyticalPatternService],
):
    """
    Creating an AP whose input node does not belong to any stored dataset
    must raise ValidationError.  The AP must not be persisted.
    """
    _, ap_svc = services

    orphan_data_id = str(uuid4())
    ap = _make_ap(orphan_data_id)

    with pytest.raises(ValidationError):
        ap_svc.create(ap)


def test_create_ap_referencing_existing_dataset_succeeds(
    services: tuple[DatasetService, AnalyticalPatternService],
):
    """
    After the referenced dataset is present, creating the same AP must
    succeed and the AP must be retrievable.
    """
    ds_svc, ap_svc = services

    # Ingest a real dataset so its Data nodes are stored in Neo4j
    profile = json.loads(_LIGHT_PROFILE_PATH.read_text())
    dataset = ds_svc.ingest(profile)

    # Pick any Data node from the stored dataset as the AP's input target
    data_node = next(n for n in dataset.nodes if "Data" in n.labels)
    data_node_id = str(data_node.id)

    ap = _make_ap(data_node_id)
    ap_id = ap_svc.create(ap)

    # AP must now be retrievable
    retrieved = ap_svc.get(ap_id)
    assert retrieved is not None
    assert str(retrieved.root.id) == ap_id
