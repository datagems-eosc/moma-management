"""
Cross-concern integration tests that exercise multiple domain layers together.

These tests use a real Neo4j container so they cover the full stack from
service down to repository.
"""

import json
from pathlib import Path
from typing import AsyncGenerator, Generator
from uuid import uuid4

import pytest
import pytest_asyncio
from neo4j import AsyncGraphDatabase
from testcontainers.neo4j import Neo4jContainer

from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.domain.exceptions import ConflictError, ValidationError
from moma_management.domain.generated.edges.edge_schema import Edge
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.repository.analytical_pattern import (
    Neo4jAnalyticalPatternRepository,
)
from moma_management.repository.dataset import Neo4jDatasetRepository
from moma_management.repository.ml_model import Neo4jMlModelRepository
from moma_management.services.analytical_pattern import AnalyticalPatternService
from moma_management.services.dataset import DatasetService
from moma_management.services.ml_model import MlModelService

# ---------------------------------------------------------------------------
# Fixtures — shared Neo4j container for the whole module
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def neo4j_container_module() -> Generator[Neo4jContainer, None, None]:
    container = Neo4jContainer(image="neo4j:latest")
    container.start()
    yield container
    container.stop()


@pytest_asyncio.fixture(scope="module")
async def services(
    neo4j_container_module: Neo4jContainer,
    mapping_file: Path,
) -> AsyncGenerator[tuple[DatasetService, AnalyticalPatternService], None]:
    """Return (DatasetService, AnalyticalPatternService) sharing one Neo4j session."""
    uri = neo4j_container_module.get_connection_url()
    auth = (neo4j_container_module.username, neo4j_container_module.password)
    driver = AsyncGraphDatabase.driver(uri, auth=auth)
    async with driver.session() as session:
        ds_repo = Neo4jDatasetRepository(session)
        ap_repo = Neo4jAnalyticalPatternRepository(session)
        ds_svc = DatasetService(ds_repo, mapping_file)
        ap_svc = AnalyticalPatternService(ap_repo, ds_svc)
        yield ds_svc, ap_svc
    await driver.close()


@pytest_asyncio.fixture(scope="module")
async def services_with_ml(
    neo4j_container_module: Neo4jContainer,
    mapping_file: Path,
) -> AsyncGenerator[tuple[DatasetService, AnalyticalPatternService, MlModelService], None]:
    """Return (DatasetService, AnalyticalPatternService, MlModelService) sharing one Neo4j session."""
    uri = neo4j_container_module.get_connection_url()
    auth = (neo4j_container_module.username, neo4j_container_module.password)
    driver = AsyncGraphDatabase.driver(uri, auth=auth)
    async with driver.session() as session:
        ds_repo = Neo4jDatasetRepository(session)
        ap_repo = Neo4jAnalyticalPatternRepository(session)
        ml_repo = Neo4jMlModelRepository(session)
        ds_svc = DatasetService(ds_repo, mapping_file)
        ap_svc = AnalyticalPatternService(ap_repo, ds_svc)
        ml_svc = MlModelService(ml_repo)
        yield ds_svc, ap_svc, ml_svc
    await driver.close()


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

@pytest.mark.asyncio
async def test_create_ap_referencing_nonexistent_dataset_fails(
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
        await ap_svc.create(ap)


@pytest.mark.asyncio
async def test_create_ap_referencing_existing_dataset_succeeds(
    services: tuple[DatasetService, AnalyticalPatternService],
):
    """
    After the referenced dataset is present, creating the same AP must
    succeed and the AP must be retrievable.
    """
    ds_svc, ap_svc = services

    # Ingest a real dataset so its Data nodes are stored in Neo4j
    profile = json.loads(_LIGHT_PROFILE_PATH.read_text())
    dataset = await ds_svc.ingest(profile)

    # Pick any Data node from the stored dataset as the AP's input target
    data_node = next(n for n in dataset.nodes if "Data" in n.labels)
    data_node_id = str(data_node.id)

    ap = _make_ap(data_node_id)
    ap_id = await ap_svc.create(ap)

    # AP must now be retrievable
    retrieved = await ap_svc.get(ap_id)
    assert retrieved is not None

    # Clean up so subsequent tests are not affected
    await ap_svc.delete(ap_id)


@pytest.mark.asyncio
async def test_delete_dataset_blocked_when_ap_references_it(
    services: tuple[DatasetService, AnalyticalPatternService],
):
    """
    Deleting a dataset that is referenced by an AP must raise ConflictError.
    After the AP is deleted, the dataset deletion must succeed.
    """
    ds_svc, ap_svc = services

    # Ingest a dataset
    profile = json.loads(_LIGHT_PROFILE_PATH.read_text())
    dataset = await ds_svc.ingest(profile)
    ds_node = next(n for n in dataset.nodes if "sc:Dataset" in n.labels)
    ds_id = str(ds_node.id)

    # Create an AP that references a data node from this dataset
    data_node = next(n for n in dataset.nodes if "Data" in n.labels)
    ap = _make_ap(str(data_node.id))
    ap_id = await ap_svc.create(ap)

    # Deleting the dataset must be blocked
    with pytest.raises(ConflictError):
        await ds_svc.delete(ds_id)

    # After deleting the AP, dataset deletion must succeed
    await ap_svc.delete(ap_id)
    await ds_svc.delete(ds_id)


@pytest.mark.asyncio
async def test_delete_ml_model_blocked_when_referenced_by_ap(
    services_with_ml: tuple[DatasetService, AnalyticalPatternService, MlModelService],
):
    """
    Deleting an ML_Model that is referenced by an AP (via a perform_inference
    edge from an Operator) must raise ConflictError.
    After the AP is deleted, deleting the ML_Model must succeed.
    """
    _, ap_svc, ml_svc = services_with_ml

    # Create an ML_Model
    ml_node = await ml_svc.create(name="gpt-cross-test", type="LLM")
    ml_model_id = str(ml_node.id)

    # Create an AP whose Operator performs inference using the ML_Model.
    # The ML_Model node must be included so the edge-constraint validator can
    # resolve its label; it is stored via MERGE so it won't be duplicated.
    # No input edges are needed, so dataset validation is skipped.
    root_id = str(uuid4())
    op_id = str(uuid4())
    ap = AnalyticalPattern(
        nodes=[
            Node(id=root_id, labels=["Analytical_Pattern"],
                 properties={"name": "ml-cross-test-ap"}),
            Node(id=op_id, labels=["Operator"],
                 properties={"name": "infer-op"}),
            Node(id=ml_model_id, labels=["ML_Model"],
                 properties=ml_node.properties),
        ],
        edges=[
            Edge(**{"from": root_id, "to": op_id, "labels": ["consist_of"]}),
            Edge(**{"from": op_id, "to": ml_model_id,
                 "labels": ["perform_inference"]}),
        ],
    )
    ap_id = await ap_svc.create(ap)

    # Attempt to delete the ML_Model while it is referenced must be blocked
    with pytest.raises(ConflictError):
        await ml_svc.delete(ml_model_id)

    # Delete the AP first
    await ap_svc.delete(ap_id)

    # Now the ML_Model deletion must succeed
    await ml_svc.delete(ml_model_id)
