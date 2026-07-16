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
from moma_management.domain.dataset_relationship import DatasetRelationship
from moma_management.domain.exceptions import (
    ConflictError,
    NotFoundError,
    ValidationError,
)
from moma_management.domain.generated.edges.edge_schema import Edge
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.repository.analytical_pattern import (
    Neo4jAnalyticalPatternRepository,
)
from moma_management.repository.dataset import Neo4jDatasetRepository
from moma_management.repository.dataset_relationship import (
    Neo4jDatasetRelationshipRepository,
)
from moma_management.repository.ml_model import Neo4jMlModelRepository
from moma_management.services.analytical_pattern import AnalyticalPatternService
from moma_management.services.dataset import DatasetService
from moma_management.services.dataset_relationship import DatasetRelationshipService
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
        rel_repo = Neo4jDatasetRelationshipRepository(session)
        ds_svc = DatasetService(ds_repo, mapping_file, rel_repo)
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
        rel_repo = Neo4jDatasetRelationshipRepository(session)
        ds_svc = DatasetService(ds_repo, mapping_file, rel_repo)
        ap_svc = AnalyticalPatternService(ap_repo, ds_svc)
        ml_svc = MlModelService(ml_repo)
        yield ds_svc, ap_svc, ml_svc
    await driver.close()


@pytest_asyncio.fixture(scope="module")
async def rel_services(
    neo4j_container_module: Neo4jContainer,
    mapping_file: Path,
) -> AsyncGenerator[tuple[DatasetService, DatasetRelationshipService], None]:
    """Return (DatasetService, DatasetRelationshipService) sharing one Neo4j session."""
    uri = neo4j_container_module.get_connection_url()
    auth = (neo4j_container_module.username, neo4j_container_module.password)
    driver = AsyncGraphDatabase.driver(uri, auth=auth)
    async with driver.session() as session:
        ds_repo = Neo4jDatasetRepository(session)
        rel_repo = await Neo4jDatasetRelationshipRepository.create_with_indexes(session)
        ds_svc = DatasetService(ds_repo, mapping_file, rel_repo)
        rel_svc = DatasetRelationshipService(rel_repo, ds_svc)
        yield ds_svc, rel_svc
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
            Edge(**{"from": data_node_id, "to": op_id, "labels": ["input"]}),
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


# ---------------------------------------------------------------------------
# DatasetRelationship cross-concern tests
#
# Relationship graphs are loaded from the JSON fixtures in
# assets/dataset_relationships/ (the same fixtures used by
# tests/dataset_relationships/test_dataset_relationship_lifecycle.py),
# re-IDing every internal node and re-targeting the fixture's two placeholder
# sc:Dataset nodes onto real, ingested dataset IDs.
# ---------------------------------------------------------------------------

_DL_FIXTURES_DIR = Path(__file__).parent.parent / \
    "assets" / "dataset_relationships"
_DL_MINIMAL_PATH = _DL_FIXTURES_DIR / "dl_minimal.json"
_DL_FULL_EXAMPLE_PATH = _DL_FIXTURES_DIR / "dl_full_example.json"

_MATHE_PROFILE_PATH = (
    Path(__file__).parent.parent / "assets" /
    "profiles" / "light" / "mathe_light.json"
)


def _load_relationship_fixture(
    path: Path, dataset_id_a: str, dataset_id_b: str,
) -> DatasetRelationship:
    """Load a DatasetRelationship fixture, re-ID its internal nodes, and
    re-target its two placeholder sc:Dataset nodes onto the given real IDs.
    """
    raw = json.loads(path.read_text())
    placeholder_ds_ids = [
        n["id"] for n in raw["nodes"] if "sc:Dataset" in n["labels"]]
    assert len(placeholder_ds_ids) == 2, (
        f"Fixture {path.name} must have exactly two sc:Dataset placeholders")
    id_map = {placeholder_ds_ids[0]: dataset_id_a,
              placeholder_ds_ids[1]: dataset_id_b}
    for node in raw["nodes"]:
        if node["id"] not in id_map:
            id_map[node["id"]] = str(uuid4())
        node["id"] = id_map[node["id"]]
        if node.get("properties") is None:
            node["properties"] = {}
    for edge in raw["edges"]:
        edge["from"] = id_map[edge["from"]]
        edge["to"] = id_map[edge["to"]]
    return DatasetRelationship.model_validate(raw)


async def _ingest_dataset_id(ds_svc: DatasetService, profile_path: Path) -> str:
    """Ingest *profile_path* and return its root sc:Dataset id."""
    profile = json.loads(profile_path.read_text())
    dataset = await ds_svc.ingest(profile)
    ds_node = next(n for n in dataset.nodes if "sc:Dataset" in n.labels)
    return str(ds_node.id)


@pytest.mark.asyncio
async def test_create_relationship_referencing_nonexistent_dataset_fails(
    rel_services: tuple[DatasetService, DatasetRelationshipService],
):
    """Creating a relationship whose datasets are not stored anywhere must raise ValidationError."""
    _, rel_svc = rel_services

    rel = _load_relationship_fixture(
        _DL_MINIMAL_PATH, str(uuid4()), str(uuid4()))
    with pytest.raises(ValidationError):
        await rel_svc.create(rel)


@pytest.mark.asyncio
async def test_create_relationship_referencing_existing_dataset_succeeds(
    rel_services: tuple[DatasetService, DatasetRelationshipService],
):
    """Creating a relationship between two real, stored datasets must succeed and be retrievable.

    Uses the full dl_full_example.json fixture (BasicDLElement +
    PropertyComparison + TextEvidence) to prove the whole realistic subgraph
    round-trips through the service, not just a minimal graph.
    """
    ds_svc, rel_svc = rel_services

    ds_id_a = await _ingest_dataset_id(ds_svc, _LIGHT_PROFILE_PATH)
    ds_id_b = await _ingest_dataset_id(ds_svc, _MATHE_PROFILE_PATH)

    rel = _load_relationship_fixture(_DL_FULL_EXAMPLE_PATH, ds_id_a, ds_id_b)
    rel_id = await rel_svc.create(rel)

    retrieved = await rel_svc.get(rel_id)
    assert retrieved is not None
    assert any("PropertyComparison" in n.labels for n in retrieved.nodes)
    assert any("TextEvidence" in n.labels for n in retrieved.nodes)

    # Clean up so subsequent tests are not affected
    await rel_svc.delete(rel_id)
    await ds_svc.delete(ds_id_a)
    await ds_svc.delete(ds_id_b)


@pytest.mark.asyncio
async def test_create_duplicate_relationship_for_same_dataset_pair_fails(
    rel_services: tuple[DatasetService, DatasetRelationshipService],
):
    """A second relationship for the same (unordered) dataset pair must be rejected with ConflictError."""
    ds_svc, rel_svc = rel_services

    ds_id_a = await _ingest_dataset_id(ds_svc, _LIGHT_PROFILE_PATH)
    ds_id_b = await _ingest_dataset_id(ds_svc, _MATHE_PROFILE_PATH)

    rel_id = await rel_svc.create(
        _load_relationship_fixture(_DL_MINIMAL_PATH, ds_id_a, ds_id_b))

    # Same pair, reversed order -> still a duplicate
    with pytest.raises(ConflictError):
        await rel_svc.create(
            _load_relationship_fixture(_DL_MINIMAL_PATH, ds_id_b, ds_id_a))

    # Clean up
    await rel_svc.delete(rel_id)
    await ds_svc.delete(ds_id_a)
    await ds_svc.delete(ds_id_b)


@pytest.mark.asyncio
async def test_get_dataset_does_not_include_relationships(
    rel_services: tuple[DatasetService, DatasetRelationshipService],
):
    """Retrieving a dataset must not pull in any DatasetRelationship referencing it."""
    ds_svc, rel_svc = rel_services

    ds_id_a = await _ingest_dataset_id(ds_svc, _LIGHT_PROFILE_PATH)
    ds_id_b = await _ingest_dataset_id(ds_svc, _MATHE_PROFILE_PATH)

    rel_id = await rel_svc.create(
        _load_relationship_fixture(_DL_FULL_EXAMPLE_PATH, ds_id_a, ds_id_b))

    dataset = await ds_svc.get(ds_id_a)
    labels_in_dataset = {label for n in dataset.nodes for label in n.labels}
    assert "BasicDLElement" not in labels_in_dataset
    assert "PropertyComparison" not in labels_in_dataset
    assert "TextEvidence" not in labels_in_dataset
    # The other dataset must not leak into this dataset's subgraph either
    assert str(ds_id_b) not in {str(n.id) for n in dataset.nodes}

    # Clean up
    await rel_svc.delete(rel_id)
    await ds_svc.delete(ds_id_a)
    await ds_svc.delete(ds_id_b)


@pytest.mark.asyncio
async def test_delete_dataset_cascades_relationship_deletion(
    rel_services: tuple[DatasetService, DatasetRelationshipService],
):
    """
    Deleting a dataset targeted by a relationship must delete the relationship
    too (relationships are weak references), while leaving the OTHER dataset
    in the pair untouched.
    """
    ds_svc, rel_svc = rel_services

    ds_id_a = await _ingest_dataset_id(ds_svc, _LIGHT_PROFILE_PATH)
    ds_id_b = await _ingest_dataset_id(ds_svc, _MATHE_PROFILE_PATH)

    rel_id = await rel_svc.create(
        _load_relationship_fixture(_DL_FULL_EXAMPLE_PATH, ds_id_a, ds_id_b))

    await ds_svc.delete(ds_id_a)

    with pytest.raises(NotFoundError):
        await rel_svc.get(rel_id)

    # The other dataset in the pair must be untouched
    other = await ds_svc.get(ds_id_b)
    assert other is not None

    # Clean up
    await ds_svc.delete(ds_id_b)
