"""
All tests that involve complex ingestion pipeline.
"""
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from moma_management.domain.exceptions import (
    ConversionError,
    NotFoundError,
    ValidationError,
)
from moma_management.services.dataset import DatasetService

RECORDSET_LABEL = "cr:RecordSet"
DISTRIBUTION_LABEL = "cr:FileObject"


def test_layered_ingestion_light_heavy(layered_profile: tuple[Path, Path], dataset_service: DatasetService):
    """
    For some reasons, a dataset cna be ingested in multiple layers (light, heavy) and the user may choose to ingest only a subset of the layers. 
    This test ensures that ingesting a dataset in multiple layers produces the same result as ingesting it all at once.
    """

    light_profile, heavy_profile = layered_profile
    light_profile = json.load(light_profile.open("r"))
    heavy_profile = json.load(heavy_profile.open("r"))

    # Sanity check : "light" ingestion contains distribution nodes but no recordset nodes
    light_dataset = dataset_service.ingest(light_profile)

    light_rc_nodes = list(light_dataset.find_all(RECORDSET_LABEL))
    assert len(light_rc_nodes) == 0, "Light ingestion should create no rc nodes"

    light_distrib_nodes = list(light_dataset.find_all(DISTRIBUTION_LABEL))
    assert len(
        light_distrib_nodes) > 0, "Light ingestion should create at least one distribution node"

    # Add the "heavy" part, which should add recordset nodes but not change the distribution nodes
    heavy_dataset = dataset_service.ingest(heavy_profile)

    heavy_rc_nodes = list(heavy_dataset.find_all(RECORDSET_LABEL))
    assert len(
        heavy_rc_nodes) > 0, "Heavy ingestion should create at least one rc node"

    heavy_distrib_nodes = list(heavy_dataset.find_all(DISTRIBUTION_LABEL))

    # Intersection of light and heavy distrib nodes should be the same as both sets, meaning that heavy ingestion should not create or delete distribution nodes
    intersect = (
        set(n.id for n in heavy_distrib_nodes) &
        set(n.id for n in light_distrib_nodes)
    )
    distrib_same = (
        len(intersect) == len(light_distrib_nodes) == len(heavy_distrib_nodes)
    )
    assert distrib_same, "Heavy ingestion should not create or delete distribution nodes"

    # The final dataset should be the same regardless of the ingestion order or layering
    assert light_dataset.root_id == heavy_dataset.root_id
    old = dataset_service.get(light_dataset.root_id)
    new = dataset_service.get(heavy_dataset.root_id)

    assert new == old


def test_layered_ingestion_heavy_light(layered_profile: tuple[Path, Path], dataset_service: DatasetService):
    """
    For some reasons, a dataset cna be ingested in multiple layers (light, heavy) and the user may choose to ingest only a subset of the layers. 
    This test ensures that ingesting a dataset in multiple layers produces the same result as ingesting it all at once.
    """
    light_profile, heavy_profile = layered_profile
    heavy_profile = json.load(heavy_profile.open("r"))
    light_profile = json.load(light_profile.open("r"))

    # Sanity check : "heavy" ingestion contains both recordset nodes and distribution nodes
    heavy_dataset = dataset_service.ingest(heavy_profile)

    heavy_rc_nodes = list(heavy_dataset.find_all(RECORDSET_LABEL))
    assert len(
        heavy_rc_nodes) > 0, "Heavy ingestion should create at least one rc node"

    heavy_distrib_nodes = list(heavy_dataset.find_all(DISTRIBUTION_LABEL))
    assert len(
        heavy_distrib_nodes) > 0, "Heavy ingestion should create at least one distribution node"

    # Add the "light" part, which should not change the distribution nodes
    light_dataset = dataset_service.ingest(light_profile)

    light_distrib_nodes = list(light_dataset.find_all(DISTRIBUTION_LABEL))

    # Intersection of light and heavy distrib nodes should be the same as both sets, meaning that light ingestion should not create or delete distribution nodes
    intersect = (
        set(n.id for n in light_distrib_nodes) &
        set(n.id for n in heavy_distrib_nodes)
    )
    distrib_same = (
        len(intersect) == len(heavy_distrib_nodes) == len(light_distrib_nodes)
    )
    assert distrib_same, "Light ingestion should not create or delete distribution nodes"

    # The final dataset should be the same regardless of the ingestion order or layering
    assert light_dataset.root_id == heavy_dataset.root_id
    old = dataset_service.get(light_dataset.root_id)
    new = dataset_service.get(heavy_dataset.root_id)

    assert new == old


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


def test_convert_invalid_croissant_raises_conversion_error(dataset_service: DatasetService):
    """A mapping engine failure must be wrapped in ConversionError."""
    with patch("moma_management.services.dataset.croissant_to_pgjson", side_effect=ValueError("bad input")):
        with pytest.raises(ConversionError):
            dataset_service.convert({})


def test_validate_invalid_pgjson_raises_validation_error(dataset_service: DatasetService):
    """Structurally invalid PG-JSON must raise ValidationError."""
    with pytest.raises(ValidationError):
        dataset_service.validate({"nodes": "not-a-list"})


def test_get_missing_dataset_raises_not_found(dataset_service: DatasetService):
    """Getting a non-existent dataset ID must raise NotFoundError."""
    with pytest.raises(NotFoundError):
        dataset_service.get("does-not-exist")


def test_delete_missing_dataset_raises_not_found(dataset_service: DatasetService):
    """Deleting a non-existent dataset ID must raise NotFoundError."""
    with pytest.raises(NotFoundError):
        dataset_service.delete("does-not-exist")


def test_get_non_dataset_node_raises_not_found(
    dataset_service: DatasetService,
    dataset_repository,
):
    """Getting an ID that exists in the graph but is NOT a sc:Dataset must raise NotFoundError."""
    non_dataset_id = "non-dataset-node-get"
    dataset_repository._session.run(
        "CREATE (n:SomeLabel {id: $id})", id=non_dataset_id
    )
    with pytest.raises(NotFoundError):
        dataset_service.get(non_dataset_id)


def test_delete_non_dataset_node_raises_not_found(
    dataset_service: DatasetService,
    dataset_repository,
):
    """Deleting an ID that exists in the graph but is NOT a sc:Dataset must raise NotFoundError."""
    non_dataset_id = "non-dataset-node-delete"
    dataset_repository._session.run(
        "CREATE (n:SomeLabel {id: $id})", id=non_dataset_id
    )
    with pytest.raises(NotFoundError):
        dataset_service.delete(non_dataset_id)


def test_ingest_repo_error_propagates(mapping_file: Path):
    """A repository failure during ingest must propagate out of the service."""
    repo = MagicMock()
    repo.create.side_effect = RuntimeError("Neo4j is down")
    svc = DatasetService(repo=repo, mapping_file=mapping_file)
    light_profile = next(
        (Path(__file__).parent.parent.parent / "assets" /
         "profiles" / "light").glob("*.json")
    )
    with pytest.raises(RuntimeError, match="Neo4j is down"):
        svc.ingest(json.loads(light_profile.read_text()))
