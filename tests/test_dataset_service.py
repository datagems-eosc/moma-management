"""
All tests that involve complex ingestion pipeline.
"""
import json
from pathlib import Path

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
