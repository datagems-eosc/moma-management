
import json
from pathlib import Path

import pytest
import yaml

from moma_management.domain.mapping_engine import croissant_to_pgjson
from moma_management.domain.generated.moma_schema import MoMaGraphModel
from moma_management.legacy.converters import Croissant2PGjson
from tests.utils import normalize, save


def test_old_vs_new_light(light_profile: Path, generated_dir: Path, mapping_file: Path):
    """
    Compare the old and new implementations of the Croissant to PgJSon transformation on a light profile.
    Saves the outputs to files for visual inspection if needed.
    """
    profile = json.load(light_profile.open("r"))
    mapping = yaml.safe_load(mapping_file.open("r"))

    old = Croissant2PGjson(profile)
    new = croissant_to_pgjson(profile, mapping)

    save(old, generated_dir / "old" / light_profile.name)
    save(new, generated_dir / "new" / light_profile.name)

    assert normalize(new) == normalize(old)


@pytest.mark.skip(reason="Skipping heavy comparison, the old model seems to duplicates some edges, so it can't be used as a reference for now. Will need to investigate and fix the old model before re-enabling this test.")
def test_old_vs_new_heavy(heavy_profile: Path, generated_dir: Path, mapping_file: Path):
    """
    Compare the old and new implementations of the Croissant to PgJSon transformation on a heavy profile.
    Saves the outputs to files for visual inspection if needed.
    """
    profile = json.load(heavy_profile.open("r"))
    mapping = yaml.safe_load(mapping_file.open("r"))

    old = Croissant2PGjson(profile)
    new = croissant_to_pgjson(profile, mapping)

    save(old, generated_dir / "old" / heavy_profile.name)
    save(new, generated_dir / "new" / heavy_profile.name)

    assert normalize(new) == normalize(old)


def test_generation_cycle(light_profile: Path, mapping_file: Path):
    """
    Match auto-generated PG-JSON against the MoMaGraphModel schema to ensure it can be parsed correctly.
    Then
    """
    profile = json.load(light_profile.open("r"))
    mapping = yaml.safe_load(mapping_file.open("r"))

    new_model = croissant_to_pgjson(profile, mapping)
    graph = MoMaGraphModel.model_validate(new_model)
    assert normalize(new_model) == normalize(graph.model_dump(by_alias=True))
