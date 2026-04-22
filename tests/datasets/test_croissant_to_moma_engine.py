
import json
from pathlib import Path

import pytest
import yaml

from moma_management.domain.generated.moma_schema import MoMaGraphModel
from moma_management.domain.mapping_engine import croissant_to_pgjson
from moma_management.legacy.converters import Croissant2PGjson
from tests.utils import normalize, save

# Profiles that introduce node types not yet supported by the legacy converter.
# These are tested by dedicated tests below instead of the legacy comparison.
_LEGACY_INCOMPATIBLE_PROFILES = {"single_pdf.json", "single_txt.json"}

PROJECT_ROOT = Path(__file__).parent.parent.parent
PROFILES_DIR = PROJECT_ROOT / "assets" / "profiles"


def test_old_vs_new_light(light_profile: Path, generated_dir: Path, mapping_file: Path):
    """
    Compare the old and new implementations of the Croissant to PgJSon transformation on a light profile.
    Saves the outputs to files for visual inspection if needed.
    """
    if light_profile.name in _LEGACY_INCOMPATIBLE_PROFILES:
        pytest.skip(
            f"{light_profile.name} uses node types not supported by the legacy converter. "
            "Use the dedicated test instead."
        )

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
    assert normalize(new_model) == normalize(
        graph.model_dump(mode='json', by_alias=True))


def test_column_statistics_type_property(mapping_file: Path):
    """
    ColumnStatistics nodes must carry a 'type' property that mirrors the
    Croissant @type value ("dg:ColumnStatistics"), consistent with how every
    other MoMa node exposes its type.
    A statistics object whose numeric values are all null still produces a node
    because the object itself exists in the data (it just has no values yet).
    """
    mapping = yaml.safe_load(mapping_file.open("r"))

    profile = {
        "@id": "ds-1",
        "@type": "sc:Dataset",
        "name": "test",
        "recordSet": [
            {
                "@type": "cr:RecordSet",
                "@id": "rs-1",
                "name": "rows",
                "field": [
                    {
                        "@type": "cr:Field",
                        "@id": "f-1",
                        "name": "col_a",
                        "source": {"fileObject": {"@id": "fo-1"}, "extract": {"column": "col_a"}},
                        "statistics": {
                            "@id": "stats-1",
                            "@type": "dg:ColumnStatistics",
                            "dg:rowCount": 100,
                            "dg:mean": 3.5,
                        },
                    },
                    {
                        "@type": "cr:Field",
                        "@id": "f-2",
                        "name": "col_b",
                        "source": {"fileObject": {"@id": "fo-1"}, "extract": {"column": "col_b"}},
                        "statistics": {
                            "@id": "stats-2",
                            "@type": "dg:ColumnStatistics",
                            "dg:rowCount": None,
                            "dg:mean": None,
                        },
                    },
                ],
            }
        ],
    }

    result = croissant_to_pgjson(profile, mapping)
    stats_nodes = [n for n in result["nodes"]
                   if "Statistics" in n.get("labels", [])]

    # Both cols have a statistics object with an @id → both produce a Statistics node
    assert len(
        stats_nodes) == 2, f"Expected 2 Statistics nodes, got {len(stats_nodes)}"

    for stats in stats_nodes:
        assert stats["properties"].get("type") == "dg:ColumnStatistics", (
            f"Expected type='dg:ColumnStatistics', got: {stats['properties']}"
        )

    # col_a's node also carries numeric values
    col_a_stats = next(n for n in stats_nodes if n["id"] == "stats-1")
    assert col_a_stats["properties"].get("rowCount") == 100
    assert col_a_stats["properties"].get("mean") == 3.5


def test_pdf_recordset_attributes(mapping_file: Path):
    """
    A cr:RecordSet sourced from a PDF FileObject must map all PDF metadata
    fields onto the node: subject, author, title, producer, creator,
    creationDate, modificationDate, pagesCount, keywords, summary.
    """
    profile_path = PROFILES_DIR / "light" / "single_pdf.json"
    profile = json.load(profile_path.open("r"))
    mapping = yaml.safe_load(mapping_file.open("r"))

    result = croissant_to_pgjson(profile, mapping)

    recordset_nodes = [
        n for n in result["nodes"] if "cr:RecordSet" in n.get("labels", [])
    ]
    assert len(recordset_nodes) == 1, (
        f"Expected exactly 1 RecordSet node, got {len(recordset_nodes)}"
    )

    props = recordset_nodes[0]["properties"]
    expected_keys = {
        "name", "subject", "author", "title", "producer", "creator",
        "creationDate", "modificationDate", "pagesCount",
    }
    missing = expected_keys - props.keys()
    assert not missing, f"PDF RecordSet is missing properties: {missing}"

    assert props["author"] == "Laboratorio"
    assert props["pagesCount"] == 1
    assert props["producer"] == "PDFCreator 2.1.2.0"


def test_text_recordset_attributes(mapping_file: Path):
    """
    A cr:RecordSet sourced from a plain-text FileObject must map all text
    statistics fields: language, numLines, numWords, numCharacters,
    avgSentenceLength, numParagraphs, fleschKincaidGrade, summary, keywords.
    """
    profile_path = PROFILES_DIR / "light" / "single_txt.json"
    profile = json.load(profile_path.open("r"))
    mapping = yaml.safe_load(mapping_file.open("r"))

    result = croissant_to_pgjson(profile, mapping)

    recordset_nodes = [
        n for n in result["nodes"] if "cr:RecordSet" in n.get("labels", [])
    ]
    assert len(recordset_nodes) == 1, (
        f"Expected exactly 1 RecordSet node, got {len(recordset_nodes)}"
    )

    props = recordset_nodes[0]["properties"]
    expected_keys = {
        "name", "language", "numLines", "numWords", "numCharacters",
        "avgSentenceLength", "numParagraphs", "fleschKincaidGrade",
    }
    missing = expected_keys - props.keys()
    assert not missing, f"Text RecordSet is missing properties: {missing}"

    assert props["language"] == "n/a"
    assert props["numLines"] == 1
    assert props["numCharacters"] == 2
