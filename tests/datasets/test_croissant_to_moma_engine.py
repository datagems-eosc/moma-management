
import json
from pathlib import Path

import pytest
import yaml

from moma_management.domain.dataset import Dataset
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
        graph.model_dump(mode='json', by_alias=True, exclude_none=True))


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
                            "rowCount": 100,
                            "mean": 3.5,
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
                            "rowCount": None,
                            "mean": None,
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


def test_column_statistics_new_fields_mapped(mapping_file: Path):
    """
    New ColumnStatistics fields (variance, range, percentile05, percentile95,
    generatedAt) must be mapped through to the Statistics node when present.
    Null values (non-numeric columns) must not appear in the node properties.
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
                        "name": "numeric_col",
                        "source": {"fileObject": {"@id": "fo-1"}, "extract": {"column": "numeric_col"}},
                        "statistics": {
                            "@id": "stats-1",
                            "@type": "dg:ColumnStatistics",
                            "rowCount": 200,
                            "mean": 5.0,
                            "variance": 2.5,
                            "range": 10.0,
                            "percentile05": 1.2,
                            "percentile95": 9.8,
                            "generatedAt": "2026-06-18T08:00:00+00:00",
                        },
                    },
                    {
                        "@type": "cr:Field",
                        "@id": "f-2",
                        "name": "text_col",
                        "source": {"fileObject": {"@id": "fo-1"}, "extract": {"column": "text_col"}},
                        "statistics": {
                            "@id": "stats-2",
                            "@type": "dg:ColumnStatistics",
                            "rowCount": 200,
                            "mean": None,
                            "variance": None,
                            "range": None,
                            "percentile05": None,
                            "percentile95": None,
                            "generatedAt": None,
                        },
                    },
                ],
            }
        ],
    }

    result = croissant_to_pgjson(profile, mapping)
    stats_nodes = {n["id"]: n for n in result["nodes"] if "Statistics" in n.get("labels", [])}

    assert len(stats_nodes) == 2

    # Numeric column: all new fields present
    numeric = stats_nodes["stats-1"]["properties"]
    assert numeric.get("variance") == 2.5
    assert numeric.get("range") == 10.0
    assert numeric.get("percentile05") == 1.2
    assert numeric.get("percentile95") == 9.8
    assert numeric.get("generatedAt") == "2026-06-18T08:00:00+00:00"

    # Non-numeric column: null fields are absent from properties
    text = stats_nodes["stats-2"]["properties"]
    assert "variance" not in text
    assert "range" not in text
    assert "percentile05" not in text
    assert "percentile95" not in text
    assert "generatedAt" not in text
    assert text.get("rowCount") == 200


# Dataset attributes whose @context term expands to a dg:/sc: IRI. The producer
# emits the bare term, so the mapping must look them up by the bare term too.
_CONTEXT_TERM_DATASET_ATTRIBUTES = {
    "headline": "a headline",
    "keywords": ["math"],
    "fieldOfScience": ["MATHEMATICS"],
    "status": "loaded",
    "access": "public",
    "doi": "10.1234/abcd",
    "uploadedBy": "ADMIN",
    "archivedAt": "s3://bucket/ds-1",
}


def test_dataset_context_term_attributes_are_mapped(mapping_file: Path):
    """
    These attributes reach us as bare @context terms ("doi"), not as the compact
    IRI the term expands to ("dg:doi"). Looking them up by the prefixed spelling
    drops them from the Dataset node without any error.
    """
    mapping = yaml.safe_load(mapping_file.open("r"))

    profile = {
        "@id": "ds-1",
        "@type": "sc:Dataset",
        "name": "test",
        **_CONTEXT_TERM_DATASET_ATTRIBUTES,
    }

    result = croissant_to_pgjson(profile, mapping)
    props = next(n for n in result["nodes"] if n["id"] == "ds-1")["properties"]

    missing = _CONTEXT_TERM_DATASET_ATTRIBUTES.keys() - props.keys()
    assert not missing, f"Dropped dataset attributes: {missing}"
    for key, value in _CONTEXT_TERM_DATASET_ATTRIBUTES.items():
        assert props[key] == value


def test_column_semantic_type_is_mapped(mapping_file: Path):
    """semanticType is a bare @context term as well, same failure mode as doi."""
    mapping = yaml.safe_load(mapping_file.open("r"))

    profile = {
        "@id": "ds-1",
        "@type": "sc:Dataset",
        "name": "test",
        "distribution": [
            {
                "@type": "cr:FileObject",
                "@id": "fo-1",
                "name": "data.csv",
                "encodingFormat": "text/csv",
            }
        ],
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
                        "semanticType": "identifier",
                    }
                ],
            }
        ],
    }

    result = croissant_to_pgjson(profile, mapping)
    column = next(n for n in result["nodes"] if n["id"] == "f-1")

    assert column["properties"].get("semanticType") == "identifier"


def test_column_describes_itself(mapping_file: Path):
    """
    description/dataType/type are declared on the Column schema and sent by every
    producer, but the Column variant did not map them, so they never reached the
    graph.
    """
    mapping = yaml.safe_load(mapping_file.open("r"))

    profile = {
        "@id": "ds-1",
        "@type": "sc:Dataset",
        "name": "test",
        "distribution": [
            {
                "@type": "cr:FileObject",
                "@id": "fo-1",
                "name": "data.csv",
                "encodingFormat": "text/csv",
            }
        ],
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
                        "description": "the first column",
                        "dataType": "sc:Integer",
                        "source": {"fileObject": {"@id": "fo-1"}, "extract": {"column": "col_a"}},
                    }
                ],
            }
        ],
    }

    result = croissant_to_pgjson(profile, mapping)
    props = next(n for n in result["nodes"] if n["id"] == "f-1")["properties"]

    assert props.get("description") == "the first column"
    assert props.get("dataType") == "sc:Integer"
    assert props.get("type") == "cr:Field"


def _json_stations_graph(mapping_file: Path) -> dict:
    profile = json.load((PROFILES_DIR / "heavy" / "json_stations_heavy.json").open("r"))
    return croissant_to_pgjson(profile, yaml.safe_load(mapping_file.open("r")))


def test_json_recordset_attributes(mapping_file: Path):
    """
    A cr:RecordSet read from a JSON file has no source of its own (only its
    fields do), so it must take its fields' file to get the JSON variant.
    """
    result = _json_stations_graph(mapping_file)

    props = next(n for n in result["nodes"]
                 if "cr:RecordSet" in n["labels"])["properties"]

    assert props["recordPath"] == "$.stations[*]"
    assert props["jsonShape"] == "wrapped_records"
    assert props["recordCount"] == 412
    assert props["schemaConsistencyPercentage"] == 93.0


def test_json_fields_are_mapped(mapping_file: Path):
    """
    Fields of a JSON file become JSONField nodes, linked like CSV columns, with
    their JSON statistics. Before JSONField they had no labels, and the whole
    dataset failed validation.
    """
    result = _json_stations_graph(mapping_file)
    by_id = {n["id"]: n for n in result["nodes"]}
    edges = {(e["from"], e["labels"][0], e["to"]) for e in result["edges"]}

    rs = next(n for n in result["nodes"] if "cr:RecordSet" in n["labels"])
    file = next(n for n in result["nodes"] if "JSON" in n["labels"])
    fields = [n for n in result["nodes"] if "JSONField" in n["labels"]]

    assert len(fields) == 11
    for f in fields:
        assert set(f["labels"]) == {"Column", "JSONField", "cr:Field"}
        assert (rs["id"], "field", f["id"]) in edges
        assert (f["id"], "source/fileObject", file["id"]) in edges

    operator = next(f for f in fields if f["properties"]["name"] == "operator")
    assert operator["properties"]["jsonPath"] == "$.stations[*].operator"
    assert operator["properties"]["repeated"] is False

    stats_id = next(to for frm, label, to in edges
                    if frm == operator["id"] and label == "statistics")
    stats = by_id[stats_id]["properties"]
    assert stats["missingCount"] == 173
    assert json.loads(stats["jsonTypes"]) == {"string": 239}
    assert json.loads(stats["topValues"])[0] == {"value": "NOA", "count": 182}

    Dataset.model_validate(result)


@pytest.mark.parametrize("encoding_format", ["application/json", "application/jsonl", "application/jsonlines"])
def test_json_files_labelled(mapping_file: Path, encoding_format: str):
    """JSON and JSON Lines files get the JSON labels, and a file set contains its files."""
    mapping = yaml.safe_load(mapping_file.open("r"))

    profile = {
        "@id": "ds-1",
        "@type": "sc:Dataset",
        "name": "test",
        "distribution": [
            {"@type": "cr:FileSet", "@id": "fs-1", "name": "readings",
             "encodingFormat": encoding_format},
            {"@type": "cr:FileObject", "@id": "fo-1", "name": "part-1",
             "encodingFormat": encoding_format, "containedIn": {"@id": "fs-1"}},
        ],
    }

    result = croissant_to_pgjson(profile, mapping)
    labels = {n["id"]: n["labels"] for n in result["nodes"]}

    assert "JSONSet" in labels["fs-1"]
    assert "JSON" in labels["fo-1"]
    assert {"from": "fo-1", "to": "fs-1", "labels": ["containedIn"], "properties": {}} in result["edges"]


def test_recordset_reading_several_files_gets_no_variant(mapping_file: Path):
    """A RecordSet whose fields read different files has no single source, so it keeps the generic mapping."""
    mapping = yaml.safe_load(mapping_file.open("r"))

    profile = {
        "@id": "ds-1",
        "@type": "sc:Dataset",
        "name": "test",
        "distribution": [
            {"@type": "cr:FileObject", "@id": "fo-1", "name": "a.json", "encodingFormat": "application/json"},
            {"@type": "cr:FileObject", "@id": "fo-2", "name": "b.json", "encodingFormat": "application/json"},
        ],
        "recordSet": [
            {
                "@type": "cr:RecordSet",
                "@id": "rs-1",
                "name": "joined",
                "recordPath": "$[*]",
                "field": [
                    {"@type": "cr:Field", "@id": "f-1", "name": "a",
                     "source": {"fileObject": {"@id": "fo-1"}, "extract": {"jsonPath": "$[*].a"}}},
                    {"@type": "cr:Field", "@id": "f-2", "name": "b",
                     "source": {"fileObject": {"@id": "fo-2"}, "extract": {"jsonPath": "$[*].b"}}},
                ],
            }
        ],
    }

    result = croissant_to_pgjson(profile, mapping)
    props = next(n for n in result["nodes"] if n["id"] == "rs-1")["properties"]

    assert props == {"name": "joined"}
