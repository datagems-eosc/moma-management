from json import dumps
from pathlib import Path
from typing import Any, Dict
from uuid import uuid4

from moma_management.domain.dataset_relationship import DatasetRelationship
from moma_management.domain.generated.edges.edge_schema import Edge
from moma_management.domain.generated.nodes.node_schema import Node

# ---------------------------------------------------------------------------
# Fixed UUIDs for test fixtures
# These constants replace human-readable names that are not valid UUIDs,
# which are required by the generated Node/Edge schemas.
# ---------------------------------------------------------------------------

# populated_repository fixture
DS_ALPHA_ID = "a0000000-0000-0000-0000-000000000001"
DS_ALPHA_FILE_ID = "a0000000-0000-0000-0000-000000000002"
DS_BETA_ID = "b0000000-0000-0000-0000-000000000001"
DS_BETA_FILE_ID = "b0000000-0000-0000-0000-000000000002"
DS_GAMMA_ID = "c0000000-0000-0000-0000-000000000001"
DS_GAMMA_FILE_ID = "c0000000-0000-0000-0000-000000000002"

# mixed_date_repository fixture
DS_DATE_A_ID = "d0000000-0000-0000-0000-0000000000a1"
DS_DATE_A_FILE_ID = "d0000000-0000-0000-0000-0000000000a2"
DS_DATE_B_ID = "d0000000-0000-0000-0000-0000000000b1"
DS_DATE_B_FILE_ID = "d0000000-0000-0000-0000-0000000000b2"
DS_DATE_C_ID = "d0000000-0000-0000-0000-0000000000c1"
DS_DATE_C_FILE_ID = "d0000000-0000-0000-0000-0000000000c2"
DS_DATE_D_ID = "d0000000-0000-0000-0000-0000000000d1"
DS_DATE_D_FILE_ID = "d0000000-0000-0000-0000-0000000000d2"

# mixed_types_repository fixture
DS_MIXED_ID = "e0000000-0000-0000-0000-000000000001"
DS_MIXED_CSV_FILE_ID = "e0000000-0000-0000-0000-000000000002"
DS_MIXED_PDF_FILE_ID = "e0000000-0000-0000-0000-000000000003"
DS_CSV_ONLY_ID = "f0000000-0000-0000-0000-000000000001"
DS_CSV_ONLY_FILE_ID = "f0000000-0000-0000-0000-000000000002"
DS_PDF_ONLY_ID = "f0000000-0000-0000-0000-000000000011"
DS_PDF_ONLY_FILE_ID = "f0000000-0000-0000-0000-000000000012"

# encoding_format_repository fixture (cr:FileObject-only scenario)
DS_JSON_ID = "a1000000-0000-0000-0000-000000000001"
DS_JSON_FILE_ID = "a1000000-0000-0000-0000-000000000002"

# test_prevent_ap_or_ml_traversal fixture
DS_FORBIDDEN_TEST_ID = "00000000-dead-0000-0000-000000000001"
BLUE_FILE_NODE_ID = "00000000-dead-0000-0000-000000000002"
ORANGE_NODE_BASE = "00000000-dead-0000-0000-{index:012x}"


def _strip_null_props(props: Dict[str, Any]) -> Dict[str, Any]:
    """Remove keys whose value is None or an empty list (Neo4j drops them)."""
    return {
        k: v for k, v in props.items()
        if v is not None and not (isinstance(v, list) and len(v) == 0)
    }


def normalize(pg_json: dict) -> dict:
    """
    Sort a PgJSon node and edges by their ids to ease visual comparison
    This also consider that null properties are artifacts and should be ignored
    """

    nodes = []
    for n in pg_json.get("nodes", []):
        nodes.append({
            **n,
            "labels": sorted(n.get("labels", [])),
        })

    edges = []
    for e in pg_json.get("edges", []):
        edges.append({
            **e,
            "labels": sorted(e.get("labels", [])),
        })

    return {
        "nodes": sorted(nodes, key=lambda n: n["id"]),
        "edges": sorted(
            edges,
            key=lambda e: (e["from"], e["to"], e["labels"]
                           [0] if e["labels"] else ""),
        ),
    }


def save(pg_json: dict, output_path: Path):
    """
    Save the PgJSon to a file, normalizing it first for easier visual comparison.
    """
    output_path.parent.mkdir(exist_ok=True, parents=True)
    normalized = normalize(pg_json)
    output_path.write_text(dumps(normalized, indent=2, sort_keys=True))


def make_file_object_relationship(
    ds_a: str, ds_b: str, file_a: str, file_b: str, with_distribution: bool = False,
) -> DatasetRelationship:
    """Build the dataset-linking FileObject shape (issue #30):

    BasicDLElement -HAS_TARGET-> both datasets,
    BasicDLElement -HAS_DETAIL-> FileObjectLinkingElement
    -HAS_COMPARISON-> FileObjectComparison -HAS_TARGET-> both files,
    and optionally ds_a -distribution-> file_a, ds_b -distribution-> file_b.
    """
    root_id, fole_id, foc_id = str(uuid4()), str(uuid4()), str(uuid4())
    return DatasetRelationship(
        nodes=[
            Node(id=root_id, labels=["BasicDLElement"], properties={"similarityScore": 85.07}),
            Node(id=ds_a, labels=["sc:Dataset"], properties={}),
            Node(id=ds_b, labels=["sc:Dataset"], properties={}),
            Node(id=fole_id, labels=["FileObjectLinkingElement"],
                 properties={"date": "2026-05-16", "similarityScore": 100.0, "usesMetrics": "avg"}),
            Node(id=foc_id, labels=["FileObjectComparison"],
                 properties={"targetProperty": "filename", "similarityScore": 100.0}),
            Node(id=file_a, labels=["cr:FileObject"], properties={}),
            Node(id=file_b, labels=["cr:FileObject"], properties={}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": ds_a, "labels": ["HAS_TARGET"]}),
            Edge(**{"from": root_id, "to": ds_b, "labels": ["HAS_TARGET"]}),
            Edge(**{"from": root_id, "to": fole_id, "labels": ["HAS_DETAIL"]}),
            Edge(**{"from": fole_id, "to": foc_id, "labels": ["HAS_COMPARISON"]}),
            Edge(**{"from": foc_id, "to": file_a, "labels": ["HAS_TARGET"]}),
            Edge(**{"from": foc_id, "to": file_b, "labels": ["HAS_TARGET"]}),
        ] + ([
            Edge(**{"from": ds_a, "to": file_a, "labels": ["distribution"]}),
            Edge(**{"from": ds_b, "to": file_b, "labels": ["distribution"]}),
        ] if with_distribution else []),
    )
