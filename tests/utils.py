from json import dumps
from pathlib import Path
from typing import Any, Dict


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
