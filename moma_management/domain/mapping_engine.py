"""
Thrown-togetether mapping engine to convert Croissant JSON-LD to PG-JSON according to a mapping specification.
"""
from __future__ import annotations

from typing import Any


def _get(data: dict, path: str) -> Any:
    """Traverse a dot-separated path through nested dicts.

    "@type" / "@id" are treated as ordinary keys (no special handling needed
    since they exist as-is in the Croissant JSON).
    """
    val = data
    for key in path.split("."):
        if not isinstance(val, dict):
            return None
        val = val.get(key)
    return val


def _clean(value: Any) -> Any:
    """Remove None items from lists; pass scalars through unchanged."""
    if isinstance(value, list):
        return [v for v in value if v is not None]
    return value


def _make_edge(from_id: str, to_id: str, label: str) -> dict:
    return {"from": from_id, "to": to_id, "labels": [label], "properties": {}}


def _resolve_labels(data: dict, spec) -> list[str]:
    """Return the label list for *data* given a labels spec.

    Spec can be:
    - a plain list                    ->  used as-is
    - a dict with default/by_encoding ->  pick by encodingFormat, with optional
                                          when/then/else conditional

    "@field" tokens in a list (e.g. "@type") are expanded to their data values.
    """
    if not isinstance(spec, dict):
        labels = list(spec)
    else:
        default = spec.get("default", [])
        encoding = data.get("encodingFormat", "").lower()
        override = spec.get("by_encoding", {}).get(encoding, default)

        if isinstance(override, dict) and "when" in override:
            field_present = _get(data, override["when"]) is not None
            labels = override["then"] if field_present else override["else"]
        else:
            labels = list(override)

    # Expand "@field" placeholders (e.g. "@type") to their actual data values
    return [data.get(label) if label.startswith("@") else label for label in labels]


def _resolve_properties(
    data: dict,
    prop_spec,
    exclude: list | None = None,
    strip_prefix: str | None = None,
) -> dict:
    """Build a properties dict from the mapping's properties declaration.

    Two forms:
    - prop_spec == "*"   wildcard: copy all non-None fields except those in *exclude*
    - prop_spec is dict  explicit mapping: { moma_property: croissant_path }
                         croissant_path uses dot-notation; "@type" / "@id" work as keys

    Reading the rules:
        "MoMa property <moma_property> comes from Croissant field <croissant_path>"
    """
    if prop_spec == "*":
        excluded = set(exclude or [])
        props = {}
        for k, v in data.items():
            if v is None or k in excluded:
                continue
            key = k.removeprefix(strip_prefix) if strip_prefix else k
            props[key] = _clean(v)
        return props

    props = {}
    for moma_key, croissant_path in (prop_spec or {}).items():
        if (value := _get(data, str(croissant_path))) is not None:
            props[moma_key] = _clean(value)
    return props


def _active_variant(data: dict, spec: dict) -> dict:
    """Merge the first matching variant into *spec*, or return *spec* unchanged.

    A variant with 'when' matches when that dot-path is present in the data.
    A variant with 'else: true' is the unconditional fallback.
    """
    for variant in spec.get("variants", []):
        if "when" in variant:
            if _get(data, variant["when"]) is not None:
                return {**spec, **variant}
        elif variant.get("else"):
            return {**spec, **variant}
    return spec


def _build(
    data: dict,
    spec: dict,
    parent_id: str | None,
) -> tuple[list[dict], list[dict]]:
    """Build nodes and edges for one Croissant item according to *spec*.

    Returns (nodes, edges).
    """
    node_id = _get(data, spec["id"])
    if not node_id:
        return [], []

    active = _active_variant(data, spec)

    labels = _resolve_labels(data, active.get("labels", []))
    properties = _resolve_properties(
        data, active.get("properties"), active.get("exclude"), active.get("strip_prefix"))

    # Wildcard node with no properties -> skip (e.g. empty statistics object)
    if active.get("properties") == "*" and not properties:
        return [], []

    nodes: list[dict] = [
        {"id": node_id, "labels": labels, "properties": properties}]
    edges: list[dict] = []

    # $self / $parent are special tokens; any other value is a dot-path into data.
    def _resolve_endpoint(token: str) -> str | None:
        if token == "$self":
            return node_id
        if token == "$parent":
            return parent_id
        return _get(data, token)

    for edge_spec in active.get("edges", []):
        from_id = _resolve_endpoint(edge_spec.get("from", "$parent"))
        to_id = _resolve_endpoint(edge_spec.get("to",   "$self"))
        if from_id and to_id:
            edges.append(_make_edge(from_id, to_id, edge_spec["label"]))

    return nodes, edges


def croissant_to_pgjson(data: dict, mapping: dict) -> dict:
    """Convert a Croissant JSON-LD dict to PG-JSON using the given mapping.

    The mapping is a dict loaded from mapping.yml.  Each top-level key is a
    MoMa node type; the spec under it declares:

        croissant_type  which Croissant @type this node comes from
        id              dot-path to the node's identifier in Croissant
        labels          MoMa labels (plain list or by_encoding dict)
        properties      { moma_property: croissant_path } – explicit field mapping
        children        [ { node: <key>, path: <field> } ] – nested Croissant items
        edges           edge specs using $self / $parent tokens
        variants        conditional sub-specs (when / else)
    """
    nodes: list[dict] = []
    edges: list[dict] = []

    # Inject node_type from the mapping key so variants can override it selectively.
    for key, spec in mapping.items():
        spec.setdefault("node_type", key)

    def _collect(items, spec: dict, parent_id: str | None = None) -> None:
        for item in (items if isinstance(items, list) else [items]):
            ns, es = _build(item, spec, parent_id)
            nodes.extend(ns)
            edges.extend(es)
            if ns:
                node_id = ns[0]["id"]
                for child_ref in spec.get("children", []):
                    child_spec = mapping[child_ref["node"]]
                    _collect(
                        item.get(child_ref["path"], []), child_spec, parent_id=node_id)

    root_spec = next(iter(mapping.values()))
    _collect(data, root_spec)

    return {"nodes": nodes, "edges": edges}
