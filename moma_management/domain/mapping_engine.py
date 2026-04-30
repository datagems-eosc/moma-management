from __future__ import annotations

from typing import Any, Dict, List, Tuple

from pydantic import BaseModel

# =========================================================
# Errors
# =========================================================


class MappingError(Exception):
    pass


# =========================================================
# JSON path helper
# =========================================================

def get_path(data: dict, path: str) -> Any:
    """Simple dot-path resolver."""
    cur = data
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


# =========================================================
# Truth evaluator
# =========================================================

def truthy(data: dict, expr: str) -> bool:
    if "||" in expr:
        return any(truthy(data, part.strip()) for part in expr.split("||"))
    if "&&" in expr:
        return all(truthy(data, part.strip()) for part in expr.split("&&"))
    if "^=" in expr:
        left, right = expr.split("^=", 1)
        return str(get_path(data, left.strip())).startswith(right.strip().strip("'\""))
    if "==" in expr:
        left, right = expr.split("==", 1)
        return str(get_path(data, left.strip())) == right.strip().strip("'\"")
    return get_path(data, expr) is not None


# =========================================================
# Label resolver (NO None allowed)
# =========================================================

def resolve_labels(data: dict, spec: Any) -> List[str]:
    if not spec:
        return []

    if isinstance(spec, list) and spec and isinstance(spec[0], dict):
        for rule in spec:
            if "case" in rule and truthy(data, rule["case"]):
                return _expand_labels(data, rule["value"])
            if "default" in rule:
                return _expand_labels(data, rule["default"])

    return _expand_labels(data, spec)


def _expand_labels(data: dict, labels: List[str]) -> List[str]:
    out: List[str] = []

    for l in labels:
        if isinstance(l, str) and l.startswith("@"):
            key = l[1:]  # strip the dereference marker
            # try the bare key first, then the original @-prefixed key
            v = data.get(key) if key in data else data.get(l)
            if v is not None:   # 🔥 critical fix
                out.append(v)
        else:
            if l is not None:
                out.append(l)

    return out


# =========================================================
# Schema registry from generated Pydantic models
# =========================================================

SchemaRegistry = Dict[str, set]

# PG-JSON envelope fields that live at node top-level, not inside properties.
_PGJSON_ENVELOPE_FIELDS = frozenset({"id", "labels", "properties"})


def get_model_fields(model: type[BaseModel]) -> set:
    """Extract JSON-side field names (aliases where present) from a Pydantic model,
    excluding PG-JSON envelope fields that are structural, not content."""
    fields: set = set()
    for name, info in model.model_fields.items():
        alias = info.alias if info.alias else name
        if alias not in _PGJSON_ENVELOPE_FIELDS:
            fields.add(alias)
    return fields


def build_schema_registry() -> SchemaRegistry:
    """Build a type-name → field-set mapping from the generated Pydantic classes."""
    from moma_management.domain.generated.nodes.dataset.column_schema import (
        PgProperties as ColumnProps,
    )
    from moma_management.domain.generated.nodes.dataset.data_schema import (
        PgProperties as DataProps,
    )
    from moma_management.domain.generated.nodes.dataset.dataset_schema import (
        PgProperties as DatasetProps,
    )

    return {
        "Dataset": get_model_fields(DatasetProps),
        "Distribution": get_model_fields(DataProps),
        "Column": get_model_fields(ColumnProps),
    }


# =========================================================
# Map resolver (STRICT schema projection)
# =========================================================

def resolve_map(data: dict, spec: Dict[str, str], schema_fields: set) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    for k, path in (spec or {}).items():
        v = get_path(data, path)
        if v is None:
            continue
        if k in schema_fields:
            out[k] = v

    return out


# =========================================================
# Schema completion (compatibility layer)
# =========================================================

def apply_schema_defaults(node: dict, schema_fields: set) -> None:
    props = node.get("properties", {})

    for field in schema_fields:
        if field in ("id", "type"):
            continue

        # old engine behavior: missing = empty string
        props.setdefault(field, "")

    node["properties"] = props


# =========================================================
# Variant resolver
# =========================================================

def resolve_variant(data: dict, spec: dict) -> dict:
    variants = spec.get("variants")
    if not variants:
        return spec

    for v in variants:
        if "when" in v and truthy(data, v["when"]):
            return {**spec, **v}
        if "default" in v:
            return {**spec, **v}

    return spec


# =========================================================
# Edge resolver
# =========================================================

def resolve_edges(
    data: dict,
    spec: dict,
    node_id: str,
    parent_id: str | None,
) -> List[dict]:
    """Build PG-JSON edges from the 'edges' section of a mapping spec."""
    edge_specs = spec.get("edges")
    if not edge_specs:
        return []

    edges: List[dict] = []
    for edge_spec in edge_specs:
        from_ref = edge_spec["from"]
        to_ref = edge_spec["to"]
        label = edge_spec["label"]

        from_id = _resolve_edge_ref(data, from_ref, node_id, parent_id)
        to_id = _resolve_edge_ref(data, to_ref, node_id, parent_id)

        if from_id and to_id:
            edges.append({
                "from": from_id,
                "to": to_id,
                "labels": [label],
                "properties": {},
            })

    return edges


def _resolve_edge_ref(
    data: dict,
    ref: str,
    node_id: str,
    parent_id: str | None,
) -> str | None:
    """Resolve an edge endpoint reference ('self', 'parent', or a dot-path)."""
    if ref == "self":
        return node_id
    if ref == "parent":
        return parent_id
    return get_path(data, ref)


# =========================================================
# Node builder
# =========================================================

def _resolve_schema_fields(
    type_name: str,
    spec: dict,
    schema_registry: SchemaRegistry,
) -> set:
    """Look up schema fields for *type_name*; fall back to the spec's own map keys."""
    fields = schema_registry.get(type_name)
    if fields is not None:
        return fields
    return set(spec.get("map", {}).keys())


def build_node(
    data: dict,
    spec: dict,
    parent_id: str | None,
    type_name: str,
    schema_registry: SchemaRegistry,
) -> Tuple[List[dict], List[dict]]:

    node_id = get_path(data, spec["id"])
    if not node_id:
        return [], []

    if "match" in spec and not truthy(data, spec["match"]):
        return [], []

    schema_fields = _resolve_schema_fields(type_name, spec, schema_registry)

    node = {
        "id": node_id,
        "labels": resolve_labels(data, spec.get("labels")),
        "properties": resolve_map(data, spec.get("map"), schema_fields),
    }

    edges = resolve_edges(data, spec, node_id, parent_id)

    return [node], edges


# =========================================================
# Recursive traversal
# =========================================================

def run_spec(
    data: dict,
    spec: dict,
    mapping: dict,
    parent_id: str | None,
    type_name: str,
    schema_registry: SchemaRegistry,
) -> Tuple[List[dict], List[dict]]:

    spec = resolve_variant(data, spec)

    # A variant may declare its own type (e.g. Column, Text, PDF).
    effective_type = spec.get("type", type_name)

    nodes, edges = build_node(data, spec, parent_id,
                              effective_type, schema_registry)

    if not nodes:
        return [], []

    node_id = nodes[0]["id"]

    children = spec.get("children", {})

    if not isinstance(children, dict):
        raise MappingError(f"Invalid children format in node {node_id}")

    for key, child_type in children.items():

        if child_type not in mapping:
            raise MappingError(
                f"Unknown child type '{child_type}' in node '{node_id}'. "
                f"Available: {list(mapping.keys())}"
            )

        child_spec = mapping[child_type]
        child_data = data.get(key, [])

        if isinstance(child_data, list):
            for item in child_data:
                n, e = run_spec(item, child_spec, mapping,
                                node_id, child_type, schema_registry)
                nodes.extend(n)
                edges.extend(e)

        elif isinstance(child_data, dict):
            n, e = run_spec(child_data, child_spec,
                            mapping, node_id, child_type, schema_registry)
            nodes.extend(n)
            edges.extend(e)

    return nodes, edges


# =========================================================
# Entry point
# =========================================================

def _enrich_field_sources(data: dict) -> None:
    """Inject resolved source distribution into each field's data.

    For each field in every recordSet, resolve the ``source.fileObject.@id``
    or ``source.fileSet.@id`` back to the matching distribution entry and
    store it as ``_source`` on the field dict.  This lets variant conditions
    reference properties of the source distribution (e.g.
    ``_source.encodingFormat``).

    Also injects ``_source`` into each RecordSet itself by resolving the
    ``source.@id`` (direct FileObject ref used in PDF profiles), as well as
    ``source.fileObject.@id`` / ``source.fileSet.@id``.
    """
    dist_index: dict[str, dict] = {}
    for dist in data.get("distribution", []):
        did = dist.get("@id")
        if did:
            dist_index[did] = dist

    for rs in data.get("recordSet", []):
        # Enrich the RecordSet itself so variant conditions can use _source.
        rs_source = rs.get("source", {})
        if isinstance(rs_source, dict):
            # Direct @id reference (e.g. PDF profiles: source: {"@id": "..."})
            direct_id = rs_source.get("@id")
            if direct_id and direct_id in dist_index:
                rs["_source"] = dist_index[direct_id]
            else:
                for ref_key in ("fileObject", "fileSet"):
                    ref = rs_source.get(ref_key)
                    if isinstance(ref, dict):
                        ref_id = ref.get("@id")
                        if ref_id and ref_id in dist_index:
                            rs["_source"] = dist_index[ref_id]
                            break

        for field in rs.get("field", []):
            source = field.get("source", {})
            for ref_key in ("fileObject", "fileSet"):
                ref = source.get(ref_key)
                if isinstance(ref, dict):
                    ref_id = ref.get("@id")
                    if ref_id and ref_id in dist_index:
                        field["_source"] = dist_index[ref_id]
                        break


def croissant_to_pgjson(
    data: dict,
    mapping: dict,
    schema_registry: SchemaRegistry | None = None,
) -> dict:
    if schema_registry is None:
        schema_registry = build_schema_registry()

    _enrich_field_sources(data)

    root = next(iter(mapping))
    root_spec = mapping[root]

    nodes, edges = run_spec(
        data,
        root_spec,
        mapping,
        parent_id=None,
        type_name=root,
        schema_registry=schema_registry,
    )

    return {
        "nodes": nodes,
        "edges": edges
    }
