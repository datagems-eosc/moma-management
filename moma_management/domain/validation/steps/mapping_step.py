from moma_management.domain.pg_json_graph import MomaEntity

from ..schema_error import SchemaError
from .step import ValidationStep


class MappingStep(ValidationStep):
    def handle(self, data: MomaEntity) -> list[SchemaError]:
        raw = data.model_dump(by_alias=True, mode="json")
        return _validate_mappings(raw) + self._chain(data)


def _extract_bracket_value(expr: str, prefix: str) -> str | None:
    """Extract the first bracket key from notation ``prefix['key']``."""
    marker = f"{prefix}['"
    if not expr.startswith(marker):
        return None
    rest = expr[len(marker):]
    end = rest.find("']")
    if end == -1:
        return None
    return rest[:end]


def _extract_bracket_path(expr: str, prefix: str) -> list[str] | None:
    """Extract all bracket keys from ``prefix['k1']['k2']...`` as a list."""
    marker = f"{prefix}['"
    if not expr.startswith(marker):
        return None
    rest = expr[len(marker):]
    segments = []
    while rest:
        end = rest.find("']")
        if end == -1:
            break
        segments.append(rest[:end])
        rest = rest[end + 2:]
        if rest.startswith("['"):
            rest = rest[2:]
        else:
            break
    return segments or None


def _resolve_param_type(param: dict, nested: list[str]) -> str | None:
    """Walk nested property segments through a parameter's ``properties`` to find the leaf type."""
    current = param
    for segment in nested:
        props = current.get("properties") or {}
        if segment not in props:
            return current.get("type")
        current = props[segment]
    return current.get("type")


def _validate_mappings(data: dict) -> list[SchemaError]:
    """Validate ``mapping`` dicts on AP edges.

    For every key/value pair in an edge's ``mapping``:

    * **input** edge (``Node -[input]-> Operator``):
      key ``to['inputs']['<param>']`` — ``<param>`` must exist in the
      Operator's declared ``inputs`` array.
      value ``from['<prop>']`` — for a ``ResultType`` node ``<prop>`` must equal
      the node's ``name`` value; for a ``Data`` node ``<prop>`` must be a key
      in the node's ``properties``.

    * **output** edge (``Operator -[output]-> Node``):
      key ``to['<prop>']`` — same rules as above for the target node.
      value ``from['outputs']['<param>']`` — ``<param>`` must exist in the
      Operator's declared ``outputs`` array.

    Additionally checks that the declared ``type`` of the matched Operator
    parameter agrees with the concrete ``ResultType`` label on the target node
    (e.g. a parameter typed ``"string"`` must connect to a
    ``["ResultType", "string"]`` node).
    """
    errors: list[SchemaError] = []
    nodes: list[dict] = data.get("nodes") or []
    edges: list[dict] = data.get("edges") or []

    node_by_id: dict[str, dict] = {str(n.get("id", "")): n for n in nodes}

    for i, edge in enumerate(edges):
        props = edge.get("properties") or {}
        mapping = props.get("mapping")
        if not mapping:
            continue

        from_id = str(edge.get("from", ""))
        to_id = str(edge.get("to", ""))
        from_node = node_by_id.get(from_id, {})
        to_node = node_by_id.get(to_id, {})
        edge_label = (edge.get("labels") or [""])[0]

        from_props: dict = from_node.get("properties") or {}
        from_labels: list[str] = from_node.get("labels") or []
        to_props: dict = to_node.get("properties") or {}
        to_labels: list[str] = to_node.get("labels") or []
        is_result_type = "ResultType" in to_labels
        is_dataset = "sc:Dataset" in to_labels

        def _check_node_prop(prop: str, node_props: dict, node_labels: list[str], path: str) -> None:
            if "ResultType" in node_labels:
                rt_name = node_props.get("name", "")
                if prop != rt_name:
                    errors.append(SchemaError(
                        keyword="mappingProperty",
                        instancePath=path,
                        schemaPath="#/x-mapping-rules/propertyExistence",
                        params={"propName": prop, "nodeName": rt_name},
                        message=(
                            f"ResultType property reference '{prop}' does not "
                            f"match node name '{rt_name}'"
                        ),
                    ))
            else:
                if prop not in node_props:
                    errors.append(SchemaError(
                        keyword="mappingProperty",
                        instancePath=path,
                        schemaPath="#/x-mapping-rules/propertyExistence",
                        params={"propName": prop},
                        message=f"Property '{prop}' not found on target node",
                    ))

        for key, value in mapping.items():
            path_prefix = f"/edges/{i}/properties/mapping"

            if edge_label == "input":
                # After edge reversal: Node -[input]-> Operator
                # operator is to_node; data/RT source is from_node
                # key: to['inputs']['<param>'] — operator input param
                # value: from['<prop>'] — source node property
                input_is_result_type = "ResultType" in from_labels
                input_is_dataset = "sc:Dataset" in from_labels

                param_name = _extract_bracket_value(key, "to['inputs']")
                if param_name is not None:
                    declared = to_props.get("inputs") or []
                    known = {p["name"] for p in declared if isinstance(
                        p, dict) and "name" in p}
                    if param_name not in known:
                        errors.append(SchemaError(
                            keyword="mappingParameter",
                            instancePath=f"{path_prefix}/{key}",
                            schemaPath="#/x-mapping-rules/parameterExistence",
                            params={"paramName": param_name,
                                    "direction": "inputs"},
                            message=(
                                f"Operator input parameter '{param_name}' "
                                f"is not declared in 'inputs'"
                            ),
                        ))

                # value: from['<prop>'] — source node property
                prop_name = _extract_bracket_value(value, "from")
                if prop_name is not None and not input_is_dataset:
                    _check_node_prop(prop_name, from_props,
                                     from_labels, f"{path_prefix}/{key}")

                # Type-compatibility check (ResultType only)
                if param_name is not None and prop_name is not None and input_is_result_type and not input_is_dataset:
                    declared = to_props.get("inputs") or []
                    param = next(
                        (p for p in declared if isinstance(p, dict)
                         and p.get("name") == param_name),
                        None,
                    )
                    if param:
                        rt_type = next(
                            (lbl for lbl in from_labels if lbl != "ResultType"), None)
                        if rt_type and param.get("type") != rt_type:
                            errors.append(SchemaError(
                                keyword="mappingTypeCompatibility",
                                instancePath=f"{path_prefix}/{key}",
                                schemaPath="#/x-mapping-rules/typeCompatibility",
                                params={"paramType": param.get(
                                    "type"), "resultType": rt_type},
                                message=(
                                    f"Type mismatch: operator input '{param_name}' has type "
                                    f"'{param.get('type')}' but ResultType node is '{rt_type}'"
                                ),
                            ))

            elif edge_label == "output":
                # key: to['<prop>'] — target node property
                prop_name = _extract_bracket_value(key, "to")
                if prop_name is not None and not is_dataset:
                    _check_node_prop(prop_name, to_props,
                                     to_labels, f"{path_prefix}/{key}")

                # value: from['outputs']['<param>'] — operator output param
                param_name = _extract_bracket_value(value, "from['outputs']")
                if param_name is not None:
                    declared = from_props.get("outputs") or []
                    known = {p["name"] for p in declared if isinstance(
                        p, dict) and "name" in p}
                    if param_name not in known:
                        errors.append(SchemaError(
                            keyword="mappingParameter",
                            instancePath=f"{path_prefix}/{key}",
                            schemaPath="#/x-mapping-rules/parameterExistence",
                            params={"paramName": param_name,
                                    "direction": "outputs"},
                            message=(
                                f"Operator output parameter '{param_name}' "
                                f"is not declared in 'outputs'"
                            ),
                        ))

                # Type-compatibility check (ResultType only)
                if prop_name is not None and param_name is not None and is_result_type and not is_dataset:
                    declared = from_props.get("outputs") or []
                    param = next(
                        (p for p in declared if isinstance(p, dict)
                         and p.get("name") == param_name),
                        None,
                    )
                    if param:
                        output_path = _extract_bracket_path(value, "from['outputs']") or [param_name]
                        nested_path = output_path[1:]
                        resolved_type = _resolve_param_type(param, nested_path)
                        rt_type = next(
                            (lbl for lbl in to_labels if lbl != "ResultType"), None)
                        if rt_type and resolved_type != rt_type:
                            nested_suffix = ("." + ".".join(nested_path)) if nested_path else ""
                            errors.append(SchemaError(
                                keyword="mappingTypeCompatibility",
                                instancePath=f"{path_prefix}/{key}",
                                schemaPath="#/x-mapping-rules/typeCompatibility",
                                params={"paramType": resolved_type, "resultType": rt_type},
                                message=(
                                    f"Type mismatch: operator output '{param_name}{nested_suffix}' has type "
                                    f"'{resolved_type}' but ResultType node is '{rt_type}'"
                                ),
                            ))

    return errors
