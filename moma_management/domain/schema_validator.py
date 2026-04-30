"""Local JSON-Schema validator with AJV-style error output.

Validates raw PG-JSON dicts against the Draft 7 schemas in ``domain/schema/``
using the ``jsonschema`` library (with ``referencing`` for ``$ref`` resolution).
Additionally checks graph-level structural rules (root node, connectivity,
edge constraints) and returns all violations as a flat list of
:class:`SchemaError` objects whose shape matches the AJV error format used by
*ap-management*.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterator, List, Set, Tuple

from jsonschema import Draft202012Validator
from jsonschema import ValidationError as JsonSchemaValidationError
from pydantic import BaseModel
from referencing import Registry, Resource

from moma_management.domain import EDGE_CONSTRAINTS_PATH, SCHEMA_DIR


class SchemaError(BaseModel):
    """Single validation error in AJV format."""

    keyword: str
    instancePath: str
    schemaPath: str
    params: dict[str, Any]
    message: str


# ---------------------------------------------------------------------------
# Helpers – graph-structure checks (mirrored from PgJsonGraph / domain models)
# ---------------------------------------------------------------------------

def _dfs_iter_undirected(
    nodes: list[dict],
    edges: list[dict],
    start_id: str,
) -> Iterator[str]:
    """Iterative undirected DFS yielding reachable node IDs."""
    visited: Set[str] = set()
    stack: list[Tuple[str, str | None]] = [(start_id, None)]

    adj: dict[str, list[str]] = defaultdict(list)
    for edge in edges:
        f = str(edge.get("from", ""))
        t = str(edge.get("to", ""))
        adj[f].append(t)
        adj[t].append(f)

    while stack:
        node, parent = stack.pop()
        if node in visited:
            continue
        visited.add(node)
        yield node
        for neighbor in adj[node]:
            if neighbor != parent:
                stack.append((neighbor, node))


def _validate_structure(
    data: dict,
    root_label: str,
    graph_type: str,
) -> list[SchemaError]:
    """Check root-node and connectivity rules for a graph type."""
    errors: list[SchemaError] = []
    raw_nodes = data.get("nodes")
    raw_edges = data.get("edges")
    nodes = raw_nodes if isinstance(raw_nodes, list) else []
    edges = raw_edges if isinstance(raw_edges, list) else []

    root_nodes = [
        (i, n) for i, n in enumerate(nodes)
        if root_label in (n.get("labels") or [])
    ]

    if len(root_nodes) == 0:
        errors.append(SchemaError(
            keyword=f"{graph_type}Structure",
            instancePath="/nodes",
            schemaPath=f"#/x-{graph_type}-rules/root",
            params={"expectedLabel": root_label},
            message=(
                f"No node with label '{root_label}' found. "
                f"The graph must have exactly one root node."
            ),
        ))
        return errors  # cannot continue without a root

    if len(root_nodes) > 1:
        ids = ", ".join(str(n.get("id", "")) for _, n in root_nodes)
        errors.append(SchemaError(
            keyword=f"{graph_type}Structure",
            instancePath="/nodes",
            schemaPath=f"#/x-{graph_type}-rules/root",
            params={"expectedLabel": root_label, "foundIds": ids},
            message=(
                f"Multiple nodes with label '{root_label}' found ({ids}). "
                f"The graph must have exactly one root node."
            ),
        ))
        return errors

    root_idx, root_node = root_nodes[0]
    root_id = str(root_node.get("id", ""))

    # Root must not have incoming edges
    edges_to_root = [
        (i, e) for i, e in enumerate(edges)
        if str(e.get("to", "")) == root_id
    ]
    if edges_to_root:
        for ei, e in edges_to_root:
            errors.append(SchemaError(
                keyword=f"{graph_type}Structure",
                instancePath=f"/edges/{ei}/to",
                schemaPath=f"#/x-{graph_type}-rules/root",
                params={"rootId": root_id, "from": str(e.get("from", ""))},
                message=(
                    f"The root '{root_label}' node must not have incoming edges. "
                    f"Edge from {e.get('from')} → {root_id}."
                ),
            ))

    # All nodes must be reachable from root (undirected)
    all_ids = {str(n.get("id", "")) for n in nodes}
    reachable = set(_dfs_iter_undirected(nodes, edges, root_id))

    if reachable - all_ids:
        extra = ", ".join(sorted(reachable - all_ids))
        errors.append(SchemaError(
            keyword=f"{graph_type}Structure",
            instancePath="/edges",
            schemaPath=f"#/x-{graph_type}-rules/connectivity",
            params={"unknownIds": extra},
            message=(
                f"Graph traversal returned unknown node IDs: {extra}. "
                f"Edges may reference missing nodes."
            ),
        ))

    unreachable = all_ids - reachable
    if unreachable:
        errors.append(SchemaError(
            keyword=f"{graph_type}Structure",
            instancePath="/nodes",
            schemaPath=f"#/x-{graph_type}-rules/connectivity",
            params={"unreachableIds": ", ".join(sorted(unreachable))},
            message=(
                f"Graph is not fully connected. "
                f"Unreachable nodes from root: {', '.join(sorted(unreachable))}"
            ),
        ))

    return errors


# ---------------------------------------------------------------------------
# Main validator class
# ---------------------------------------------------------------------------

class LocalSchemaValidator:
    """Validates raw PG-JSON dicts against local JSON schemas."""

    def __init__(self, base_path: Path = SCHEMA_DIR) -> None:
        self._base_path = base_path
        self._registry: Registry | None = None

    def _fetch_resource(self, uri: str) -> Resource:
        file_path = self._base_path / uri.lstrip("/")
        contents = json.loads(file_path.read_text())
        return Resource.from_contents(contents)

    def _get_registry(self) -> Registry:
        if self._registry is None:
            self._registry = Registry(retrieve=self._fetch_resource)
        return self._registry

    # -- JSON-Schema validation ------------------------------------------

    def validate(
        self,
        data: dict,
        schema_path: str = "moma.schema.json",
    ) -> list[SchemaError]:
        """Validate *data* against the JSON schema at *schema_path*.

        Returns a list of AJV-style errors (empty when valid).
        """
        file_path = self._base_path / schema_path
        schema = json.loads(file_path.read_text())
        registry = self._get_registry()

        validator = Draft202012Validator(schema, registry=registry)
        return [self._wrap_to_ajv(e) for e in validator.iter_errors(data)]

    @staticmethod
    def _wrap_to_ajv(err: JsonSchemaValidationError) -> SchemaError:
        return SchemaError(
            keyword=str(err.validator),
            instancePath="/" + "/".join(map(str, err.path)),
            schemaPath="#/" + "/".join(map(str, err.schema_path)),
            params={},
            message=err.message,
        )

    # -- Edge-constraint validation --------------------------------------

    @staticmethod
    def validate_edge_constraints(
        data: dict,
        constraints_path: Path = EDGE_CONSTRAINTS_PATH,
    ) -> list[SchemaError]:
        """Check that every edge satisfies the declared constraints."""
        constraints: list[dict] = json.loads(constraints_path.read_text())
        if not constraints:
            return []

        raw_nodes = data.get("nodes")
        raw_edges = data.get("edges")
        nodes = raw_nodes if isinstance(raw_nodes, list) else []
        edges = raw_edges if isinstance(raw_edges, list) else []

        node_labels: dict[str, list[str]] = {
            str(n.get("id", "")): n.get("labels", []) for n in nodes
        }

        errors: list[SchemaError] = []
        for i, edge in enumerate(edges):
            from_id = str(edge.get("from", ""))
            to_id = str(edge.get("to", ""))
            from_labels = node_labels.get(from_id, [])
            to_labels = node_labels.get(to_id, [])
            edge_labels = edge.get("labels") or []
            edge_label = edge_labels[0] if edge_labels else ""

            if from_id and from_id not in node_labels:
                errors.append(SchemaError(
                    keyword="edgeRelationship",
                    instancePath=f"/edges/{i}/from",
                    schemaPath="#/x-edge-relationship-rules",
                    params={"edgeIndex": i, "nodeId": from_id},
                    message=f"Edge 'from' node with ID '{from_id}' does not exist",
                ))
                continue

            if to_id and to_id not in node_labels:
                errors.append(SchemaError(
                    keyword="edgeRelationship",
                    instancePath=f"/edges/{i}/to",
                    schemaPath="#/x-edge-relationship-rules",
                    params={"edgeIndex": i, "nodeId": to_id},
                    message=f"Edge 'to' node with ID '{to_id}' does not exist",
                ))
                continue

            allowed = any(
                c["label"] == edge_label
                and c["fromLabel"] in from_labels
                and c["toLabel"] in to_labels
                for c in constraints
            )

            if not allowed:
                # Find what labels *are* allowed between these node types
                allowed_labels = [
                    c["label"] for c in constraints
                    if c["fromLabel"] in from_labels
                    and c["toLabel"] in to_labels
                ]
                allowed_msg = (
                    f"Allowed relationships between these nodes: "
                    f"{', '.join(allowed_labels)}"
                    if allowed_labels
                    else "No valid relationships allowed between these node types"
                )
                errors.append(SchemaError(
                    keyword="edgeRelationship",
                    instancePath=f"/edges/{i}/labels",
                    schemaPath=f"#/x-edge-relationship-rules/{edge_label}",
                    params={
                        "edgeIndex": i,
                        "edgeLabel": edge_label,
                        "fromLabels": from_labels,
                        "toLabels": to_labels,
                    },
                    message=(
                        f"Invalid edge '{edge_label}' from "
                        f"[{', '.join(from_labels)}] to [{', '.join(to_labels)}]. "
                        f"{allowed_msg}"
                    ),
                ))

        return errors

    # -- Orchestrator ----------------------------------------------------

    def validate_graph(
        self,
        data: dict,
        schema_path: str = "moma.schema.json",
        constraints_path: Path = EDGE_CONSTRAINTS_PATH,
        graph_type: str | None = None,
    ) -> list[SchemaError]:
        """Run schema, edge-constraint, and structural validation.

        *graph_type* should be ``"ap"`` or ``"dataset"`` to enable structural
        checks.  When ``None``, only schema + edge validation is performed.
        """
        errors = self.validate(data, schema_path)
        errors.extend(self.validate_edge_constraints(data, constraints_path))

        if graph_type == "ap":
            errors.extend(_validate_structure(
                data, "Analytical_Pattern", "ap"))
        elif graph_type == "dataset":
            errors.extend(_validate_structure(data, "sc:Dataset", "dataset"))

        return errors
