from json import loads
from pathlib import Path
from typing import TYPE_CHECKING, List

from jsonschema import Draft202012Validator, ValidationError
from referencing import Registry, Resource

if TYPE_CHECKING:
    from moma_management.domain.pg_json_graph import MomaEntity

from ... import EDGE_CONSTRAINTS_PATH, SCHEMA_DIR
from ..schema_error import SchemaError
from .step import ValidationStep


class SchemaStep(ValidationStep):
    """"
    This class is meant to validate that every nodes and edges in the graph conform to the constraints defined in the JSON schema.
    """
    # Where to find the JSON schema file(s) for validation
    _schema_dir: Path = SCHEMA_DIR

    # The schema file path to use for validation (relative to _schema_dir)
    _schema: Path

    # Cached label-ancestry map, built once from the dataset node schemas
    _label_ancestors_cache: dict[str, frozenset[str]] | None = None
    _edge_constraints_cache: list[dict] | None = None

    _validator_cache: dict[str, "Draft202012Validator"] = {}

    def __init__(self, schema_name: str = "moma.schema.json") -> None:
        super().__init__()
        self._schema = self._schema_dir / schema_name
        if not self._schema.exists() or not self._schema.is_file():
            raise ValueError(f"Schema file not found: {self._schema}")

        self._registry = Registry(retrieve=self._fetch_schema)

    def handle(self, data: MomaEntity) -> List[SchemaError]:
        # mode="json" serialises UUIDs → strings, which the JSON schema expects.
        raw = data.model_dump(by_alias=True, mode="json")
        key = str(self._schema)
        if key not in SchemaStep._validator_cache:
            schema = loads(self._schema.read_text())
            SchemaStep._validator_cache[key] = Draft202012Validator(
                schema, registry=self._registry
            )
        validator = SchemaStep._validator_cache[key]
        errors = [self._wrap_to_ajv(e) for e in validator.iter_errors(raw)]
        errors += self.validate_edge_constraints(raw)
        return errors + self._chain(data)

    @staticmethod
    def _wrap_to_ajv(err: ValidationError) -> SchemaError:
        return SchemaError(
            keyword=str(err.validator),
            instancePath="/" + "/".join(map(str, err.path)),
            schemaPath="#/" + "/".join(map(str, err.schema_path)),
            params={},
            message=err.message,
        )

    def _fetch_schema(self, uri: str) -> Resource:
        """Resolve a $ref uri to it's corresponding JSON schema file."""
        file_path = self._schema_dir / uri.lstrip("/")
        contents = loads(file_path.read_text())
        return Resource.from_contents(contents)

    @classmethod
    def validate_edge_constraints(
        cls,
        data: dict,
        constraints_path: Path = EDGE_CONSTRAINTS_PATH,
    ) -> list[SchemaError]:
        """Check that every edge satisfies the declared constraints.

        Node labels are expanded with schema-derived ancestors before matching
        so that e.g. ``"RelationalDatabase"`` also satisfies a constraint
        requiring ``"Data"``.
        """
        if SchemaStep._edge_constraints_cache is None:
            SchemaStep._edge_constraints_cache = loads(constraints_path.read_text())
        constraints: list[dict] = SchemaStep._edge_constraints_cache
        if not constraints:
            return []

        raw_nodes = data.get("nodes")
        raw_edges = data.get("edges")
        nodes = raw_nodes if isinstance(raw_nodes, list) else []
        edges = raw_edges if isinstance(raw_edges, list) else []

        if SchemaStep._label_ancestors_cache is None:
            SchemaStep._label_ancestors_cache = SchemaStep._build_label_ancestors(
                SCHEMA_DIR / "nodes" / "dataset"
            )
        ancestors = SchemaStep._label_ancestors_cache

        def _expand(labels: list[str]) -> set[str]:
            expanded = set(labels)
            for lbl in labels:
                expanded |= ancestors.get(lbl, frozenset())
            return expanded

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

            expanded_from = _expand(from_labels)
            expanded_to = _expand(to_labels)

            allowed = any(
                c["label"] == edge_label
                and c["fromLabel"] in expanded_from
                and c["toLabel"] in expanded_to
                for c in constraints
            )

            if not allowed:
                allowed_labels = [
                    c["label"] for c in constraints
                    if c["fromLabel"] in expanded_from
                    and c["toLabel"] in expanded_to
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

    @classmethod
    def _build_label_ancestors(cls, dataset_schema_dir: Path) -> dict[str, frozenset[str]]:
        """Walk all JSON schema files in *dataset_schema_dir*, follow ``allOf.$ref``
        chains, and return a mapping ``{label: frozenset_of_ancestor_labels}``.

        Used to expand node labels during edge-constraint checking so that a node
        with label ``"RelationalDatabase"`` is also matched by a constraint that
        requires ``"Data"``.
        """
        # First pass: collect all (filename → title) and raw schema data
        title_by_file: dict[str, str] = {}
        schema_data: dict[str, dict] = {}
        for schema_file in dataset_schema_dir.glob("*.schema.json"):
            try:
                data = loads(schema_file.read_text())
                title = data.get("title")
                if title:
                    title_by_file[schema_file.name] = title
                    schema_data[schema_file.name] = data
            except Exception:
                continue

        # Second pass: build direct parent map {title: parent_title | None}
        direct_parent: dict[str, str | None] = {
            t: None for t in title_by_file.values()}
        for fname, data in schema_data.items():
            title = title_by_file[fname]
            pg_props = data.get("$defs", {}).get("pgProperties", {})
            for entry in pg_props.get("allOf", []):
                ref = entry.get("$ref", "")
                if ref:
                    parent_file = ref.split("#")[0]
                    parent_title = title_by_file.get(parent_file)
                    if parent_title:
                        direct_parent[title] = parent_title
                        break

        # Third pass: transitive closure via memoised recursion
        ancestors: dict[str, frozenset[str]] = {}

        def _get(label: str, visiting: frozenset[str]) -> frozenset[str]:
            if label in ancestors:
                return ancestors[label]
            parent = direct_parent.get(label)
            if not parent or parent in visiting:
                ancestors[label] = frozenset()
                return ancestors[label]
            parent_set = frozenset({parent}) | _get(parent, visiting | {label})
            ancestors[label] = parent_set
            return parent_set

        for label in direct_parent:
            _get(label, frozenset())

        return ancestors
