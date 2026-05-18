from typing import List, Self

from moma_management.domain.pg_json_graph import MomaEntity
from moma_management.domain.validation.schema_error import SchemaError

from .step import ValidationStep


class StructureStep(ValidationStep):
    """
    Validate the structural rules of the graph type, such as:
    * There is exactly one root node with the expected label for the graph type (Analytical_Pattern, Dataset, etc.)
    * The root node has no incoming edges (is truly a root)
    * All nodes are reachable from the root node (the graph is fully connected)

    """

    def __init__(self) -> None:
        super().__init__()

    def handle(self, data: MomaEntity) -> List[SchemaError]:
        errors = self._validate_structure(
            data,
            data._root_label,
            type(data).__name__,
        )
        return errors + self._chain(data)

    def _validate_structure(
        self: Self,
        data: MomaEntity,
        root_label: str,
        graph_type: str,
    ) -> List[SchemaError]:
        """Check root-node and connectivity rules for a graph type."""
        errors: List[SchemaError] = []
        nodes = data.nodes
        edges = data.edges or []

        root_nodes = [
            (i, n) for i, n in enumerate(nodes)
            if root_label in (n.labels or [])
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
            ids = ", ".join(str(n.id) for _, n in root_nodes)
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
        root_id = str(root_node.id)

        # Root must not have incoming edges
        edges_to_root = [
            (i, e) for i, e in enumerate(edges)
            if str(e.to) == root_id
        ]
        if edges_to_root:
            for ei, e in edges_to_root:
                errors.append(SchemaError(
                    keyword=f"{graph_type}Structure",
                    instancePath=f"/edges/{ei}/to",
                    schemaPath=f"#/x-{graph_type}-rules/root",
                    params={"rootId": root_id, "from": str(e.from_)},
                    message=(
                        f"The root '{root_label}' node must not have incoming edges. "
                        f"Edge from {e.from_} → {root_id}."
                    ),
                ))

        # All nodes must be reachable from root (undirected)
        all_ids = {str(n.id) for n in nodes}
        reachable = set(data)

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
