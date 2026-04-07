from typing import Self

from pydantic import model_validator

from moma_management.domain import EDGE_CONSTRAINTS_PATH
from moma_management.domain.generated.nodes import node_schema
from moma_management.domain.pg_json_graph import PgJsonGraph

_ROOT_LABEL = "Analytical_Pattern"


class AnalyticalPattern(PgJsonGraph):
    _edge_constraints_path = EDGE_CONSTRAINTS_PATH
    """
    A validated PG-JSON graph whose root node carries the label
    ``Analytical_Pattern``.

    Structure expected (per the MoMa ontology image):
    - Exactly **one** root ``Analytical_Pattern`` node
    - The root has no incoming edges
    - All nodes are reachable from the root (undirected DFS)
    - Operators are connected to the root via ``consist_of`` edges
    - Data nodes are connected to Operators via ``input``/``output`` edges
    - Users are connected to Operators via ``uses`` edges
    - Tasks reference this AP externally via ``is_accomplished_by`` (not stored
      in the AP subgraph itself)
    """

    @property
    def root(self) -> node_schema.Node:
        """Return the single ``Analytical_Pattern`` root node."""
        return next(n for n in self.nodes if _ROOT_LABEL in n.labels)

    @model_validator(mode="after")
    def check_root_node(self: Self) -> Self:
        root_nodes = [n for n in self.nodes if _ROOT_LABEL in n.labels]

        if len(root_nodes) == 0:
            raise ValueError(
                f"No node with label '{_ROOT_LABEL}' found. "
                "An AnalyticalPattern must have exactly one root node."
            )
        if len(root_nodes) > 1:
            ids = ", ".join(str(n.id) for n in root_nodes)
            raise ValueError(
                f"Multiple nodes with label '{_ROOT_LABEL}' found ({ids}). "
                "An AnalyticalPattern must have exactly one root node."
            )

        root_id = str(root_nodes[0].id)

        # Root must not have incoming edges
        edges_to_root = [e for e in (self.edges or []) if str(e.to) == root_id]
        if edges_to_root:
            sources = ", ".join(
                f"({e.from_} -> {e.to})" for e in edges_to_root
            )
            raise ValueError(
                f"The root '{_ROOT_LABEL}' node must not have incoming edges. "
                f"Did you leave a Task edge in the AP graph? "
                f"Incoming edges: {sources}"
            )

        # All nodes must be reachable from the root (undirected)
        reachable = set(self._dfs_iter_undirected(root_id))
        all_ids = {str(n.id) for n in self.nodes}

        if reachable != all_ids:
            if reachable - all_ids:
                extra = ", ".join(sorted(reachable - all_ids))
                raise ValueError(
                    f"Graph traversal returned unknown node IDs: {extra}. "
                    "Edges may reference missing nodes."
                )
            unreachable = ", ".join(sorted(all_ids - reachable))
            raise ValueError(
                f"Graph is not fully connected. "
                f"Unreachable nodes from root: {unreachable}"
            )

        return self

    # normalize / difference / __eq__ / _dfs_iter_undirected inherited from PgJsonGraph
