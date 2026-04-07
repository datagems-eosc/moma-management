from typing import Iterator, Self

from pydantic import model_validator

from moma_management.domain import EDGE_CONSTRAINTS_PATH
from moma_management.domain.generated.nodes import node_schema
from moma_management.domain.pg_json_graph import PgJsonGraph


class Dataset(PgJsonGraph):
    _edge_constraints_path = EDGE_CONSTRAINTS_PATH

    @property
    def root_id(self) -> str:
        """Return the id of the sc:Dataset root node."""
        root = next(n for n in self.nodes if "sc:Dataset" in n.labels)
        return str(root.id)

    @model_validator(mode="after")
    def check_root_node(self: Self) -> Self:
        ROOT_LABEL = "sc:Dataset"

        # Check that the root node is truly a root (no edges lead to it)
        edges_to_root = [e for e in self.edges if str(e.to) == self.root_id]
        if edges_to_root:
            edge_sources = ", ".join(
                f"({e.from_} -> {e.to})" for e in edges_to_root)
            raise ValueError(
                f"The root '{ROOT_LABEL}' node is not a root. "
                f"The following edges lead to it: {edge_sources}"
            )

        # Ensure the undirected graph is properly connected to the root
        # i.e : "Ensure all nodes are reachable from the root, no matter the direction"
        reachable = set(self._dfs_iter_undirected(self.root_id))
        all_ids = {str(n.id) for n in self.nodes}

        if reachable != all_ids:
            if reachable - all_ids:
                # Reaching more nodes than existing ones -> An edge references a missing node
                extra = ", ".join(sorted(reachable - all_ids))
                raise ValueError(
                    f"Graph traversal returned unknown node IDs: {extra}. "
                    f"Edges may reference missing nodes."
                )

            if all_ids - reachable:
                # There are nodes not reachable from the root
                unreachable = ", ".join(sorted(all_ids - reachable))
                raise ValueError(
                    f"Graph is not fully connected. "
                    f"Unreachable nodes from root: {unreachable}"
                )
        return self

    def find_all(self, label: str) -> Iterator[node_schema.Node]:
        """Return all nodes with a given label."""
        yield from (n for n in self.nodes if label in n.labels)

    # normalize / difference / __eq__ / _dfs_iter_undirected inherited from PgJsonGraph
