from collections import defaultdict
from typing import Iterator, Self, Set, Tuple, cast

from deepdiff import DeepDiff
from pydantic import model_validator

from moma_management.domain.generated.moma_schema import MoMaGraphModel
from moma_management.domain.generated.nodes import node_schema


class Dataset(MoMaGraphModel):

    @property
    def root_id(self) -> str:
        """Return the id of the sc:Dataset root node."""
        root = next(n for n in self.nodes if "sc:Dataset" in n.labels)
        return root.id

    @model_validator(mode="after")
    def check_root_node(self: Self) -> Self:
        ROOT_LABEL = "sc:Dataset"

        # Check that the root node is truly a root (no edges lead to it)
        edges_to_root = [e for e in self.edges if e.to == self.root_id]
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
        all_ids = {n.id for n in self.nodes}

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

    def normalize(self) -> Self:
        """
        Normalize the AP in place:
        - Sorts nodes by id
        - Sorts edges by from_, to, labels
        - Sorts labels alphabetically
        - Strips None and empty-list property values (Neo4j silently drops them)
        While the order itself doesn't matter at all, this allows for comparison
        """
        for n in self.nodes:
            if getattr(n, "labels", None):
                n.labels = sorted(n.labels)
            n.properties = {
                k: v for k, v in n.properties.items()
                if v is not None and not (isinstance(v, list) and len(v) == 0)
            }
        self.nodes.sort(key=lambda n: n.id)

        for e in self.edges:
            if getattr(e, "labels", None):
                e.labels = sorted(e.labels)
        self.edges.sort(key=lambda e: (e.from_, e.to, tuple(e.labels)))
        return self

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Dataset):
            return NotImplemented

        # Simple assertions before doing the expensive computation
        assert other is not None
        assert len(other.nodes) == len(other.nodes)
        assert len(other.edges) == len(other.edges)

        # NOTE : Casting to Self is not necessary but it prevent a warning
        # as Pylance doesn't recognize the pseudo class "Self" as the same as
        # the complete class "Dataset"
        # So this is safe to do
        return self.difference(cast(Self, other)) == {}

    def _dfs_iter_undirected(self, start_id: str) -> Iterator[str]:
        """
        Iterative DFS for undirected graphs starting from `start_id`.
        Yields all node IDs reachable from start.
        """
        visited: Set[str] = set()
        stack: list[Tuple[str, str | None]] = [
            (start_id, None)]  # (node, parent)

        # Build undirected adjacency list
        adj: dict[str, list[str]] = defaultdict(list)
        for edge in self.edges:
            adj[edge.from_].append(edge.to)
            adj[edge.to].append(edge.from_)

        while stack:
            node, parent = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            yield node

            for neighbor in adj[node]:
                if neighbor != parent:
                    stack.append((neighbor, node))

    def difference(self, other: Self) -> DeepDiff:
        return DeepDiff(self.normalize(), other.normalize(), ignore_order=True)
