from collections import defaultdict
from pathlib import Path
from typing import ClassVar, Iterator, List, Optional, Self, Set, Tuple, cast

from deepdiff import DeepDiff
from pydantic import model_validator

from moma_management.domain.generated.moma_schema import MoMaGraphModel


class PgJsonGraph(MoMaGraphModel):
    """
    Base class for validated PG-JSON graph models (``Dataset``,
    ``AnalyticalPattern``).

    Provides shared graph utilities:
    - undirected DFS traversal
    - canonical normalization for equality checks
    - value equality via deep-diff
    - optional edge-constraint enforcement (opt-in per subclass via
      ``_edge_constraints_path``)
    """

    # Subclasses set this to a Path pointing at their edge-constraints JSON
    # file.  When set, ``check_edge_constraints`` is automatically run.
    _edge_constraints_path: ClassVar[Optional[Path]] = None
    # Cache loaded per subclass (populated lazily)
    _edge_constraints_cache: ClassVar[Optional[List[dict]]] = None

    @classmethod
    def _get_constraints(cls) -> List[dict]:
        if cls._edge_constraints_path is None:
            return []
        if cls._edge_constraints_cache is None:
            import json
            cls._edge_constraints_cache = json.loads(
                cls._edge_constraints_path.read_text()
            )
        return cls._edge_constraints_cache

    @model_validator(mode="after")
    def check_edge_constraints(self: Self) -> Self:
        """
        Validate that every edge in the graph satisfies the declared
        constraints for this graph type.

        Skipped entirely when ``_edge_constraints_path`` is ``None``.
        """
        constraints = self.__class__._get_constraints()
        if not constraints:
            return self

        node_labels: dict[str, list[str]] = {
            str(n.id): n.labels for n in self.nodes
        }
        violations: list[str] = []

        for edge in self.edges or []:
            from_labels = node_labels.get(str(edge.from_), [])
            to_labels = node_labels.get(str(edge.to), [])
            edge_label = edge.labels[0] if edge.labels else ""

            allowed = any(
                c["label"] == edge_label
                and c["fromLabel"] in from_labels
                and c["toLabel"] in to_labels
                for c in constraints
            )

            if not allowed:
                violations.append(
                    f"({edge.from_}){from_labels} -[{edge_label}]-> "
                    f"({edge.to}){to_labels}"
                )

        if violations:
            raise ValueError(
                "Edges violate graph constraints:\n"
                + "\n".join(f"  {v}" for v in violations)
            )

        return self

    def _dfs_iter_undirected(self, start_id: str) -> Iterator[str]:
        """
        Iterative DFS for undirected graphs starting from *start_id*.
        Yields all node IDs reachable from start.
        """
        visited: Set[str] = set()
        stack: list[Tuple[str, str | None]] = [(start_id, None)]

        adj: dict[str, list[str]] = defaultdict(list)
        for edge in self.edges or []:
            adj[str(edge.from_)].append(str(edge.to))
            adj[str(edge.to)].append(str(edge.from_))

        while stack:
            node, parent = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            yield node
            for neighbor in adj[node]:
                if neighbor != parent:
                    stack.append((neighbor, node))

    def normalize(self) -> Self:
        """
        Return *self* in canonical form (sorts nodes, edges, and labels
        in-place; strips ``None`` and empty-list properties).

        Useful for equality checks and deep-diffs.
        """
        for n in self.nodes:
            if getattr(n, "labels", None):
                n.labels = sorted(n.labels)
            n.properties = {
                k: v
                for k, v in n.properties.items()
                if v is not None and not (isinstance(v, list) and len(v) == 0)
            }
        self.nodes.sort(key=lambda n: str(n.id))

        for e in self.edges or []:
            if getattr(e, "labels", None):
                e.labels = sorted(e.labels)
        if self.edges:
            self.edges.sort(
                key=lambda e: (str(e.from_), str(e.to), tuple(e.labels))
            )
        return self

    def difference(self, other: Self) -> DeepDiff:
        return DeepDiff(self.normalize(), other.normalize(), ignore_order=True)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.difference(cast(Self, other)) == {}
