from collections import defaultdict
from typing import TYPE_CHECKING, ClassVar, Iterator, Optional, Self, Set, Tuple, cast

from deepdiff import DeepDiff
from pydantic import model_validator

from moma_management.domain.generated.moma_schema import MoMaGraphModel

if TYPE_CHECKING:
    from .validation import ValidationStep


class MomaEntity(MoMaGraphModel):
    """
    Any PG-JSON graph that can be validated and normalized according to a schema and edge constraints.
    """

    # Label that identifies the root node of the graph; used to find the root for traversal and validation.
    # NOTE: Must be overridden by subclasses to enable root-based validation and traversal utilities.
    _root_label: ClassVar[Optional[str]] = None

    # Chain of validation steps to run on this graph; must be set by subclasses.
    validation_chain: ClassVar[ValidationStep] = None

    @model_validator(mode="after")
    def validate(self: Self) -> Self:
        errors = self.validation_chain.handle(self)
        if errors:
            raise ValueError(
                f"Dataset validation failed with errors: {errors}")
        return self

    def __iter__(self) -> Iterator[str]:
        """
        Iterative DFS for undirected graphs starting from the root node.
        Yields all node IDs reachable from root.
        """
        root = next(
            (str(n.id) for n in self.nodes
             if self.__class__._root_label in (n.labels or [])),
            None,
        )
        if root is None:
            return
        visited: Set[str] = set()
        stack: list[Tuple[str, str | None]] = [(root, None)]

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
