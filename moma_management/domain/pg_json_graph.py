from collections import defaultdict
from collections.abc import Iterable
from functools import cache
from importlib import import_module
from json import loads
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Iterator, Optional, Self, Set, Tuple, cast

from deepdiff import DeepDiff
from pydantic import model_validator

from moma_management.domain.generated.edges.edge_schema import EdgeLabel
from moma_management.domain.generated.moma_schema import MoMaGraphModel
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.domain.node_properties import NodeProperties

if TYPE_CHECKING:
    from .validation import ValidationStep


@cache
def _rules() -> list[tuple[frozenset[str], type[NodeProperties]]]:
    """(the labels a node carries, the class of its type), read from the rules of
    node.schema.json the validation applies."""
    nodes = Path(__file__).parent / "schema" / "nodes"
    rules: list[tuple[frozenset[str], type[NodeProperties]]] = []
    for rule in loads((nodes / "node.schema.json").read_text())["allOf"]:
        labels = rule["if"]["properties"]["labels"]
        ref = rule["then"]["properties"]["properties"]["$ref"].split("#")[0]
        title = loads((nodes / ref).read_text())["$defs"]["pgProperties"]["title"]
        module = import_module(
            "moma_management.domain.generated.nodes." + ref.removesuffix(".schema.json").replace("/", ".") + "_schema")
        rules.append((frozenset(c["contains"]["const"] for c in labels.get("allOf", [labels])), getattr(module, title)))
    return rules


def node_type(labels: Iterable[str]) -> type[NodeProperties] | None:
    """The class of a node with *labels*: the most specific one its labels select
    (["CsvSet", "Data"] -> CsvSet). None when no rule applies."""
    found = [model for required, model in _rules() if required <= set(labels)]
    specific = [m for m in found if not any(o is not m and issubclass(o, m) for o in found)]
    if len(specific) > 1:
        raise ValueError(f"Labels {sorted(labels)} select unrelated models: {[m.__name__ for m in specific]}")
    return specific[0] if specific else None


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

    def nodes_of[N: NodeProperties](self, kind: type[N], among: Iterable[Node] | None = None) -> list[N]:
        """The nodes that are a *kind*, a subtype included: ``nodes_of(Data)`` also returns
        the CsvSet nodes, as ``CsvSet``. A read-only view: the graph keeps its PG-JSON nodes."""
        typed: list[N] = []
        for node in self.nodes if among is None else among:
            cls = node_type(node.labels)
            if cls is not None and issubclass(cls, kind):
                read = cls.model_validate(cast(dict[str, Any], node.properties))  # pyright: ignore[reportUnknownMemberType]
                assert isinstance(read, kind)
                read._id, read._labels = node.id, list(node.labels)  # pyright: ignore[reportPrivateUsage]
                typed.append(read)
        return typed

    def neighbors[N: NodeProperties](
        self, node: NodeProperties | Node, edge: EdgeLabel, kind: type[N], *, incoming: bool = False
    ) -> list[N]:
        """The *kind* nodes *node* reaches through its *edge* edges, or that reach it
        through theirs when *incoming*."""
        ends = {str(e.from_ if incoming else e.to) for e in self.edges or []
                if edge in e.labels and str(e.to if incoming else e.from_) == str(node.id)}
        return self.nodes_of(kind, [n for n in self.nodes if str(n.id) in ends])

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
