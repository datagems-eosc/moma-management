import re
from typing import Any, Dict, List, NamedTuple, Self, Tuple, cast

from pydantic import model_validator

from moma_management.domain.generated.edges.edge_schema import Edge, EdgeLabel
from moma_management.domain.generated.nodes import node_schema
from moma_management.domain.pg_json_graph import MomaEntity

from .validation import MappingStep, SchemaStep, StructureStep
from .validation.steps.mapping_step import extract_bracket_value

# Labels that say a node holds data without saying which kind.
_GENERIC_DATA_LABELS = {"data", "resulttype", "cr:fileobject", "cr:fileset"}


class Wire(NamedTuple):
    """An output of one operator feeding an input of another."""

    producer_id: str
    output: str
    consumer_id: str
    input: str


class Port(NamedTuple):
    """An input or an output declared by an operator of an AP."""

    operator_id: str
    name: str
    type: str
    is_input: bool
    # Whether another operator of the AP feeds this input or reads this output.
    wired: bool

    def __str__(self) -> str:
        return f"{self.name} ({self.type})" if self.type else self.name


class AnalyticalPattern(MomaEntity):

    # The label that identifies the root node of an AnalyticalPattern graph.
    _root_label = "Analytical_Pattern"

    # The chain of steps required to verify that a PG-JSON graph conforms to the AnalyticalPattern spec.
    validation_chain = SchemaStep() & StructureStep() & MappingStep()

    @property
    def root(self) -> node_schema.Node:
        """Return the single ``Analytical_Pattern`` root node."""
        return next(n for n in self.nodes if self.__class__._root_label in n.labels)

    @property
    def wiring(self) -> List[Wire]:
        """Every connection between two operators of the AP.

        An output and an input are wired when they share a node, the ``ResultType`` AP
        Management's Composer puts between two operators. The port names are read from
        the edge mappings: ``to['sql_query']: from['outputs']['query']`` on the output
        edge, ``to['inputs']['sql']: from['sql_query']`` on the input edge. An edge
        without a mapping still orders the two operators, but its ports are named after
        the node's id, so no declared port is marked as wired.
        """
        edges = self.edges or []
        # data node id -> (the operator writing it, {key in the node: output name})
        written: Dict[str, Tuple[str, Dict[str, str]]] = {}
        for edge in edges:
            if EdgeLabel.output in edge.labels:
                names = {self._key(key, "to"): self._key(str(source), "from['outputs']")
                         for key, source in self._mapping(edge).items()}
                written[str(edge.to)] = (str(edge.from_), names)

        wires: List[Wire] = []
        for edge in edges:
            data, consumer = str(edge.from_), str(edge.to)
            if EdgeLabel.input not in edge.labels or data not in written:
                continue
            producer, names = written[data]
            if producer == consumer:
                continue
            for target, source in (self._mapping(edge) or {data: data}).items():
                key = self._key(str(source), "from")
                wires.append(Wire(producer, names.get(key, key), consumer, self._key(target, "to['inputs']")))
        return wires

    @property
    def operators(self) -> List[node_schema.Node]:
        """The Operator nodes in execution order: every producer before its consumers.

        The order follows the dataflow (``wiring``), not the ``follows`` edges: the
        Composer writes those later -> earlier and hand-written APs the other way round.
        ``step``, then node order, break the ties.
        """
        wiring = self.wiring
        left = sorted((n for n in self.nodes if "Operator" in n.labels), key=self._step)
        ordered: List[node_schema.Node] = []
        while left:
            placed = {str(n.id) for n in ordered}
            # A cycle leaves no operator ready: take the first one rather than loop forever.
            ready = next(
                (n for n in left
                 if all(w.producer_id in placed for w in wiring if w.consumer_id == str(n.id))),
                left[0])
            left.remove(ready)
            ordered.append(ready)
        return ordered

    def ports(self) -> List[Port]:
        """Every input and output the operators declare, in execution order, each one
        saying whether it is wired to another operator of the AP."""
        wiring = self.wiring
        wired = {(w.consumer_id, True, w.input) for w in wiring} \
            | {(w.producer_id, False, w.output) for w in wiring}
        ports: List[Port] = []
        for operator in self.operators:
            id_ = str(operator.id)
            for is_input, key in ((True, "inputs"), (False, "outputs")):
                declared: List[Dict[str, Any]] = self._properties(operator).get(key) or []
                for port in declared:
                    name = port["name"]
                    ports.append(Port(id_, name, port["type"], is_input, (id_, is_input, name) in wired))
        return ports

    def describe(self) -> str:
        """
        What the AP does, forged from its operators: the text a task is matched against.

        The root node's own description cannot be relied on: the Composer keeps the root
        of the first AP, so "Text to SQL" composed with "SQL Provenance" still reads
        "translates it into SQL". A single-operator AP yields one sentence:
            "Translates a natural-language string into a safe SQL query, takes nl (string)
            as input and outputs query (string)."
        A multi-operator AP yields what goes in and what comes out, then one such
        sentence per step. Two APs made of the same operators wired the same way get the
        same description. An AP without operators gets "".
        """
        operators, wiring, ports = self.operators, self.wiring, self.ports()
        position = {str(op.id): i for i, op in enumerate(operators, start=1)}
        feeder = {(w.consumer_id, w.input): w.producer_id for w in wiring}
        producers = {w.producer_id for w in wiring}

        clauses: List[str] = []
        for operator in operators:
            id_, properties = str(operator.id), self._properties(operator)
            inputs = [port for port in ports if port.operator_id == id_ and port.is_input]
            outputs = [str(port) for port in ports if port.operator_id == id_ and not port.is_input]

            io: List[str] = []
            if inputs:
                io.append("takes " + self._join([
                    f"{port} from step {position[feeder[id_, port.name]]}" if port.wired else str(port)
                    for port in inputs]) + " as input")
            if outputs:
                io.append(f"outputs {self._join(outputs)}")
            # Datasets the operator touches directly. For an operator declaring no ports
            # (the pre-typed AP syntax) this is all that is known about what it consumes.
            for verb, label in (("reads", EdgeLabel.input), ("writes", EdgeLabel.output)):
                kinds = self._dataset_kinds(id_, label)
                if kinds:
                    io.append(f"{verb} {self._join(kinds)}")

            action = str(properties.get("description") or properties.get("name") or "").strip().rstrip(".").strip()
            # Lower the leading capital so the clause reads on after "Step 1:", except
            # for acronyms ("SQL ...").
            if len(action) > 1 and action[0].isupper() and action[1].islower():
                action = action[0].lower() + action[1:]
            clauses.append(f"{action}, {self._join(io)}" if io else action)

        if len(clauses) <= 1:
            return "".join(clause[:1].upper() + clause[1:] + "." for clause in clauses)

        takes = list(dict.fromkeys(str(port) for port in ports if port.is_input and not port.wired))
        # Only the operators nothing reads from deliver the AP's result.
        returns = list(dict.fromkeys(
            str(port) for port in ports if not port.is_input and port.operator_id not in producers))
        summary = " and ".join(
            f"{verb} {self._join(names)}" for verb, names in (("takes", takes), ("produces", returns)) if names
        ) or "runs"
        steps = " ".join(f"Step {i}: {clause}." for i, clause in enumerate(clauses, start=1))
        return f"{summary[0].upper()}{summary[1:]} in {len(clauses)} steps. {steps}"

    def _dataset_kinds(self, operator_id: str, label: EdgeLabel) -> List[str]:
        """The kinds of the datasets an operator reads (``input``) or writes (``output``),
        e.g. ["relational database data"]. ``ResultType`` nodes are operator-to-operator
        plumbing, not datasets, and are left out."""
        nodes = {str(node.id): node for node in self.nodes}
        kinds: List[str] = []
        for edge in self.edges or []:
            operator, data = (edge.to, edge.from_) if label is EdgeLabel.input else (edge.from_, edge.to)
            node = nodes.get(str(data))
            if label not in edge.labels or str(operator) != operator_id or node is None:
                continue
            if "resulttype" in (name.lower() for name in node.labels):
                continue
            specific = [name for name in node.labels if name.lower() not in _GENERIC_DATA_LABELS]
            # "RelationalDatabase" -> "relational database", "sc:Dataset" -> "dataset"
            words = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", specific[0].split(":")[-1]).replace("_", " ").split() \
                if specific else ["dataset"]
            kind = " ".join(w if w.isupper() and len(w) > 1 else w.lower() for w in words)
            kinds.append("a dataset" if kind == "dataset" else f"{kind} data")
        return list(dict.fromkeys(kinds))

    @model_validator(mode="after")
    def validate(self: Self) -> Self:
        errors = self.__class__.validation_chain.handle(self)
        if errors:
            if any(e.keyword == "edgeRelationship" for e in errors):
                raise ValueError(f"Edges violate graph constraints: {errors}")
            raise ValueError(
                f"AnalyticalPattern validation failed with errors: {errors}")
        return self

    @staticmethod
    def _properties(node: node_schema.Node) -> Dict[str, Any]:
        # The generated model types the keys with a pydantic constraint, which type
        # checkers cannot read: say once, here, that they are strings.
        return cast(Dict[str, Any], node.properties)  # pyright: ignore[reportUnknownMemberType]

    @staticmethod
    def _mapping(edge: Edge) -> Dict[str, Any]:
        return (edge.properties.mapping if edge.properties else None) or {}

    @staticmethod
    def _key(expression: str, prefix: str) -> str:
        """The first key after `prefix` in a mapping expression, read the way the mapping
        validation reads it: ``to['inputs']['sql']`` after ``to['inputs']`` -> ``sql``. A
        path going inside a port still names the port:
        ``from['outputs']['payload']['query']`` -> ``payload``."""
        return extract_bracket_value(expression, prefix) or expression

    @staticmethod
    def _step(node: node_schema.Node) -> float:
        value = AnalyticalPattern._properties(node).get("step")
        return value if isinstance(value, (int, float)) else float("inf")

    @staticmethod
    def _join(parts: List[str]) -> str:
        if len(parts) <= 1:
            return "".join(parts)
        return f"{', '.join(parts[:-1])} and {parts[-1]}"
