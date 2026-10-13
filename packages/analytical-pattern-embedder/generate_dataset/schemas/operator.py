"""The operator, the building block of every AP, and its ports."""

from uuid import UUID, uuid5

from pydantic import BaseModel
from pydantic.json_schema import SkipJsonSchema

from generate_dataset.util.string import snake, to_pascal_case

# Ids are derived from the operator labels, so regenerating the dataset keeps them.
NAMESPACE = UUID("6f1d3c52-9b0e-4f5a-8d27-3c1a4be7a901")

OperatorLabel = str
# A chain is a set of operator label in their execution order
# Ex : "Text_To_SQL" > "Explain_SQL"
OperatorChain = tuple[OperatorLabel, ...]


class Port(BaseModel):
    name: str
    type: str
    # What the value IS, where `type` only says how it is stored. Two string ports can hold
    # an SQL statement or a city name; types alone would let either feed the other. Two
    # ports are wired only when kind and type are both equal. Kinds only serve to build
    # plausible chains and never reach the AP graphs. For instance:
    #
    #   Text_To_SQL   outputs  query (string)  kind sql_query   --.  wired: same kind,
    #   SQL_Query     takes    sql   (string)  kind sql_query   <-'  same type
    #   Geocode       takes    city  (string)  kind place_name       not wired: another kind
    #
    # The names differ (`query`, `sql`), which is why the LLM tags them: it is the kind
    # that says these two are the same thing.
    kind: str
    # Read from the real operators and written back to the AP graphs, where AP Management's
    # Composer reads them. The LLM is neither shown nor asked for them: the operators it
    # writes get the values below.
    required: SkipJsonSchema[bool] = True
    default: SkipJsonSchema[object] = ""

    @staticmethod
    def from_property(port: dict) -> Port:
        """A port of a real operator. Its kind is not known yet: the LLM gives it."""
        return Port(name=port["name"], type=port["type"], kind="",
                    required=port.get("required", True), default=port.get("default", ""))

    def to_property(self) -> dict:
        """The port as an operator node of an AP graph declares it."""
        return {"name": self.name, "type": self.type, "required": self.required, "default": self.default}

    @staticmethod
    def unique(ports: list[Port]) -> list[Port]:
        """The ports normalized, a port whose name was already taken left out."""
        unique: dict[str, Port] = {}
        for port in ports:
            name = snake(port.name)
            unique.setdefault(name, port.model_copy(update={"name": name, "kind": snake(port.kind)}))
        return list(unique.values())


class Operator(BaseModel):
    """One step of an AP. The LLM writes the first five fields; the others are filled in
    here and left out of the schema the LLM is given."""

    label: str
    name: str
    description: str
    inputs: list[Port]
    outputs: list[Port]
    # The subject area, one of DOMAINS ("sql_analytics", "geospatial"): Text_To_SQL is
    # `sql_analytics`, Geocode is `geospatial`. A kind says what a value is; a domain says
    # what an operator is about. The domain is never used to wire operators. Step 2 uses it
    # to pick negatives for a task from the same area, and step 4 holds out whole domains.
    domain: SkipJsonSchema[str] = ""
    # Taken from an AP that exists, rather than written by the LLM.
    real: SkipJsonSchema[bool] = False

    @staticmethod
    def type_by_kind(pool: list[Operator]) -> dict[str, str]:
        """The type of each kind used by a port of the pool: {"sql_query": "string", ...}.
        A kind has one type, the one of the first port that used it."""
        type_by_kind: dict[str, str] = {}
        for op in pool:
            for port in op.ports:
                type_by_kind.setdefault(port.kind, port.type)
        return type_by_kind

    @property
    def ports(self) -> list[Port]:
        return [*self.inputs, *self.outputs]

    @property
    def id(self) -> str:
        return str(uuid5(NAMESPACE, f"operator:{self.label}"))

    def pascal_label(self) -> str:
        """The label as Pascal_Case_With_Underscores, ending in _Operator."""
        label = to_pascal_case(self.label)
        if label.lower().split("_")[-1] == "operator":
            return label
        return f"{label}_Operator" if label else "Operator"

    def feeds(self, consumer: Operator) -> list[tuple[Port, Port]]:
        """The (output, input) pairs through which this operator can feed `consumer`: same
        kind and type, each output used at most once -- an operator joining two tables
        must not be handed the same table twice."""
        pairs: list[tuple[Port, Port]] = []
        used = set()
        for input_ in consumer.inputs:
            for output in self.outputs:
                if output.name not in used and (output.kind, output.type) == (input_.kind, input_.type):
                    pairs.append((output, input_))
                    used.add(output.name)
                    break
        return pairs

    def to_dict(self, *, with_kinds: bool) -> dict:
        """The operator as the LLM is shown it. The kinds are left out when the LLM is
        the one asked to give them."""
        hidden = {"required", "default"} | (set() if with_kinds else {"kind"})
        return {
            "label": self.label,
            "name": self.name,
            "description": self.description,
            "inputs": [port.model_dump(exclude=hidden) for port in self.inputs],
            "outputs": [port.model_dump(exclude=hidden) for port in self.outputs],
        }
