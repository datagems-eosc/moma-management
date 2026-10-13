"""The AP of the dataset: its operators and its graph. Its tasks are kept apart, by AP id."""

import random
from dataclasses import dataclass
from functools import cached_property
from typing import Literal
from uuid import uuid5

from pydantic import BaseModel
from pydantic.json_schema import SkipJsonSchema

from generate_dataset.schemas.operator import NAMESPACE, Operator, OperatorChain, Port
from moma_management.domain.analytical_pattern import AnalyticalPattern

# Negatives given to every training task.
HARD_NEGATIVES = 3


class Task(BaseModel):
    style: Literal["user", "technical", "terse"]
    text: str
    # train, dev or test: set by step 4, left out of the schema the LLM is given.
    use: SkipJsonSchema[str] = ""


@dataclass
class AP:
    id: str
    # In execution order.
    operators: list[Operator]
    graph: AnalyticalPattern
    # Forged from the operators by `AnalyticalPattern.describe`: what the analytical pattern embedder indexes.
    description: str
    # train, dev or test (step 4).
    split: str = "train"

    @cached_property
    def chain(self) -> OperatorChain:
        """The labels of the operators: what two APs are compared on."""
        return tuple(op.label for op in self.operators)

    @property
    def name(self) -> str:
        return self.graph.root.properties["name"]

    @property
    def root_description(self) -> str:
        return self.graph.root.properties["description"]

    def register(self, name: str, description: str) -> None:
        """Gives the AP the name and description its registrant would write."""
        self.graph.root.properties.update(name=name, description=description)

    def assign_split(self, operator_split: dict[str, str]) -> None:
        """The AP belongs to the most held-out split among its operators."""
        splits = {operator_split[label] for label in self.chain}
        self.split = next(split for split in (
            "test", "dev", "train") if split in splits)

    def relation_to(self, gold: AP) -> str | None:
        """
        What this AP is to a task written for the `gold` AP:

        - "exact": the AP the task was written for.
        - "building_block": a contiguous part of it, which the Matchmaker can compose with
          others into the whole.
        - "superset": it contains the whole chain and more. It does what the task asks and
          then something else, which is neither a match nor a mistake: such APs are left
          out when a ranking is scored, and are never used as negatives.
        - None: unrelated.
        """
        def contains(chain: OperatorChain, part: OperatorChain) -> bool:
            return any(chain[i:i + len(part)] == part for i in range(len(chain) - len(part) + 1))

        if self.chain == gold.chain:
            return "exact"
        if contains(gold.chain, self.chain):
            return "building_block"
        if contains(self.chain, gold.chain):
            return "superset"
        return None

    def hard_negatives(
        self, candidates: list[AP], rng: random.Random
    ) -> list[AP]:
        """The APs most easily mistaken for this one among those unrelated to its tasks:
        first the ones sharing an operator with it, then the ones from its domains, then
        any."""
        domains = {op.domain for op in self.operators}
        sharing, same_domain, others = [], [], []
        for candidate in candidates:
            if candidate.relation_to(self):
                continue
            if set(candidate.chain) & set(self.chain):
                sharing.append(candidate)
            elif domains & {op.domain for op in candidate.operators}:
                same_domain.append(candidate)
            else:
                others.append(candidate)
        picked: list[AP] = []
        for group in (sharing, same_domain, others):
            rng.shuffle(group)
            picked += group[:HARD_NEGATIVES - len(picked)]
        return picked

    @classmethod
    def build(cls, operators: list[Operator]) -> AP:
        """
        The AP that runs `operators` one after the other, as PG-JSON.

        The graph has the shape AP Management's Composer gives a composed AP: a `ResultType`
        node between two wired operators, `output` and `input` edges carrying the mapping,
        a `follows` edge from the later operator to the earlier one. Each input is wired to
        the nearest operator before it that outputs the same kind and type; the inputs left
        unwired are the parameters of the AP.
        """
        chain = tuple(op.label for op in operators)
        root_id = str(uuid5(NAMESPACE, "ap:" + ">".join(chain)))
        nodes = [{
            "id": root_id,
            "labels": ["Analytical_Pattern"],
            # Step 3 overwrites name and description with what the LLM writes, as the person
            # registering the AP would; the description is what today's search indexes.
            "properties": {"name": f"{operators[0].name} AP", "description": operators[0].description,
                           "process": operators[0].domain},
        }]
        edges = []
        for position, consumer in enumerate(operators):
            # input name -> where it comes from
            wiring: dict[str, tuple[Operator, Port]] = {}
            for producer in reversed(operators[:position]):
                for output, input_ in producer.feeds(consumer):
                    wiring.setdefault(input_.name, (producer, output))
            if position and not wiring:
                raise SystemExit(
                    f"{' > '.join(chain)}: {consumer.label} takes nothing from the operators before it")

            for input_name, (producer, output) in wiring.items():
                result_id = str(
                    uuid5(NAMESPACE, f"result:{'>'.join(chain)}:{consumer.label}:{input_name}"))
                nodes.append({"id": result_id, "labels": ["ResultType", output.type],
                              "properties": {"name": output.name}})
                edges += [
                    {"from": producer.id, "to": result_id, "labels": ["output"],
                     "properties": {"mapping": {f"to['{output.name}']": f"from['outputs']['{output.name}']"}}},
                    {"from": result_id, "to": consumer.id, "labels": ["input"],
                     "properties": {"mapping": {f"to['inputs']['{input_name}']": f"from['{output.name}']"}}},
                ]
            for producer_id in dict.fromkeys(producer.id for producer, _ in wiring.values()):
                edges.append({"from": consumer.id, "to": producer_id,
                             "labels": ["follows"], "properties": {}})

            nodes.append({
                "id": consumer.id,
                "labels": [consumer.label, "Operator"],
                "properties": {
                    "name": consumer.name,
                    "description": consumer.description,
                    "version": "1.0.0",
                    "step": 1,
                    "inputs": [port.to_property() for port in consumer.inputs],
                    "outputs": [port.to_property() for port in consumer.outputs],
                },
            })
            edges.append({"from": root_id, "to": consumer.id,
                         "labels": ["consist_of"]})
        graph = AnalyticalPattern.model_validate(
            {"nodes": nodes, "edges": edges})
        # What this method writes must read back as the operators it was given.
        assert [op.labels[0] for op in graph.operators] == list(chain), chain
        return cls(id=root_id, operators=operators, graph=graph, description=graph.describe())
