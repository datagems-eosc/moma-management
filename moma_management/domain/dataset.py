from typing import Iterator, Self

from pydantic import model_validator

from moma_management.domain.generated.nodes import node_schema
from moma_management.domain.pg_json_graph import MomaEntity

from .validation import SchemaStep, StructureStep


class Dataset(MomaEntity):
    # The label that identifies the root node of a Dataset graph.
    _root_label = "sc:Dataset"

    # The chain of steps required to verify that a PG-JSON graph conforms to the Dataset spec.
    validation_chain = SchemaStep() & StructureStep()

    @property
    def root_id(self) -> str:
        """Return the id of the sc:Dataset root node."""
        root = next(
            n for n in self.nodes if self.__class__._root_label in n.labels)
        return str(root.id)

    def find_all(self, label: str) -> Iterator[node_schema.Node]:
        """Return all nodes with a given label."""
        yield from (n for n in self.nodes if label in n.labels)

    @model_validator(mode="after")
    def validate(self: Self) -> Self:
        errors = self.__class__.validation_chain.handle(self)
        if errors:
            if any(e.keyword == "edgeRelationship" for e in errors):
                raise ValueError(f"Edges violate graph constraints: {errors}")
            raise ValueError(
                f"Dataset validation failed with errors: {errors}")
        return self
