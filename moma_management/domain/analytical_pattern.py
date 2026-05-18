from typing import Self

from pydantic import model_validator

from moma_management.domain.generated.nodes import node_schema
from moma_management.domain.pg_json_graph import MomaEntity

from .validation import MappingStep, SchemaStep, StructureStep


class AnalyticalPattern(MomaEntity):

    # The label that identifies the root node of an AnalyticalPattern graph.
    _root_label = "Analytical_Pattern"

    # The chain of steps required to verify that a PG-JSON graph conforms to the AnalyticalPattern spec.
    validation_chain = SchemaStep() & StructureStep() & MappingStep()

    @property
    def root(self) -> node_schema.Node:
        """Return the single ``Analytical_Pattern`` root node."""
        return next(n for n in self.nodes if self.__class__._root_label in n.labels)

    @model_validator(mode="after")
    def validate(self: Self) -> Self:
        errors = self.__class__.validation_chain.handle(self)
        if errors:
            if any(e.keyword == "edgeRelationship" for e in errors):
                raise ValueError(f"Edges violate graph constraints: {errors}")
            raise ValueError(
                f"AnalyticalPattern validation failed with errors: {errors}")
        return self
