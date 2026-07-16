from typing import Self

from pydantic import model_validator

from moma_management.domain.generated.edges.edge_schema import EdgeLabel
from moma_management.domain.pg_json_graph import MomaEntity

from .validation import SchemaStep, StructureStep


class DatasetRelationship(MomaEntity):
    """A Dataset Relationship ("dataset linking") graph.

    Rooted at a ``BasicDLElement`` node, it links exactly two ``sc:Dataset``
    nodes via ``HAS_TARGET`` edges (directly and/or through intermediate
    ``PropertyComparison``/``TextEvidence`` nodes).
    """

    # The label that identifies the root node of a DatasetRelationship graph.
    _root_label = "BasicDLElement"

    # The chain of steps required to verify that a PG-JSON graph conforms to the DatasetRelationship spec.
    validation_chain = SchemaStep() & StructureStep()

    @property
    def root(self):
        """Return the single ``BasicDLElement`` root node."""
        return next(n for n in self.nodes if self.__class__._root_label in n.labels)

    @property
    def target_dataset_ids(self) -> tuple[str, str]:
        """Return the ids of the two ``sc:Dataset`` nodes this relationship links.

        Only considers the root's own direct ``HAS_TARGET`` edges — the root
        is always directly linked to both datasets (see ``validate``).
        """
        root_id = str(self.root.id)
        ids = sorted({
            str(e.to)
            for e in (self.edges or [])
            if EdgeLabel.has_target in e.labels and str(e.from_) == root_id
        })
        return ids[0], ids[1]

    @model_validator(mode="after")
    def validate(self: Self) -> Self:
        errors = self.__class__.validation_chain.handle(self)
        if errors:
            if any(e.keyword == "edgeRelationship" for e in errors):
                raise ValueError(f"Edges violate graph constraints: {errors}")
            raise ValueError(
                f"DatasetRelationship validation failed with errors: {errors}")

        root_id = str(self.root.id)
        root_target_ids = {
            str(e.to)
            for e in (self.edges or [])
            if EdgeLabel.has_target in e.labels and str(e.from_) == root_id
        }
        if len(root_target_ids) != 2:
            raise ValueError(
                f"DatasetRelationship root must directly reference exactly two "
                f"datasets via HAS_TARGET, found {len(root_target_ids)}: "
                f"{sorted(root_target_ids)}"
            )

        all_target_ids = {
            str(e.to)
            for e in (self.edges or [])
            if EdgeLabel.has_target in e.labels
        }
        if all_target_ids != root_target_ids:
            raise ValueError(
                "DatasetRelationship has HAS_TARGET edges pointing to dataset(s) "
                "other than the two the root directly targets: "
                f"{sorted(all_target_ids - root_target_ids)}"
            )
        return self
