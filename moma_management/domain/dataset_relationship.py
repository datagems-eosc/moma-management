from typing import ClassVar, Self

from pydantic import model_validator

from moma_management.domain.generated.edges.edge_schema import EdgeLabel
from moma_management.domain.pg_json_graph import MomaEntity

from .validation import SchemaStep, StructureStep


class DatasetRelationship(MomaEntity):
    """A Dataset Relationship ("dataset linking") graph.

    Rooted at a ``BasicDLElement`` node, it links exactly two ``sc:Dataset``
    nodes via ``HAS_TARGET`` edges (directly and/or through intermediate
    ``PropertyComparison``/``TextEvidence`` nodes). Optionally, a
    ``FileObjectLinkingElement`` (``HAS_DETAIL``) links similar
    ``cr:FileObject`` nodes of those two datasets through
    ``FileObjectComparison`` nodes.
    """

    # The label that identifies the root node of a DatasetRelationship graph.
    _root_label = "BasicDLElement"

    # Nodes that already exist in the datasets: the relationship points at
    # them but never owns, writes or traverses them.
    REFERENCE_LABELS: ClassVar[frozenset[str]] = frozenset({"sc:Dataset", "cr:FileObject"})

    # The chain of steps required to verify that a PG-JSON graph conforms to the DatasetRelationship spec.
    validation_chain = SchemaStep() & StructureStep()

    @property
    def root(self):
        """Return the single ``BasicDLElement`` root node."""
        return next(n for n in self.nodes if self.__class__._root_label in n.labels)

    @property
    def target_dataset_ids(self) -> tuple[str, ...]:
        """
        Return the ids of the ``sc:Dataset`` nodes this relationship links.
        Return an empty tuple if the relationship is invalid (e.g. root has no targets, or more than two).

        Only considers the root's own direct ``HAS_TARGET`` edges.
        """
        root_id = str(self.root.id)
        return tuple(sorted({
            str(e.to)
            for e in (self.edges or [])
            if EdgeLabel.has_target in e.labels and str(e.from_) == root_id
        }))

    @property
    def file_object_ids(self) -> set[str]:
        """Return the ids of the ``cr:FileObject`` nodes this relationship references."""
        return {str(n.id) for n in self.nodes if "cr:FileObject" in n.labels}

    @property
    def file_memberships(self) -> set[tuple[str, str]]:
        """Return the (dataset id, file id) pairs stated by ``distribution`` edges.

        Optional on input (checked against what MoMa stores, never written);
        always present in what MoMa returns.
        """
        return {
            (str(e.from_), str(e.to))
            for e in (self.edges or [])
            if EdgeLabel.distribution in e.labels
        }

    @model_validator(mode="after")
    def validate(self: Self) -> Self:
        # Checked before the edge rules: an edge such as `distribution` would
        # otherwise fail there with a label error that hides the real reason.
        # The only exception: a dataset's `distribution` edge to one of the
        # linked files, which states which dataset the file belongs to.
        labels_by_id = {str(n.id): set(n.labels) for n in self.nodes}
        reference_ids = {i for i, labels in labels_by_id.items() if self.REFERENCE_LABELS & labels}
        edges_from_references = [
            f"{e.from_} -{'/'.join(label.value for label in e.labels)}-> {e.to}"
            for e in (self.edges or [])
            if str(e.from_) in reference_ids and not (
                e.labels == [EdgeLabel.distribution]
                and "sc:Dataset" in labels_by_id[str(e.from_)]
                and "cr:FileObject" in labels_by_id.get(str(e.to), set())
            )
        ]
        if edges_from_references:
            raise ValueError(
                "A dataset relationship cannot modify datasets. These edges start "
                "from a dataset or a file, so they would change that dataset: "
                f"{'; '.join(edges_from_references)}. Remove them: a relationship "
                "can only point at datasets and files (with HAS_TARGET, plus "
                "optionally a dataset's `distribution` edge to a linked file), "
                "whose content MoMa already stores."
            )

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

        dataset_ids = {str(n.id) for n in self.nodes if "sc:Dataset" in n.labels}
        all_target_ids = {
            str(e.to)
            for e in (self.edges or [])
            if EdgeLabel.has_target in e.labels and str(e.to) in dataset_ids
        }
        if all_target_ids != root_target_ids:
            raise ValueError(
                "DatasetRelationship has HAS_TARGET edges pointing to dataset(s) "
                "other than the two the root directly targets: "
                f"{sorted(all_target_ids - root_target_ids)}"
            )

        foreign_sources = sorted({d for d, _ in self.file_memberships} - root_target_ids)
        if foreign_sources:
            raise ValueError(
                "DatasetRelationship has distribution edges starting from dataset(s) "
                f"other than the two the root directly targets: {foreign_sources}"
            )

        compared_files = {
            str(e.to)
            for e in (self.edges or [])
            if EdgeLabel.has_target in e.labels
            and "FileObjectComparison" in labels_by_id.get(str(e.from_), set())
        }
        uncompared_files = sorted(self.file_object_ids - compared_files)
        if uncompared_files:
            raise ValueError(
                "DatasetRelationship has file object(s) that no FileObjectComparison "
                f"targets: {uncompared_files}"
            )
        return self
