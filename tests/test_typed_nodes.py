"""Typed reads of a graph's nodes (``nodes_of``)."""
from uuid import uuid4

from moma_management.domain.dataset import Dataset
from moma_management.domain.generated.nodes.dataset.csvSet_schema import CsvSet
from moma_management.domain.generated.nodes.dataset.data_schema import Data
from moma_management.domain.generated.nodes.node_schema import Node


def test_typed_read_keeps_undeclared_properties():
    # Data's schema does not say unevaluatedProperties, CsvSet's does: both keep them
    dataset = Dataset.model_construct(nodes=[
        Node(id=uuid4(), labels=["CsvSet", "Data"], properties={"name": "s", "custom": 1}),
        Node(id=uuid4(), labels=["CSV", "Data", "cr:FileObject"],
             properties={"name": "a.csv", "probabilityColumn": "p"}),
    ], edges=[])

    nodes = {n.name: n for n in dataset.nodes_of(Data)}

    assert isinstance(nodes["s"], CsvSet)
    assert nodes["s"].model_extra == {"custom": 1}
    assert nodes["a.csv"].model_extra == {"probabilityColumn": "p"}
