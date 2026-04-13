"""
Storage (integration) tests for Neo4jMlModelRepository.
"""

from typing import Generator
from uuid import uuid4

import pytest
from neo4j import GraphDatabase
from testcontainers.neo4j import Neo4jContainer

from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.domain.generated.edges.edge_schema import Edge
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.repository.analytical_pattern import (
    Neo4jAnalyticalPatternRepository,
)
from moma_management.repository.ml_model import Neo4jMlModelRepository


@pytest.fixture
def ml_model_repository(neo4j_container: Neo4jContainer) -> Generator[Neo4jMlModelRepository, None, None]:
    """ML_Model repository backed by a fresh Neo4j container."""
    uri = neo4j_container.get_connection_url()
    auth = (neo4j_container.username, neo4j_container.password)
    driver = GraphDatabase.driver(uri, auth=auth)
    with driver.session() as session:
        yield Neo4jMlModelRepository(session)
    driver.close()


# ---------------------------------------------------------------------------
# CRUD Tests
# ---------------------------------------------------------------------------


def test_create_ml_model(ml_model_repository: Neo4jMlModelRepository):
    """A created ML_Model node must be retrievable with its properties intact."""
    node = Node(
        id=uuid4(),
        labels=["ML_Model"],
        properties={"name": "GPT-4", "type": "LLM"},
    )
    created = ml_model_repository.create(node)
    assert str(created.id) == str(node.id)

    retrieved = ml_model_repository.get(str(node.id))
    assert retrieved is not None
    assert str(retrieved.id) == str(node.id)
    assert "ML_Model" in retrieved.labels
    assert retrieved.properties["name"] == "GPT-4"
    assert retrieved.properties["type"] == "LLM"


def test_get_nonexistent_ml_model_returns_none(ml_model_repository: Neo4jMlModelRepository):
    """get() must return None when the ML_Model ID does not exist."""
    result = ml_model_repository.get(str(uuid4()))
    assert result is None


def test_update_ml_model(ml_model_repository: Neo4jMlModelRepository):
    """update() must modify the properties of an existing ML_Model."""
    node = Node(
        id=uuid4(),
        labels=["ML_Model"],
        properties={"name": "BERT", "type": "classification"},
    )
    ml_model_repository.create(node)

    updated_node = Node(
        id=node.id,
        labels=["ML_Model"],
        properties={"name": "BERT-v2", "type": "classification"},
    )
    result = ml_model_repository.update(updated_node)
    assert result["status"] == "success"
    assert result["updated"] == 1

    retrieved = ml_model_repository.get(str(node.id))
    assert retrieved.properties["name"] == "BERT-v2"


def test_update_nonexistent_ml_model_returns_zero(ml_model_repository: Neo4jMlModelRepository):
    """update() must return updated=0 for a non-existing node."""
    node = Node(
        id=uuid4(),
        labels=["ML_Model"],
        properties={"name": "ghost", "type": "none"},
    )
    result = ml_model_repository.update(node)
    assert result["updated"] == 0


def test_delete_ml_model(ml_model_repository: Neo4jMlModelRepository):
    """delete() must remove the node and return 1."""
    node = Node(
        id=uuid4(),
        labels=["ML_Model"],
        properties={"name": "to-delete", "type": "regression"},
    )
    ml_model_repository.create(node)
    deleted = ml_model_repository.delete(str(node.id))
    assert deleted == 1

    assert ml_model_repository.get(str(node.id)) is None


def test_delete_nonexistent_returns_zero(ml_model_repository: Neo4jMlModelRepository):
    """delete() must return 0 when the node does not exist."""
    result = ml_model_repository.delete(str(uuid4()))
    assert result == 0


def test_list_ml_models(ml_model_repository: Neo4jMlModelRepository):
    """list() must return all ML_Model nodes."""
    n1 = Node(id=uuid4(), labels=["ML_Model"],
              properties={"name": "m1", "type": "a"})
    n2 = Node(id=uuid4(), labels=["ML_Model"],
              properties={"name": "m2", "type": "b"})
    ml_model_repository.create(n1)
    ml_model_repository.create(n2)

    models = ml_model_repository.list()
    ids = {str(m.id) for m in models}
    assert str(n1.id) in ids
    assert str(n2.id) in ids


# ---------------------------------------------------------------------------
# has_referencing_aps Tests
# ---------------------------------------------------------------------------


def test_has_referencing_aps_false_when_no_ap(ml_model_repository: Neo4jMlModelRepository):
    """has_referencing_aps() returns False when no AP references the ML_Model."""
    node = Node(id=uuid4(), labels=["ML_Model"], properties={
                "name": "standalone", "type": "x"})
    ml_model_repository.create(node)
    assert ml_model_repository.has_referencing_aps(str(node.id)) is False


def test_has_referencing_aps_true_when_ap_exists(neo4j_container: Neo4jContainer):
    """has_referencing_aps() returns True when an AP operator references the ML_Model via perform_inference."""
    uri = neo4j_container.get_connection_url()
    auth = (neo4j_container.username, neo4j_container.password)
    driver = GraphDatabase.driver(uri, auth=auth)
    with driver.session() as session:
        ml_repo = Neo4jMlModelRepository(session)
        ap_repo = Neo4jAnalyticalPatternRepository(session)

        ml_id = str(uuid4())
        op_id = str(uuid4())
        ap_id = str(uuid4())

        # Create ML_Model
        ml_node = Node(id=ml_id, labels=["ML_Model"], properties={
                       "name": "used-model", "type": "LLM"})
        ml_repo.create(ml_node)

        # Create AP with Operator that has perform_inference edge to ML_Model
        ap = AnalyticalPattern(
            nodes=[
                Node(id=ap_id, labels=["Analytical_Pattern"],
                     properties={"name": "ap-using-ml"}),
                Node(id=op_id, labels=["Operator"],
                     properties={"name": "inference-op"}),
                Node(id=ml_id, labels=["ML_Model"], properties={
                     "name": "used-model", "type": "LLM"}),
            ],
            edges=[
                Edge(**{"from": ap_id, "to": op_id, "labels": ["consist_of"]}),
                Edge(**{"from": op_id, "to": ml_id,
                     "labels": ["perform_inference"]}),
            ],
        )
        ap_repo.create(ap)

        assert ml_repo.has_referencing_aps(ml_id) is True

    driver.close()


# ---------------------------------------------------------------------------
# Forbidden-edge isolation test
# ---------------------------------------------------------------------------


def test_ap_retrieval_excludes_ml_model_nodes(neo4j_container: Neo4jContainer):
    """Retrieving an AP must NOT include ML_Model nodes connected via perform_inference."""
    uri = neo4j_container.get_connection_url()
    auth = (neo4j_container.username, neo4j_container.password)
    driver = GraphDatabase.driver(uri, auth=auth)
    with driver.session() as session:
        ml_repo = Neo4jMlModelRepository(session)
        ap_repo = Neo4jAnalyticalPatternRepository(session)

        ml_id = str(uuid4())
        op_id = str(uuid4())
        ap_id = str(uuid4())

        # Create ML_Model
        ml_node = Node(id=ml_id, labels=["ML_Model"], properties={
                       "name": "hidden-model", "type": "LLM"})
        ml_repo.create(ml_node)

        # Create AP with operator linked to ML_Model via perform_inference
        ap = AnalyticalPattern(
            nodes=[
                Node(id=ap_id, labels=["Analytical_Pattern"],
                     properties={"name": "ap-test"}),
                Node(id=op_id, labels=["Operator"],
                     properties={"name": "op-test"}),
                Node(id=ml_id, labels=["ML_Model"], properties={
                     "name": "hidden-model", "type": "LLM"}),
            ],
            edges=[
                Edge(**{"from": ap_id, "to": op_id, "labels": ["consist_of"]}),
                Edge(**{"from": op_id, "to": ml_id,
                     "labels": ["perform_inference"]}),
            ],
        )
        ap_repo.create(ap)

        # Retrieve AP — should NOT include the ML_Model node
        retrieved = ap_repo.get(ap_id)
        assert retrieved is not None
        node_ids = {str(n.id) for n in retrieved.nodes}
        assert ap_id in node_ids
        assert op_id in node_ids
        assert ml_id not in node_ids, "ML_Model node should be excluded from AP retrieval"

        # Edges should not include perform_inference
        edge_labels = {label for e in (retrieved.edges or [])
                       for label in e.labels}
        assert "perform_inference" not in edge_labels

    driver.close()
