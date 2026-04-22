"""Storage integration tests for Evaluation persistence and AP interaction."""

from typing import AsyncGenerator
from uuid import uuid4

import pytest
import pytest_asyncio
from neo4j import AsyncGraphDatabase
from testcontainers.neo4j import Neo4jContainer

from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.domain.evaluation import Evaluation, EvaluationDimension
from moma_management.domain.generated.edges.edge_schema import Edge
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.repository.analytical_pattern import (
    Neo4jAnalyticalPatternRepository,
)
from moma_management.repository.evaluation import Neo4jEvaluationRepository


def _make_ap(
    root_id: str | None = None,
    operator_id: str | None = None,
) -> AnalyticalPattern:
    """Build a minimal valid AP with one operator."""
    root_id = root_id or str(uuid4())
    operator_id = operator_id or str(uuid4())
    return AnalyticalPattern(
        nodes=[
            Node(
                id=root_id,
                labels=["Analytical_Pattern"],
                properties={"name": "test-ap", "description": "A test AP"},
            ),
            Node(
                id=operator_id,
                labels=["Operator"],
                properties={"name": "test-operator"},
            ),
        ],
        edges=[
            Edge(**{"from": root_id, "to": operator_id,
                 "labels": ["consist_of"]}),
        ],
    )


@pytest_asyncio.fixture(scope="module")
async def ap_repository(neo4j_container_module: Neo4jContainer) -> AsyncGenerator[Neo4jAnalyticalPatternRepository, None]:
    """AP repository backed by a module-scoped Neo4j container."""
    uri = neo4j_container_module.get_connection_url()
    auth = (neo4j_container_module.username, neo4j_container_module.password)
    driver = AsyncGraphDatabase.driver(uri, auth=auth)
    async with driver.session() as session:
        yield Neo4jAnalyticalPatternRepository(session)
    await driver.close()


@pytest_asyncio.fixture(scope="module")
async def evaluation_repository(neo4j_container_module: Neo4jContainer) -> AsyncGenerator[Neo4jEvaluationRepository, None]:
    """Evaluation repository backed by a module-scoped Neo4j container."""
    uri = neo4j_container_module.get_connection_url()
    auth = (neo4j_container_module.username, neo4j_container_module.password)
    driver = AsyncGraphDatabase.driver(uri, auth=auth)
    async with driver.session() as session:
        yield Neo4jEvaluationRepository(session)
    await driver.close()


@pytest.mark.asyncio
async def test_create_and_get_evaluation(
    ap_repository: Neo4jAnalyticalPatternRepository,
    evaluation_repository: Neo4jEvaluationRepository,
):
    """Stored evaluations must be retrievable with their dimensions intact."""
    ap = _make_ap()
    ap_id = str(ap.root.id)
    execution_id = str(uuid4())

    await ap_repository.create(ap)
    await evaluation_repository.create(
        execution_id=execution_id,
        ap_id=ap_id,
        evaluation=Evaluation({EvaluationDimension.system: {"latency": 42}}),
    )

    retrieved = await evaluation_repository.get(execution_id)

    assert retrieved is not None
    assert retrieved["execution_id"] == execution_id
    assert retrieved["ap_id"] == ap_id
    assert retrieved["dimensions"] == ["system"]
    assert retrieved["evaluation"].root[EvaluationDimension.system] == {
        "latency": 42}


@pytest.mark.asyncio
async def test_evaluation_is_excluded_from_ap_get_and_deleted_with_ap(
    ap_repository: Neo4jAnalyticalPatternRepository,
    evaluation_repository: Neo4jEvaluationRepository,
):
    """AP retrieval must exclude evaluations, and AP deletion must remove them."""
    ap = _make_ap()
    ap_id = str(ap.root.id)
    execution_id = str(uuid4())

    await ap_repository.create(ap)
    await evaluation_repository.create(
        execution_id=execution_id,
        ap_id=ap_id,
        evaluation=Evaluation({EvaluationDimension.data: {"quality": 0.9}}),
    )

    retrieved_ap = await ap_repository.get(ap_id)
    assert retrieved_ap is not None
    retrieved_ids = {str(node.id) for node in retrieved_ap.nodes}
    assert execution_id not in retrieved_ids

    await ap_repository.delete(ap_id)
    assert await ap_repository.get(ap_id) is None
    assert await evaluation_repository.get(execution_id) is None
