"""
Storage (integration) tests for Neo4jTaskRepository.
"""

from typing import AsyncGenerator
from uuid import uuid4

import pytest
import pytest_asyncio
from neo4j import AsyncGraphDatabase
from testcontainers.neo4j import Neo4jContainer

from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.repository.task import Neo4jTaskRepository


@pytest_asyncio.fixture
async def task_repository(neo4j_container: Neo4jContainer) -> AsyncGenerator[Neo4jTaskRepository, None]:
    """Task repository backed by a fresh Neo4j container."""
    uri = neo4j_container.get_connection_url()
    auth = (neo4j_container.username, neo4j_container.password)
    driver = AsyncGraphDatabase.driver(uri, auth=auth)
    async with driver.session() as session:
        yield Neo4jTaskRepository(session)
    await driver.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_task(task_repository: Neo4jTaskRepository):
    """A created Task node must be retrievable with its properties intact."""
    task = Node(
        id=uuid4(),
        labels=["Task"],
        properties={"name": "my-task", "description": "do something useful"},
    )
    created = await task_repository.create(task)
    assert str(created.id) == str(task.id)

    retrieved = await task_repository.get(str(task.id))
    assert retrieved is not None
    assert str(retrieved.id) == str(task.id)
    assert "Task" in retrieved.labels
    assert retrieved.properties["name"] == "my-task"


@pytest.mark.asyncio
async def test_get_nonexistent_task_returns_none(task_repository: Neo4jTaskRepository):
    """get() must return None when the Task ID does not exist."""
    result = await task_repository.get(str(uuid4()))
    assert result is None
