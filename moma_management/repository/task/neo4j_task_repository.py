from logging import getLogger
from typing import Optional

from neo4j import AsyncSession

from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.repository.neo4j_pgson_mixin import Neo4jPgJsonMixin
from moma_management.repository.task.task_repository import TaskRepository

logger = getLogger(__name__)


class Neo4jTaskRepository(Neo4jPgJsonMixin, TaskRepository):
    """Synchronous Neo4j-backed implementation of ``TaskRepository``."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, task: Node) -> Node:
        """Store the Task node via MERGE/SET and return it."""
        await self._session.execute_write(self.create_pgson_node, task)
        return task

    async def get(self, task_id: str) -> Optional[Node]:
        """Retrieve a Task node by its ID."""
        query = """//cypher
            MATCH (t:Task {id: $task_id})
            RETURN t
        """
        result = await self._session.run(query, task_id=str(task_id))
        record = await result.single()
        if record is None:
            return None
        return Node(**self._deserialize_node(record["t"]))
