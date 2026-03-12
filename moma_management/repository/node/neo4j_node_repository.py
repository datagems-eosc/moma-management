from logging import getLogger
from typing import Optional

from neo4j import Session

from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.repository.neo4j_pgson_mixin import Neo4jPgJsonMixin

logger = getLogger(__name__)


class Neo4jNodeRepository(Neo4jPgJsonMixin):
    """Neo4j-backed implementation of NodeRepository."""

    def __init__(self, session: Session):
        self._session = session

    def create(self, node: Node) -> str:
        """Store a single node using MERGE/SET."""
        try:
            self._session.execute_write(self.create_pgson_node, node)
            return "success"
        except Exception as e:
            logger.error("Neo4j node create failed: %s", e)
            return f"Error: {str(e)}"

    def get(self, node_id: str) -> Optional[Node]:
        """Retrieve a single node by its ID."""
        query = """//cypher
            MATCH (n {id: $nodeId})
            RETURN n
        """
        record = self._session.run(query, nodeId=node_id).single()
        if record is None:
            return None
        return Node(**self._deserialize_node(record["n"]))

    def update(self, node: Node) -> dict:
        """Update properties of an existing node."""
        try:
            props = self._sanitize_properties(node.properties)
            query = """//cypher
                MATCH (n {id: $nodeId})
                SET n += $props
                RETURN count(n) AS updated
            """
            result = self._session.run(query, nodeId=node.id, props=props)
            record = result.single()
            updated = record["updated"] if record else 0
            return {"status": "success", "updated": updated}
        except Exception as e:
            logger.error("Neo4j node update failed: %s", e)
            return {"error": str(e), "updated": 0}

    def delete(self, node_id: str) -> int:
        """Detach-delete a single node by ID. Returns 1 on success, 0 if not found."""
        query = """//cypher
            MATCH (n {id: $nodeId})
            DETACH DELETE n
            RETURN 1 AS deleted
        """
        record = self._session.run(query, nodeId=node_id).single()
        return record["deleted"] if record else 0
