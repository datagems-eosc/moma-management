from logging import getLogger
from typing import List, Optional

from neo4j import Session

from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.repository.ml_model.ml_model_repository import MlModelRepository
from moma_management.repository.neo4j_pgson_mixin import Neo4jPgJsonMixin

logger = getLogger(__name__)


class Neo4jMlModelRepository(Neo4jPgJsonMixin, MlModelRepository):
    """Synchronous Neo4j-backed implementation of ``MlModelRepository``."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def create(self, node: Node) -> Node:
        """Store the ML_Model node via MERGE/SET and return it."""
        self._session.execute_write(self.create_pgson_node, node)
        return node

    def get(self, ml_model_id: str) -> Optional[Node]:
        """Retrieve an ML_Model node by its ID."""
        query = """//cypher
            MATCH (n:ML_Model {id: $ml_model_id})
            RETURN n
        """
        record = self._session.run(
            query, ml_model_id=str(ml_model_id)).single()
        if record is None:
            return None
        return Node(**self._deserialize_node(record["n"]))

    def update(self, node: Node) -> dict:
        """Update properties of an existing ML_Model node."""
        try:
            props = self._sanitize_properties(node.properties)
            query = """//cypher
                MATCH (n:ML_Model {id: $nodeId})
                SET n += $props
                RETURN count(n) AS updated
            """
            result = self._session.run(query, nodeId=str(node.id), props=props)
            record = result.single()
            updated = record["updated"] if record else 0
            return {"status": "success", "updated": updated}
        except Exception as e:
            logger.error("Neo4j ML_Model update failed: %s", e)
            return {"error": str(e), "updated": 0}

    def delete(self, ml_model_id: str) -> int:
        """Delete an ML_Model node by ID. Returns 1 on success, 0 if not found."""
        query = """//cypher
            MATCH (n:ML_Model {id: $ml_model_id})
            DETACH DELETE n
            RETURN 1 AS deleted
        """
        record = self._session.run(
            query, ml_model_id=str(ml_model_id)).single()
        return record["deleted"] if record else 0

    def list(self) -> List[Node]:
        """Return all ML_Model nodes."""
        query = """//cypher
            MATCH (n:ML_Model)
            RETURN n
        """
        records = list(self._session.run(query))
        return [Node(**self._deserialize_node(r["n"])) for r in records]

    def has_referencing_aps(self, ml_model_id: str) -> bool:
        """Return True if at least one AP has an Operator with a perform_inference edge to this ML_Model."""
        query = """//cypher
            MATCH (:Analytical_Pattern)-[:consist_of]->(:Operator)-[:perform_inference]->(m:ML_Model {id: $ml_model_id})
            RETURN true AS referenced
            LIMIT 1
        """
        record = self._session.run(
            query, ml_model_id=str(ml_model_id)).single()
        return record is not None
