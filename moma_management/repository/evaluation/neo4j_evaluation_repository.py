import json
from datetime import datetime, timezone
from logging import getLogger
from typing import Any, Optional, Self

from neo4j import AsyncManagedTransaction, AsyncSession

from moma_management.domain.evaluation import Evaluation, EvaluationRecord
from moma_management.repository.evaluation.evaluation_repository import (
    EvaluationRepository,
)

logger = getLogger(__name__)


class Neo4jEvaluationRepository(EvaluationRepository):
    """Neo4j-backed implementation of ``EvaluationRepository``."""

    _INDEX_STATEMENTS: list[str] = [
        "CREATE CONSTRAINT evaluation_id_unique IF NOT EXISTS "
        "FOR (n:Evaluation) REQUIRE n.id IS UNIQUE",
        "CREATE INDEX evaluation_ap_id IF NOT EXISTS "
        "FOR (n:Evaluation) ON (n.ap_id)",
    ]
    _indexes_ensured: bool = False

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @classmethod
    async def create_with_indexes(cls, session: AsyncSession) -> Self:
        repo = cls(session)
        if not cls._indexes_ensured:
            for stmt in cls._INDEX_STATEMENTS:
                await session.run(stmt)
            cls._indexes_ensured = True
            logger.info("Neo4jEvaluationRepository indexes ensured")
        return repo

    @staticmethod
    async def _create_evaluation(
        tx: AsyncManagedTransaction,
        execution_id: str,
        ap_id: str,
        evaluation: Evaluation,
        created_at: str,
    ) -> None:
        await tx.run(
            """//cypher
            MATCH (ap:Analytical_Pattern {id: $ap_id})
            MERGE (evaluation:Evaluation {id: $execution_id})
            SET evaluation.ap_id = $ap_id,
                evaluation.evaluation = $evaluation,
                evaluation.dimensions = $dimensions,
                evaluation.created_at = $created_at
            MERGE (evaluation)-[:measure]->(ap)
            """,
            execution_id=execution_id,
            ap_id=ap_id,
            evaluation=json.dumps(
                {dim.value: val for dim, val in evaluation.root.items()},
                sort_keys=True,
            ),
            dimensions=[dim.value for dim in evaluation.root.keys()],
            created_at=created_at,
        )

    async def create(
        self,
        execution_id: str,
        ap_id: str,
        evaluation: Evaluation,
    ) -> None:
        """Store an Evaluation node and its edge to the AP."""
        await self._session.execute_write(
            self._create_evaluation,
            execution_id,
            ap_id,
            evaluation,
            datetime.now(timezone.utc).isoformat(),
        )

    async def get(self, execution_id: str) -> Optional[EvaluationRecord]:
        """Retrieve an Evaluation by execution ID."""
        result = await self._session.run(
            """//cypher
            MATCH (evaluation:Evaluation {id: $execution_id})
            RETURN evaluation
            """,
            execution_id=str(execution_id),
        )
        record = await result.single()
        if record is None:
            return None
        return self._deserialize_evaluation(record["evaluation"])

    async def list_by_ap(
        self,
        ap_id: str,
        execution_id: str | None = None,
        dimension: str | None = None,
    ) -> list[EvaluationRecord]:
        """List Evaluations linked to an AP, optionally filtered."""
        where_clauses: list[str] = []
        params: dict[str, Any] = {"ap_id": str(ap_id)}

        if execution_id is not None:
            where_clauses.append("evaluation.id = $execution_id")
            params["execution_id"] = execution_id
        if dimension is not None:
            where_clauses.append(
                "$dimension IN coalesce(evaluation.dimensions, [])")
            params["dimension"] = dimension

        where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        result = await self._session.run(
            f"""//cypher
            MATCH (evaluation:Evaluation)-[:measure]->(:Analytical_Pattern {{id: $ap_id}})
            {where}
            RETURN evaluation
            ORDER BY evaluation.created_at DESC, evaluation.id ASC
            """,
            **params,
        )
        records = await result.values("evaluation")
        return [self._deserialize_evaluation(record[0]) for record in records]

    async def delete(self, execution_id: str) -> int:
        """Delete an Evaluation by execution ID."""
        result = await self._session.run(
            """//cypher
            MATCH (evaluation:Evaluation {id: $execution_id})
            WITH evaluation
            DETACH DELETE evaluation
            RETURN 1 AS deleted
            """,
            execution_id=str(execution_id),
        )
        record = await result.single()
        if record is None:
            return 0
        return int(record["deleted"])

    @staticmethod
    def _deserialize_evaluation(node: Any) -> EvaluationRecord:
        raw = dict(node)
        evaluation_json = raw.get("evaluation")
        raw_evaluation = json.loads(evaluation_json) if isinstance(
            evaluation_json, str) else {}
        filtered = {k: v for k, v in raw_evaluation.items() if v is not None}
        return EvaluationRecord(
            execution_id=raw["id"],
            ap_id=raw.get("ap_id", ""),
            evaluation=Evaluation.model_validate(
                filtered) if filtered else None,
            dimensions=raw.get("dimensions") or [],
            created_at=raw.get("created_at"),
        )
