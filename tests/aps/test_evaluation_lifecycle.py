"""
Lifecycle tests for evaluation operations through AnalyticalPatternService
and NodeService.

Scenarios covered
-----------------
1. add_evaluation on a non-existent AP          → NotFoundError
2. add_evaluation with an invalid type string   → ValueError (enum rejects it)
3. add_evaluation on an existing AP             → success, eval node appended
4. delete an evaluation via NodeService         → repo.delete called
5. delete an AP that has evaluations            → repo.delete called (cascade)
"""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.domain.exceptions import NotFoundError
from moma_management.domain.generated.edges.edge_schema import Edge
from moma_management.domain.generated.nodes.ap.evaluation_schema import (
    Type as EvaluationType,
)
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.services.analytical_pattern import AnalyticalPatternService
from moma_management.services.node import NodeService

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ap() -> AnalyticalPattern:
    root_id = str(uuid4())
    op_id = str(uuid4())
    return AnalyticalPattern(
        nodes=[
            Node(id=root_id, labels=["Analytical_Pattern"],
                 properties={"name": "test-ap"}),
            Node(id=op_id, labels=["Operator"], properties={"name": "op"}),
        ],
        edges=[
            Edge(**{"from": root_id, "to": op_id, "labels": ["consist_of"]}),
        ],
    )


def _make_ap_service(ap: AnalyticalPattern | None) -> tuple[AnalyticalPatternService, AsyncMock]:
    """Return (service, repo_mock) with repo.get pre-configured."""
    repo = AsyncMock()
    repo.get.return_value = ap
    return AnalyticalPatternService(repo, AsyncMock()), repo


# ---------------------------------------------------------------------------
# 1. add_evaluation on a non-existent AP → NotFoundError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_add_eval_on_nonexistent_ap_raises_not_found():
    svc, repo = _make_ap_service(ap=None)

    with pytest.raises(NotFoundError):
        await svc.add_evaluation(
            ap_id=str(uuid4()),
            type=EvaluationType.system_evaluation,
            eval='{"latency": 42}',
            execution_id=uuid4(),
        )

    repo.create.assert_not_called()


# ---------------------------------------------------------------------------
# 2. add_evaluation with an invalid type → rejected before reaching the service
# ---------------------------------------------------------------------------


def test_add_eval_invalid_type_rejected_by_enum():
    """EvaluationType enum raises ValueError for unknown dimension strings."""
    with pytest.raises(ValueError):
        EvaluationType("not_a_real_evaluation_type")


# ---------------------------------------------------------------------------
# 3. add_evaluation on an existing AP with each valid type → success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("eval_type", list(EvaluationType))
async def test_add_eval_on_existing_ap_succeeds(eval_type: EvaluationType):
    ap = _make_ap()
    svc, repo = _make_ap_service(ap=ap)

    eval_id = await svc.add_evaluation(
        ap_id=str(ap.root.id),
        type=eval_type,
        eval='{"value": 1}',
        execution_id=uuid4(),
    )

    # Returns a non-empty string ID
    assert eval_id

    # Repo was asked to persist the updated AP
    repo.create.assert_called_once()

    # Evaluation node was appended with the right label
    eval_nodes = [n for n in ap.nodes if "Evaluation" in n.labels]
    assert len(eval_nodes) == 1
    assert eval_type.value in eval_nodes[0].labels
    assert str(eval_nodes[0].id) == eval_id

    # is_measured_by edge was appended
    eval_edges = [e for e in ap.edges if "is_measured_by" in e.labels]
    assert len(eval_edges) == 1
    assert str(eval_edges[0].to) == eval_id


# ---------------------------------------------------------------------------
# 4. delete an evaluation via NodeService → succeeds
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_evaluation_via_node_service():
    eval_node = Node(
        id=str(uuid4()),
        labels=["Evaluation", "SystemEvaluation"],
        properties={"executionId": str(uuid4()), "evaluation": "{}"},
    )

    node_repo = AsyncMock()
    node_repo.get.return_value = eval_node
    node_repo.delete.return_value = 1
    node_svc = NodeService(node_repo)

    deleted = await node_svc.delete(str(eval_node.id))

    node_repo.delete.assert_called_once_with(str(eval_node.id))
    assert deleted == 1


# ---------------------------------------------------------------------------
# 5. delete AP that has evaluations → repo.delete is called (cascade)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_ap_with_evaluation_calls_repo_delete():
    ap = _make_ap()
    svc, repo = _make_ap_service(ap=ap)

    # First, attach an evaluation to the AP
    await svc.add_evaluation(
        ap_id=str(ap.root.id),
        type=EvaluationType.system_evaluation,
        eval='{"latency": 5}',
        execution_id=uuid4(),
    )

    # repo.get must still return the AP when delete checks for its existence
    repo.get.return_value = ap
    await svc.delete(ap_id=str(ap.root.id))

    repo.delete.assert_called_once_with(str(ap.root.id))
