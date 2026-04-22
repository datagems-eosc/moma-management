import json
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from pydantic import ValidationError as PydanticValidationError

from moma_management.domain.evaluation import Evaluation, EvaluationDimension
from moma_management.domain.exceptions import NotFoundError
from moma_management.services.evaluation import EvaluationService

_EVALUATIONS_DIR = Path(__file__).parent.parent.parent / \
    "assets" / "aps" / "evaluations"


@pytest.mark.asyncio
async def test_create_generates_execution_id_and_persists_snapshot():
    repo = AsyncMock()
    ap_service = AsyncMock()

    svc = EvaluationService(repo, ap_service)
    evaluation = Evaluation({EvaluationDimension.system: {
                            "latency": 12}, EvaluationDimension.human: {"score": 4}})
    result = await svc.create(
        ap_id="ap-123",
        evaluation=evaluation,
    )

    ap_service.get.assert_called_once_with("ap-123")
    assert result["ap_id"] == "ap-123"
    assert set(result["dimensions"]) == {"system", "human"}
    uuid4_result = result["execution_id"]
    assert isinstance(uuid4_result, str)

    repo.create.assert_called_once_with(
        execution_id=uuid4_result,
        ap_id="ap-123",
        evaluation=evaluation,
    )


@pytest.mark.asyncio
async def test_create_uses_provided_execution_id():
    repo = AsyncMock()
    ap_service = AsyncMock()
    execution_id = str(uuid4())

    svc = EvaluationService(repo, ap_service)
    result = await svc.create(
        ap_id="ap-123",
        execution_id=execution_id,
        evaluation=Evaluation(
            {EvaluationDimension.data: {"completeness": 0.9}}),
    )

    assert result["execution_id"] == execution_id
    repo.create.assert_called_once()
    assert repo.create.call_args.kwargs["execution_id"] == execution_id


@pytest.mark.asyncio
async def test_create_fails_without_dimensions():
    with pytest.raises(PydanticValidationError, match="(?i)at least one"):
        Evaluation({})


def test_create_fails_with_unknown_dimension():
    with pytest.raises(PydanticValidationError):
        Evaluation({"unknown_dim": {"value": 1}})


@pytest.mark.asyncio
async def test_get_raises_not_found():
    repo = AsyncMock()
    repo.get.return_value = None
    svc = EvaluationService(repo, AsyncMock())

    with pytest.raises(NotFoundError):
        await svc.get("missing")


@pytest.mark.asyncio
async def test_list_by_ap_checks_ap_exists():
    repo = AsyncMock()
    ap_service = AsyncMock()
    ap_service.get.side_effect = NotFoundError("missing")
    svc = EvaluationService(repo, ap_service)

    with pytest.raises(NotFoundError):
        await svc.list_by_ap("missing")

    repo.list_by_ap.assert_not_called()


@pytest.mark.asyncio
async def test_delete_raises_not_found_when_repo_deletes_nothing():
    repo = AsyncMock()
    repo.delete.return_value = 0
    svc = EvaluationService(repo, AsyncMock())

    with pytest.raises(NotFoundError):
        await svc.delete("missing")


@pytest.mark.parametrize("filename", ["MCQGen.json", "OfferRecom.json", "QuizComp.json"])
@pytest.mark.asyncio
async def test_create_from_real_payload(filename: str):
    payload = json.loads((_EVALUATIONS_DIR / filename).read_text())
    evaluation = Evaluation.model_validate(payload)

    repo = AsyncMock()
    ap_service = AsyncMock()
    svc = EvaluationService(repo, ap_service)

    result = await svc.create(ap_id="ap-test", evaluation=evaluation)

    assert set(result["dimensions"]) == set(payload.keys())
    assert result["evaluation"] is evaluation
    repo.create.assert_called_once_with(
        execution_id=result["execution_id"],
        ap_id="ap-test",
        evaluation=evaluation,
    )
