"""
Lifecycle integration tests for every AP fixture in assets/aps/.

For each JSON file at the top level of assets/aps/ the test:
  1. Loads and re-IDs the fixture (avoids collisions between fixtures sharing node IDs)
  2. Creates the AP in Neo4j
  3. Retrieves it and asserts it is not None
  4. Deletes it
  5. Retrieves it again and asserts it is None

All tests share a single module-scoped Neo4j container.
"""

import asyncio
import json
from pathlib import Path
from typing import AsyncGenerator, Generator
from uuid import uuid4

import pytest
import pytest_asyncio
from neo4j import AsyncGraphDatabase
from testcontainers.neo4j import Neo4jContainer

from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.domain.exceptions import ConflictError
from moma_management.repository.analytical_pattern import (
    Neo4jAnalyticalPatternRepository,
)

# ---------------------------------------------------------------------------
# Fixture discovery
# ---------------------------------------------------------------------------

_APS_DIR = Path(__file__).parents[2] / "assets" / "aps"
_AP_FIXTURES = sorted(_APS_DIR.glob("*.json"))


def _load_rereid(path: Path) -> AnalyticalPattern:
    """Load an AP fixture and replace all node IDs with fresh UUIDs.

    This prevents collisions when two fixtures happen to share the same IDs
    (e.g. ap_sql_select and ap_sql_select_without_dataset).
    """
    raw = json.loads(path.read_text())
    id_map: dict[str, str] = {n["id"]: str(uuid4()) for n in raw["nodes"]}
    for node in raw["nodes"]:
        node["id"] = id_map[node["id"]]
        if node.get("properties") is None:
            node["properties"] = {}
    for edge in raw["edges"]:
        edge["from"] = id_map[edge["from"]]
        edge["to"] = id_map[edge["to"]]
    return AnalyticalPattern.model_validate(raw)


# ---------------------------------------------------------------------------
# Shared infrastructure
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def neo4j_container_module() -> Generator[Neo4jContainer, None, None]:
    container = Neo4jContainer(image="neo4j:latest")
    container.start()
    yield container
    container.stop()


@pytest_asyncio.fixture(scope="module")
async def ap_repository(
    neo4j_container_module: Neo4jContainer,
) -> AsyncGenerator[Neo4jAnalyticalPatternRepository, None]:
    uri = neo4j_container_module.get_connection_url()
    auth = (neo4j_container_module.username, neo4j_container_module.password)
    driver = AsyncGraphDatabase.driver(uri, auth=auth)
    async with driver.session() as session:
        yield Neo4jAnalyticalPatternRepository(session)
    await driver.close()


# ---------------------------------------------------------------------------
# Lifecycle tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("ap_path", _AP_FIXTURES, ids=lambda p: p.name)
async def test_ap_fixture_lifecycle(
    ap_path: Path,
    ap_repository: Neo4jAnalyticalPatternRepository,
) -> None:
    """Create → get → delete → get for each AP fixture."""
    ap = _load_rereid(ap_path)
    root_id = str(ap.root.id)

    await ap_repository.create(ap)

    retrieved = await ap_repository.get(root_id)
    assert retrieved is not None, f"AP from {ap_path.name} was not found after creation"

    await ap_repository.delete(root_id)

    deleted = await ap_repository.get(root_id)
    assert deleted is None, f"AP from {ap_path.name} still exists after deletion"


# ---------------------------------------------------------------------------
# Nodes owned by one AP
# ---------------------------------------------------------------------------

_TWO_OPERATORS = _APS_DIR / "ap_sql_typed.json"


def _with_new_root(ap: AnalyticalPattern) -> AnalyticalPattern:
    """`ap` under another root id: a second AP made of the same operator nodes."""
    raw = ap.model_dump(by_alias=True, mode="json")
    old, new = str(ap.root.id), str(uuid4())
    for node in raw["nodes"]:
        if node["id"] == old:
            node["id"] = new
    for edge in raw["edges"]:
        edge["from"] = new if edge["from"] == old else edge["from"]
        edge["to"] = new if edge["to"] == old else edge["to"]
    return AnalyticalPattern.model_validate(raw)


def _ids(ap: AnalyticalPattern, keep) -> list[str]:
    return [str(n.id) for n in ap.nodes if keep(n.labels)]


async def _existing(repo: Neo4jAnalyticalPatternRepository, ids: list[str]) -> int:
    result = await repo._session.run("MATCH (n) WHERE n.id IN $ids RETURN count(n) AS n", ids=ids)
    return (await result.single())["n"]


@pytest.mark.asyncio
async def test_create_rejects_operators_of_another_ap(ap_repository: Neo4jAnalyticalPatternRepository) -> None:
    ap = _load_rereid(_TWO_OPERATORS)
    await ap_repository.create(ap)
    await ap_repository.create(ap)  # the same AP again is fine

    with pytest.raises(ConflictError, match=str(ap.root.id)):
        await ap_repository.create(_with_new_root(ap))
    await ap_repository.delete(str(ap.root.id))


@pytest.mark.asyncio
async def test_delete_leaves_an_ap_sharing_its_operators(ap_repository: Neo4jAnalyticalPatternRepository) -> None:
    """APs stored before ids were checked may share operator nodes."""
    first = _load_rereid(_TWO_OPERATORS)
    second = _with_new_root(first)
    await ap_repository.create(first)
    await ap_repository._session.execute_write(ap_repository.create_pgson, second)

    read = await ap_repository.get(str(first.root.id))
    assert [str(n.id) for n in read.nodes if "Analytical_Pattern" in n.labels] == [str(first.root.id)]

    await ap_repository.delete(str(first.root.id))
    assert await ap_repository.get(str(first.root.id)) is None
    kept = await ap_repository.get(str(second.root.id))
    assert len([n for n in kept.nodes if "Operator" in n.labels]) == 2
    await ap_repository.delete(str(second.root.id))


@pytest.mark.asyncio
async def test_delete_removes_internal_result_nodes_only(ap_repository: Neo4jAnalyticalPatternRepository) -> None:
    ap = _load_rereid(_TWO_OPERATORS)
    internal = _ids(ap, lambda labels: "ResultType" in labels and "Data" not in labels)
    data = _ids(ap, lambda labels: "Data" in labels)
    assert internal and data
    await ap_repository.create(ap)

    await ap_repository.delete(str(ap.root.id))
    assert await _existing(ap_repository, internal) == 0
    assert await _existing(ap_repository, data) == len(data)


@pytest.mark.asyncio
async def test_concurrent_creates_all_get_their_embedding(neo4j_container_module: Neo4jContainer) -> None:
    driver = AsyncGraphDatabase.driver(
        neo4j_container_module.get_connection_url(),
        auth=(neo4j_container_module.username, neo4j_container_module.password))
    aps = [_load_rereid(_TWO_OPERATORS) for _ in range(10)]

    async def create(ap: AnalyticalPattern) -> None:
        async with driver.session() as session:
            await Neo4jAnalyticalPatternRepository(session).create(ap, embedding=[0.1, 0.2, 0.3])

    async with driver.session() as session:
        await Neo4jAnalyticalPatternRepository.ensure_vector_index(session, 3)
        await asyncio.gather(*map(create, aps))
        result = await session.run(
            "MATCH (n:Analytical_Pattern) WHERE n.id IN $ids AND n.description_embedding IS NOT NULL "
            "RETURN count(n) AS n", ids=[str(ap.root.id) for ap in aps])
        assert (await result.single())["n"] == len(aps)

        with pytest.raises(RuntimeError, match="3-dimension"):
            await Neo4jAnalyticalPatternRepository.ensure_vector_index(session, 4)
        repo = Neo4jAnalyticalPatternRepository(session)
        for ap in aps:
            await repo.delete(str(ap.root.id))
    await driver.close()


@pytest.mark.asyncio
async def test_concurrent_creates_sharing_operators_store_one(neo4j_container_module: Neo4jContainer) -> None:
    """Checked one at a time: otherwise each would find the shared ids still free."""
    driver = AsyncGraphDatabase.driver(
        neo4j_container_module.get_connection_url(),
        auth=(neo4j_container_module.username, neo4j_container_module.password))
    async with driver.session() as session:
        # What create_with_indexes runs once per process, for this test's database.
        for statement in Neo4jAnalyticalPatternRepository._INDEX_STATEMENTS:
            await session.run(statement)
    first = _load_rereid(_TWO_OPERATORS)
    aps = [first] + [_with_new_root(first) for _ in range(7)]

    async def create(ap: AnalyticalPattern) -> bool:
        async with driver.session() as session:
            try:
                await Neo4jAnalyticalPatternRepository(session).create(ap)
                return True
            except ConflictError:
                return False

    assert sum(await asyncio.gather(*map(create, aps))) == 1
    async with driver.session() as session:
        repo = Neo4jAnalyticalPatternRepository(session)
        for ap in aps:
            await repo.delete(str(ap.root.id))
    await driver.close()
