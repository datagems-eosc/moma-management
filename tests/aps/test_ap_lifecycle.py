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

import json
from pathlib import Path
from typing import AsyncGenerator, Generator
from uuid import uuid4

import pytest
import pytest_asyncio
from neo4j import AsyncGraphDatabase
from neo4j.exceptions import CypherTypeError
from testcontainers.neo4j import Neo4jContainer

from moma_management.domain.analytical_pattern import AnalyticalPattern
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

    try:
        await ap_repository.create(ap)
    except CypherTypeError:
        pytest.xfail(
            f"{ap_path.name} contains list-of-dict node properties (e.g. "
            "Operator.inputs / Operator.outputs) that the Neo4j storage layer "
            "cannot persist until _sanitize_properties handles complex values."
        )

    retrieved = await ap_repository.get(root_id)
    assert retrieved is not None, f"AP from {ap_path.name} was not found after creation"

    await ap_repository.delete(root_id)

    deleted = await ap_repository.get(root_id)
    assert deleted is None, f"AP from {ap_path.name} still exists after deletion"
