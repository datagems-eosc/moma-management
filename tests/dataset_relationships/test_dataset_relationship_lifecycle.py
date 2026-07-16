"""
Lifecycle integration tests for every DatasetRelationship fixture in
assets/dataset_relationships/.

For each JSON file at the top level of assets/dataset_relationships/ the test:
  1. Loads and re-IDs the fixture (avoids collisions between fixtures sharing node IDs)
  2. Creates the relationship in Neo4j
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
from testcontainers.neo4j import Neo4jContainer

from moma_management.domain.dataset_relationship import DatasetRelationship
from moma_management.repository.dataset_relationship import (
    Neo4jDatasetRelationshipRepository,
)

# ---------------------------------------------------------------------------
# Fixture discovery
# ---------------------------------------------------------------------------

_DL_DIR = Path(__file__).parents[2] / "assets" / "dataset_relationships"
_DL_FIXTURES = sorted(_DL_DIR.glob("*.json"))


def _load_rereid(path: Path) -> DatasetRelationship:
    """Load a DatasetRelationship fixture and replace all node IDs with fresh UUIDs.

    This prevents collisions when two fixtures happen to share the same IDs.
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
    return DatasetRelationship.model_validate(raw)


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
async def relationship_repository(
    neo4j_container_module: Neo4jContainer,
) -> AsyncGenerator[Neo4jDatasetRelationshipRepository, None]:
    uri = neo4j_container_module.get_connection_url()
    auth = (neo4j_container_module.username, neo4j_container_module.password)
    driver = AsyncGraphDatabase.driver(uri, auth=auth)
    async with driver.session() as session:
        yield Neo4jDatasetRelationshipRepository(session)
    await driver.close()


# ---------------------------------------------------------------------------
# Lifecycle tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("dl_path", _DL_FIXTURES, ids=lambda p: p.name)
async def test_relationship_fixture_lifecycle(
    dl_path: Path,
    relationship_repository: Neo4jDatasetRelationshipRepository,
) -> None:
    """Create → get → delete → get for each DatasetRelationship fixture."""
    rel = _load_rereid(dl_path)
    root_id = str(rel.root.id)

    await relationship_repository.create(rel)

    retrieved = await relationship_repository.get(root_id)
    assert retrieved is not None, f"Relationship from {dl_path.name} was not found after creation"

    await relationship_repository.delete(root_id)

    deleted = await relationship_repository.get(root_id)
    assert deleted is None, f"Relationship from {dl_path.name} still exists after deletion"
