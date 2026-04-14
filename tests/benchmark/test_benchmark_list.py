import os
import shutil
import tarfile
import tempfile
from pathlib import Path
from typing import AsyncGenerator, Generator

import pytest
import pytest_asyncio
from neo4j import AsyncGraphDatabase
from pyinstrument import Profiler
from testcontainers.neo4j import Neo4jContainer

from moma_management.domain.filters import DatasetFilter
from moma_management.repository.dataset import Neo4jDatasetRepository
from moma_management.services.dataset import DatasetService

PROJECT_ROOT = Path(__file__).parent.parent.parent
BACKUP_DIR = PROJECT_ROOT / "assets" / "backups"
MAPPING_FILE = PROJECT_ROOT / "moma_management" / "domain" / "mapping.yml"


@pytest.fixture(scope="session")
def neo4j_container_with_backup() -> Generator[Neo4jContainer, None, None]:
    """Session-scoped Neo4j container pre-loaded from the latest backup archive."""
    backups = sorted(BACKUP_DIR.glob("*.tar.gz"),
                     key=lambda p: p.stat().st_mtime)
    if not backups:
        pytest.skip("No backup archives found in assets/backups/")
    latest = backups[-1]

    tmpdir = tempfile.mkdtemp(prefix="neo4j_restore_")
    try:
        with tarfile.open(latest, "r:gz") as tf:
            tf.extractall(tmpdir)

        # The archive contains data/databases/ and data/transactions/.
        # Grant full access so Neo4j (uid 7474 inside the container) can write.
        for root, dirs, files in os.walk(tmpdir):
            os.chmod(root, 0o777)
            for f in files:
                os.chmod(os.path.join(root, f), 0o666)

        data_dir = Path(tmpdir) / "data"
        if not data_dir.is_dir():
            pytest.skip(
                f"Backup archive {latest.name} does not contain a top-level 'data/' directory"
            )

        container = Neo4jContainer(image="neo4j:latest")
        container.with_volume_mapping(str(data_dir), "/data", "rw")
        container.start()
        yield container
        container.stop()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@pytest_asyncio.fixture(scope="session")
async def backup_dataset_service(
    neo4j_container_with_backup: Neo4jContainer,
) -> AsyncGenerator[DatasetService, None]:
    """DatasetService wired to the backup-restored Neo4j container."""
    uri = neo4j_container_with_backup.get_connection_url()
    auth = (neo4j_container_with_backup.username,
            neo4j_container_with_backup.password)
    driver = AsyncGraphDatabase.driver(uri, auth=auth)
    async with driver.session() as session:
        repo = Neo4jDatasetRepository(session)
        yield DatasetService(repo, MAPPING_FILE)
    await driver.close()


@pytest.mark.asyncio
async def test_dataset_list_full_db(backup_dataset_service: DatasetService) -> None:
    """Profile dataset.list() against a fully populated database restored from backup."""
    result = await backup_dataset_service.list(DatasetFilter())
    assert len(result) > 0, "Expected at least one dataset in the restored backup"
