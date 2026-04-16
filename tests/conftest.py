from pathlib import Path
from typing import AsyncGenerator, Generator, List

import pytest
import pytest_asyncio
from neo4j import AsyncGraphDatabase, GraphDatabase
from pyinstrument import Profiler
from testcontainers.neo4j import Neo4jContainer

from moma_management.repository.dataset import Neo4jDatasetRepository
from moma_management.repository.node import Neo4jNodeRepository
from moma_management.services.dataset import DatasetService
from tests.utils import (
    DS_ALPHA_FILE_ID,
    DS_ALPHA_ID,
    DS_BETA_FILE_ID,
    DS_BETA_ID,
    DS_CSV_ONLY_FILE_ID,
    DS_CSV_ONLY_ID,
    DS_DATE_A_FILE_ID,
    DS_DATE_A_ID,
    DS_DATE_B_FILE_ID,
    DS_DATE_B_ID,
    DS_DATE_C_FILE_ID,
    DS_DATE_C_ID,
    DS_DATE_D_FILE_ID,
    DS_DATE_D_ID,
    DS_GAMMA_FILE_ID,
    DS_GAMMA_ID,
    DS_MIXED_CSV_FILE_ID,
    DS_MIXED_ID,
    DS_MIXED_PDF_FILE_ID,
    DS_PDF_ONLY_FILE_ID,
    DS_PDF_ONLY_ID,
)

PROJECT_ROOT = Path(__file__).parent.parent
GENERATED_DIR = PROJECT_ROOT / "generated"
DATASETS_DIR = PROJECT_ROOT / "assets" / "datasets"


def _light_profile_paths():
    light_dir = PROJECT_ROOT / "assets" / "profiles" / "light"
    return list(light_dir.glob("*.json"))


@pytest.fixture(params=_light_profile_paths(), ids=lambda p: p.name)
def light_profile(request) -> Path:
    """
    Parameterized fixture that provides paths to light profile JSON files for testing.
    """
    return request.param


def _heavy_profile_paths():
    heavy_dir = PROJECT_ROOT / "assets" / "profiles" / "heavy"
    return list(heavy_dir.glob("*.json"))


@pytest.fixture(params=_heavy_profile_paths(), ids=lambda p: p.name)
def heavy_profile(request) -> Path:
    """
    Parameterized fixture that provides paths to heavy profile JSON files for testing.
    """
    return request.param


def _layered_profile_pairs():
    light_dir = PROJECT_ROOT / "assets" / "profiles" / "light"
    heavy_dir = PROJECT_ROOT / "assets" / "profiles" / "heavy"

    def base_name(path: Path) -> str:
        stem = path.stem
        for suffix in ("._light", "_light", "_heavy"):
            if stem.endswith(suffix):
                return stem[: -len(suffix)]
        return stem

    light_by_base = {base_name(p): p for p in light_dir.glob("*.json")}
    heavy_by_base = {base_name(p): p for p in heavy_dir.glob("*.json")}
    return [(light_by_base[k], heavy_by_base[k]) for k in sorted(light_by_base) if k in heavy_by_base]


@pytest.fixture(params=_layered_profile_pairs(), ids=lambda pair: pair[0].stem.replace("_light", "").replace("._light", ""))
def layered_profile(request) -> tuple[Path, Path]:
    """
    Parameterized fixture that provides (light, heavy) profile path tuples for layered ingestion testing.
    """
    return request.param


def _dataset_paths() -> List[Path]:
    return sorted(DATASETS_DIR.rglob("*.json"))


@pytest.fixture(params=_dataset_paths(), ids=lambda p: f"{p.parent.name}/{p.name}")
def dataset_file(request) -> Path:
    """Parameterised fixture over every dataset JSON file."""
    return request.param


@pytest.fixture(scope="session")
def generated_dir() -> Path:
    GENERATED_DIR.mkdir(exist_ok=True)
    return GENERATED_DIR


@pytest.fixture(scope="session")
def mapping_file() -> Path:
    return PROJECT_ROOT / "moma_management" / "domain" / "mapping.yml"


@pytest.fixture(scope="function")
def neo4j_container() -> Generator[Neo4jContainer]:
    """Neo4j disposable container for just this testing session"""
    # NOTE : I'm not freezing the neo4j version in testing to check for regression/breaking changes in CI
    container = Neo4jContainer(image="neo4j:latest")
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="class")
def neo4j_container_class() -> Generator[Neo4jContainer, None, None]:
    """Class-scoped Neo4j container shared by all tests in a class."""
    container = Neo4jContainer(image="neo4j:latest")
    container.start()
    yield container
    container.stop()


@pytest_asyncio.fixture
async def dataset_repository(neo4j_container: Neo4jContainer) -> AsyncGenerator[Neo4jDatasetRepository, None]:
    """Provide a Neo4jDatasetRepository backed by a throwaway Neo4j container session."""
    uri = neo4j_container.get_connection_url()
    auth = (neo4j_container.username, neo4j_container.password)
    driver = AsyncGraphDatabase.driver(uri, auth=auth)
    async with driver.session() as session:
        yield Neo4jDatasetRepository(session)
    await driver.close()


@pytest_asyncio.fixture(scope="class")
async def populated_repository(
    neo4j_container_class: Neo4jContainer,
) -> AsyncGenerator[Neo4jDatasetRepository, None]:
    """
    Controlled repo for filter tests.

    NOTE: This must evolve with the MoMa spec

    Dataset layout
    --------------
    ds-alpha  id=ds-alpha  datePublished=2024-01-15  dg:status=published
              └─ cr:FileObject + CSV  (distribution edge)
    ds-beta   id=ds-beta   datePublished=2024-06-01  dg:status=draft
              └─ cr:FileObject + CSV  (distribution edge)
    ds-gamma  id=ds-gamma  datePublished=2025-03-01  dg:status=published
              └─ cr:FileObject  (no CSV label)  (distribution edge)

    Labels are stored with Neo4j-compatible ``__`` encoding so that the
    ``MATCH (n:`sc:Dataset`)`` clause in ``list()`` resolves them.
    """
    from moma_management.domain.dataset import Dataset
    from moma_management.domain.generated.edges.edge_schema import Edge
    from moma_management.domain.generated.nodes.node_schema import Node

    def _make_dataset(
        ds_id: str,
        file_id: str,
        date_published: str,
        status: str,
        file_node_extra_labels: List[str],
    ) -> Dataset:
        return Dataset(
            nodes=[
                Node(
                    id=ds_id,
                    # sc:Dataset → backtick-escaped by create_pgson_node so
                    # MATCH (n:`sc:Dataset`) in the list() Cypher query matches.
                    labels=["sc:Dataset"],
                    properties={
                        "datePublished": date_published,
                        "status": status,
                    },
                ),
                Node(
                    id=file_id,
                    # cr:FileObject → matches allowedLabels in list()
                    labels=["cr:FileObject", "Data"] + file_node_extra_labels,
                    properties={},
                ),
            ],
            edges=[
                Edge(**{"from": ds_id, "to": file_id,
                     "labels": ["distribution"]}),
            ],
        )

    uri = neo4j_container_class.get_connection_url()
    auth = (neo4j_container_class.username, neo4j_container_class.password)
    driver = AsyncGraphDatabase.driver(uri, auth=auth)
    async with driver.session() as session:
        repo = Neo4jDatasetRepository(session)
        for ds in [
            _make_dataset(DS_ALPHA_ID, DS_ALPHA_FILE_ID,
                          "2024-01-15", "published", ["CSV"]),
            _make_dataset(DS_BETA_ID,  DS_BETA_FILE_ID,
                          "2024-06-01", "draft",     ["CSV"]),
            _make_dataset(DS_GAMMA_ID, DS_GAMMA_FILE_ID,
                          "2025-03-01", "published", []),
        ]:
            await repo.create(ds)
        yield repo
    await driver.close()


@pytest_asyncio.fixture
async def dataset_service(dataset_repository: Neo4jDatasetRepository, mapping_file: Path) -> DatasetService:
    """Provide a DatasetService with a Neo4jDatasetRepository and mapping file."""
    return DatasetService(dataset_repository, mapping_file)


@pytest_asyncio.fixture(scope="class")
async def mixed_date_repository(
    neo4j_container_class: Neo4jContainer,
) -> AsyncGenerator[Neo4jDatasetRepository, None]:
    """
    Repository pre-loaded with four datasets whose ``datePublished`` values are
    written in *different* input formats.  Used to verify that date
    normalisation and sort-by-date both work regardless of input format.

    Dataset layout (chronological order, i.e. expected ASC result)
    -----------------------------------------------------------------
    ds-date-a  datePublished="15-01-2023"  (DD-MM-YYYY) → 2023-01-15  (oldest)
    ds-date-b  datePublished="2024-01-15"  (ISO)         → 2024-01-15
    ds-date-c  datePublished="01/06/2024"  (DD/MM/YYYY)  → 2024-06-01
    ds-date-d  datePublished="2025-03-01"  (ISO)         → 2025-03-01  (newest)
    """
    from moma_management.domain.dataset import Dataset
    from moma_management.domain.generated.edges.edge_schema import Edge
    from moma_management.domain.generated.nodes.node_schema import Node

    def _make(ds_id: str, file_id: str, date_published: str) -> Dataset:
        return Dataset(
            nodes=[
                Node(
                    id=ds_id,
                    labels=["sc:Dataset"],
                    properties={"datePublished": date_published,
                                "status": "published"},
                ),
                Node(id=file_id, labels=[
                     "cr:FileObject", "Data"], properties={}),
            ],
            edges=[Edge(**{"from": ds_id, "to": file_id,
                        "labels": ["distribution"]})],
        )

    uri = neo4j_container_class.get_connection_url()
    auth = (neo4j_container_class.username, neo4j_container_class.password)
    driver = AsyncGraphDatabase.driver(uri, auth=auth)
    async with driver.session() as session:
        repo = Neo4jDatasetRepository(session)
        for ds in [
            _make(DS_DATE_A_ID, DS_DATE_A_FILE_ID,
                  "15-01-2023"),   # DD-MM-YYYY
            _make(DS_DATE_B_ID, DS_DATE_B_FILE_ID, "2024-01-15"),    # ISO
            _make(DS_DATE_C_ID, DS_DATE_C_FILE_ID,
                  "01/06/2024"),    # DD/MM/YYYY
            _make(DS_DATE_D_ID, DS_DATE_D_FILE_ID, "2025-03-01"),    # ISO
        ]:
            await repo.create(ds)
        yield repo
    await driver.close()


@pytest_asyncio.fixture(scope="class")
async def mixed_types_repository(
    neo4j_container_class: Neo4jContainer,
) -> AsyncGenerator[Neo4jDatasetRepository, None]:
    """
    Repository for testing that mimeType filtering selects datasets by the
    presence of a matching file-object type, but always returns the FULL
    subgraph (i.e. every file-object type on the dataset, not only those that
    matched the filter).

    Dataset layout
    --------------
    ds-mixed   sc:Dataset
               ├─ cr:FileObject + CSV     (distribution)
               └─ cr:FileObject + PDFSet  (distribution)

    ds-csv-only  sc:Dataset
                 └─ cr:FileObject + CSV   (distribution)

    ds-pdf-only  sc:Dataset
                 └─ cr:FileObject + PDFSet (distribution)
    """
    from moma_management.domain.dataset import Dataset
    from moma_management.domain.generated.edges.edge_schema import Edge
    from moma_management.domain.generated.nodes.node_schema import Node

    def _make_multi(ds_id: str, file_specs: List[tuple]) -> Dataset:
        """
        file_specs: list of (file_id, extra_labels) pairs, one per file node.
        """
        nodes = [
            Node(
                id=ds_id,
                labels=["sc:Dataset"],
                properties={"datePublished": "2024-01-01",
                            "status": "published"},
            )
        ]
        edges = []
        for fid, extra_labels in file_specs:
            nodes.append(
                Node(id=fid, labels=["cr:FileObject", "Data"] + extra_labels, properties={}))
            edges.append(
                Edge(**{"from": ds_id, "to": fid, "labels": ["distribution"]}))
        return Dataset(nodes=nodes, edges=edges)

    uri = neo4j_container_class.get_connection_url()
    auth = (neo4j_container_class.username, neo4j_container_class.password)
    driver = AsyncGraphDatabase.driver(uri, auth=auth)
    async with driver.session() as session:
        repo = Neo4jDatasetRepository(session)
        for ds in [
            _make_multi(DS_MIXED_ID,    [(DS_MIXED_CSV_FILE_ID, [
                        "CSV"]), (DS_MIXED_PDF_FILE_ID, ["PDFSet"])]),
            _make_multi(DS_CSV_ONLY_ID, [(DS_CSV_ONLY_FILE_ID, ["CSV"])]),
            _make_multi(DS_PDF_ONLY_ID, [(DS_PDF_ONLY_FILE_ID, ["PDFSet"])]),
        ]:
            await repo.create(ds)
        yield repo
    await driver.close()


@pytest_asyncio.fixture(scope="function")
async def node_repository(neo4j_container: Neo4jContainer) -> AsyncGenerator[Neo4jNodeRepository, None]:
    """Provide a Neo4jNodeRepository backed by a throwaway Neo4j container session."""
    uri = neo4j_container.get_connection_url()
    auth = (neo4j_container.username, neo4j_container.password)
    driver = AsyncGraphDatabase.driver(uri, auth=auth)
    async with driver.session() as session:
        yield Neo4jNodeRepository(session)
    await driver.close()


# ---------------------------------------------------------------------------
# Module-scoped fixtures for performance tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def neo4j_container_module() -> Generator[Neo4jContainer, None, None]:
    """Module-scoped Neo4j container shared by all tests in a module."""
    container = Neo4jContainer(image="neo4j:latest")
    container.start()
    yield container
    container.stop()


@pytest_asyncio.fixture(scope="module")
async def heavy_dataset_repository(
    neo4j_container_module: Neo4jContainer,
) -> AsyncGenerator[Neo4jDatasetRepository, None]:
    """
    Module-scoped repository pre-loaded with all heavy datasets.

    Uses ``create_with_indexes`` so indexes and VIRTUAL_BELONGS_TO backfill
    run once, mirroring production start-up.
    """
    from moma_management.domain.dataset import Dataset

    uri = neo4j_container_module.get_connection_url()
    auth = (neo4j_container_module.username, neo4j_container_module.password)
    driver = AsyncGraphDatabase.driver(uri, auth=auth)
    async with driver.session() as session:
        repo = await Neo4jDatasetRepository.create_with_indexes(session)
        heavy_dir = DATASETS_DIR / "heavy"
        for path in sorted(heavy_dir.glob("*.json")):
            ds = Dataset.model_validate_json(path.read_text())
            result = await repo.create(ds)
            assert result == "success", f"Failed to ingest {path.name}: {result}"
        yield repo
    await driver.close()


TESTS_ROOT = Path.cwd()


# @pytest.fixture(autouse=True)
# def auto_profile(request):
#     PROFILE_ROOT = (TESTS_ROOT / ".profiles")
#     # Turn profiling on
#     profiler = Profiler()
#     profiler.start()

#     yield  # Run test

#     profiler.stop()
#     PROFILE_ROOT.mkdir(exist_ok=True)
#     results_file = PROFILE_ROOT / f"{request.node.name}.html"
#     profiler.write_html(results_file)
