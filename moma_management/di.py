import logging
import os
from contextlib import asynccontextmanager
from functools import lru_cache
from pathlib import Path
from typing import AsyncGenerator, Generator, Optional

from fastapi import Depends, FastAPI
from neo4j import AsyncDriver, AsyncGraphDatabase, AsyncSession

from moma_management.repository import DatasetRepository, Neo4jDatasetRepository
from moma_management.repository.analytical_pattern import (
    AnalyticalPatternRepository,
    Neo4jAnalyticalPatternRepository,
)
from moma_management.repository.evaluation import (
    EvaluationRepository,
    Neo4jEvaluationRepository,
)
from moma_management.repository.ml_model import (
    MlModelRepository,
    Neo4jMlModelRepository,
)
from moma_management.repository.node import Neo4jNodeRepository, NodeRepository
from moma_management.repository.task import Neo4jTaskRepository, TaskRepository
from moma_management.services.analytical_pattern import AnalyticalPatternService
from moma_management.services.authentication import Authentication
from moma_management.services.authorization import DatagemsAuthorizationService
from moma_management.services.dataset import DatasetService
from moma_management.services.embeddings import Embedder, LocalEmbedder
from moma_management.services.evaluation import EvaluationService
from moma_management.services.ml_model import MlModelService
from moma_management.services.node import NodeService
from moma_management.services.task import TaskService

logger = logging.getLogger(__name__)

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "datagems")

driver: AsyncDriver
_embedder: Optional[Embedder] = None

_DEFAULT_EMBEDDER_MODEL = "all-MiniLM-L6-v2"


@asynccontextmanager
async def container_lifespan(_: FastAPI):
    """
    Lifespan context manager to setup and teardown the Neo4j driver.
    This ties the driver's lifecycle to that of the FastAPI application
    and prevents connection leaks.
    """
    global driver, _embedder
    driver = AsyncGraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD),
        max_connection_pool_size=50,
        connection_acquisition_timeout=10,
        max_transaction_retry_time=30,
    )

    embedder_model = os.getenv("EMBEDDER_MODEL", _DEFAULT_EMBEDDER_MODEL)
    if embedder_model:
        _embedder = LocalEmbedder(model_name=embedder_model)
        logger.info("Embedder loaded: %s (%d dimensions)",
                    embedder_model, _embedder.dimensions)

    yield
    await driver.close()


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """Returns an async session from the Neo4j driver."""
    async with driver.session() as session:
        yield session


def get_mapping_file() -> Path:
    """Returns the path to the mapping file."""
    return Path(os.getenv("MAPPING_FILE", "moma_management/domain/mapping.yml"))


async def get_dataset_repo(session: AsyncSession = Depends(get_db_session)) -> DatasetRepository:
    """Return the repository for Dataset graph operations."""
    return await Neo4jDatasetRepository.create_with_indexes(session)


def get_dataset_service(
    repo: DatasetRepository = Depends(get_dataset_repo),
    mapping_file: Path = Depends(get_mapping_file),
) -> DatasetService:
    """Return the service for Dataset operations."""
    return DatasetService(repo, mapping_file)


async def get_node_repo(session: AsyncSession = Depends(get_db_session)) -> NodeRepository:
    """Return the repository for single-node graph operations."""
    return Neo4jNodeRepository(session)


def get_node_service(repo: NodeRepository = Depends(get_node_repo)) -> NodeService:
    """Return the service for single-node operations."""
    return NodeService(repo)


@lru_cache(maxsize=1)
def get_authorization_service() -> Optional[DatagemsAuthorizationService]:
    """Return the dataset authorization service."""
    if not os.getenv("PERMISSIONS_GATEWAY_URL"):
        logger.warning(
            "PERMISSIONS_GATEWAY_URL not set, authorization disabled")
        return None
    return DatagemsAuthorizationService(
        gateway_url=os.getenv("PERMISSIONS_GATEWAY_URL", "")
    )


@lru_cache(maxsize=1)
def get_authentication_service() -> Optional[Authentication]:
    """Return a JwtValidator configured from environment variables."""
    if not os.getenv("OIDC_ISSUER"):
        logger.warning("OIDC_ISSUER not set, authentication disabled")
        return None

    return Authentication(
        issuer=os.getenv("OIDC_ISSUER", ""),
        ttl=int(os.getenv("JWKS_TTL_SECONDS", "300")),
        client_id=os.getenv("OIDC_CLIENT_ID") or None,
        client_secret=os.getenv("OIDC_CLIENT_SECRET") or None,
        exchange_scope=os.getenv("OIDC_EXCHANGE_SCOPE"),
    )


async def get_ap_repo(session: AsyncSession = Depends(get_db_session)) -> AnalyticalPatternRepository:
    """Return the repository for AnalyticalPattern graph operations."""
    return await Neo4jAnalyticalPatternRepository.create_with_indexes(session)


def get_embedder() -> Optional[Embedder]:
    """Return the application-wide embedder instance (or ``None`` if disabled)."""
    return _embedder


def get_ap_service(
    repo: AnalyticalPatternRepository = Depends(get_ap_repo),
    dataset_svc: DatasetService = Depends(get_dataset_service),
    embedder: Optional[Embedder] = Depends(get_embedder),
) -> AnalyticalPatternService:
    """Return the service for AnalyticalPattern operations."""
    return AnalyticalPatternService(repo, dataset_svc, embedder=embedder)


async def get_evaluation_repo(session: AsyncSession = Depends(get_db_session)) -> EvaluationRepository:
    """Return the repository for Evaluation node operations."""
    return await Neo4jEvaluationRepository.create_with_indexes(session)


def get_evaluation_service(
    repo: EvaluationRepository = Depends(get_evaluation_repo),
    ap_svc: AnalyticalPatternService = Depends(get_ap_service),
) -> EvaluationService:
    """Return the service for Evaluation operations."""
    return EvaluationService(repo, ap_svc)


async def get_task_repo(session: AsyncSession = Depends(get_db_session)) -> TaskRepository:
    """Return the repository for Task node operations."""
    return await Neo4jTaskRepository.create_with_indexes(session)


def get_task_service(
    task_repo: TaskRepository = Depends(get_task_repo),
    ap_repo: AnalyticalPatternRepository = Depends(get_ap_repo),
) -> TaskService:
    """Return the service for Task operations."""
    return TaskService(task_repo, ap_repo)


async def get_ml_model_repo(session: AsyncSession = Depends(get_db_session)) -> MlModelRepository:
    """Return the repository for ML_Model node operations."""
    return await Neo4jMlModelRepository.create_with_indexes(session)


def get_ml_model_service(
    repo: MlModelRepository = Depends(get_ml_model_repo),
) -> MlModelService:
    """Return the service for ML_Model operations."""
    return MlModelService(repo)
