import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Generator

from fastapi import Depends, FastAPI
from neo4j import Driver, GraphDatabase, Session

from moma_management.repository import DatasetRepository, Neo4jDatasetRepository
from moma_management.repository.node import Neo4jNodeRepository, NodeRepository
from moma_management.services.dataset import DatasetService
from moma_management.services.node import NodeService

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "datagems")

driver: Driver


@asynccontextmanager
async def container_lifespan(_: FastAPI):
    """
    Lifespan context manager to setup and teardown the Neo4j driver.
    This ties the driver's lifecycle to that of the FastAPI application
    and prevents connection leaks.
    """
    global driver
    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD),
    )
    yield
    driver.close()


def get_db_session() -> Generator[Session, None, None]:
    """Returns a session from the Neo4j driver."""
    with driver.session() as session:
        yield session


def get_mapping_file() -> Path:
    """Returns the path to the mapping file."""
    return Path(os.getenv("MAPPING_FILE", "moma_management/domain/mapping.yml"))


def get_dataset_repo(session: Session = Depends(get_db_session)) -> DatasetRepository:
    """Return the repository for Dataset graph operations."""
    return Neo4jDatasetRepository(session)


def get_dataset_service(repo: DatasetRepository = Depends(get_dataset_repo), mapping_file: Path = Depends(get_mapping_file)) -> DatasetService:
    """Return the service for Dataset operations."""
    return DatasetService(repo, mapping_file)


def get_node_repo(session: Session = Depends(get_db_session)) -> NodeRepository:
    """Return the repository for single-node graph operations."""
    return Neo4jNodeRepository(session)


def get_node_service(repo: NodeRepository = Depends(get_node_repo)) -> NodeService:
    """Return the service for single-node operations."""
    return NodeService(repo)
