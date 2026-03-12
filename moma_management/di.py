import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Generator, Optional

from fastapi import Depends, FastAPI, HTTPException, Request
from jose import JWTError
from neo4j import Driver, GraphDatabase, Session

from moma_management.repository import DatasetRepository, Neo4jDatasetRepository
from moma_management.repository.node import Neo4jNodeRepository, NodeRepository
from moma_management.services.authentication import Authentication
from moma_management.services.authorization import AuthorizationService, DatasetAction
from moma_management.services.dataset import DatasetService
from moma_management.services.node import NodeService

logger = logging.getLogger(__name__)

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


def get_authorization_service() -> Optional[AuthorizationService]:
    """Return the dataset authorization service."""
    if not os.getenv("PERMISSIONS_GATEWAY_URL"):
        logger.warning(
            "PERMISSIONS_GATEWAY_URL not set, authorization disabled"
        )
        return None
    return AuthorizationService(gateway_url=os.getenv("PERMISSIONS_GATEWAY_URL", ""))


def get_authentication_service() -> Optional[Authentication]:
    """Return a JwtValidator configured from environment variables."""
    if not os.getenv("OIDC_ISSUER"):
        logger.warning(
            "OIDC_ISSUER not set, authentication disabled"
        )
        return None

    return Authentication(
        issuer=os.getenv("OIDC_ISSUER", ""),
        audience=os.getenv("OIDC_AUDIENCE") or None,
        ttl=int(os.getenv("JWKS_TTL_SECONDS", "300")),
    )


def require_permission(action: DatasetAction):
    """Ensure the request is authenticated and authorized for the specified dataset action."""

    async def _check(
        request: Request,
        authentication: Optional[Authentication] = Depends(
            get_authentication_service),
        authorization: Optional[AuthorizationService] = Depends(
            get_authorization_service),
    ) -> dict | None:
        if authentication is None:
            return None  # unprotected mode

        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Not authenticated")

        token = auth_header.removeprefix("Bearer ")

        try:
            user = authentication.validate(token)
        except JWTError:
            raise HTTPException(
                status_code=401, detail="Invalid or expired token")
        except Exception:
            logger.exception("Unexpected token validation error")
            raise HTTPException(
                status_code=401, detail="Token validation failed")

        if authorization is None:
            return user

        dataset_id = request.path_params.get("id")
        if dataset_id:
            authorization.check(dataset_id, action, token)

        return user

    return _check
