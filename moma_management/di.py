import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator, Generator, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError
from neo4j import Driver, GraphDatabase, Session

from moma_management.repository import DatasetRepository, Neo4jDatasetRepository
from moma_management.repository.node import Neo4jNodeRepository, NodeRepository
from moma_management.services.authentication import Authentication
from moma_management.services.authorization import (
    DatagemsAuthorizationService,
    DatasetRole,
    GatewayError,
    UserError,
)
from moma_management.services.dataset import DatasetService
from moma_management.services.node import NodeService

bearer_scheme = HTTPBearer(auto_error=False)
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


def get_authorization_service() -> Optional[DatagemsAuthorizationService]:
    """Return the dataset authorization service."""
    if not os.getenv("PERMISSIONS_GATEWAY_URL"):
        logger.warning(
            "PERMISSIONS_GATEWAY_URL not set, authorization disabled"
        )
        return None
    return DatagemsAuthorizationService(gateway_url=os.getenv("PERMISSIONS_GATEWAY_URL", ""))


def get_authentication_service() -> Optional[Authentication]:
    """Return a JwtValidator configured from environment variables."""
    if not os.getenv("OIDC_ISSUER"):
        logger.warning(
            "OIDC_ISSUER not set, authentication disabled"
        )
        return None

    return Authentication(
        issuer=os.getenv("OIDC_ISSUER", ""),
        ttl=int(os.getenv("JWKS_TTL_SECONDS", "300")),
        client_id=os.getenv("OIDC_CLIENT_ID") or None,
        client_secret=os.getenv("OIDC_CLIENT_SECRET") or None,
        exchange_scope=os.getenv("OIDC_EXCHANGE_SCOPE"),
    )


def _authenticate(
    credentials: HTTPAuthorizationCredentials | None,
    authentication: Authentication,
) -> tuple[str, dict]:
    """Validate bearer credentials and return (raw_token, JWT claims). Raises 401 on failure."""
    if credentials is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    token = credentials.credentials
    try:
        return token, authentication.validate(token)
    except JWTError as exc:
        logger.warning("JWT validation failed: %s", exc)
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    except Exception:
        logger.exception("Unexpected error during token validation")
        raise HTTPException(status_code=401, detail="Token validation failed")


def _exchange(authentication: Authentication, token: str) -> str:
    """Exchange *token* for a dg-app-api scoped token. Raises 502 on failure."""
    try:
        return authentication.exchange_token(token)
    except Exception:
        logger.exception("Token exchange failed")
        raise HTTPException(status_code=502, detail="Token exchange failed")


def require_permission(action: DatasetRole):
    """Validate the caller's token and enforce *action* on the requested dataset.

    Flow:
    1. Validate the incoming Bearer token (RS256, issuer check).
    2. Exchange it for a ``dg-app-api`` scoped token via Token Exchange.
    3. Query the permissions gateway to verify the caller holds the required role.
    """

    async def _check(
        request: Request,
        credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
        authentication: Optional[Authentication] = Depends(
            get_authentication_service),
        authorization: Optional[DatagemsAuthorizationService] = Depends(
            get_authorization_service),
    ) -> dict | None:
        if authentication is None:
            return None
        token, user = _authenticate(credentials, authentication)

        dataset_id = request.path_params.get("id")

        # Only CREATE is allowed without a dataset ID
        if not dataset_id and action != DatasetRole.CREATE:
            # This is a sefety measure to prevent accidental exposure of dataset ids when the path parameter is missing.
            raise ValueError("Dataset ID not found in path parameters")

        gw_token = _exchange(authentication, token)
        try:
            ok = authorization.has_dataset_permission(
                gw_token, action, dataset_id)
            if not ok:
                raise HTTPException(
                    status_code=403, detail="Forbidden: insufficient permissions")

        except UserError as exc:
            logger.warning(
                "Authorization failed: %s %s: %s",
                exc.status_code,
                exc.text,
            )
            raise HTTPException(
                status_code=exc.status_code, detail=f"Gateway error. {exc.text}")
        except GatewayError:
            raise HTTPException(
                status_code=502, detail="Permission gateway unavailable. Please see logs for details.")

        return user

    return _check


def get_allowed_datasets_ids() -> AsyncGenerator[List[str]]:
    """Authenticate the caller and return the dataset IDs they are allowed to browse.

    Returns ``None`` when authentication is disabled (no filtering applied).
    Returns an empty list when authentication is enabled but the gateway reports
    no accessible datasets, effectively returning an empty listing.
    """

    async def _check(
        credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
        authentication: Optional[Authentication] = Depends(
            get_authentication_service),
        authorization: Optional[DatagemsAuthorizationService] = Depends(
            get_authorization_service),
    ) -> list[str] | None:

        if authentication is None:
            return None  # Auth disabled – return everything
        token, _ = _authenticate(credentials, authentication)

        gateway_token = _exchange(authentication, token)
        if authorization is None:
            return None  # Authz disabled – RBAC disabled

        try:
            return authorization.get_browseable_dataset_ids(gateway_token)
        except GatewayError:
            raise HTTPException(
                status_code=502, detail="Permission gateway unavailable")
        except UserError as exc:
            logger.warning(
                "Authorization failed: %s %s: %s",
                exc.status_code,
                exc.text,
            )
            raise HTTPException(
                status_code=exc.status_code, detail=f"Gateway error. {exc.text}")

    return _check
