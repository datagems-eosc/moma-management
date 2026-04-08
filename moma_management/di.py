import logging
import os
from contextlib import asynccontextmanager
from enum import Enum
from pathlib import Path
from typing import AsyncGenerator, Generator, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError
from neo4j import Driver, GraphDatabase, Session

from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.domain.filters import DatasetFilter
from moma_management.repository import DatasetRepository, Neo4jDatasetRepository
from moma_management.repository.analytical_pattern import (
    AnalyticalPatternRepository,
    Neo4jAnalyticalPatternRepository,
)
from moma_management.repository.node import Neo4jNodeRepository, NodeRepository
from moma_management.repository.task import Neo4jTaskRepository, TaskRepository
from moma_management.services.analytical_pattern import AnalyticalPatternService
from moma_management.services.authentication import Authentication
from moma_management.services.authorization import (
    DatagemsAuthorizationService,
    DatasetRole,
    GatewayError,
    UserError,
)
from moma_management.services.dataset import DatasetService
from moma_management.services.embeddings import Embedder, LocalEmbedder
from moma_management.services.node import NodeService
from moma_management.services.task import TaskService

bearer_scheme = HTTPBearer(auto_error=False)
logger = logging.getLogger(__name__)

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "datagems")

driver: Driver
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
    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD),
    )

    embedder_model = os.getenv("EMBEDDER_MODEL", _DEFAULT_EMBEDDER_MODEL)
    if embedder_model:
        _embedder = LocalEmbedder(model_name=embedder_model)
        logger.info("Embedder loaded: %s (%d dimensions)",
                    embedder_model, _embedder.dimensions)

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


def get_dataset_service(
    repo: DatasetRepository = Depends(get_dataset_repo),
    mapping_file: Path = Depends(get_mapping_file),
) -> DatasetService:
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
            "PERMISSIONS_GATEWAY_URL not set, authorization disabled")
        return None
    return DatagemsAuthorizationService(
        gateway_url=os.getenv("PERMISSIONS_GATEWAY_URL", "")
    )


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


def require_authentication():
    """Validate the caller's bearer token without enforcing any RBAC role.

    Returns the JWT claims dict when authentication is enabled, ``None`` when
    it is disabled (``OIDC_ISSUER`` not set).
    """

    async def _check(
        credentials: HTTPAuthorizationCredentials | None = Depends(
            bearer_scheme),
        authentication: Optional[Authentication] = Depends(
            get_authentication_service),
    ) -> dict | None:
        if authentication is None:
            return None
        _, user = _authenticate(credentials, authentication)
        return user

    return _check


class IdType(str, Enum):
    """Describes what kind of resource the ``id`` path parameter refers to."""
    Dataset = "Dataset"
    Node = "Node"
    AP = "AP"


def require_permission(
    action: DatasetRole,
    *,
    id_type: IdType = IdType.Dataset,
    require_all: bool = False,
):
    """Validate the caller's token and enforce *action*.

    ``id_type=IdType.Dataset`` (default): the ``id`` path parameter is a
    dataset ID and is passed directly to the permissions gateway.  A missing ID
    is only acceptable for ``DatasetRole.CREATE``.

    ``id_type=IdType.Node``: the ``id`` path parameter is a node ID.  The
    parent dataset(s) are resolved first via a subgraph lookup, and the caller
    must hold *action* on at least one of them.  404 (rather than 403) is
    returned on denial to avoid leaking whether the node lives in an
    inaccessible dataset.

    ``id_type=IdType.AP``: the ``id`` path parameter is an AnalyticalPattern
    root node ID.  The parent dataset(s) are resolved via the AP's ``input``
    edges, and the caller must hold *action* on at least one of them.

    ``require_all``: when ``True``, the caller must hold *action* on **all**
    resolved datasets, not just one.
    """

    async def _check(
        request: Request,
        credentials: HTTPAuthorizationCredentials | None = Depends(
            bearer_scheme),
        user: dict | None = Depends(require_authentication()),
        authorization: Optional[DatagemsAuthorizationService] = Depends(
            get_authorization_service
        ),
        dataset_svc: DatasetService = Depends(get_dataset_service),
        ap_svc: AnalyticalPatternService = Depends(get_ap_service),
    ) -> dict | None:

        # Auth disabled -> Skip
        if user is None:
            return None

        # Safe: require_authentication() already validated credentials, so
        # credentials is non-None here.
        token = credentials.credentials

        # Authorization disabled -> Skip grants
        if authorization is None:
            return user

        # Now, we have to know if the user can perform the requested action.
        # A permission can be either asked for datasets or nodes within datasets
        path_id = request.path_params.get("id")
        dataset_ids = []
        match id_type:
            # For nodes, we needs to check the dataset they belongs to
            case IdType.Node:
                result = dataset_svc.list(DatasetFilter(nodeIds=[path_id]))
                dataset_ids = [
                    n.id
                    for d in result.get("datasets", [])
                    for n in d.nodes
                    if "sc:Dataset" in n.labels
                ]
                if not dataset_ids:
                    raise HTTPException(
                        status_code=404, detail=f"Node '{path_id}' not found."
                    )

            # For dataset, we can retrieve the ID from the request path.
            # The only exception is for dataset creation
            case IdType.Dataset:
                if not path_id and action != DatasetRole.CREATE:
                    raise ValueError("Dataset ID not found in path parameters")
                dataset_ids = [path_id]

            # For APs, resolve parent datasets via the AP's input edges.
            # Returns 404 to prevent enumeration of inaccessible APs.
            case IdType.AP:
                # NotFoundError -> 404 via app handler
                ap_result = ap_svc.get(path_id)
                input_node_ids = [
                    str(e.to)
                    for e in (ap_result.edges or [])
                    if "input" in e.labels
                ]
                if not input_node_ids:
                    # AP has no input constraint → no dataset to check, grant access
                    return user
                result = dataset_svc.list(
                    DatasetFilter(nodeIds=input_node_ids))
                dataset_ids = [
                    str(n.id)
                    for d in result.get("datasets", [])
                    for n in d.nodes
                    if "sc:Dataset" in n.labels
                ]
                if not dataset_ids:
                    raise HTTPException(
                        status_code=404,
                        detail=f"AnalyticalPattern '{path_id}' not found.",
                    )

            case _:
                raise ValueError("Wrong auth type")

        # Now, we query the authorization gateway to make sure the user has the correct permission
        # on the requested datasets

        # NOTE: This is kept but commented out in case we have to do the token exchange again
        # gw_token = _exchange(authentication, token)
        try:
            # NOTE: This consider that having acess to ONE dataset is enough to access the node, even if it belongs to other datasets.
            # For now I don't know if it's possible to have a node belonging to multiple datasets, but if it is the case,
            # we might want to enforce permissions on ALL parent datasets instead of just one, at least for non-idempotent actions
            check = all if require_all else any
            allowed = check(
                authorization.has_dataset_permission(token, action, ds_id)
                for ds_id in dataset_ids
            )
        except UserError as exc:
            logger.warning(
                "Authorization failed: %s %s: %s",
                exc.status_code,
                exc.text,
            )
            raise HTTPException(
                status_code=exc.status_code, detail=f"Gateway error. {exc.text}"
            )
        except GatewayError:
            raise HTTPException(
                status_code=502,
                detail="Permission gateway unavailable. Please see logs for details.",
            )

        if not allowed:
            raise HTTPException(
                status_code=403, detail="Forbidden: insufficient permissions or not a dataset id"
            )

        return user

    return _check


def require_browse_for_ap_creation():
    """Check that the caller can BROWSE each dataset referenced by the new AP's input edges.

    This differs from ``require_permission`` in that the dataset IDs are
    derived from the request *body* (the AP's ``input`` edges) rather than
    a path parameter.
    """

    async def _check(
        candidate: AnalyticalPattern,
        credentials: HTTPAuthorizationCredentials | None = Depends(
            bearer_scheme),
        authentication: Optional[Authentication] = Depends(
            get_authentication_service),
        authorization: Optional[DatagemsAuthorizationService] = Depends(
            get_authorization_service
        ),
        dataset_svc: DatasetService = Depends(get_dataset_service),
    ) -> dict | None:
        if authentication is None:
            return None

        token, user = _authenticate(credentials, authentication)

        if authorization is None:
            return user

        input_node_ids = [
            str(e.to)
            for e in (candidate.edges or [])
            if "input" in e.labels
        ]
        if not input_node_ids:
            # No input references → no dataset constraint, grant access
            return user

        result = dataset_svc.list(DatasetFilter(nodeIds=input_node_ids))
        dataset_ids = [
            str(n.id)
            for d in result.get("datasets", [])
            for n in d.nodes
            if "sc:Dataset" in n.labels
        ]
        if not dataset_ids:
            raise HTTPException(
                status_code=404,
                detail="Referenced input dataset(s) not found.",
            )

        try:
            allowed = all(
                authorization.has_dataset_permission(
                    token, DatasetRole.BROWSE, ds_id)
                for ds_id in dataset_ids
            )
        except UserError as exc:
            logger.warning("Authorization failed: %s %s",
                           exc.status_code, exc.text)
            raise HTTPException(
                status_code=exc.status_code, detail=f"Gateway error. {exc.text}"
            )
        except GatewayError:
            raise HTTPException(
                status_code=502,
                detail="Permission gateway unavailable. Please see logs for details.",
            )

        if not allowed:
            raise HTTPException(
                status_code=403,
                detail="Forbidden: cannot browse referenced dataset.",
            )
        return user

    return _check


def get_ap_repo(session: Session = Depends(get_db_session)) -> AnalyticalPatternRepository:
    """Return the repository for AnalyticalPattern graph operations."""
    return Neo4jAnalyticalPatternRepository(session)


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


def get_task_repo(session: Session = Depends(get_db_session)) -> TaskRepository:
    """Return the repository for Task node operations."""
    return Neo4jTaskRepository(session)


def get_task_service(
    task_repo: TaskRepository = Depends(get_task_repo),
    ap_repo: AnalyticalPatternRepository = Depends(get_ap_repo),
) -> TaskService:
    """Return the service for Task operations."""
    return TaskService(task_repo, ap_repo)


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
            get_authorization_service
        ),
    ) -> list[str] | None:

        if authentication is None:
            return None  # Auth disabled – return everything
        token, _ = _authenticate(credentials, authentication)

        # gateway_token = _exchange(authentication, token)
        if authorization is None:
            return None  # Authz disabled – RBAC disabled

        try:
            return authorization.get_browseable_dataset_ids(token)
        except GatewayError:
            raise HTTPException(
                status_code=502, detail="Permission gateway unavailable"
            )
        except UserError as exc:
            logger.warning(
                "Authorization failed: %s %s: %s",
                exc.status_code,
                exc.text,
            )
            raise HTTPException(
                status_code=exc.status_code, detail=f"Gateway error. {exc.text}"
            )

    return _check
