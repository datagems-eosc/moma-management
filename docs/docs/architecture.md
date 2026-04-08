# Service Architecture

The MoMa Management API is a [FastAPI](https://fastapi.tiangolo.com/) service that manages **Metadata Object Model (MoMa)** property graphs stored in [Neo4j](https://neo4j.com/). It sits between external clients (such as the DataGEMS platform or integrating tools) and the Neo4j graph database.

## Layers

The service follows a layered architecture:

```
┌──────────────────────────────────────────────┐
│              HTTP Clients                    │
│  (DataGEMS UI, integrators, CLI)             │
└──────────────┬───────────────────────────────┘
               │ HTTP / REST (port 5000)
┌──────────────▼───────────────────────────────┐
│            FastAPI Application               │
│  ┌──────────┐ ┌────┐ ┌───────┐ ┌──────────┐ │
│  │ Datasets │ │APs │ │ Tasks │ │  Nodes   │ │
│  └────┬─────┘ └─┬──┘ └───┬───┘ └────┬─────┘ │
│       │         │        │          │        │
│  ┌────▼─────────▼────────▼──────────▼─────┐  │
│  │           Services layer               │  │
│  │  DatasetService · APService            │  │
│  │  TaskService   · NodeService           │  │
│  └──────────────────┬─────────────────────┘  │
│  ┌──────────────────▼─────────────────────┐  │
│  │          Repository layer              │  │
│  │  Neo4jDatasetRepository                │  │
│  │  Neo4jAnalyticalPatternRepository      │  │
│  │  Neo4jTaskRepository · Neo4jNodeRepo   │  │
│  └──────────────────┬─────────────────────┘  │
└─────────────────────┼────────────────────────┘
                      │ Bolt protocol
┌─────────────────────▼────────────────────────┐
│              Neo4j                            │
│  (MoMa property graph + vector index)        │
└──────────────────────────────────────────────┘
```

## Components

### API layer (`moma_management/api/v1/`)

All routes are mounted under the `/v1` prefix via a single router. Four resource groups are exposed:

- **Datasets** – CRUD on dataset subgraphs, Croissant ingestion/conversion, and schema validation.
- **Analytical Patterns** – CRUD on AP subgraphs with natural-language semantic search.
- **Tasks** – Task node creation and AP lookup by task.
- **Nodes** – Retrieval and partial update of individual graph nodes.
- **Health** – A lightweight liveness probe at `/health`.

Authentication and authorization are enforced as FastAPI dependencies injected into each route handler via `require_permission(action)` or `require_authentication()`.

### Services layer (`moma_management/services/`)

- **`DatasetService`** – Orchestrates the conversion of Croissant profiles to PG-JSON, schema validation, and delegates persistence to the repository.
- **`AnalyticalPatternService`** – AP CRUD, input-dataset validation, and semantic search (via an embedder).
- **`TaskService`** – Task creation and AP-id lookup.
- **`NodeService`** – Retrieves and patches individual Neo4j nodes.
- **`Embedder` / `LocalEmbedder`** – Produces vector embeddings from AP descriptions using `sentence-transformers` for semantic search.
- **`Authentication`** – Validates RS256 JWTs against JWKS published by the configured OIDC issuer (with in-memory cache). Optionally exchanges tokens using RFC 8693 for scope-specific credentials.
- **`AuthorizationService`** – Delegates per-dataset permission checks to an external permissions gateway over HTTP using the original or exchanged Bearer token.

### Domain layer (`moma_management/domain/`)

- **`MappingEngine`** – Reads `mapping.yml` and transforms Croissant field values into the PG-JSON node/edge structure expected by the MoMa schema.
- **`PgJsonGraph`** – Base Pydantic model for validated PG-JSON graphs, providing edge-constraint enforcement, DFS connectivity checks, and canonical equality.
- **`Dataset`** / **`AnalyticalPattern`** – Concrete graph models with root-node and structural validation.
- **`LocalSchemaValidator`** – Validates raw PG-JSON dicts against Draft 7 JSON schemas and returns AJV-style errors.
- **`filters.py`** – Query filter and pagination models.
- **`generated/`** – Pydantic v2 models auto-generated from the JSON Schema files in `schema/` via `make gen`.

### Repository layer (`moma_management/repository/`)

Defines abstract interfaces (`DatasetRepository`, `AnalyticalPatternRepository`, `TaskRepository`, `NodeRepository`) with Neo4j-backed implementations. All graph I/O uses the official `neo4j` Python driver with PG-JSON serialization helpers provided by `Neo4jPGSONMixin`. The AP repository also manages a Neo4j vector index for semantic search.

## Authentication flow

When a request arrives at a protected endpoint:

```
Client                  API                OIDC Issuer      Permissions Gateway
  │                      │                     │                    │
  │ Authorization: Bearer │                     │                    │
  ├─────────────────────►│                     │                    │
  │     (JWT token)      │                     │                    │
  │                      │ Validate JWT        │                    │
  │                      ├────────────────────►│                    │
  │                      │◄─ Fetch JWKS        │                    │
  │                      │                     │                    │
  │                      │ (Optional) Exchange token                 │
  │                      │ for scope-specific credentials            │
  │                      │                     │                    │
  │                      │ POST /authz/check   │                    │
  │                      ├───────────────────────────────────────►│
  │                      │ with action & dataset ID                 │
  │                      │◄─ Permission granted                     │
  │                      │                    │                    │
  │◄─────────────────────┤                    │                    │
  │  Response            │                    │                    │
```

1. **JWT Validation** – Verifies signature, issuer, audience, and expiration claims.
2. **Token Exchange** – If configured, exchanges the user token for an application-scoped token.
3. **Permission Check** – Queries the gateway to verify authorization for the requested action.

See [Security](security.md) for details.

## Data flow – dataset ingestion

```
Client                Service               Neo4j
  │                      │                    │
  │ POST /datasets        │                    │
  │ (Croissant JSON)      │                    │
  ├─────────────────────►│                    │
  │                      │ MappingEngine      │
  │                      │ converts to PG-JSON│
  │                      ├───────────────────►│
  │                      │  MERGE nodes/edges │
  │◄─────────────────────┤                    │
  │  Dataset (PG-JSON)   │                    │
```

## External dependencies

| Dependency | Role |
|---|---|
| Neo4j ≥ 5 | Primary graph data store (including vector index for semantic search) |
| OIDC issuer (DataGEMS AAI) | JWT signing keys (JWKS) |
| Permissions gateway | Dataset-level authorization checks |
