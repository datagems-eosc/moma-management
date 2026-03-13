# Service Architecture

The MoMa Management API is a [FastAPI](https://fastapi.tiangolo.com/) service that manages **Metadata Object Model (MoMa)** property graphs stored in [Neo4j](https://neo4j.com/). It sits between external clients (such as the DataGEMS platform or integrating tools) and the Neo4j graph database.

## Layers

The service follows a layered architecture:

```
┌──────────────────────────────────────┐
│           HTTP Clients               │
│  (DataGEMS UI, integrators, CLI)     │
└──────────────┬───────────────────────┘
               │ HTTP / REST (port 5000)
┌──────────────▼───────────────────────┐
│          FastAPI Application         │
│  ┌────────────┐  ┌────────────────┐  │
│  │  Datasets  │  │     Nodes      │  │
│  │  API (v1)  │  │   API (v1)     │  │
│  └─────┬──────┘  └──────┬─────────┘  │
│        │                │            │
│  ┌─────▼────────────────▼─────────┐  │
│  │         Services layer         │  │
│  │  DatasetService / NodeService  │  │
│  └─────────────┬──────────────────┘  │
│  ┌─────────────▼──────────────────┐  │
│  │        Repository layer        │  │
│  │  Neo4jDatasetRepository        │  │
│  │  Neo4jNodeRepository           │  │
│  └─────────────┬──────────────────┘  │
└────────────────┼─────────────────────┘
                 │ Bolt protocol
┌────────────────▼─────────────────────┐
│              Neo4j                   │
│     (MoMa property graph store)      │
└──────────────────────────────────────┘
```

## Components

### API layer (`moma_management/api/v1/`)

All routes are mounted under the `/v1` prefix via a single router. Two resource groups are exposed:

- **Datasets** – CRUD operations on dataset subgraphs plus a stateless conversion endpoint.
- **Nodes** – Retrieval and partial update of individual graph nodes.
- **Health** – A lightweight liveness probe at `/health`.

Authentication and authorization are enforced as FastAPI dependencies injected into each route handler via `require_permission(action)`.

### Services layer (`moma_management/services/`)

- **`DatasetService`** – Orchestrates the conversion of Croissant profiles to PG-JSON and delegates persistence to the repository.
- **`NodeService`** – Retrieves and patches individual Neo4j nodes.
- **`Authentication`** – Validates RS256 JWTs against JWKS published by the configured OIDC issuer (with in-memory cache).
- **`AuthorizationService`** – Delegates per-dataset permission checks to an external permissions gateway over HTTP.

### Domain layer (`moma_management/domain/`)

- **`MappingEngine`** – Reads `mapping.yml` and transforms Croissant field values into the PG-JSON node/edge structure expected by the MoMa schema.
- **`Dataset`** / **`filters.py`** – Pydantic models representing the MoMa graph and query parameters.
- **`generated/`** – Pydantic v2 models auto-generated from the JSON Schema files in `schema/` via `make gen`.

### Repository layer (`moma_management/repository/`)

Defines abstract interfaces (`DatasetRepository`, `NodeRepository`) with Neo4j-backed implementations. All graph I/O uses the official `neo4j` Python driver with PG-JSON serialization helpers provided by `Neo4jPGSONMixin`.

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
| Neo4j ≥ 5 | Primary graph data store |
| OIDC issuer (DataGEMS AAI) | JWT signing keys (JWKS) |
| Permissions gateway | Dataset-level authorization checks |
