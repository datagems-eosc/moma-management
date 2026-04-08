# API Overview

The MoMa Management API is a RESTful HTTP API built with [FastAPI](https://fastapi.tiangolo.com/). All routes are versioned under `/v1` and grouped into four resource families: **datasets**, **analytical patterns**, **tasks**, and **nodes**.

The full machine-readable specification is available in the [OpenAPI Reference](openapi.md). The interactive docs (Swagger UI) are served at `/docs` when the service is running.

## Conventions

### Content type

All request and response bodies use `application/json`. Clients must set the `Content-Type: application/json` header for requests that carry a body.

### Authentication

All endpoints require a valid Bearer JWT in the `Authorization` header unless noted otherwise (validation, conversion, and health endpoints are public). See the [Security](security.md) page for details.

### Pagination

List endpoints (`GET /datasets`) support cursor-free page-based pagination controlled by two query parameters:

| Parameter | Type | Default | Constraints |
|---|---|---|---|
| `page` | integer | `1` | ≥ 1 |
| `pageSize` | integer | `25` | 1 – 100 |

Responses include the requested page of items directly as a JSON array alongside total count metadata.

### Filtering & sorting

`GET /datasets` accepts several optional query parameters to filter results by node IDs, dataset properties, node labels, MIME types, and publication date range. Results can be sorted by one or more fields with a configurable `ASC`/`DESC` direction.

### Identifiers

All resource identifiers (`id`) are strings. Dataset and node IDs are assigned at ingestion time and correspond to the `id` property stored on the root node in the Neo4j graph.

## Resource endpoints

### Datasets

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/datasets` | `CREATE` | Create a dataset from a PG-JSON body |
| `POST` | `/datasets/croissant` | `CREATE` | Ingest a Croissant profile and persist it as a PG-JSON subgraph |
| `GET` | `/datasets` | `BROWSE` | List datasets with optional filtering and pagination |
| `GET` | `/datasets/{id}` | `BROWSE` | Retrieve the full dataset subgraph (nodes + edges) |
| `DELETE` | `/datasets/{id}` | `DELETE` | Delete the dataset and its entire connected subgraph |
| `POST` | `/datasets/convert` | none | Convert a Croissant profile to PG-JSON without persisting |
| `POST` | `/datasets/validate` | none | Validate a PG-JSON dataset against the MoMa schema |

### Analytical Patterns

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/aps` | `BROWSE` on input datasets | Create an AP (input edges must reference existing datasets) |
| `GET` | `/aps` | `BROWSE` | List APs (supports semantic search via `q`) |
| `GET` | `/aps/{id}` | `BROWSE` on input datasets | Retrieve an AP by root node ID |
| `DELETE` | `/aps/{id}` | `BROWSE` on all input datasets | Delete an AP (dataset nodes are left intact) |
| `POST` | `/aps/validate` | none | Validate a PG-JSON AP against the MoMa schema |

`GET /aps` search parameters:

| Parameter | Type | Default | Constraints |
|---|---|---|---|
| `q` | string | — | Natural-language query for semantic search |
| `top_k` | integer | `10` | 1–100 |
| `threshold` | float | `0.0` | 0.0–1.0 |

### Tasks

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/tasks` | authenticated | Create a new Task node |
| `GET` | `/tasks/{id}/aps` | authenticated | Get AP IDs accomplished by a task |

### Nodes

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/nodes/{id}` | `BROWSE` on parent dataset | Retrieve a single graph node by ID |
| `PATCH` | `/nodes/{id}` | `EDIT` on parent dataset | Partially update properties of an existing node |

### Health

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/health` | none | Liveness probe – returns `200 OK` if the service is up |

## PG-JSON format

The service uses a Property Graph JSON (PG-JSON) representation for datasets. A dataset is modelled as a subgraph with:

- **Nodes** – each has an `id`, a list of `labels`, and a `properties` map.
- **Edges** – each has an `id`, a `label`, `from`/`to` node IDs, and a `properties` map.

The exact schema is defined in `moma_management/domain/schema/moma.schema.json` and the corresponding Pydantic v2 models live in `moma_management/domain/generated/`.

## Croissant input format

Ingestion and conversion endpoints accept dataset profiles in [Croissant](https://docs.mlcommons.org/croissant/docs/) format — a JSON-LD vocabulary for ML dataset descriptions. The service's mapping engine (configured via `mapping.yml`) transforms Croissant fields into the MoMa PG-JSON structure.
