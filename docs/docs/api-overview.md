# API Overview

The MoMa Management API is a RESTful HTTP API built with [FastAPI](https://fastapi.tiangolo.com/). All routes are versioned under `/v1` and grouped into two resource families: **datasets** and **nodes**.

The full machine-readable specification is available in the [OpenAPI Reference](openapi.md). The interactive docs (Swagger UI) are served at `/docs` when the service is running.

## Conventions

### Content type

All request and response bodies use `application/json`. Clients must set the `Content-Type: application/json` header for requests that carry a body.

### Authentication

All endpoints require a valid Bearer JWT in the `Authorization` header. See the [Security](security.md) page for details.

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

| Method | Path | Auth action | Description |
|---|---|---|---|
| `POST` | `/datasets` | `edit` | Ingest a Croissant profile and persist it as a PG-JSON subgraph |
| `GET` | `/datasets` | `browse` | List datasets with optional filtering and pagination |
| `GET` | `/datasets/{id}` | `browse` | Retrieve the full dataset subgraph (nodes + edges) |
| `DELETE` | `/datasets/{id}` | `delete` | Delete the dataset and its entire connected subgraph |
| `POST` | `/datasets/convert` | `browse` | Convert a Croissant profile to PG-JSON without persisting |

### Nodes

| Method | Path | Auth action | Description |
|---|---|---|---|
| `GET` | `/nodes/{id}` | `browse` | Retrieve a single graph node by ID |
| `PATCH` | `/nodes/{id}` | `edit` | Partially update properties of an existing node |

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
