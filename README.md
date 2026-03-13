# MoMa Management API

[![Commit activity](https://img.shields.io/github/commit-activity/m/datagems-eosc/moma-management)](https://github.com/datagems-eosc/moma-management/commits/main)
[![License](https://img.shields.io/github/license/datagems-eosc/moma-management)](LICENSE)

## Overview

The **MoMa Management API** is a [FastAPI](https://fastapi.tiangolo.com/) service for managing the **MoMa** (Metadata Object Model) property graph stored in [Neo4j](https://neo4j.com/). It accepts dataset profiles in [Croissant](https://docs.mlcommons.org/croissant/docs/) format, converts them to PG-JSON according to the MoMa schema, and persists the result to Neo4j. Individual graph nodes can also be retrieved and updated independently through the nodes API.

## Features

- **Dataset CRUD** – Ingest, retrieve, list, and delete dataset subgraphs stored in Neo4j.
- **Croissant conversion** – Convert a Croissant profile to PG-JSON on the fly without persisting it (`/datasets/convert`).
- **Node management** – Retrieve and partially update individual graph nodes (`/nodes/{id}`).
- **Authentication** – Bearer JWT validation against a configurable OIDC issuer (RS256); disabled when `OIDC_ISSUER` is unset.
- **Authorization** – Per-dataset permission checks delegated to an external gateway; disabled when `PERMISSIONS_GATEWAY_URL` is unset.
- **Schema code generation** – Pydantic v2 models are generated from JSON Schema via `make gen`.

## Quick Start

**Prerequisites:** [uv](https://docs.astral.sh/uv/) and a running Neo4j instance.

```bash
git clone https://github.com/datagems-eosc/moma-management.git
cd moma-management

# Install dependencies (remove --all-groups for production)
uv sync --all-groups

# Start the API (default: http://localhost:5000)
uv run python moma_management/main.py
```

**Interactive docs:** http://localhost:5000/docs

## Docker

The `Dockerfile` exposes two build targets:

```bash
# Run the test suite
docker build --target test -t moma-test .
docker run moma-test

# Build and run the production image
docker build --target prod -t moma-api .
docker run -p 5000:5000 \
  -e NEO4J_URI=bolt://<host>:7687 \
  -e NEO4J_USER=neo4j \
  -e NEO4J_PASSWORD=secret \
  moma-api
```

## Configuration

Configuration is managed entirely through environment variables:

| Variable | Default | Description |
|---|---|---|
| `NEO4J_URI` | `bolt://localhost:7687` | Neo4j Bolt connection URI |
| `NEO4J_USER` | `neo4j` | Neo4j username |
| `NEO4J_PASSWORD` | `datagems` | Neo4j password |
| `MAPPING_FILE` | `moma_management/domain/mapping.yml` | Path to the Croissant → PG-JSON field mapping |
| `ROOT_PATH` | *(empty)* | ASGI root path (useful when behind a reverse proxy) |
| `OIDC_ISSUER` | *(empty)* | OIDC issuer URL for JWT validation (auth disabled if unset) |
| `OIDC_AUDIENCE` | *(empty)* | Expected JWT audience claim (optional) |
| `JWKS_TTL_SECONDS` | `300` | How long to cache the JWKS from the OIDC issuer |
| `PERMISSIONS_GATEWAY_URL` | *(empty)* | External gateway URL for dataset-level authorization (disabled if unset) |

## API Endpoints

### Datasets (`/datasets`)

| Method | Path | Description |
|---|---|---|
| `POST` | `/datasets` | Ingest a Croissant profile → store as PG-JSON in Neo4j |
| `GET` | `/datasets` | List datasets with filtering and pagination |
| `GET` | `/datasets/{id}` | Retrieve the full dataset subgraph by ID |
| `DELETE` | `/datasets/{id}` | Delete a dataset and its connected subgraph |
| `POST` | `/datasets/convert` | Convert a Croissant profile to PG-JSON (no persistence) |

#### `GET /datasets` query parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `nodeIds` | `string[]` | `[]` | Filter by node IDs |
| `properties` | `DatasetProperty[]` | `[]` | Filter by dataset properties |
| `types` | `NodeLabel[]` | `[]` | Filter by node label |
| `mimeTypes` | `MimeType[]` | `[]` | Filter by MIME type |
| `orderBy` | `DatasetSortField[]` | `[]` | Sort fields |
| `direction` | `ASC` \| `DESC` | `ASC` | Sort direction |
| `publishedFrom` | `date` | — | Published date lower bound |
| `publishedTo` | `date` | — | Published date upper bound |
| `status` | `Status` | — | Filter by dataset status |
| `page` | `int ≥ 1` | `1` | Page number |
| `pageSize` | `1–100` | `25` | Items per page |

### Nodes (`/nodes`)

| Method | Path | Description |
|---|---|---|
| `GET` | `/nodes/{id}` | Retrieve a single graph node by ID |
| `PATCH` | `/nodes/{id}` | Partially update properties of an existing node |

### Health

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Returns `200 OK` when the service is up |

## Project Structure

```
moma_management/
├── main.py                    # FastAPI app entry point (port 5000)
├── di.py                      # Dependency injection (Neo4j driver, auth, service wiring)
├── api/v1/
│   ├── health.py              # GET /health
│   ├── datasets/
│   │   ├── ingest.py          # POST   /datasets
│   │   ├── list.py            # GET    /datasets
│   │   ├── get.py             # GET    /datasets/{id}
│   │   ├── delete.py          # DELETE /datasets/{id}
│   │   └── convert.py         # POST   /datasets/convert
│   └── nodes/
│       ├── get.py             # GET   /nodes/{id}
│       └── update.py          # PATCH /nodes/{id}
├── services/
│   ├── dataset.py             # DatasetService: convert / ingest / get / delete / list
│   ├── node.py                # NodeService: get / update
│   ├── authentication.py      # JWT validation (OIDC/JWKS, RS256)
│   └── authorization.py       # Dataset-level permission checks via external gateway
├── domain/
│   ├── dataset.py             # Dataset model with graph validation
│   ├── filters.py             # Query filter / pagination models
│   ├── mapping_engine.py      # Croissant → PG-JSON conversion logic
│   ├── mapping.yml            # Field mapping configuration
│   ├── generated/             # Pydantic v2 models (generated via `make gen`)
│   └── schema/                # JSON Schema source files
│       └── moma.schema.json
├── repository/
│   ├── dataset/               # Abstract + Neo4j-backed dataset repository
│   ├── node/                  # Abstract + Neo4j-backed node repository
│   └── neo4j_pgson_mixin.py   # Shared Neo4j PG-JSON helpers
└── legacy/
    └── converters.py          # Deprecated converters (kept for reference)
assets/
├── datasets/                  # Sample Croissant input files (light / heavy variants)
└── profiles/                  # Sample profile files
tests/
├── conftest.py
├── test_croissant_to_moma_engine.py
├── test_dataset_service.py
├── test_dataset_storage.py
└── test_node_storage.py
```

## Testing

Tests use [testcontainers](https://testcontainers-python.readthedocs.io/) to spin up a Neo4j instance automatically.

```bash
# Install all dependency groups
uv sync --all-groups

# Run the full test suite (4 parallel workers)
uv run pytest
```

## Development

```bash
# Regenerate Pydantic models from JSON Schema
make gen
```

The `make gen` command runs `datamodel-codegen` against `moma_management/domain/schema/` and writes generated Pydantic v2 models to `moma_management/domain/generated/`.

## Tech Stack

| Component | Library |
|---|---|
| Web framework | [FastAPI](https://fastapi.tiangolo.com/) |
| Graph database | [Neo4j](https://neo4j.com/) (via `neo4j` Python driver) |
| Data validation | [Pydantic v2](https://docs.pydantic.dev/) |
| Schema codegen | [datamodel-code-generator](https://koxudaxi.github.io/datamodel-code-generator/) |
| Package manager | [uv](https://docs.astral.sh/uv/) |
| Test runner | pytest + [testcontainers](https://testcontainers-python.readthedocs.io/) |
| Python | ≥ 3.14 |

## Documentation

Full documentation is available at: https://datagems-eosc.github.io/moma-management/

