# MoMa Management API

A **FastAPI** application for managing the MoMa (Metadata Object Model) property graph stored in **Neo4j**. The service accepts dataset profiles in [Croissant](https://docs.mlcommons.org/croissant/docs/) format, converts them to PG-JSON according to the MoMa schema, and persists the result to Neo4j.

---

## Project Structure

```
moma_management/
├── main.py                    # FastAPI app entry point (port 5000)
├── di.py                      # Dependency injection (Neo4j driver, service wiring)
├── api/v1/
│   ├── health.py              # GET /health
│   └── datasets/
│       ├── ingest.py          # POST   /datasets
│       ├── list.py            # GET    /datasets
│       ├── get.py             # GET    /datasets/{id}
│       ├── delete.py          # DELETE /datasets/{id}
│       ├── convert.py         # POST   /datasets/convert
│       └── validate.py        # POST   /datasets/validate
├── services/
│   └── dataset.py             # DatasetService: convert / validate / ingest / get / delete / list
├── domain/
│   ├── dataset.py             # Dataset model with graph validation
│   ├── filters.py             # Query filter / pagination models
│   ├── mapping_engine.py      # Croissant → PG-JSON conversion logic
│   ├── mapping.yml            # Field mapping configuration
│   └── schema/                # JSON Schema source files (input to code generation)
│       └── moma.schema.json
├── repository/
│   ├── dataset/
│   │   ├── dataset_repository.py       # Abstract repository interface
│   │   └── neo4j_dataset_repository.py # Neo4j-backed implementation
│   └── neo4j_pgson_mixin.py            # Shared Neo4j PG-JSON helpers
└── legacy/
    └── converters.py          # Deprecated converters (kept for reference)
assets/
├── datasets/                  # Sample Croissant input files (light / heavy variants)
└── profiles/                  # Sample profile files
tests/
├── conftest.py
├── test_croissant_to_moma_engine.py
├── test_dataset_service.py
└── test_dataset_storage.py
```

---

## Running Locally

**Prerequisites:** [uv](https://docs.astral.sh/uv/) and a running Neo4j instance.

```bash
git clone https://github.com/datagems-eosc/moma-management.git
cd moma-management

# Install dependencies
uv sync

# Start the API (default: http://localhost:5000)
uv run python moma_management/main.py
```

**Interactive docs:** http://localhost:5000/docs

---

## Docker

The `Dockerfile` has two build targets:

```bash
# Run tests
docker build --target test -t moma-test .
docker run moma-test

# Build and run production image
docker build --target prod -t moma-api .
docker run -p 5000:5000 \
  -e NEO4J_URI=bolt://<host>:7687 \
  -e NEO4J_USER=neo4j \
  -e NEO4J_PASSWORD=datagems \
  moma-api
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `NEO4J_URI` | `bolt://localhost:7687` | Neo4j Bolt connection URI |
| `NEO4J_USER` | `neo4j` | Neo4j username |
| `NEO4J_PASSWORD` | `datagems` | Neo4j password |
| `MAPPING_FILE` | `moma_management/domain/mapping.yml` | Path to the Croissant→PG-JSON field mapping |
| `ROOT_PATH` | *(empty)* | ASGI root path (useful when behind a reverse proxy) |

---

## API Endpoints

All endpoints are mounted under the `/datasets` prefix (v1 router).

### `POST /datasets`
Ingest a dataset profile into the MoMa repository.
- **Input:** Dataset profile in **Croissant** format.
- **Action:** Converts to PG-JSON, validates against the MoMa schema, and stores the graph in Neo4j.
- **Returns:** The ingested `Dataset` as PG-JSON.

### `GET /datasets`
List datasets stored in the repository with optional filtering and pagination.

**Query parameters:**

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

### `GET /datasets/{id}`
Retrieve the full dataset subgraph (nodes + edges) by dataset ID.
- **Returns:** `Dataset` PG-JSON, or `404` if not found.

### `DELETE /datasets/{id}`
Delete a dataset and its entire connected subgraph from Neo4j.
- **Returns:** `204 No Content`.

### `POST /datasets/convert`
Convert a Croissant profile to PG-JSON without persisting it.
- **Input:** Dataset profile in **Croissant** format.
- **Returns:** `Dataset` PG-JSON.

### `POST /datasets/validate`
Validate a PG-JSON payload against the MoMa graph schema.
- **Input:** PG-JSON object.
- **Returns:** Validated `Dataset` or validation error details.

### `GET /health`
Returns `200 OK` when the service is up.

---

## Development

```bash
# Install all dependency groups (including dev/test tools)
uv sync --all-groups

# Run tests (4 parallel workers, uses testcontainers to spin up Neo4j)
uv run pytest

# Regenerate Pydantic models from JSON Schema
make gen
```

The `make gen` command runs `datamodel-codegen` against `moma_management/domain/schema/` and writes generated Pydantic v2 models to `moma_management/domain/generated/`.

---

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

