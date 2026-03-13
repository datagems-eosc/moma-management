# MoMa Management API

[![Commit activity](https://img.shields.io/github/commit-activity/m/datagems-eosc/moma-management)](https://github.com/datagems-eosc/moma-management/commits/main)
[![License](https://img.shields.io/github/license/datagems-eosc/moma-management)](LICENSE)

## Overview

The **MoMa Management API** manage CRUD operation on MoMa, a data-flox graph.

## Quick Start

Just run the .devcontainer provided. 
Once inside the devcontainer, you can then install the dependencies like so

```sh
uv sync --all-groups

```

And run the project :
```sh
# You can also use the debug configuration if you are using vscode
uv run python moma_management/main.py
```

Once launched, the APi is avilable here
**Interactive docs:** http://localhost:5000/docs

## Running the container

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

| Variable                  | Required | Default                              | Description                                                              |
| ------------------------- | -------- | ------------------------------------ | ------------------------------------------------------------------------ |
| `NEO4J_URI`               | yes      | `bolt://localhost:7687`              | Neo4j Bolt connection URI                                                |
| `NEO4J_USER`              | yes      | `neo4j`                              | Neo4j username                                                           |
| `NEO4J_PASSWORD`          | yes      | `datagems`                           | Neo4j password                                                           |
| `MAPPING_FILE`            | no       | `moma_management/domain/mapping.yml` | Path to the Croissant → PG-JSON field mapping                            |
| `ROOT_PATH`               | no       | *(empty)*                            | ASGI root path (useful when behind a reverse proxy)                      |
| `OIDC_ISSUER`             | no       | *(empty)*                            | OIDC issuer URL for JWT validation (auth disabled if unset)              |
| `OIDC_AUDIENCE`           | no       | *(empty)*                            | Expected JWT audience claim (optional)                                   |
| `JWKS_TTL_SECONDS`        | no       | `300`                                | How long to cache the JWKS from the OIDC issuer                          |
| `PERMISSIONS_GATEWAY_URL` | no       | *(empty)*                            | External gateway URL for dataset-level authorization (disabled if unset) |

## API Endpoints

### Datasets (`/datasets`)

| Method   | Path                | Description                                             |
| -------- | ------------------- | ------------------------------------------------------- |
| `POST`   | `/datasets`         | Ingest a Croissant profile → store as PG-JSON in Neo4j  |
| `GET`    | `/datasets`         | List datasets with filtering and pagination             |
| `GET`    | `/datasets/{id}`    | Retrieve the full dataset subgraph by ID                |
| `DELETE` | `/datasets/{id}`    | Delete a dataset and its connected subgraph             |
| `POST`   | `/datasets/convert` | Convert a Croissant profile to PG-JSON (no persistence) |

#### `GET /datasets` query parameters

| Parameter       | Type                 | Default | Description                  |
| --------------- | -------------------- | ------- | ---------------------------- |
| `nodeIds`       | `string[]`           | `[]`    | Filter by node IDs           |
| `properties`    | `DatasetProperty[]`  | `[]`    | Filter by dataset properties |
| `types`         | `NodeLabel[]`        | `[]`    | Filter by node label         |
| `mimeTypes`     | `MimeType[]`         | `[]`    | Filter by MIME type          |
| `orderBy`       | `DatasetSortField[]` | `[]`    | Sort fields                  |
| `direction`     | `ASC` \| `DESC`      | `ASC`   | Sort direction               |
| `publishedFrom` | `date`               | —       | Published date lower bound   |
| `publishedTo`   | `date`               | —       | Published date upper bound   |
| `status`        | `Status`             | —       | Filter by dataset status     |
| `page`          | `int ≥ 1`            | `1`     | Page number                  |
| `pageSize`      | `1–100`              | `25`    | Items per page               |

### Nodes (`/nodes`)

| Method  | Path          | Description                                     |
| ------- | ------------- | ----------------------------------------------- |
| `GET`   | `/nodes/{id}` | Retrieve a single graph node by ID              |
| `PATCH` | `/nodes/{id}` | Partially update properties of an existing node |

### Health

| Method | Path      | Description                             |
| ------ | --------- | --------------------------------------- |
| `GET`  | `/health` | Returns `200 OK` when the service is up |

## Project Structure

```
moma_management/
├── main.py                    # FastAPI app entry point (port 5000)
├── di.py                      # Dependency injection (Neo4j driver, auth, service wiring)
├── api/v1/                    # Front facing API
├── services/
│   ├── dataset.py             # Dataset CRUD
│   ├── node.py                # Node CRUD
│   ├── authentication.py      # JWT validation (OIDC/JWKS, RS256)
│   └── authorization.py       # Dataset-level permission checks via external gateway
├── domain/
│   ├── dataset.py             # Dataset model with graph validation
│   ├── filters.py             # Query filter / pagination models
│   ├── mapping_engine.py      # Croissant → PG-JSON conversion logic
│   ├── mapping.yml            # Field mapping configuration
│   ├── generated/             # Pydantic v2 models (generated via `make gen`)
│   └── schema/                # JSON Schema source files
├── repository/                # Object to DB layer
└── legacy/
    └── converters.py          # Deprecated converters (kept for reference)
```

## Testing

Tests use [testcontainers](https://testcontainers-python.readthedocs.io/) to spin up a Neo4j instance automatically.

```bash
# Install all dependency groups
uv sync --all-groups

# Run the full test suite (4 parallel workers)
uv run pytest
```

## Updating the Moma model
To update the MoMa model, two step are involved.

First, update the json schema in `domain/schema`.

For example, let's add a new property to the `data` node 
```jsonc
// domain/schema/nodes/data.schema.json
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Data",
    "type": "object",
    "properties": {
        "id": {
            "type": "string"
        },
        "type": {
            "type": "string",
            "enum": [
                "Data"
            ]
        },
        "name": {
            "type": "string"
        },
        "description": {
            "type": "string"
        },
        "contentSize": {
            "type": "string"
        },
        "contentUrl": {
            "type": "string",
            "format": "uri"
        },
        "encodingFormat": {
            "type": "string"
        },
        "sha256": {
            "type": "string"
        },
        // THIS IS NEW
        "myProp": {
            "type": "thing"
        }
    },
    "required": [
        "id",
        "type"
    ]
}
```

We can then update the pydantic models using:

```sh
make gen
```

This will recreate the `domain/generated/nodes/data_schema.py` with the updated property

```py
class Type(Enum):
    data = 'Data'


class Data(BaseModel):
    id: str
    type: Type
    name: str | None = None
    description: str | None = None
    content_size: str | None = Field(None, alias='contentSize')
    content_url: AnyUrl | None = Field(None, alias='contentUrl')
    encoding_format: str | None = Field(None, alias='encodingFormat')
    sha256: str | None = None
    my_prop: str | None = None = Field(None, alias='myProp')
```

The second step is map this property from the Croissant format to the MoMa format. 
To achieve this, we must edit `domain/mapping.yml`. Croissant `Data` maps to a `Distribution` Moma node in this case.

```yml
Distribution:
  properties:
    type:           "@type"
    name:           name
    description:    description
    encodingFormat: encodingFormat
    contentSize:    contentSize
    contentUrl:     contentUrl
    sha256:         sha256
    # NEW
    # myProp is the name of the property we've added
    # myCroissantProp is the name of the property we want to map to in the croissant format
    myProp:         myCroissantProp
```

## Documentation

Full documentation is available at: https://datagems-eosc.github.io/moma-management/

