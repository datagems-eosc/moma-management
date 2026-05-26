# Configuration

The MoMa Management API is configured entirely through **environment variables**. There are no configuration files to manage — all settings are read at startup via `os.getenv()` with built-in defaults where applicable.

## Environment variables

### Neo4j connection

| Variable | Default | Description |
|---|---|---|
| `NEO4J_URI` | `bolt://localhost:7687` | Neo4j Bolt connection URI |
| `NEO4J_USER` | `neo4j` | Neo4j username |
| `NEO4J_PASSWORD` | `datagems` | Neo4j password |

### Service behaviour

| Variable | Default | Description |
|---|---|---|
| `MAPPING_FILE` | `moma_management/domain/mapping.yml` | Path to the Croissant → PG-JSON field mapping file |
| `ROOT_PATH` | *(empty)* | ASGI root path prefix (useful when running behind a reverse proxy or API gateway) |
| `PROFILING` | `false` | Set to `true` to enable the request profiling middleware. Responses become an HTML profiling report when enabled. |

### Authentication

| Variable | Default | Description |
|---|---|---|
| `OIDC_ISSUER` | *(empty)* | Base URL of the OIDC issuer (e.g. `https://aai.datagems.eu/realms/datagems`). **Authentication is disabled when this is unset.** |
| `OIDC_CLIENT_ID` | *(empty)* | OIDC client ID. Used as the expected `aud` claim value in JWT validation and as the token-exchange client identifier. Required when token exchange is enabled. |
| `OIDC_CLIENT_SECRET` | *(empty)* | OIDC client secret for RFC 8693 token exchange. Must be set together with `OIDC_CLIENT_ID` and `OIDC_EXCHANGE_SCOPE`. |
| `OIDC_EXCHANGE_SCOPE` | *(empty)* | Scope requested during token exchange (e.g. `dg-app-api`). Must be set together with `OIDC_CLIENT_ID` and `OIDC_CLIENT_SECRET`. |
| `JWKS_TTL_SECONDS` | `300` | How long (seconds) to cache the JWKS fetched from the OIDC issuer before refreshing. |

### Authorization

| Variable | Default | Description |
|---|---|---|
| `PERMISSIONS_GATEWAY_URL` | *(empty)* | Base URL of the external permissions gateway used for per-dataset authorization checks. **Authorization is disabled when this is unset.** |

### Semantic search

| Variable | Default | Description |
|---|---|---|
| `EMBEDDER_MODEL` | `all-MiniLM-L6-v2` | Sentence-transformers model name used to embed AP descriptions for semantic search. Set to an empty string to disable the embedder. |

## Secrets

The variables `NEO4J_PASSWORD`, `OIDC_CLIENT_SECRET`, and `OIDC_ISSUER` contain sensitive data. In production these should be supplied via a secrets manager (e.g. Kubernetes Secrets, Docker secrets, Vault) rather than plain environment variables or `.env` files.

## Mapping file

The `MAPPING_FILE` points to a YAML file that controls how Croissant fields are translated into MoMa PG-JSON nodes and edges. The default file ships with the repository at `moma_management/domain/mapping.yml`. A custom path can be provided to override the mapping without rebuilding the image.
