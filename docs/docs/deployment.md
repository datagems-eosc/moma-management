# Deployment

The service is distributed as a Docker image and is part of the DataGEMS platform, following the DataGEMS release and deployment procedures. This section covers the practical steps needed to run the service.

## Docker

The service ships with a multi-stage `Dockerfile` that produces two targets:

| Target | Purpose |
|---|---|
| `test` | Runs the full pytest suite inside the container |
| `prod` | Minimal production image that starts the FastAPI server |

The production image is built and pushed to the GitHub Container Registry (`ghcr.io/datagems-eosc/moma-management`) automatically on each versioned tag. See the [Automations](automations.md) section for details.

### Running the production image

```bash
docker run -p 5000:5000 \
  -e NEO4J_URI=bolt://<host>:7687 \
  -e NEO4J_USER=neo4j \
  -e NEO4J_PASSWORD=<password> \
  -e OIDC_ISSUER=https://aai.datagems.eu/realms/datagems \
  -e PERMISSIONS_GATEWAY_URL=https://<gateway>/api \
  ghcr.io/datagems-eosc/moma-management:<version>
```

## Configuration

All runtime configuration is provided through environment variables. The full list of supported variables and their defaults is described in the [Configuration](configuration.md) section.

## Dependencies

Before starting the service, ensure the following are reachable:

- **Neo4j** (Bolt endpoint specified by `NEO4J_URI`) – required for all data operations.
- **OIDC issuer** (`OIDC_ISSUER`) – required when authentication is enabled; the service fetches JWKS on first request.
- **Permissions gateway** (`PERMISSIONS_GATEWAY_URL`) – required when authorization is enabled.
