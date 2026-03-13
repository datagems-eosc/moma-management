# Maintenance

The service is part of the DataGEMS platform, following the DataGEMS release and deployment procedures. This section describes operational maintenance topics.

## Healthchecks

The service exposes a `/health` endpoint that returns `200 OK` when the service is running. This endpoint does not require authentication and can be used by load balancers, orchestrators, or monitoring systems as a liveness probe.

## Versions & Updates

The service follows [Semantic Versioning](https://semver.org/):

- **MAJOR** (X.0.0): Breaking changes that are incompatible with previous versions.
- **MINOR** (X.Y.0): New features added in a backward-compatible way.
- **PATCH** (X.Y.Z): Bug fixes and security patches that do not affect compatibility.

Releases are published as Git tags in the [code repository](https://github.com/datagems-eosc/moma-management/releases) and automatically trigger the Docker image build and GitHub Release workflows.

## Schema regeneration

When the MoMa JSON Schema (`moma_management/domain/schema/`) changes, the generated Pydantic models must be regenerated:

```bash
make gen
```

This regenerates all models in `moma_management/domain/generated/` and should be committed alongside schema changes.

## Backups

All state persisted by the service resides in the Neo4j graph database described in the [Datastores](datastore.md) section. Backup strategy should follow the Neo4j backup and restore procedures appropriate for the deployed Neo4j version and edition.

## Troubleshooting

Troubleshooting is primarily done through the logging output described in the [Logging](logging.md) section. Running the service with `DEBUG` log level provides detailed information on requests, JWKS fetches, and Neo4j operations.

An example of a Verbose response that returns 200 OK for healthy state is:
```json
{
    "status": "Healthy",
    "duration": "00:00:00.0216780",
    "results": {
        "privateMemory": {
            "status": "Healthy",
            "description": null,
            "duration": "00:00:00.0015544",
            "tags": null,
            "exception": null,
            "data": {}
        },
        "processMemory": {
            "status": "Healthy",
            "description": "Allocated megabytes in memory: 408 mb",
            "duration": "00:00:00.0000382",
            "tags": null,
            "exception": null,
            "data": {}
        },
        "db": {
            "status": "Healthy",
            "description": null,
            "duration": "00:00:00.0211601",
            "tags": null,
            "exception": null,
            "data": {}
        }
    }
}
```

## Verions & Updates

The service follows a semantic versioning scheme and structures versions as MAJOR.MINOR.PATCH:

* MAJOR (X.0.0): Breaking changes that are incompatible with previous versions.
* MINOR (X.Y.0): New features added in a backward-compatible way.
* PATCH (X.Y.Z): Bug fixes and security patches that do not affect compatibility.

## Backups

All state persisted by the service is maintained in the relational database as described in the respective [datastores](datastore.md) section.

To keep backups of the state, the respective utilities must be scheduled to run in a consistent manner.

## Troubleshooting

Troubleshooting is primarily done through the logging mechanisms that are available and are described in the respective [logging](logging.md) section.
