# Logging

The MoMa Management API uses Python's standard `logging` module for structured output. All log messages are written to stdout, making them easy to collect and forward in containerized or cloud environments.

## Log levels

The service emits log messages at the following levels:

| Level | Usage |
|---|---|
| `DEBUG` | Detailed diagnostic information (disabled in production by default) |
| `INFO` | Normal operational events (startup, JWKS refresh, etc.) |
| `WARNING` | Non-fatal issues, such as authentication or authorization being disabled due to missing configuration |
| `ERROR` | Errors that prevented a request from completing normally |

Log level can be controlled via standard Python logging configuration or by setting the `LOG_LEVEL` environment variable if configured in the deployment environment.

## What is logged

Key events that produce log entries include:

- **Service startup** – Neo4j driver initialisation and shutdown.
- **JWKS refresh** – When the service fetches or refreshes the public key set from the OIDC issuer; the issuer URL is logged at `INFO`.
- **Auth warnings** – A `WARNING` is emitted at startup when `OIDC_ISSUER` or `PERMISSIONS_GATEWAY_URL` is not set, indicating that authentication or authorization is running in disabled mode.
- **Permission gateway errors** – `ERROR` level when the permission gateway is unreachable.
- **Unhandled exceptions** – `EXCEPTION` (with stack trace) when an unexpected error occurs during token validation.

## Log format

Log entries follow Python's default format:

```
<level>:<logger_name>:<message>
```

For example:

```
INFO:moma_management.di:Refreshing JWKS from https://aai.datagems.eu/realms/datagems/protocol/openid-connect/certs
WARNING:moma_management.di:OIDC_ISSUER not set, authentication disabled
ERROR:moma_management.services.authorization:Permission gateway unreachable: ...
```

In production deployments logs are typically aggregated to the [DataGEMS Logging Service](https://datagems-eosc.github.io/dg-logging-service) where they can be queried and analyzed.
