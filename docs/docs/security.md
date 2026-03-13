# Security

Key aspects of the Security checklist and practices that DataGEMS services must pass have been defined in the processes and documents governing the platform development and quality assurance. This section describes the security mechanisms implemented in the MoMa Management API.

## Authentication

All endpoints exposed by this service require authentication using Bearer tokens in the form of JWTs (JSON Web Tokens). Clients must include a valid token in the `Authorization` header of each HTTP request:

```
Authorization: Bearer <token>
```

The service only accepts JWTs issued by a trusted identity provider, the [DataGEMS AAI service](https://github.com/datagems-eosc/dg-aai). When a token is received, the service performs the following validation steps:

1. Verifies the token signature using the public keys fetched from the OIDC issuer's JWKS endpoint (`<OIDC_ISSUER>/protocol/openid-connect/certs`).
2. Checks the `iss` (issuer) claim to confirm it matches the configured `OIDC_ISSUER`.
3. Validates the `aud` (audience) claim against `OIDC_AUDIENCE` if that variable is set.
4. Checks the `exp` (expiration) claim to confirm the token has not expired.

Only RS256-signed tokens are accepted. JWKS are cached in memory for `JWKS_TTL_SECONDS` seconds (default 300) to reduce latency.

> **Note:** Authentication can be disabled for local development by leaving `OIDC_ISSUER` unset. A warning is logged at startup when running in this mode.

The relevant configuration variables are described in the [Configuration](configuration.md) section.

## Authorization

When an authenticated request reaches the service, the caller's permission to perform the requested action is verified by delegating to an **external permissions gateway** (`PERMISSIONS_GATEWAY_URL`). The gateway is queried with the dataset ID and the original Bearer token, and must confirm the action is permitted before the operation proceeds.

The following action verbs are checked per endpoint:

| Action | Endpoints |
|---|---|
| `browse` | `GET /datasets`, `GET /datasets/{id}`, `GET /nodes/{id}`, `POST /datasets/convert` |
| `edit` | `POST /datasets`, `PATCH /nodes/{id}` |
| `delete` | `DELETE /datasets/{id}` |

> **Note:** Authorization can be disabled for local development by leaving `PERMISSIONS_GATEWAY_URL` unset. A warning is logged at startup when running in this mode.

## Secrets

Sensitive configuration values (`NEO4J_PASSWORD`, `OIDC_ISSUER`, etc.) should be supplied via a secrets manager in production rather than plain environment variables or committed `.env` files. See the [Configuration](configuration.md) section for details.
