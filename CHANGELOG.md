# Changelog

### Breaking Changes


### New Features

- Added `APP_PORT` and `APP_CONCURRENCY` environment variables to configure the server port (default `5000`) and the number of Uvicorn worker processes (default `1`).

### Bug Fixes

- `correlation_id` is now included in error responses even when an unhandled exception is thrown by a middleware.
- Authentication: fixed the client-ID check to use the `azp` claim instead of the wrong claim.

### Performance

- Pydantic validation is now skipped when reading records from Neo4j across all model types (`Dataset`, `AnalyticalPattern`, `Node`, ML Model, Task). Data stored in the database is already well-formed, so re-validating on every read was unnecessary overhead.
- `GET /datasets` list query now fetches all page subgraphs in a single batch Cypher query instead of issuing one `get()` call per dataset. Also fixes a pagination bug where `total` was reported as `0` on pages past the last result.
- Added an in-memory cache for JSON Schema objects to avoid repeated file I/O on every validation call.
- Increased the maximum Neo4j connection pool size to 100.