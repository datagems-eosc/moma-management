# Data Stores

The MoMa Management API uses **Neo4j** as its sole data store. All MoMa property graph data — nodes, edges, and their properties — is persisted there.

## Neo4j graph database

Neo4j is a native property graph database accessed via the official Python driver using the Bolt protocol. The service manages two kinds of graph objects:

- **Nodes** – represent entities in the MoMa model (datasets, distributions, record sets, fields, etc.). Each node carries a set of labels and a properties map.
- **Edges** – directed relationships between nodes, each with a label and optional properties.

The graph schema is formally defined in `moma_management/domain/schema/moma.schema.json` and translated into Pydantic v2 models via `make gen`.

## Connection

The service connects to Neo4j using the Bolt URI, username, and password configured through the `NEO4J_URI`, `NEO4J_USER`, and `NEO4J_PASSWORD` environment variables (see [Configuration](configuration.md)). A single driver instance is created at startup and torn down gracefully on shutdown via the FastAPI lifespan context manager.

## Version requirements

The service requires **Neo4j 5+**. Tested against the `neo4j` Python driver v6.
