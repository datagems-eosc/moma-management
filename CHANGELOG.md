# Changelog

### Breaking Changes

- Nodes: every `Data` node (`CsvSet`, `Table`, `RelationalDatabase`, `PDFSet`…) is now a `ResultType` and needs a `name`. An upload or AP with a nameless data node is rejected. Checked 2026-11-10: every Data node stored on dev (885) and prod (942) has one.
- moma-domain: each generated properties class is named after its node label (`Table`, `PDFSet`, `SQLOperator`, `Statistics`…) instead of `PgProperties`, and inherits from its parent node's class (`Table(Data)`).
- moma-domain: `AnalyticalPattern.operators` returns typed `Operator` objects, which still expose `id` and `labels`; read properties as attributes (`op.description`), not `op.properties["description"]`.

### New Features

- moma-domain: typed graph traversal. `node_type(labels)` gives the class of a node, `entity.nodes_of(Table)` the typed nodes of a graph, and `entity.neighbors(node, EdgeLabel.input, Data, incoming=True)` the typed neighbours along an edge. The package ships `py.typed`, so pyright and Pylance check code that uses it.
- Nodes: `contentUrl` and the dataset `url` accept relative references (`/s3/…` paths, table names), which they already held, so typed reads no longer fail on them.

### Bug Fixes

### Performance
