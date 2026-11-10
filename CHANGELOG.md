# Changelog

### Breaking Changes

### New Features

### Bug Fixes

- moma-domain: a typed read (`nodes_of`, `neighbors`, `operators`) dropped the properties a node's schema does not declare, but only for some classes: `Data` and `Operator` lost them, `Table` and `CsvSet` kept them. Every node class now keeps them in `model_extra`, as the JSON Schemas already allow.

### Performance
