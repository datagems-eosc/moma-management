# Changelog

## [Unreleased]

### ⚠ Breaking Changes

- **Strict unknown properties on AP nodes** — `Operator`, `SQL_Operator`, `Analytical_Pattern`, `Task`, `Evaluation`, and `User` nodes now reject any extra fields in their `properties` object. Payloads with undeclared fields will return a `422` error.

- **Stricter node ID validation** — Every node `id` must now be a well-formed UUID v4 (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`). Previously, some malformed IDs may have passed validation depending on the format checker.

- **Node `properties` content validated by type** — Node `properties` are now validated against the schema for the node's type (inferred from `labels`). Required fields such as `name` on `Operator` and `Analytical_Pattern` nodes are now enforced; invalid property values will return a `422` error.

### New Features

- **Structured input/output for Operators** — `Operator` and `SQL_Operator` nodes now support optional `inputs` and `outputs` arrays in their `properties`. Each entry declares a parameter `name` and `type` (a JSON Schema primitive or a dataset subtype such as `RelationalDatabase`). This lets you describe the exact data contract of each operator step. Alongside this, a new `ResultType` node family (`StringResult`, `NumberResult`, `BooleanResult`, `ArrayResult`, `ObjectResult`) can be added to an AP and linked to operators via `input`/`output` edges to represent transient values flowing between steps.
