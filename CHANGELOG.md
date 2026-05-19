# Changelog

### Breaking Changes

- **Dataset status enum values updated**: The `status` field on `sc:Dataset` nodes now uses the enum values shared with downstream services. Existing stored data is not affected, but clients sending the old values must be updated.

### New Features

- **`sc:Dataset` as Operator input / output**: Operators can now be connected to `sc:Dataset` nodes via `input` and `output` edges (whole-dataset reference semantics). Mapping validation is skipped for these edges — the link is treated as `Any` and cannot be type-checked at AP design time. `Data` sub-nodes remain the correct target when a specific distribution is needed.
- **`IntervalColumnStatistics` node**: Added a new node to compute statistics for interval columns in streaming datasets.
- **Column statistics prefix-less convention**: The `Statistics` node now follows the prefix-less naming convention. Added `columnStatistics.schema.json` and updated `mapping.yml` and the mapping engine accordingly.

### Bug Fixes

- **MIME type filtering**: Corrected file-format mapping rules in `filters.py` and `mapping.yml` so that MIME-type filters match the right node labels.
- **Dataset read validation errors returned as 500**: Unexpected `ValidationError` exceptions raised during dataset reads are now surfaced as `500 Internal Server Error` instead of `404 Not Found`.
