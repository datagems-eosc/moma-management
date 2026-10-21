# Changelog

### Breaking Changes

### New Features

- Datasets: JSON and JSON Lines record sets (#29). A field read from a JSON file is a `JSONField` (also a `Column`) with its `jsonPath` and `repeated`. Its record set keeps `recordPath`, `jsonShape`, `recordCount`, `maxDepth` and `schemaConsistencyPercentage`. Column statistics gain `topValues` and `jsonTypes`. `application/jsonl` and `application/jsonlines` files are labelled `JSON` / `JSONSet`.

### Bug Fixes

- Datasets: a profile with a JSON record set was rejected, because its fields had no labels and their `statistics` edges failed the edge constraints.

### Performance
