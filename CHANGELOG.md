# Changelog

### Breaking Changes

- Edges: `containedIn` runs from the item to its container (`Table → RelationalDatabase`, `CSV → CsvSet`, `ColumnTable → Table`), as the README, the Croissant mapping and every stored dataset already have it. The specific rules of `edge_constraints.json` said the opposite. Between two Data nodes both directions are still accepted (generic `Data → Data` rule), but a column's edge to its table must now go `ColumnTable → Table` / `ColumnCSV → CSV`: the reverse is rejected. Checked 2026-11-10: no dataset on dev or prod has a `containedIn` edge on a column.

### New Features

### Bug Fixes

### Performance
