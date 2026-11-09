# Changelog

### Breaking Changes

### New Features

### Bug Fixes

- Datasets: `GET /datasets/{id}` and `GET /datasets` answered `500` for a dataset holding a list of objects or a list with nulls, such as `DataQualityError.examples` or a column `sample` with missing values. Such lists are stored as JSON text in Neo4j, and these two endpoints returned the text instead of the list.

### Performance
