# Changelog

### Breaking Changes


### New Features


### Bug Fixes

- Dataset relationships: `GET /datasets/relationships/{id}` and `GET /datasets/{id}/relationships` no longer fail with `list index out of range` / a `HAS_TARGET` validation error. The repository stripped `HAS_TARGET` edges and the linked `sc:Dataset` nodes from the graph it returned, producing a relationship that could not survive its own model validation. Reads now stop *at* the dataset nodes instead of forbidding the edge, so the datasets come back as reference stubs (id + labels) along with every `HAS_TARGET` edge, and a full subgraph now round-trips without losing edges.
- Dataset relationships: creating a relationship no longer writes the `sc:Dataset` nodes carried in the request body. They are references to existing datasets, so properties or extra labels sent on them are ignored instead of overwriting the stored dataset.
- Authorization: edge labels read back from Neo4j were plain strings rather than `EdgeLabel` members, so `EdgeLabel.input in edge.labels` was always `False` in the `IdType.AP` permission check. That made it resolve zero input datasets and take the "no dataset to check, grant access" branch, granting `BROWSE` on every analytical-pattern endpoint to any authenticated user. Labels are now resolved to their enum member on deserialisation.
- Authorization: an empty resolved dataset list is now denied explicitly instead of being granted by `all([]) == True`.
- Mapping validation: nested output property paths (e.g. `from['outputs']['payload']['query']`) now resolve to the correct leaf type instead of the top-level parameter type, preventing false `mappingTypeCompatibility` errors when an `object` output is partially mapped to a `ResultType` node.

### Performance
