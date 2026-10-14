# Changelog

### Breaking Changes

- Analytical patterns: `POST /aps` answers `409 Conflict` when one of the AP's operators, or one of their internal `ResultType` nodes, already belongs to another AP. Nodes are stored by id, so the two APs would share the node. Give the nodes new ids and post again; posting the same AP again is still accepted.
- Analytical patterns: deleting an AP no longer deletes the `Task` nodes linked to it.
- Startup fails when the AP vector index holds vectors of another size than the embedder's, which is what is left after changing `EMBEDDER_MODEL` without re-embedding the stored APs.

### New Features

- Datasets: `GET /datasets` accepts three new metadata filters (datagems-eosc/data-model-management#7). `like` is a free-text search over `name`, `headline`, `keywords` and `description`, backed by a new Neo4j full-text index (`dataset_fulltext`, created at startup): all words must match, each as a case-insensitive prefix, with `*` and `?` wildcards, and results are ordered by relevance unless `orderBy` is set. `license` keeps datasets whose license contains the given text. `fieldsOfScience` keeps datasets having at least one of the given fields of science.
- AP search: using a custom embedder, EmbeddingGemma 2 fine-tuned to match tasks to Analytical Patterns (`packages/analytical-pattern-embedder`).

### Bug Fixes

- Analytical patterns: deleting an AP deleted every AP sharing a node with it, and every AP those shared one with, up to ten edges away. It now deletes only what the AP owns: its root, its evaluations, its operators and their internal `ResultType` nodes, which were never deleted before. Reading an AP no longer walks past another AP's root.
- Startup creates every constraint and index, the AP vector index included. They were created by the first requests, and requests doing so at the same time failed with a 500, some after storing an AP without its embedding.
- Analytical patterns: APs are stored one at a time, so that two requests reusing the same node ids cannot both pass the ownership check.
- Analytical patterns: `DELETE /aps/{id}` returns once the AP is deleted. It could return while the deletion was still running, and a read in between failed on a half-deleted AP.
- Analytical patterns: an AP and its embedding are stored in one transaction, so an interrupted request can no longer leave an AP that search never finds.
- Dataset relationships: `GET /datasets/relationships/{id}` and `GET /datasets/{id}/relationships` no longer fail with `list index out of range` / a `HAS_TARGET` validation error. The repository stripped `HAS_TARGET` edges and the linked `sc:Dataset` nodes from the graph it returned, producing a relationship that could not survive its own model validation. Reads now stop *at* the dataset nodes instead of forbidding the edge, so the datasets come back as reference stubs (id + labels) along with every `HAS_TARGET` edge, and a full subgraph now round-trips without losing edges.
- Dataset relationships: creating a relationship no longer writes the `sc:Dataset` nodes carried in the request body. They are references to existing datasets, so properties or extra labels sent on them are ignored instead of overwriting the stored dataset.
- Authorization: edge labels read back from Neo4j were plain strings rather than `EdgeLabel` members, so `EdgeLabel.input in edge.labels` was always `False` in the `IdType.AP` permission check. That made it resolve zero input datasets and take the "no dataset to check, grant access" branch, granting `BROWSE` on every analytical-pattern endpoint to any authenticated user. Labels are now resolved to their enum member on deserialisation.
- Authorization: an empty resolved dataset list is now denied explicitly instead of being granted by `all([]) == True`.
- Mapping validation: nested output property paths (e.g. `from['outputs']['payload']['query']`) now resolve to the correct leaf type instead of the top-level parameter type, preventing false `mappingTypeCompatibility` errors when an `object` output is partially mapped to a `ResultType` node.

### Performance
