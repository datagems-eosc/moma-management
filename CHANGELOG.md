# Changelog

### Breaking Changes

### New Features

- Dataset relationships: file-level linking (#30). A relationship can now carry `BasicDLElement -HAS_DETAIL-> FileObjectLinkingElement -HAS_COMPARISON-> FileObjectComparison -HAS_TARGET-> cr:FileObject`, with the new `FileObjectLinkingElement` (`date`, `similarityScore`, `usesMetrics`) and `FileObjectComparison` (`targetProperty`, `similarityScore`) nodes. Existing relationships are unchanged and need no migration.
  - Linked datasets and files are references: they are never written, and reads return them as `{id, label}` stubs. Send them with empty `properties` (prefixed keys such as `dg:description` or `sc:contentUrl` are rejected).
  - Every linked file must be one of the files MoMa stores for the two linked datasets, and every `FileObjectComparison` must compare files of both datasets.
  - `sc:Dataset -distribution-> cr:FileObject` edges are optional in the POST payload: they must match what MoMa stores and are never written. Reads return the stored ones for the linked files, so a reader knows which dataset each file belongs to. Any other edge starting from a linked dataset or file is rejected ("A dataset relationship cannot modify datasets").
  - Deleting a dataset also deletes the file-level part of its relationships.

### Bug Fixes

- Dataset relationships: the id of a node inside a dataset (a file, a column) could pass as a linked dataset when creating a relationship. Only `sc:Dataset` nodes count now.

### Performance
