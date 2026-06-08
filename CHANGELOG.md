# Changelog

### Breaking Changes


### New Features


### Bug Fixes

- Mapping validation: nested output property paths (e.g. `from['outputs']['payload']['query']`) now resolve to the correct leaf type instead of the top-level parameter type, preventing false `mappingTypeCompatibility` errors when an `object` output is partially mapped to a `ResultType` node.

### Performance
