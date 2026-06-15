# moma-domain

Lightweight Python library exposing the MoMa PG-JSON domain models, validation chain, and mapping engine — without the API service stack.

## Install

**From a GitHub Release (pip-native):**
```bash
pip install "https://github.com/datagems-eosc/moma-management/releases/download/v2.1.0/moma_domain-2.1.0-py3-none-any.whl"
```

**From GHCR OCI artifact (requires [oras](https://oras.land)):**
```bash
oras pull ghcr.io/datagems-eosc/moma-domain:v2.1.0
pip install moma_domain-*.whl
```

**In a uv project:**
```toml
[tool.uv.sources]
moma-domain = { url = "https://github.com/datagems-eosc/moma-management/releases/download/v2.1.0/moma_domain-2.1.0-py3-none-any.whl" }
```

## Usage

```python
from moma_management.domain.dataset import Dataset
from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.domain.filters import DatasetFilter, MimeType
from moma_management.domain.exceptions import ValidationError, ValidationError, NotFoundError
from moma_management.domain.generated.nodes.node_schema import Edge
from moma_management.domain import SCHEMA_DIR, EDGE_CONSTRAINTS_PATH
```
