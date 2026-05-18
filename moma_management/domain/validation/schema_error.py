
from typing import Any

from pydantic import BaseModel


class SchemaError(BaseModel):
    """Single validation error in AJV format."""

    keyword: str
    instancePath: str
    schemaPath: str
    params: dict[str, Any]
    message: str
