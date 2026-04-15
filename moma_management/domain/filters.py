from datetime import date
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field

from moma_management.domain.generated.nodes.dataset.dataset_schema import (
    Dataset,
    Status,
)


class MimeType(str, Enum):
    """Supported MIME types for dataset file objects."""
    EXCEL = "application/vnd.ms-excel"
    JUPYTER = "application/x-ipynb+json"
    DOCX = "application/docx"
    PPTX = "application/pptx"
    PDF = "application/pdf"
    JPEG = "image/jpeg"
    PNG = "image/png"
    CSV = "text/csv"
    SQL = "text/sql"
    SHEET = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    DOCUMENT = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"


MIME_TYPE_TO_NODE_LABEL: dict["MimeType", str] = {
    MimeType.EXCEL:    "EXCEL",
    MimeType.JUPYTER:  "JSONSet",
    MimeType.DOCX:     "DOCXSet",
    MimeType.PPTX:     "PPTXSet",
    MimeType.PDF:      "PDFSet",
    MimeType.JPEG:     "JPEGSet",
    MimeType.PNG:      "PNGSet",
    MimeType.CSV:      "CSV",
    MimeType.SQL:      "Table",
    MimeType.SHEET:    "Sheet",
    MimeType.DOCUMENT: "DocumentSet",
}


class DatasetSortField(str, Enum):
    """Dataset node properties that are available as sort keys."""
    ID = "id"
    NAME = "name"
    DATE_PUBLISHED = "datePublished"
    VERSION = "version"
    STATUS = "status"
    HEADLINE = "headline"
    UPLOADED_BY = "uploadedBy"


class SortDirection(str, Enum):
    """Sort order for dataset list queries."""
    ASC = "asc"
    DESC = "desc"


class NodeLabel(str, Enum):
    """Node labels available for filtering datasets by their connected file nodes."""
    FILE_OBJECT = "cr:FileObject"
    FILE_SET = "cr:FileSet"
    FIELD = "cr:Field"
    TEXT_SET = "TextSet"
    IMAGE_SET = "ImageSet"
    CSV = "CSV"
    TABLE = "Table"
    RELATIONAL_DB = "RelationalDatabase"
    PDF = "PDFSet"
    COLUMN = "Column"


# DatasetProperty is built from Dataset.model_fields so it always stays in sync
# with the schema. Aliases are used as values (e.g. "dg:headline") so they map
# directly to Neo4j property keys. The two special traversal entries trigger
# connected-node expansion rather than returning a scalar property.
_SCALAR_PROPS = {
    python_name.upper(): (field_info.alias or python_name)
    for python_name, field_info in Dataset.model_fields.items()
}
DatasetProperty = Enum(
    "DatasetProperty",
    {**_SCALAR_PROPS, "DISTRIBUTION": "distribution", "RECORD_SET": "recordSet"},
    type=str,
)
DatasetProperty.__doc__ = (
    "Selectable dataset node properties. "
    "DISTRIBUTION and RECORD_SET trigger traversal of connected nodes "
    "rather than returning a scalar property."
)


class DatasetFilter(BaseModel):
    """Filtering, sorting, and pagination criteria for dataset list queries."""

    nodeIds:    List[str] = Field(default_factory=list)
    properties: List[DatasetProperty] = Field(  # pyright: ignore[reportInvalidTypeForm]
        default_factory=list,
        description="Properties to include. Empty list returns all. "
                    "'distribution' and 'recordSet' include connected nodes.",
    )
    types:      List[NodeLabel] = Field(default_factory=list)
    mimeTypes:  List[MimeType] = Field(default_factory=list)
    orderBy:    List[DatasetSortField] = Field(default_factory=list)
    direction:  SortDirection = Field(default=SortDirection.ASC)
    publishedFrom: Optional[date] = None
    publishedTo:   Optional[date] = None
    status:        Optional[Status] = None
    page:     int = Field(default=1,  ge=1)
    pageSize: int = Field(default=10, ge=1, le=100)

    @property
    def resolved_types(self) -> List[str]:
        """Node labels to filter on: explicit types + those derived from mimeTypes."""
        return [t.value for t in self.types] + [MIME_TYPE_TO_NODE_LABEL[mt] for mt in self.mimeTypes]
