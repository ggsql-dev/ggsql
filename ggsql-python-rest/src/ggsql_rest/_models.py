"""Pydantic request/response models."""

from pydantic import BaseModel


# === Requests ===


class QueryRequest(BaseModel):
    """Request body for ggsql query execution."""

    query: str
    connection: str | None = None


class SqlRequest(BaseModel):
    """Request body for pure SQL execution."""

    query: str
    connection: str | None = None


# === Responses ===


class SessionResponse(BaseModel):
    """Response for session creation."""

    session_id: str


class UploadResponse(BaseModel):
    """Response for file upload."""

    table_name: str
    row_count: int
    columns: list[str]


class TablesResponse(BaseModel):
    """Response for listing tables."""

    tables: list[str]


class QueryMetadata(BaseModel):
    """Metadata about query execution."""

    rows: int
    columns: list[str]
    layers: int


class QueryResponse(BaseModel):
    """Response for ggsql query execution."""

    spec: dict
    metadata: QueryMetadata


class SqlResponse(BaseModel):
    """Response for pure SQL execution."""

    rows: list[dict]
    columns: list[str]
    row_count: int
    truncated: bool


# === Errors ===


class ErrorDetail(BaseModel):
    """Error details."""

    message: str
    type: str


class ErrorResponse(BaseModel):
    """Error response."""

    status: str = "error"
    error: ErrorDetail
