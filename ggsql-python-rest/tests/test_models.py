"""Tests for Pydantic models."""

from ggsql_rest._models import (
    QueryRequest,
    SqlRequest,
    SessionResponse,
    UploadResponse,
    TablesResponse,
    QueryMetadata,
    QueryResponse,
    SqlResponse,
    ErrorDetail,
    ErrorResponse,
)


def test_query_request_with_connection():
    req = QueryRequest(query="SELECT * FROM t VISUALISE x, y DRAW point", connection="warehouse")
    assert req.query == "SELECT * FROM t VISUALISE x, y DRAW point"
    assert req.connection == "warehouse"


def test_query_request_without_connection():
    req = QueryRequest(query="SELECT * FROM t VISUALISE x, y DRAW point")
    assert req.connection is None


def test_query_response():
    resp = QueryResponse(
        spec={"mark": "point"},
        metadata=QueryMetadata(rows=10, columns=["x", "y"], layers=1),
    )
    assert resp.spec == {"mark": "point"}
    assert resp.metadata.rows == 10


def test_error_response():
    resp = ErrorResponse(error=ErrorDetail(message="bad query", type="ParseError"))
    assert resp.status == "error"
    assert resp.error.message == "bad query"
