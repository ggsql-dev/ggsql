# ggsql-python-rest Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a Python REST server for ggsql with SQLAlchemy database backend support and hybrid execution (SQL remote, VISUALISE local).

**Architecture:** FastAPI server wrapping ggsql-python. ConnectionRegistry maps named connections to request-aware factory functions. SessionManager provides isolated DuckDB instances per client. Hybrid execution runs SQL on remote databases, registers results locally, and processes VISUALISE in DuckDB.

**Tech Stack:** FastAPI, SQLAlchemy, ggsql (Python bindings), Polars, Pydantic

---

### Task 1: Project Setup

**Files:**
- Create: `ggsql-python-rest/pyproject.toml`
- Create: `ggsql-python-rest/src/ggsql_rest/__init__.py`

**Step 1: Create pyproject.toml**

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "ggsql-rest"
version = "0.1.0"
description = "REST API server for ggsql with SQLAlchemy backend support"
readme = "README.md"
requires-python = ">=3.10"
license = { text = "MIT" }
dependencies = [
    "fastapi>=0.100",
    "uvicorn>=0.20",
    "ggsql>=0.1.0",
    "sqlalchemy>=2.0",
    "polars>=1.0",
    "python-multipart>=0.0.6",
]

[project.optional-dependencies]
test = ["pytest>=7.0", "httpx>=0.24"]
dev = ["ruff>=0.1"]

[tool.hatch.build.targets.wheel]
packages = ["src/ggsql_rest"]
```

**Step 2: Create __init__.py stub**

```python
"""ggsql REST API server with SQLAlchemy backend support."""

__version__ = "0.1.0"
```

**Step 3: Verify structure**

Run: `ls -la ggsql-python-rest/src/ggsql_rest/`
Expected: `__init__.py` exists

**Step 4: Commit**

```bash
git add ggsql-python-rest/
git commit -m "chore: initialize ggsql-python-rest package structure"
```

---

### Task 2: Pydantic Models

**Files:**
- Create: `ggsql-python-rest/src/ggsql_rest/_models.py`
- Test: `ggsql-python-rest/tests/test_models.py`

**Step 1: Write the failing test**

```python
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
```

**Step 2: Run test to verify it fails**

Run: `cd ggsql-python-rest && python -m pytest tests/test_models.py -v`
Expected: FAIL with import error

**Step 3: Write implementation**

```python
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
```

**Step 4: Run test to verify it passes**

Run: `cd ggsql-python-rest && python -m pytest tests/test_models.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add ggsql-python-rest/src/ggsql_rest/_models.py ggsql-python-rest/tests/test_models.py
git commit -m "feat: add Pydantic request/response models"
```

---

### Task 3: Error Handling

**Files:**
- Create: `ggsql-python-rest/src/ggsql_rest/_errors.py`
- Test: `ggsql-python-rest/tests/test_errors.py`

**Step 1: Write the failing test**

```python
"""Tests for error handling."""

from ggsql_rest._errors import ApiError, session_not_found, connection_not_found


def test_api_error():
    err = ApiError(404, "NotFound", "Resource not found")
    assert err.status_code == 404
    assert err.error_type == "NotFound"
    assert err.message == "Resource not found"


def test_session_not_found():
    err = session_not_found("abc123")
    assert err.status_code == 404
    assert err.error_type == "SessionNotFound"
    assert "abc123" in err.message


def test_connection_not_found():
    err = connection_not_found("warehouse")
    assert err.status_code == 400
    assert err.error_type == "ConnectionNotFound"
    assert "warehouse" in err.message
```

**Step 2: Run test to verify it fails**

Run: `cd ggsql-python-rest && python -m pytest tests/test_errors.py -v`
Expected: FAIL with import error

**Step 3: Write implementation**

```python
"""Error handling utilities."""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


class ApiError(Exception):
    """Custom API error with HTTP status code."""

    def __init__(self, status_code: int, error_type: str, message: str):
        self.status_code = status_code
        self.error_type = error_type
        self.message = message
        super().__init__(message)


def session_not_found(session_id: str) -> ApiError:
    """Create a session not found error."""
    return ApiError(404, "SessionNotFound", f"Session '{session_id}' not found")


def connection_not_found(name: str) -> ApiError:
    """Create a connection not found error."""
    return ApiError(400, "ConnectionNotFound", f"Unknown connection: '{name}'")


def register_error_handlers(app: FastAPI) -> None:
    """Register error handlers on the FastAPI app."""

    @app.exception_handler(ApiError)
    async def handle_api_error(request: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "status": "error",
                "error": {"type": exc.error_type, "message": exc.message},
            },
        )
```

**Step 4: Run test to verify it passes**

Run: `cd ggsql-python-rest && python -m pytest tests/test_errors.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add ggsql-python-rest/src/ggsql_rest/_errors.py ggsql-python-rest/tests/test_errors.py
git commit -m "feat: add error handling utilities"
```

---

### Task 4: Session Manager

**Files:**
- Create: `ggsql-python-rest/src/ggsql_rest/_sessions.py`
- Test: `ggsql-python-rest/tests/test_sessions.py`

**Step 1: Write the failing test**

```python
"""Tests for session management."""

from datetime import timedelta
from ggsql_rest._sessions import Session, SessionManager


def test_session_creation():
    session = Session("test123", timeout_mins=30)
    assert session.id == "test123"
    assert session.tables == []
    assert not session.is_expired()


def test_session_touch():
    session = Session("test123", timeout_mins=30)
    first_access = session.last_accessed
    session.touch()
    assert session.last_accessed >= first_access


def test_session_expiry():
    session = Session("test123", timeout_mins=0)
    # With 0 timeout, session expires immediately
    assert session.is_expired()


def test_session_manager_create():
    mgr = SessionManager(timeout_mins=30)
    session = mgr.create()
    assert session.id is not None
    assert len(session.id) == 32  # uuid hex


def test_session_manager_get():
    mgr = SessionManager(timeout_mins=30)
    session = mgr.create()
    retrieved = mgr.get(session.id)
    assert retrieved is not None
    assert retrieved.id == session.id


def test_session_manager_get_nonexistent():
    mgr = SessionManager(timeout_mins=30)
    assert mgr.get("nonexistent") is None


def test_session_manager_delete():
    mgr = SessionManager(timeout_mins=30)
    session = mgr.create()
    assert mgr.delete(session.id) is True
    assert mgr.get(session.id) is None


def test_session_manager_delete_nonexistent():
    mgr = SessionManager(timeout_mins=30)
    assert mgr.delete("nonexistent") is False


def test_session_manager_cleanup_expired():
    mgr = SessionManager(timeout_mins=0)  # Immediate expiry
    session = mgr.create()
    session_id = session.id
    mgr.cleanup_expired()
    assert mgr.get(session_id) is None
```

**Step 2: Run test to verify it fails**

Run: `cd ggsql-python-rest && python -m pytest tests/test_sessions.py -v`
Expected: FAIL with import error

**Step 3: Write implementation**

```python
"""Session management for isolated DuckDB instances."""

import uuid
from datetime import datetime, timedelta

from ggsql import DuckDBReader


class Session:
    """A user session with an isolated DuckDB instance."""

    def __init__(self, session_id: str, timeout_mins: int = 30):
        self.id = session_id
        self.created_at = datetime.now()
        self.last_accessed = datetime.now()
        self.timeout = timedelta(minutes=timeout_mins)
        self.duckdb = DuckDBReader("duckdb://memory")
        self.tables: list[str] = []

    def touch(self) -> None:
        """Update last accessed time."""
        self.last_accessed = datetime.now()

    def is_expired(self) -> bool:
        """Check if session has expired."""
        return datetime.now() - self.last_accessed > self.timeout


class SessionManager:
    """Manages user sessions."""

    def __init__(self, timeout_mins: int = 30):
        self._sessions: dict[str, Session] = {}
        self._timeout_mins = timeout_mins

    def create(self) -> Session:
        """Create a new session."""
        session_id = uuid.uuid4().hex
        session = Session(session_id, self._timeout_mins)
        self._sessions[session_id] = session
        return session

    def get(self, session_id: str) -> Session | None:
        """Get a session by ID, or None if not found or expired."""
        session = self._sessions.get(session_id)
        if session is None:
            return None
        if session.is_expired():
            del self._sessions[session_id]
            return None
        session.touch()
        return session

    def delete(self, session_id: str) -> bool:
        """Delete a session. Returns True if deleted, False if not found."""
        return self._sessions.pop(session_id, None) is not None

    def cleanup_expired(self) -> None:
        """Remove all expired sessions."""
        expired = [sid for sid, s in self._sessions.items() if s.is_expired()]
        for sid in expired:
            del self._sessions[sid]
```

**Step 4: Run test to verify it passes**

Run: `cd ggsql-python-rest && python -m pytest tests/test_sessions.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add ggsql-python-rest/src/ggsql_rest/_sessions.py ggsql-python-rest/tests/test_sessions.py
git commit -m "feat: add session management"
```

---

### Task 5: Connection Registry

**Files:**
- Create: `ggsql-python-rest/src/ggsql_rest/_connections.py`
- Test: `ggsql-python-rest/tests/test_connections.py`

**Step 1: Write the failing test**

```python
"""Tests for connection registry."""

import pytest
from unittest.mock import MagicMock
from sqlalchemy import create_engine

from ggsql_rest._connections import ConnectionRegistry


def test_register_and_list():
    registry = ConnectionRegistry()
    registry.register("test", lambda req: create_engine("sqlite:///:memory:"))
    assert "test" in registry.list_connections()


def test_get_engine():
    registry = ConnectionRegistry()
    engine = create_engine("sqlite:///:memory:")
    registry.register("test", lambda req: engine)

    mock_request = MagicMock()
    mock_request.headers = {}

    result = registry.get_engine("test", mock_request)
    assert result is engine


def test_get_engine_caches_by_user():
    registry = ConnectionRegistry()
    call_count = 0

    def factory(req):
        nonlocal call_count
        call_count += 1
        return create_engine("sqlite:///:memory:")

    registry.register("test", factory)

    mock_request = MagicMock()
    mock_request.headers = {"X-User-Id": "user1"}

    # First call creates engine
    registry.get_engine("test", mock_request)
    assert call_count == 1

    # Second call with same user returns cached
    registry.get_engine("test", mock_request)
    assert call_count == 1

    # Different user creates new engine
    mock_request.headers = {"X-User-Id": "user2"}
    registry.get_engine("test", mock_request)
    assert call_count == 2


def test_get_engine_unknown():
    registry = ConnectionRegistry()
    mock_request = MagicMock()
    mock_request.headers = {}

    with pytest.raises(KeyError, match="Unknown connection"):
        registry.get_engine("nonexistent", mock_request)


def test_extract_user_id():
    registry = ConnectionRegistry()

    mock_request = MagicMock()
    mock_request.headers = {"X-User-Id": "user123"}
    assert registry.extract_user_id(mock_request) == "user123"

    mock_request.headers = {}
    assert registry.extract_user_id(mock_request) == "anonymous"
```

**Step 2: Run test to verify it fails**

Run: `cd ggsql-python-rest && python -m pytest tests/test_connections.py -v`
Expected: FAIL with import error

**Step 3: Write implementation**

```python
"""Connection registry for named database connections."""

from typing import Callable

from fastapi import Request
from sqlalchemy import Engine


class ConnectionRegistry:
    """Registry for named database connections with request-aware factories."""

    def __init__(self):
        self._factories: dict[str, Callable[[Request], Engine]] = {}
        self._engines: dict[tuple[str, str], Engine] = {}

    def register(self, name: str, factory: Callable[[Request], Engine]) -> None:
        """Register a named connection factory."""
        self._factories[name] = factory

    def get_engine(self, name: str, request: Request) -> Engine:
        """Get or create a cached engine by name and user."""
        if name not in self._factories:
            raise KeyError(f"Unknown connection: '{name}'")

        user_id = self.extract_user_id(request)
        cache_key = (name, user_id)

        if cache_key not in self._engines:
            self._engines[cache_key] = self._factories[name](request)
        return self._engines[cache_key]

    def extract_user_id(self, request: Request) -> str:
        """Extract user ID from request headers."""
        return request.headers.get("X-User-Id", "anonymous")

    def list_connections(self) -> list[str]:
        """List available connection names."""
        return list(self._factories.keys())
```

**Step 4: Run test to verify it passes**

Run: `cd ggsql-python-rest && python -m pytest tests/test_connections.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add ggsql-python-rest/src/ggsql_rest/_connections.py ggsql-python-rest/tests/test_connections.py
git commit -m "feat: add connection registry"
```

---

### Task 6: Health Route

**Files:**
- Create: `ggsql-python-rest/src/ggsql_rest/_routes/__init__.py`
- Create: `ggsql-python-rest/src/ggsql_rest/_routes/_health.py`
- Test: `ggsql-python-rest/tests/test_routes_health.py`

**Step 1: Write the failing test**

```python
"""Tests for health routes."""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from ggsql_rest._routes._health import router


def test_health():
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)

    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
```

**Step 2: Run test to verify it fails**

Run: `cd ggsql-python-rest && python -m pytest tests/test_routes_health.py -v`
Expected: FAIL with import error

**Step 3: Write implementation**

```python
# _routes/__init__.py
"""Route modules."""
```

```python
# _routes/_health.py
"""Health check routes."""

from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> dict:
    """Health check endpoint."""
    return {"status": "ok"}
```

**Step 4: Run test to verify it passes**

Run: `cd ggsql-python-rest && python -m pytest tests/test_routes_health.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add ggsql-python-rest/src/ggsql_rest/_routes/
git add ggsql-python-rest/tests/test_routes_health.py
git commit -m "feat: add health check route"
```

---

### Task 7: Session Routes

**Files:**
- Create: `ggsql-python-rest/src/ggsql_rest/_routes/_sessions.py`
- Test: `ggsql-python-rest/tests/test_routes_sessions.py`

**Step 1: Write the failing test**

```python
"""Tests for session routes."""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from ggsql_rest._sessions import SessionManager
from ggsql_rest._routes._sessions import router, get_session_manager


def create_test_app() -> tuple[FastAPI, SessionManager]:
    app = FastAPI()
    session_mgr = SessionManager(timeout_mins=30)

    app.dependency_overrides[get_session_manager] = lambda: session_mgr
    app.include_router(router)

    return app, session_mgr


def test_create_session():
    app, _ = create_test_app()
    client = TestClient(app)

    response = client.post("/sessions")
    assert response.status_code == 200
    data = response.json()
    assert "session_id" in data
    assert len(data["session_id"]) == 32


def test_delete_session():
    app, session_mgr = create_test_app()
    client = TestClient(app)

    # Create a session first
    session = session_mgr.create()

    response = client.delete(f"/sessions/{session.id}")
    assert response.status_code == 200
    assert response.json() == {"status": "deleted"}


def test_delete_session_not_found():
    app, _ = create_test_app()
    client = TestClient(app)

    response = client.delete("/sessions/nonexistent")
    assert response.status_code == 404


def test_list_tables_empty():
    app, session_mgr = create_test_app()
    client = TestClient(app)

    session = session_mgr.create()

    response = client.get(f"/sessions/{session.id}/tables")
    assert response.status_code == 200
    assert response.json() == {"tables": []}


def test_list_tables_not_found():
    app, _ = create_test_app()
    client = TestClient(app)

    response = client.get("/sessions/nonexistent/tables")
    assert response.status_code == 404
```

**Step 2: Run test to verify it fails**

Run: `cd ggsql-python-rest && python -m pytest tests/test_routes_sessions.py -v`
Expected: FAIL with import error

**Step 3: Write implementation**

```python
"""Session management routes."""

from fastapi import APIRouter, Depends, HTTPException

from .._models import SessionResponse, TablesResponse
from .._sessions import Session, SessionManager

router = APIRouter(prefix="/sessions", tags=["sessions"])

# Dependency placeholder - will be overridden by app factory
_session_manager: SessionManager | None = None


def get_session_manager() -> SessionManager:
    """Get the session manager instance."""
    if _session_manager is None:
        raise RuntimeError("SessionManager not initialized")
    return _session_manager


def get_session(
    session_id: str,
    session_mgr: SessionManager = Depends(get_session_manager),
) -> Session:
    """Get a session by ID or raise 404."""
    session = session_mgr.get(session_id)
    if session is None:
        raise HTTPException(404, f"Session '{session_id}' not found")
    return session


@router.post("", response_model=SessionResponse)
def create_session(
    session_mgr: SessionManager = Depends(get_session_manager),
) -> SessionResponse:
    """Create a new session."""
    session = session_mgr.create()
    return SessionResponse(session_id=session.id)


@router.delete("/{session_id}")
def delete_session(
    session_id: str,
    session_mgr: SessionManager = Depends(get_session_manager),
) -> dict:
    """Delete a session."""
    if not session_mgr.delete(session_id):
        raise HTTPException(404, f"Session '{session_id}' not found")
    return {"status": "deleted"}


@router.get("/{session_id}/tables", response_model=TablesResponse)
def list_tables(session: Session = Depends(get_session)) -> TablesResponse:
    """List tables available in a session."""
    return TablesResponse(tables=session.tables)
```

**Step 4: Run test to verify it passes**

Run: `cd ggsql-python-rest && python -m pytest tests/test_routes_sessions.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add ggsql-python-rest/src/ggsql_rest/_routes/_sessions.py
git add ggsql-python-rest/tests/test_routes_sessions.py
git commit -m "feat: add session routes"
```

---

### Task 8: Query Execution Core

**Files:**
- Create: `ggsql-python-rest/src/ggsql_rest/_query.py`
- Test: `ggsql-python-rest/tests/test_query.py`

**Step 1: Write the failing test**

```python
"""Tests for query execution."""

import pytest
from ggsql_rest._sessions import Session
from ggsql_rest._query import execute_ggsql


def test_execute_ggsql_local():
    """Test executing a ggsql query against local DuckDB."""
    session = Session("test", timeout_mins=30)

    # Create test data in session's DuckDB
    session.duckdb.execute_sql(
        "CREATE TABLE test AS SELECT 1 as x, 2 as y UNION SELECT 3, 4"
    )

    result = execute_ggsql(
        "SELECT * FROM test VISUALISE x, y DRAW point",
        session,
        engine=None,
    )

    assert "spec" in result
    assert "metadata" in result
    assert result["metadata"]["rows"] == 2
    assert "x" in result["metadata"]["columns"]
    assert "y" in result["metadata"]["columns"]


def test_execute_ggsql_no_visualise():
    """Test that queries without VISUALISE raise an error."""
    session = Session("test", timeout_mins=30)

    with pytest.raises(ValueError, match="VISUALISE"):
        execute_ggsql("SELECT 1 as x", session, engine=None)
```

**Step 2: Run test to verify it fails**

Run: `cd ggsql-python-rest && python -m pytest tests/test_query.py -v`
Expected: FAIL with import error

**Step 3: Write implementation**

```python
"""Query execution with hybrid local/remote support."""

import json
import uuid
from typing import Any

import polars as pl
from sqlalchemy import Engine, text

from ggsql import validate, VegaLiteWriter

from ._sessions import Session


def execute_ggsql(
    query: str,
    session: Session,
    engine: Engine | None = None,
) -> dict[str, Any]:
    """
    Execute a ggsql query with hybrid approach.

    If engine is provided, SQL portion runs on remote database,
    result is registered in session's DuckDB, and VISUALISE
    portion runs locally.
    """
    validated = validate(query)

    if not validated.has_visual():
        raise ValueError("Query must contain VISUALISE clause")

    sql_portion = validated.sql()

    if engine is not None and sql_portion.strip():
        # Execute SQL on remote database
        df = execute_remote(engine, sql_portion)

        # Register result in session's DuckDB
        table_name = f"__remote_result_{uuid.uuid4().hex[:8]}__"
        session.duckdb.register(table_name, df)

        # Rewrite query to use local table
        local_query = f"SELECT * FROM {table_name} {validated.visual()}"
    else:
        # All local
        local_query = query

    # Execute full ggsql in session's DuckDB
    spec = session.duckdb.execute(local_query)

    writer = VegaLiteWriter()
    vegalite_json = writer.render(spec)

    return {
        "spec": json.loads(vegalite_json),
        "metadata": {
            "rows": spec.metadata()["rows"],
            "columns": spec.metadata()["columns"],
            "layers": spec.metadata()["layer_count"],
        },
    }


def execute_remote(engine: Engine, sql: str) -> pl.DataFrame:
    """Execute SQL on remote database, return as Polars DataFrame."""
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        columns = list(result.keys())
        rows = result.fetchall()

        # Convert to dict of lists for Polars
        data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
        return pl.DataFrame(data)


def execute_sql(
    query: str,
    session: Session,
    engine: Engine | None = None,
    max_rows: int = 10000,
) -> dict[str, Any]:
    """Execute pure SQL query and return results as JSON."""
    if engine is not None:
        df = execute_remote(engine, query)
    else:
        df = session.duckdb.execute_sql(query)

    row_count = len(df)
    truncated = row_count > max_rows

    if truncated:
        df = df.head(max_rows)

    return {
        "rows": df.to_dicts(),
        "columns": df.columns,
        "row_count": row_count,
        "truncated": truncated,
    }
```

**Step 4: Run test to verify it passes**

Run: `cd ggsql-python-rest && python -m pytest tests/test_query.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add ggsql-python-rest/src/ggsql_rest/_query.py ggsql-python-rest/tests/test_query.py
git commit -m "feat: add query execution with hybrid support"
```

---

### Task 9: Query Routes

**Files:**
- Create: `ggsql-python-rest/src/ggsql_rest/_routes/_query.py`
- Test: `ggsql-python-rest/tests/test_routes_query.py`

**Step 1: Write the failing test**

```python
"""Tests for query routes."""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from ggsql_rest._sessions import SessionManager
from ggsql_rest._connections import ConnectionRegistry
from ggsql_rest._routes._sessions import router as sessions_router, get_session_manager
from ggsql_rest._routes._query import router as query_router, get_registry


def create_test_app() -> tuple[FastAPI, SessionManager, ConnectionRegistry]:
    app = FastAPI()
    session_mgr = SessionManager(timeout_mins=30)
    registry = ConnectionRegistry()

    app.dependency_overrides[get_session_manager] = lambda: session_mgr
    app.dependency_overrides[get_registry] = lambda: registry
    app.include_router(sessions_router)
    app.include_router(query_router)

    return app, session_mgr, registry


def test_execute_query_local():
    app, session_mgr, _ = create_test_app()
    client = TestClient(app)

    # Create session and add test data
    session = session_mgr.create()
    session.duckdb.execute_sql(
        "CREATE TABLE test AS SELECT 1 as x, 2 as y UNION SELECT 3, 4"
    )

    response = client.post(
        f"/sessions/{session.id}/query",
        json={"query": "SELECT * FROM test VISUALISE x, y DRAW point"},
    )

    assert response.status_code == 200
    data = response.json()
    assert "spec" in data
    assert "metadata" in data


def test_execute_query_session_not_found():
    app, _, _ = create_test_app()
    client = TestClient(app)

    response = client.post(
        "/sessions/nonexistent/query",
        json={"query": "SELECT * FROM test VISUALISE x, y DRAW point"},
    )

    assert response.status_code == 404


def test_execute_sql_local():
    app, session_mgr, _ = create_test_app()
    client = TestClient(app)

    session = session_mgr.create()
    session.duckdb.execute_sql(
        "CREATE TABLE test AS SELECT 1 as x, 2 as y UNION SELECT 3, 4"
    )

    response = client.post(
        f"/sessions/{session.id}/sql",
        json={"query": "SELECT * FROM test"},
    )

    assert response.status_code == 200
    data = response.json()
    assert "rows" in data
    assert "columns" in data
    assert len(data["rows"]) == 2
```

**Step 2: Run test to verify it fails**

Run: `cd ggsql-python-rest && python -m pytest tests/test_routes_query.py -v`
Expected: FAIL with import error

**Step 3: Write implementation**

```python
"""Query execution routes."""

from fastapi import APIRouter, Depends, Request

from .._models import QueryRequest, QueryResponse, QueryMetadata, SqlRequest, SqlResponse
from .._connections import ConnectionRegistry
from .._sessions import Session
from .._query import execute_ggsql, execute_sql
from ._sessions import get_session, get_session_manager

router = APIRouter(prefix="/sessions/{session_id}", tags=["query"])

# Dependency placeholder - will be overridden by app factory
_registry: ConnectionRegistry | None = None


def get_registry() -> ConnectionRegistry:
    """Get the connection registry instance."""
    if _registry is None:
        raise RuntimeError("ConnectionRegistry not initialized")
    return _registry


@router.post("/query", response_model=QueryResponse)
def query(
    request: Request,
    body: QueryRequest,
    session: Session = Depends(get_session),
    registry: ConnectionRegistry = Depends(get_registry),
) -> QueryResponse:
    """Execute a ggsql query."""
    engine = None
    if body.connection:
        engine = registry.get_engine(body.connection, request)

    result = execute_ggsql(body.query, session, engine)

    return QueryResponse(
        spec=result["spec"],
        metadata=QueryMetadata(**result["metadata"]),
    )


@router.post("/sql", response_model=SqlResponse)
def sql(
    request: Request,
    body: SqlRequest,
    session: Session = Depends(get_session),
    registry: ConnectionRegistry = Depends(get_registry),
) -> SqlResponse:
    """Execute a pure SQL query."""
    engine = None
    if body.connection:
        engine = registry.get_engine(body.connection, request)

    result = execute_sql(body.query, session, engine)

    return SqlResponse(**result)
```

**Step 4: Run test to verify it passes**

Run: `cd ggsql-python-rest && python -m pytest tests/test_routes_query.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add ggsql-python-rest/src/ggsql_rest/_routes/_query.py
git add ggsql-python-rest/tests/test_routes_query.py
git commit -m "feat: add query routes"
```

---

### Task 10: Upload Route

**Files:**
- Modify: `ggsql-python-rest/src/ggsql_rest/_routes/_sessions.py`
- Test: `ggsql-python-rest/tests/test_routes_upload.py`

**Step 1: Write the failing test**

```python
"""Tests for file upload route."""

import io
from fastapi import FastAPI
from fastapi.testclient import TestClient

from ggsql_rest._sessions import SessionManager
from ggsql_rest._routes._sessions import router, get_session_manager


def create_test_app() -> tuple[FastAPI, SessionManager]:
    app = FastAPI()
    session_mgr = SessionManager(timeout_mins=30)

    app.dependency_overrides[get_session_manager] = lambda: session_mgr
    app.include_router(router)

    return app, session_mgr


def test_upload_csv():
    app, session_mgr = create_test_app()
    client = TestClient(app)

    session = session_mgr.create()

    csv_content = b"x,y\n1,2\n3,4\n5,6"
    files = {"file": ("data.csv", io.BytesIO(csv_content), "text/csv")}

    response = client.post(f"/sessions/{session.id}/upload", files=files)

    assert response.status_code == 200
    data = response.json()
    assert data["table_name"] == "data"
    assert data["row_count"] == 3
    assert "x" in data["columns"]
    assert "y" in data["columns"]

    # Verify table is in session
    assert "data" in session.tables


def test_upload_session_not_found():
    app, _ = create_test_app()
    client = TestClient(app)

    csv_content = b"x,y\n1,2"
    files = {"file": ("data.csv", io.BytesIO(csv_content), "text/csv")}

    response = client.post("/sessions/nonexistent/upload", files=files)

    assert response.status_code == 404
```

**Step 2: Run test to verify it fails**

Run: `cd ggsql-python-rest && python -m pytest tests/test_routes_upload.py -v`
Expected: FAIL (upload route not implemented)

**Step 3: Update implementation**

Add to `_routes/_sessions.py`:

```python
"""Session management routes."""

import io
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, UploadFile
import polars as pl

from .._models import SessionResponse, TablesResponse, UploadResponse
from .._sessions import Session, SessionManager

router = APIRouter(prefix="/sessions", tags=["sessions"])

# Dependency placeholder - will be overridden by app factory
_session_manager: SessionManager | None = None


def get_session_manager() -> SessionManager:
    """Get the session manager instance."""
    if _session_manager is None:
        raise RuntimeError("SessionManager not initialized")
    return _session_manager


def get_session(
    session_id: str,
    session_mgr: SessionManager = Depends(get_session_manager),
) -> Session:
    """Get a session by ID or raise 404."""
    session = session_mgr.get(session_id)
    if session is None:
        raise HTTPException(404, f"Session '{session_id}' not found")
    return session


@router.post("", response_model=SessionResponse)
def create_session(
    session_mgr: SessionManager = Depends(get_session_manager),
) -> SessionResponse:
    """Create a new session."""
    session = session_mgr.create()
    return SessionResponse(session_id=session.id)


@router.delete("/{session_id}")
def delete_session(
    session_id: str,
    session_mgr: SessionManager = Depends(get_session_manager),
) -> dict:
    """Delete a session."""
    if not session_mgr.delete(session_id):
        raise HTTPException(404, f"Session '{session_id}' not found")
    return {"status": "deleted"}


@router.get("/{session_id}/tables", response_model=TablesResponse)
def list_tables(session: Session = Depends(get_session)) -> TablesResponse:
    """List tables available in a session."""
    return TablesResponse(tables=session.tables)


@router.post("/{session_id}/upload", response_model=UploadResponse)
async def upload_file(
    file: UploadFile,
    session: Session = Depends(get_session),
) -> UploadResponse:
    """Upload a file to the session's DuckDB instance."""
    if file.filename is None:
        raise HTTPException(400, "Filename is required")

    # Derive table name from filename
    table_name = Path(file.filename).stem.replace("-", "_").replace(" ", "_")

    # Read file content
    content = await file.read()
    extension = Path(file.filename).suffix.lower()

    # Parse based on extension
    if extension == ".csv":
        df = pl.read_csv(io.BytesIO(content))
    elif extension == ".parquet":
        df = pl.read_parquet(io.BytesIO(content))
    elif extension in (".json", ".jsonl", ".ndjson"):
        df = pl.read_json(io.BytesIO(content))
    else:
        raise HTTPException(400, f"Unsupported file format: {extension}")

    # Register in session's DuckDB
    session.duckdb.register(table_name, df)
    session.tables.append(table_name)

    return UploadResponse(
        table_name=table_name,
        row_count=len(df),
        columns=df.columns,
    )
```

**Step 4: Run test to verify it passes**

Run: `cd ggsql-python-rest && python -m pytest tests/test_routes_upload.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add ggsql-python-rest/src/ggsql_rest/_routes/_sessions.py
git add ggsql-python-rest/tests/test_routes_upload.py
git commit -m "feat: add file upload route"
```

---

### Task 11: App Factory

**Files:**
- Create: `ggsql-python-rest/src/ggsql_rest/_app.py`
- Modify: `ggsql-python-rest/src/ggsql_rest/__init__.py`
- Test: `ggsql-python-rest/tests/test_app.py`

**Step 1: Write the failing test**

```python
"""Tests for app factory."""

from fastapi.testclient import TestClient

from ggsql_rest import create_app, ConnectionRegistry


def test_create_app():
    registry = ConnectionRegistry()
    app = create_app(registry)

    client = TestClient(app)

    # Health check should work
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_create_app_with_cors():
    registry = ConnectionRegistry()
    app = create_app(registry, cors_origins=["http://localhost:3000"])

    # CORS headers should be present
    client = TestClient(app)
    response = client.options(
        "/health",
        headers={"Origin": "http://localhost:3000"},
    )
    assert "access-control-allow-origin" in response.headers


def test_full_workflow():
    registry = ConnectionRegistry()
    app = create_app(registry)
    client = TestClient(app)

    # Create session
    response = client.post("/sessions")
    assert response.status_code == 200
    session_id = response.json()["session_id"]

    # Upload data
    csv_content = b"x,y\n1,10\n2,20\n3,30"
    response = client.post(
        f"/sessions/{session_id}/upload",
        files={"file": ("test.csv", csv_content, "text/csv")},
    )
    assert response.status_code == 200

    # Query
    response = client.post(
        f"/sessions/{session_id}/query",
        json={"query": "SELECT * FROM test VISUALISE x, y DRAW point"},
    )
    assert response.status_code == 200
    assert "spec" in response.json()

    # Delete session
    response = client.delete(f"/sessions/{session_id}")
    assert response.status_code == 200
```

**Step 2: Run test to verify it fails**

Run: `cd ggsql-python-rest && python -m pytest tests/test_app.py -v`
Expected: FAIL with import error

**Step 3: Write implementation**

```python
# _app.py
"""FastAPI application factory."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from ._connections import ConnectionRegistry
from ._sessions import SessionManager
from ._errors import register_error_handlers
from ._routes import _health, _sessions, _query


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan handler."""
    # Startup
    yield
    # Shutdown - cleanup could go here


def create_app(
    registry: ConnectionRegistry,
    session_timeout_mins: int = 30,
    cors_origins: list[str] | None = None,
) -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="ggsql REST API",
        description="REST API server for ggsql with SQLAlchemy backend support",
        lifespan=lifespan,
    )

    # Initialize shared state
    session_manager = SessionManager(session_timeout_mins)

    # Set up dependency overrides
    app.dependency_overrides[_sessions.get_session_manager] = lambda: session_manager
    app.dependency_overrides[_query.get_registry] = lambda: registry

    # CORS (consumer configurable)
    if cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    # Register error handlers
    register_error_handlers(app)

    # Register routes
    app.include_router(_health.router)
    app.include_router(_sessions.router)
    app.include_router(_query.router)

    return app
```

```python
# __init__.py
"""ggsql REST API server with SQLAlchemy backend support."""

from ._app import create_app
from ._connections import ConnectionRegistry

__version__ = "0.1.0"
__all__ = ["create_app", "ConnectionRegistry"]
```

**Step 4: Run test to verify it passes**

Run: `cd ggsql-python-rest && python -m pytest tests/test_app.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add ggsql-python-rest/src/ggsql_rest/_app.py
git add ggsql-python-rest/src/ggsql_rest/__init__.py
git add ggsql-python-rest/tests/test_app.py
git commit -m "feat: add app factory and complete public API"
```

---

### Task 12: Final Integration Test

**Files:**
- Create: `ggsql-python-rest/tests/test_integration.py`

**Step 1: Write integration test**

```python
"""Integration tests with SQLAlchemy backend."""

from unittest.mock import MagicMock

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from ggsql_rest import create_app, ConnectionRegistry


def test_remote_query_with_sqlite():
    """Test hybrid execution with SQLite as remote database."""
    # Set up SQLite as "remote" database
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE sales (x INTEGER, y INTEGER)"))
        conn.execute(text("INSERT INTO sales VALUES (1, 10), (2, 20), (3, 30)"))
        conn.commit()

    # Set up app with connection registry
    registry = ConnectionRegistry()
    registry.register("test_db", lambda req: engine)

    app = create_app(registry)
    client = TestClient(app)

    # Create session
    response = client.post("/sessions")
    session_id = response.json()["session_id"]

    # Query remote database
    response = client.post(
        f"/sessions/{session_id}/query",
        json={
            "query": "SELECT * FROM sales VISUALISE x, y DRAW point",
            "connection": "test_db",
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert "spec" in data
    assert data["metadata"]["rows"] == 3


def test_sql_endpoint_with_remote():
    """Test pure SQL execution against remote database."""
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE users (id INTEGER, name TEXT)"))
        conn.execute(text("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')"))
        conn.commit()

    registry = ConnectionRegistry()
    registry.register("test_db", lambda req: engine)

    app = create_app(registry)
    client = TestClient(app)

    response = client.post("/sessions")
    session_id = response.json()["session_id"]

    response = client.post(
        f"/sessions/{session_id}/sql",
        json={
            "query": "SELECT * FROM users",
            "connection": "test_db",
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data["rows"]) == 2
    assert data["columns"] == ["id", "name"]


def test_mixed_local_and_remote():
    """Test joining uploaded data with remote query results."""
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE remote_data (id INTEGER, value INTEGER)"))
        conn.execute(text("INSERT INTO remote_data VALUES (1, 100), (2, 200)"))
        conn.commit()

    registry = ConnectionRegistry()
    registry.register("test_db", lambda req: engine)

    app = create_app(registry)
    client = TestClient(app)

    # Create session
    response = client.post("/sessions")
    session_id = response.json()["session_id"]

    # Upload local data
    csv_content = b"id,label\n1,A\n2,B"
    response = client.post(
        f"/sessions/{session_id}/upload",
        files={"file": ("labels.csv", csv_content, "text/csv")},
    )
    assert response.status_code == 200

    # Query local data
    response = client.post(
        f"/sessions/{session_id}/query",
        json={"query": "SELECT * FROM labels VISUALISE id, label DRAW point"},
    )
    assert response.status_code == 200
```

**Step 2: Run integration test**

Run: `cd ggsql-python-rest && python -m pytest tests/test_integration.py -v`
Expected: PASS

**Step 3: Run full test suite**

Run: `cd ggsql-python-rest && python -m pytest -v`
Expected: All tests PASS

**Step 4: Commit**

```bash
git add ggsql-python-rest/tests/test_integration.py
git commit -m "test: add integration tests with SQLAlchemy backend"
```

---

### Task 13: Add conftest.py and pytest configuration

**Files:**
- Create: `ggsql-python-rest/tests/conftest.py`
- Create: `ggsql-python-rest/tests/__init__.py`

**Step 1: Create test configuration**

```python
# tests/__init__.py
"""Test package."""
```

```python
# tests/conftest.py
"""Pytest configuration and fixtures."""

import pytest
from fastapi.testclient import TestClient

from ggsql_rest import create_app, ConnectionRegistry
from ggsql_rest._sessions import SessionManager


@pytest.fixture
def registry() -> ConnectionRegistry:
    """Create a fresh connection registry."""
    return ConnectionRegistry()


@pytest.fixture
def session_manager() -> SessionManager:
    """Create a fresh session manager."""
    return SessionManager(timeout_mins=30)


@pytest.fixture
def app(registry: ConnectionRegistry) -> TestClient:
    """Create a test app with fresh registry."""
    return create_app(registry)


@pytest.fixture
def client(app) -> TestClient:
    """Create a test client."""
    return TestClient(app)
```

**Step 2: Run full test suite**

Run: `cd ggsql-python-rest && python -m pytest -v`
Expected: All tests PASS

**Step 3: Commit**

```bash
git add ggsql-python-rest/tests/conftest.py ggsql-python-rest/tests/__init__.py
git commit -m "test: add pytest configuration and fixtures"
```

---

## Summary

This plan implements ggsql-python-rest in 13 tasks:

1. Project setup (pyproject.toml, __init__.py)
2. Pydantic models
3. Error handling
4. Session manager
5. Connection registry
6. Health route
7. Session routes
8. Query execution core
9. Query routes
10. Upload route
11. App factory
12. Integration tests
13. Test configuration

Each task follows TDD: write failing test, implement, verify, commit.
