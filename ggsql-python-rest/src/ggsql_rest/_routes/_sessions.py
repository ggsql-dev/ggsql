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
