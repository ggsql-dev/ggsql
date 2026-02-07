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
