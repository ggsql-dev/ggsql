"""Tests for query routes."""

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

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


@pytest.mark.anyio
async def test_execute_query_local():
    app, session_mgr, _ = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Create session via API
        create_resp = await client.post("/sessions")
        assert create_resp.status_code == 200
        session_id = create_resp.json()["session_id"]

        # Query with inline data (no need to pre-create table)
        response = await client.post(
            f"/sessions/{session_id}/query",
            json={
                "query": "SELECT * FROM (VALUES (1, 2), (3, 4)) AS test(x, y) VISUALISE x, y DRAW point"
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert "spec" in data
        assert "metadata" in data


@pytest.mark.anyio
async def test_execute_query_session_not_found():
    app, _, _ = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/sessions/nonexistent/query",
            json={"query": "SELECT * FROM test VISUALISE x, y DRAW point"},
        )

        assert response.status_code == 404


@pytest.mark.anyio
async def test_execute_sql_local():
    app, session_mgr, _ = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Create session via API
        create_resp = await client.post("/sessions")
        assert create_resp.status_code == 200
        session_id = create_resp.json()["session_id"]

        # Query with inline data
        response = await client.post(
            f"/sessions/{session_id}/sql",
            json={"query": "SELECT * FROM (VALUES (1, 2), (3, 4)) AS test(x, y)"},
        )

        assert response.status_code == 200
        data = response.json()
        assert "rows" in data
        assert "columns" in data
        assert len(data["rows"]) == 2
