"""Tests for app factory."""

import pytest
from fastapi.testclient import TestClient
from httpx import AsyncClient, ASGITransport

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


@pytest.mark.anyio
async def test_full_workflow():
    """Test full workflow: create session, query with inline data, delete."""
    registry = ConnectionRegistry()
    app = create_app(registry)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        # Create session
        response = await client.post("/sessions")
        assert response.status_code == 200
        session_id = response.json()["session_id"]

        # Query with inline data (avoids DuckDB thread safety issues in async tests)
        response = await client.post(
            f"/sessions/{session_id}/query",
            json={"query": "SELECT * FROM (VALUES (1, 10), (2, 20), (3, 30)) AS test(x, y) VISUALISE x, y DRAW point"},
        )
        assert response.status_code == 200
        data = response.json()
        assert "spec" in data
        assert "metadata" in data

        # Delete session
        response = await client.delete(f"/sessions/{session_id}")
        assert response.status_code == 200
