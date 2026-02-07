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
