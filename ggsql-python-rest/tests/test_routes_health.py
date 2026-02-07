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
