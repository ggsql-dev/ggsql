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
def app(registry: ConnectionRegistry):
    """Create a test app with fresh registry."""
    return create_app(registry)


@pytest.fixture
def client(app) -> TestClient:
    """Create a test client."""
    return TestClient(app)
