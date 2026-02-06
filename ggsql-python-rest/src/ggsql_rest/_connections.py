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
