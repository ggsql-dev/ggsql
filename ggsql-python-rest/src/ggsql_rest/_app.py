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
