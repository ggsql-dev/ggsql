"""ggsql REST API server with SQLAlchemy backend support."""

from ._app import create_app
from ._connections import ConnectionRegistry

__version__ = "0.1.0"
__all__ = ["create_app", "ConnectionRegistry"]
