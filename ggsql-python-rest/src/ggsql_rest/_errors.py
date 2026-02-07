"""Error handling utilities."""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


class ApiError(Exception):
    """Custom API error with HTTP status code."""

    def __init__(self, status_code: int, error_type: str, message: str):
        self.status_code = status_code
        self.error_type = error_type
        self.message = message
        super().__init__(message)


def session_not_found(session_id: str) -> ApiError:
    """Create a session not found error."""
    return ApiError(404, "SessionNotFound", f"Session '{session_id}' not found")


def connection_not_found(name: str) -> ApiError:
    """Create a connection not found error."""
    return ApiError(400, "ConnectionNotFound", f"Unknown connection: '{name}'")


def register_error_handlers(app: FastAPI) -> None:
    """Register error handlers on the FastAPI app."""

    @app.exception_handler(ApiError)
    async def handle_api_error(request: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "status": "error",
                "error": {"type": exc.error_type, "message": exc.message},
            },
        )
