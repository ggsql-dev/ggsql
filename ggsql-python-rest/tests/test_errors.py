"""Tests for error handling."""

from ggsql_rest._errors import ApiError, session_not_found, connection_not_found


def test_api_error():
    err = ApiError(404, "NotFound", "Resource not found")
    assert err.status_code == 404
    assert err.error_type == "NotFound"
    assert err.message == "Resource not found"


def test_session_not_found():
    err = session_not_found("abc123")
    assert err.status_code == 404
    assert err.error_type == "SessionNotFound"
    assert "abc123" in err.message


def test_connection_not_found():
    err = connection_not_found("warehouse")
    assert err.status_code == 400
    assert err.error_type == "ConnectionNotFound"
    assert "warehouse" in err.message
