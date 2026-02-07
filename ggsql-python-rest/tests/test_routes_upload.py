"""Tests for file upload route."""

import io
import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from ggsql_rest._sessions import SessionManager
from ggsql_rest._routes._sessions import router, get_session_manager


def create_test_app() -> tuple[FastAPI, SessionManager]:
    app = FastAPI()
    session_mgr = SessionManager(timeout_mins=30)

    app.dependency_overrides[get_session_manager] = lambda: session_mgr
    app.include_router(router)

    return app, session_mgr


@pytest.mark.anyio
async def test_upload_csv():
    app, session_mgr = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        csv_content = b"x,y\n1,2\n3,4\n5,6"
        files = {"file": ("data.csv", io.BytesIO(csv_content), "text/csv")}

        response = await client.post(f"/sessions/{session.id}/upload", files=files)

        assert response.status_code == 200
        data = response.json()
        assert data["table_name"] == "data"
        assert data["row_count"] == 3
        assert "x" in data["columns"]
        assert "y" in data["columns"]

        # Verify table is in session
        assert "data" in session.tables


@pytest.mark.anyio
async def test_upload_parquet():
    app, session_mgr = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        # Create a simple parquet file in memory
        import polars as pl

        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        buffer = io.BytesIO()
        df.write_parquet(buffer)
        buffer.seek(0)

        files = {"file": ("data.parquet", buffer, "application/octet-stream")}
        response = await client.post(f"/sessions/{session.id}/upload", files=files)

        assert response.status_code == 200
        data = response.json()
        assert data["table_name"] == "data"
        assert data["row_count"] == 3
        assert "a" in data["columns"]
        assert "b" in data["columns"]


@pytest.mark.anyio
async def test_upload_json():
    app, session_mgr = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        json_content = b'[{"x": 1, "y": 2}, {"x": 3, "y": 4}]'
        files = {"file": ("data.json", io.BytesIO(json_content), "application/json")}

        response = await client.post(f"/sessions/{session.id}/upload", files=files)

        assert response.status_code == 200
        data = response.json()
        assert data["table_name"] == "data"
        assert data["row_count"] == 2


@pytest.mark.anyio
async def test_upload_filename_sanitization():
    app, session_mgr = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        csv_content = b"x,y\n1,2"
        # Filename with spaces and hyphens should be converted to underscores
        files = {"file": ("my-data file.csv", io.BytesIO(csv_content), "text/csv")}

        response = await client.post(f"/sessions/{session.id}/upload", files=files)

        assert response.status_code == 200
        data = response.json()
        assert data["table_name"] == "my_data_file"


@pytest.mark.anyio
async def test_upload_unsupported_format():
    app, session_mgr = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        files = {"file": ("data.txt", io.BytesIO(b"some text"), "text/plain")}

        response = await client.post(f"/sessions/{session.id}/upload", files=files)

        assert response.status_code == 400
        assert "Unsupported file format" in response.json()["detail"]


@pytest.mark.anyio
async def test_upload_session_not_found():
    app, _ = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        csv_content = b"x,y\n1,2"
        files = {"file": ("data.csv", io.BytesIO(csv_content), "text/csv")}

        response = await client.post("/sessions/nonexistent/upload", files=files)

        assert response.status_code == 404
