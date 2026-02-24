#!/usr/bin/env python3
"""
Quota & retention integration tests.

Uses httpx.AsyncClient + FastAPI TestClient pattern — no real server needed.

Tests:
1. TTL 15s expiry → cleanup_expired deletes file
2. download_once consume → file deleted + storage_used decremented
3. quota full → 507 → consume frees space → upload resumes
"""

from __future__ import annotations

import asyncio
import base64
import tempfile
from pathlib import Path

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from etransfer.server.auth.db import UserDB
from etransfer.server.auth.models import UserTable
from etransfer.server.config import ServerSettings
from etransfer.server.main import create_app

# ── Helpers ────────────────────────────────────────────────────


def _encode_metadata(**kv: str) -> str:
    """Encode key-value pairs into TUS Upload-Metadata header format."""
    parts = []
    for k, v in kv.items():
        parts.append(f"{k} {base64.b64encode(v.encode()).decode()}")
    return ",".join(parts)


def _tus_headers(**extra: str) -> dict[str, str]:
    h = {"Tus-Resumable": "1.0.0"}
    h.update(extra)
    return h


# ── Fixtures ───────────────────────────────────────────────────


@pytest_asyncio.fixture
async def tmp_storage(tmp_path: Path):
    """Provide a temp directory for storage."""
    return tmp_path / "storage"


@pytest_asyncio.fixture
async def user_db(tmp_path: Path):
    """Create an in-memory SQLite UserDB."""
    db_path = tmp_path / "users.db"
    db = UserDB(f"sqlite+aiosqlite:///{db_path}")
    await db.connect()
    yield db
    await db.disconnect()


@pytest_asyncio.fixture
async def test_user(user_db: UserDB) -> UserTable:
    """Create a test user with role=user."""
    user = await user_db.upsert_user(
        oidc_sub="test-sub-001",
        username="testuser",
        display_name="Test User",
    )
    assert user is not None
    return user


async def _create_test_app(
    tmp_storage: Path,
    user_db: UserDB | None = None,
    *,
    default_retention: str = "permanent",
    default_retention_ttl: int | None = None,
    max_storage_size: int | None = None,
    role_quotas: dict | None = None,
    auth_token: str = "test-token",
) -> tuple:
    """Build a FastAPI app + AsyncClient for testing."""
    settings = ServerSettings(
        storage_path=tmp_storage,
        state_backend="memory",
        auth_enabled=True,
        auth_tokens=[auth_token],
        user_system_enabled=user_db is not None,
        default_retention=default_retention,
        default_retention_ttl=default_retention_ttl,
        max_storage_size=max_storage_size,
        role_quotas=role_quotas or {},
        cleanup_interval=999999,  # disable auto-cleanup
        config_watch=False,
    )

    app = create_app(settings)

    # Trigger startup manually
    try:
        async with asyncio.timeout(10):
            for handler in app.router.on_startup:
                await handler()
    except AttributeError:
        # Python < 3.11: asyncio.timeout not available
        for handler in app.router.on_startup:
            await asyncio.wait_for(handler(), timeout=10)

    # Inject user_db into app.state (may already be set by startup if user_system_enabled)
    if user_db is not None:
        app.state.user_db = user_db

    transport = ASGITransport(app=app)
    client = AsyncClient(
        transport=transport,
        base_url="http://testserver",
        headers={"X-API-Token": auth_token},
    )

    return app, client, settings


async def _tus_upload(
    client: AsyncClient,
    data: bytes,
    *,
    filename: str = "test.bin",
    retention: str | None = None,
    retention_ttl: int | None = None,
    user: UserTable | None = None,
) -> str:
    """Perform a full TUS upload (POST create + PATCH chunk). Returns file_id."""
    meta_kv: dict[str, str] = {"filename": filename, "filetype": "application/octet-stream"}
    if retention:
        meta_kv["retention"] = retention
    if retention_ttl is not None:
        meta_kv["retention_ttl"] = str(retention_ttl)

    headers = _tus_headers(
        **{
            "Upload-Length": str(len(data)),
            "Upload-Metadata": _encode_metadata(**meta_kv),
            "Content-Type": "application/offset+octet-stream",
        }
    )

    # Inject user into request state via a custom header that middleware can pick up
    # For testing, we patch request.state.user directly via middleware
    resp = await client.post("/tus", headers=headers)
    assert resp.status_code == 201, f"TUS create failed: {resp.status_code} {resp.text}"

    location = resp.headers["Location"]
    file_id = location.rsplit("/", 1)[-1]

    # PATCH — upload the data
    patch_headers = _tus_headers(
        **{
            "Upload-Offset": "0",
            "Content-Type": "application/offset+octet-stream",
        }
    )
    resp = await client.patch(f"/tus/{file_id}", headers=patch_headers, content=data)
    assert resp.status_code == 204, f"TUS patch failed: {resp.status_code} {resp.text}"

    return file_id


async def _cleanup(app) -> int:
    """Manually trigger cleanup_expired on the storage."""
    from etransfer.server.main import _storage, _user_db

    if _storage:
        return await _storage.cleanup_expired(user_db=_user_db)
    return 0


# ── Test 1: TTL expiry (15 seconds) ───────────────────────────


@pytest.mark.asyncio
async def test_ttl_expiry_auto_delete(tmp_path: Path):
    """Upload with TTL=15s, wait, cleanup → file gone."""
    tmp_storage = tmp_path / "storage"

    app, client, settings = await _create_test_app(
        tmp_storage,
        default_retention="ttl",
        default_retention_ttl=15,
    )

    try:
        data = b"hello-ttl-test" * 100
        file_id = await _tus_upload(client, data)

        # File should exist
        resp = await client.get(f"/api/files/{file_id}")
        assert resp.status_code == 200

        # Cleanup now — file should NOT be deleted (TTL not expired)
        cleaned = await _cleanup(app)
        assert cleaned == 0

        resp = await client.get(f"/api/files/{file_id}")
        assert resp.status_code == 200

        # Wait for TTL to expire
        print("Waiting 16 seconds for TTL expiry...")
        await asyncio.sleep(16)

        # Cleanup again — file SHOULD be deleted
        cleaned = await _cleanup(app)
        assert cleaned >= 1, f"Expected cleanup to delete file, but cleaned={cleaned}"

        # File should be gone
        resp = await client.get(f"/api/files/{file_id}")
        assert resp.status_code == 404

    finally:
        await client.aclose()
        for handler in app.router.on_shutdown:
            await handler()


# ── Test 2: download_once consume → delete + quota change ─────


@pytest.mark.asyncio
async def test_download_once_consume_and_quota(tmp_path: Path):
    """Upload download_once file, consume it, verify deletion and quota update."""
    tmp_storage = tmp_path / "storage"

    db_path = tmp_path / "users.db"
    user_db = UserDB(f"sqlite+aiosqlite:///{db_path}")
    await user_db.connect()

    try:
        # Create test user
        user = await user_db.upsert_user(
            oidc_sub="test-sub-consume",
            username="consumer",
        )
        assert user is not None
        user_id = user.id

        app, client, settings = await _create_test_app(
            tmp_storage,
            user_db=user_db,
            default_retention="download_once",
            role_quotas={
                "user": {
                    "max_storage_size": 1 * 1024 * 1024 * 1024,  # 1GB
                }
            },
        )

        try:
            # We need to inject user into request.state for the TUS handler
            # Add middleware that sets request.state.user for all requests
            from starlette.middleware.base import BaseHTTPMiddleware

            class InjectUserMiddleware(BaseHTTPMiddleware):
                async def dispatch(self, request, call_next):
                    request.state.user = user
                    return await call_next(request)

            app.add_middleware(InjectUserMiddleware)
            # Rebuild client after adding middleware
            transport = ASGITransport(app=app)
            client = AsyncClient(
                transport=transport,
                base_url="http://testserver",
                headers={"X-API-Token": "test-token"},
            )

            data = b"X" * 4096  # 4KB file
            file_id = await _tus_upload(client, data, retention="download_once")

            # Check user storage_used increased
            updated_user = await user_db.get_user(user_id)
            assert updated_user is not None
            assert updated_user.storage_used == len(
                data
            ), f"Expected storage_used={len(data)}, got {updated_user.storage_used}"

            # Download file (triggers download_once deletion)
            # download_once uses chunked storage: download chunk 0
            resp = await client.get(f"/api/files/{file_id}/download", params={"chunk": 0})
            assert resp.status_code == 200
            assert resp.content == data

            # Give background task time to run (_after_download)
            await asyncio.sleep(0.5)

            # File should be deleted after download_once consume
            resp = await client.get(f"/api/files/{file_id}")
            assert resp.status_code == 404, f"File should be deleted after consume, got {resp.status_code}"

            # storage_used should be back to 0
            updated_user = await user_db.get_user(user_id)
            assert updated_user is not None
            assert (
                updated_user.storage_used == 0
            ), f"Expected storage_used=0 after consume, got {updated_user.storage_used}"

        finally:
            await client.aclose()
            for handler in app.router.on_shutdown:
                await handler()

    finally:
        await user_db.disconnect()


# ── Test 3: quota full → 507 → consume → resume ──────────────


@pytest.mark.asyncio
async def test_quota_full_then_consume_then_resume(tmp_path: Path):
    """
    User quota = 8KB.
    Upload 4KB file → OK (storage_used = 4KB).
    Upload another 8KB file → 507 (would exceed quota).
    Consume first file → storage_used = 0.
    Upload 8KB file again → OK.
    """
    tmp_storage = tmp_path / "storage"

    db_path = tmp_path / "users.db"
    user_db = UserDB(f"sqlite+aiosqlite:///{db_path}")
    await user_db.connect()

    try:
        user = await user_db.upsert_user(
            oidc_sub="test-sub-quota",
            username="quotauser",
        )
        assert user is not None
        user_id = user.id

        user_quota = 8 * 1024  # 8KB

        app, client, settings = await _create_test_app(
            tmp_storage,
            user_db=user_db,
            default_retention="download_once",
            role_quotas={
                "user": {
                    "max_storage_size": user_quota,
                }
            },
        )

        try:
            from starlette.middleware.base import BaseHTTPMiddleware

            class InjectUserMiddleware(BaseHTTPMiddleware):
                async def dispatch(self, request, call_next):
                    request.state.user = user
                    return await call_next(request)

            app.add_middleware(InjectUserMiddleware)
            transport = ASGITransport(app=app)
            client = AsyncClient(
                transport=transport,
                base_url="http://testserver",
                headers={"X-API-Token": "test-token"},
            )

            # Upload 1: 4KB → should succeed
            data1 = b"A" * 4096
            file_id_1 = await _tus_upload(client, data1, filename="file1.bin")

            u = await user_db.get_user(user_id)
            assert u is not None
            assert u.storage_used == 4096, f"Expected 4096, got {u.storage_used}"

            # Upload 2: 8KB → CREATE should fail with 507 (4096 + 8192 > 8192)
            # With reservation model, quota is checked at CREATE time.
            data2 = b"B" * 8192
            meta = _encode_metadata(filename="file2.bin", filetype="application/octet-stream")
            headers = _tus_headers(
                **{
                    "Upload-Length": str(len(data2)),
                    "Upload-Metadata": meta,
                    "Content-Type": "application/offset+octet-stream",
                }
            )
            resp = await client.post("/tus", headers=headers)
            assert resp.status_code == 507, f"Expected 507 at CREATE, got {resp.status_code}: {resp.text}"

            # Consume file1 (download_once → auto-delete)
            # download_once uses chunked storage: download chunk 0
            resp = await client.get(f"/api/files/{file_id_1}/download", params={"chunk": 0})
            assert resp.status_code == 200

            # Wait for background deletion
            await asyncio.sleep(0.5)

            # storage_used should be back to 0
            u = await user_db.get_user(user_id)
            assert u is not None
            assert u.storage_used == 0, f"Expected 0 after consume, got {u.storage_used}"

            # Retry CREATE → should succeed now (storage_used=0, 0+8192 <= 8192)
            resp = await client.post("/tus", headers=headers)
            assert resp.status_code == 201, f"Expected 201 after consume freed space, got {resp.status_code}"
            location = resp.headers["Location"]
            file_id_2 = location.rsplit("/", 1)[-1]

            # PATCH should succeed
            patch_headers = _tus_headers(
                **{
                    "Upload-Offset": "0",
                    "Content-Type": "application/offset+octet-stream",
                }
            )
            resp = await client.patch(f"/tus/{file_id_2}", headers=patch_headers, content=data2)
            assert resp.status_code == 204, f"Expected 204 after consume freed space, got {resp.status_code}"

            u = await user_db.get_user(user_id)
            assert u is not None
            assert u.storage_used == 8192, f"Expected 8192, got {u.storage_used}"

        finally:
            await client.aclose()
            for handler in app.router.on_shutdown:
                await handler()

    finally:
        await user_db.disconnect()
