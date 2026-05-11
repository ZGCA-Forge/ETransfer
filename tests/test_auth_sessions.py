from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from etransfer.server.auth.db import UserDB


@pytest.mark.asyncio
async def test_expired_session_cleanup_is_idempotent(tmp_path: Path) -> None:
    db = UserDB(f"sqlite+aiosqlite:///{tmp_path / 'users.db'}")
    await db.connect()
    try:
        user = await db.upsert_user(oidc_sub="expired-session-user", username="expired")
        session = await db.create_session(user.id, ttl_hours=-1)  # type: ignore[arg-type]

        results = await asyncio.gather(
            db.get_session(session.token),
            db.get_session(session.token),
        )

        assert results == [None, None]
    finally:
        await db.disconnect()
