"""User database powered by SQLModel + SQLAlchemy async engine.

Works with any SQLAlchemy-supported async backend:
  - SQLite:      sqlite+aiosqlite:///./storage/users.db
  - MySQL:       mysql+aiomysql://user:pass@host:3306/db
  - PostgreSQL:  postgresql+asyncpg://user:pass@host:5432/db

Usage:
    db = UserDB("sqlite+aiosqlite:///users.db")
    await db.connect()
    user = await db.upsert_user(oidc_sub="abc", username="alice")
    await db.disconnect()
"""

import json
import logging
import secrets
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import Result
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlmodel import SQLModel, select

logger = logging.getLogger("etransfer.server.auth")

from etransfer.server.auth.models import (
    GroupTable,
    PendingLoginTable,
    RoleQuota,
    SessionTable,
    UserGroupLink,
    UserTable,
)


class UserDB:
    """Async user database with pluggable backend.

    Construct with a SQLAlchemy async database URL.
    """

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self._is_sqlite = database_url.startswith("sqlite")

        connect_args: dict = {}
        engine_kwargs: dict = {"echo": False}

        if self._is_sqlite:
            connect_args["check_same_thread"] = False
            # Increase busy timeout so concurrent writers wait instead of failing
            connect_args["timeout"] = 30
            # Use StaticPool to share a single connection across async tasks
            # (safe with WAL mode); avoids "database is locked" under light concurrency
            engine_kwargs["pool_pre_ping"] = True

        self._engine = create_async_engine(
            database_url,
            connect_args=connect_args,
            **engine_kwargs,
        )

    async def connect(self) -> None:
        """Create tables if they don't exist. Enable WAL for SQLite."""
        async with self._engine.begin() as conn:
            # Enable WAL journal mode for SQLite — allows concurrent readers
            # and reduces "database is locked" errors with multiple workers
            if self._is_sqlite:
                await conn.exec_driver_sql("PRAGMA journal_mode=WAL")
                await conn.exec_driver_sql("PRAGMA busy_timeout=30000")
            await conn.run_sync(SQLModel.metadata.create_all)

        backend = self.database_url.split("+")[0] if "+" in self.database_url else self.database_url.split(":")[0]
        mode = " (WAL)" if self._is_sqlite else ""
        logger.info("UserDB:%s Connected%s: %s", backend, mode, self._safe_url())

    async def disconnect(self) -> None:
        """Dispose engine and connection pool."""
        await self._engine.dispose()

    def _safe_url(self) -> str:
        """Mask password in URL for logging."""
        url = self.database_url
        if "@" in url:
            prefix = url.split("://")[0]
            rest = url.split("@", 1)[1]
            return f"{prefix}://***@{rest}"
        return url

    def _session(self) -> AsyncSession:
        return AsyncSession(self._engine, expire_on_commit=False)

    # ── User CRUD ─────────────────────────────────────────────

    async def upsert_user(
        self,
        oidc_sub: str,
        username: str,
        display_name: Optional[str] = None,
        email: Optional[str] = None,
        avatar_url: Optional[str] = None,
        is_admin: bool = False,
        groups: Optional[list[str]] = None,
    ) -> UserTable:
        """Create or update a user from OIDC profile data.

        First login creates with 'user' role.
        Subsequent logins update profile and sync group membership.
        """
        now = datetime.utcnow()
        async with self._session() as session:
            existing = await self._get_user_by_sub(session, oidc_sub)

            if existing:
                existing.username = username
                existing.display_name = display_name
                existing.email = email
                existing.avatar_url = avatar_url
                existing.is_admin = is_admin
                existing.updated_at = now
                session.add(existing)
                await session.commit()
                await session.refresh(existing)
                user = existing
            else:
                role = "admin" if is_admin else "user"
                user = UserTable(
                    oidc_sub=oidc_sub,
                    username=username,
                    display_name=display_name,
                    email=email,
                    avatar_url=avatar_url,
                    role=role,
                    is_active=True,
                    is_admin=is_admin,
                    storage_used=0,
                    created_at=now,
                    updated_at=now,
                )
                session.add(user)
                await session.commit()
                await session.refresh(user)

        if groups is not None:
            await self.sync_user_groups(user.id, groups)  # type: ignore[arg-type]

        return await self.get_user(user.id)  # type: ignore[return-value, arg-type]

    async def get_user(self, user_id: int) -> Optional[UserTable]:
        async with self._session() as session:
            return await session.get(UserTable, user_id)

    async def get_user_by_oidc_sub(self, oidc_sub: str) -> Optional[UserTable]:
        async with self._session() as session:
            return await self._get_user_by_sub(session, oidc_sub)

    async def list_users(self) -> list[UserTable]:
        async with self._session() as session:
            result: Result[Any] = await session.execute(
                select(UserTable).order_by(UserTable.created_at.desc())  # type: ignore[attr-defined]
            )
            return list(result.scalars().all())

    async def set_user_role(self, user_id: int, role: str) -> Optional[UserTable]:
        async with self._session() as session:
            user = await session.get(UserTable, user_id)
            if not user:
                return None
            user.role = role
            user.updated_at = datetime.utcnow()
            session.add(user)
            await session.commit()
            await session.refresh(user)
            return user

    async def set_user_active(self, user_id: int, active: bool) -> Optional[UserTable]:
        async with self._session() as session:
            user = await session.get(UserTable, user_id)
            if not user:
                return None
            user.is_active = active
            user.updated_at = datetime.utcnow()
            session.add(user)
            await session.commit()
            await session.refresh(user)
            return user

    async def update_storage_used(self, user_id: int, delta: int) -> None:
        async with self._session() as session:
            user = await session.get(UserTable, user_id)
            if user:
                user.storage_used = max(0, user.storage_used + delta)
                session.add(user)
                await session.commit()

    async def recalculate_storage(self, user_id: int, actual_bytes: int) -> None:
        async with self._session() as session:
            user = await session.get(UserTable, user_id)
            if user:
                user.storage_used = actual_bytes
                session.add(user)
                await session.commit()

    async def _get_user_by_sub(self, session: AsyncSession, oidc_sub: str) -> Optional[UserTable]:
        result: Result[Any] = await session.execute(select(UserTable).where(UserTable.oidc_sub == oidc_sub))
        return result.scalars().first()

    # ── Group CRUD ────────────────────────────────────────────

    async def ensure_group(self, name: str, description: Optional[str] = None) -> GroupTable:
        """Ensure a group exists (create if missing). Used during OIDC sync."""
        async with self._session() as session:
            result: Result[Any] = await session.execute(select(GroupTable).where(GroupTable.name == name))
            existing = result.scalars().first()
            if existing:
                return existing

            group = GroupTable(
                name=name,
                description=description,
                created_at=datetime.utcnow(),
            )
            session.add(group)
            await session.commit()
            await session.refresh(group)
            return group

    async def get_group(self, group_id: int) -> Optional[GroupTable]:
        async with self._session() as session:
            return await session.get(GroupTable, group_id)

    async def get_group_by_name(self, name: str) -> Optional[GroupTable]:
        async with self._session() as session:
            result: Result[Any] = await session.execute(select(GroupTable).where(GroupTable.name == name))
            return result.scalars().first()

    async def list_groups(self) -> list[GroupTable]:
        async with self._session() as session:
            result: Result[Any] = await session.execute(select(GroupTable).order_by(GroupTable.name))
            return list(result.scalars().all())

    async def update_group_quota(self, group_id: int, quota: RoleQuota) -> Optional[GroupTable]:
        async with self._session() as session:
            group = await session.get(GroupTable, group_id)
            if not group:
                return None
            group.quota_json = quota.model_dump_json()
            session.add(group)
            await session.commit()
            await session.refresh(group)
            return group

    async def delete_group(self, group_id: int) -> bool:
        async with self._session() as session:
            group = await session.get(GroupTable, group_id)
            if group:
                # Remove memberships first
                links: Result[Any] = await session.execute(
                    select(UserGroupLink).where(UserGroupLink.group_id == group_id)
                )
                for link in links.scalars().all():
                    await session.delete(link)
                await session.delete(group)
                await session.commit()
            return True

    async def get_group_member_count(self, group_id: int) -> int:
        async with self._session() as session:
            result: Result[Any] = await session.execute(select(UserGroupLink).where(UserGroupLink.group_id == group_id))
            return len(result.scalars().all())

    # ── User-Group membership ─────────────────────────────────

    async def sync_user_groups(self, user_id: int, group_names: list[str]) -> None:
        """Sync user's group membership from OIDC provider.

        Ensures all named groups exist, adds user to them,
        removes from groups not in the list.
        """
        for name in group_names:
            await self.ensure_group(name)

        current = await self.get_user_groups(user_id)
        current_names = {g.name for g in current}
        target_names = set(group_names)

        for name in target_names - current_names:
            group = await self.get_group_by_name(name)
            if group:
                await self.add_user_to_group(user_id, group.id)  # type: ignore[arg-type]

        for name in current_names - target_names:
            group = await self.get_group_by_name(name)
            if group:
                await self.remove_user_from_group(user_id, group.id)  # type: ignore[arg-type]

    async def get_user_groups(self, user_id: int) -> list[GroupTable]:
        async with self._session() as session:
            result: Result[Any] = await session.execute(
                select(GroupTable)
                .join(UserGroupLink, GroupTable.id == UserGroupLink.group_id)  # type: ignore[arg-type]
                .where(UserGroupLink.user_id == user_id)
            )
            return list(result.scalars().all())

    async def get_user_group_names(self, user_id: int) -> list[str]:
        groups = await self.get_user_groups(user_id)
        return [g.name for g in groups]

    async def add_user_to_group(self, user_id: int, group_id: int) -> bool:
        async with self._session() as session:
            result: Result[Any] = await session.execute(
                select(UserGroupLink).where(
                    UserGroupLink.user_id == user_id,
                    UserGroupLink.group_id == group_id,
                )
            )
            if result.scalars().first():
                return True
            link = UserGroupLink(user_id=user_id, group_id=group_id)
            session.add(link)
            await session.commit()
            return True

    async def remove_user_from_group(self, user_id: int, group_id: int) -> bool:
        async with self._session() as session:
            result: Result[Any] = await session.execute(
                select(UserGroupLink).where(
                    UserGroupLink.user_id == user_id,
                    UserGroupLink.group_id == group_id,
                )
            )
            link = result.scalars().first()
            if link:
                await session.delete(link)
                await session.commit()
            return True

    # ── Session management ────────────────────────────────────

    async def create_session(self, user_id: int, ttl_hours: int = 24 * 7) -> SessionTable:
        token = secrets.token_urlsafe(48)
        now = datetime.utcnow()
        expires_at = now + timedelta(hours=ttl_hours)

        session_obj = SessionTable(
            token=token,
            user_id=user_id,
            created_at=now,
            expires_at=expires_at,
        )

        async with self._session() as session:
            session.add(session_obj)
            await session.commit()
            await session.refresh(session_obj)

        return session_obj

    async def get_session(self, token: str) -> Optional[SessionTable]:
        async with self._session() as session:
            sess = await session.get(SessionTable, token)
            if not sess:
                return None

            if sess.expires_at and sess.expires_at < datetime.utcnow():  # type: ignore[operator]
                await session.delete(sess)
                await session.commit()
                return None

            return sess

    async def delete_session(self, token: str) -> None:
        async with self._session() as session:
            sess = await session.get(SessionTable, token)
            if sess:
                await session.delete(sess)
                await session.commit()

    async def delete_user_sessions(self, user_id: int) -> None:
        async with self._session() as session:
            result: Result[Any] = await session.execute(select(SessionTable).where(SessionTable.user_id == user_id))
            for sess in result.scalars().all():
                await session.delete(sess)
            await session.commit()

    async def cleanup_expired_sessions(self) -> int:
        now = datetime.utcnow()
        async with self._session() as session:
            result: Result[Any] = await session.execute(
                select(SessionTable).where(SessionTable.expires_at < now)  # type: ignore[operator]
            )
            rows = result.scalars().all()
            count = len(rows)
            for sess in rows:
                await session.delete(sess)
            await session.commit()
            return count

    # ── Pending login (CLI flow) ──────────────────────────────

    async def create_pending_login(
        self,
        state: str,
        redirect_uri: Optional[str] = None,
        authorize_url: Optional[str] = None,
    ) -> PendingLoginTable:
        pending = PendingLoginTable(
            state=state,
            created_at=datetime.utcnow(),
            redirect_uri=redirect_uri,
            authorize_url=authorize_url,
        )
        async with self._session() as session:
            session.add(pending)
            await session.commit()
            await session.refresh(pending)
        return pending

    async def get_pending_login(self, state: str) -> Optional[PendingLoginTable]:
        async with self._session() as session:
            return await session.get(PendingLoginTable, state)

    async def complete_pending_login(self, state: str, session_token: str) -> None:
        async with self._session() as session:
            pending = await session.get(PendingLoginTable, state)
            if pending:
                pending.session_token = session_token
                pending.completed = True
                session.add(pending)
                await session.commit()

    async def cleanup_pending_logins(self, max_age_seconds: int = 600) -> int:
        cutoff = datetime.utcnow() - timedelta(seconds=max_age_seconds)
        async with self._session() as session:
            result: Result[Any] = await session.execute(
                select(PendingLoginTable).where(PendingLoginTable.created_at < cutoff)
            )
            rows = result.scalars().all()
            count = len(rows)
            for p in rows:
                await session.delete(p)
            await session.commit()
            return count

    # ── Quota resolution ──────────────────────────────────────

    async def get_effective_quota(self, user: UserTable, role_quotas: dict[str, RoleQuota]) -> RoleQuota:
        """Compute effective quota for a user.

        Priority: group quota (most generous) > role quota > global default.
        None means unlimited, which always wins over any numeric limit.
        """
        role_q = role_quotas.get(user.role, RoleQuota())  # type: ignore[call-arg]
        groups = await self.get_user_groups(user.id)  # type: ignore[arg-type]

        if not groups:
            return role_q

        candidates = [role_q]
        for g in groups:
            quota_data = json.loads(g.quota_json) if g.quota_json else {}
            if not quota_data:
                continue  # skip groups with no explicit quota
            candidates.append(RoleQuota(**quota_data))

        def most_permissive(vals: list[Optional[int]]) -> Optional[int]:
            if any(v is None for v in vals):
                return None
            return max(vals)  # type: ignore[type-var]

        return RoleQuota(
            max_storage_size=most_permissive([c.max_storage_size for c in candidates]),
            max_upload_size=most_permissive([c.max_upload_size for c in candidates]),
            upload_speed_limit=most_permissive([c.upload_speed_limit for c in candidates]),
            download_speed_limit=most_permissive([c.download_speed_limit for c in candidates]),
            default_retention=role_q.default_retention,
            default_retention_ttl=role_q.default_retention_ttl,
        )


# ── Factory ───────────────────────────────────────────────────


def build_database_url(
    backend: str = "sqlite",
    sqlite_path: str = "",
    storage_path: str = "./storage",
    mysql_host: str = "127.0.0.1",
    mysql_port: int = 3306,
    mysql_user: str = "root",
    mysql_password: str = "",  # nosec B107
    mysql_database: str = "etransfer",
) -> str:
    """Build SQLAlchemy async database URL from config fields.

    Args:
        backend: "sqlite" or "mysql" (or "postgresql")
        sqlite_path: Path to SQLite file (default: {storage_path}/users.db)
        storage_path: Fallback base path for SQLite
        mysql_*: MySQL connection parameters
    """
    if backend == "mysql":
        password_part = f":{mysql_password}" if mysql_password else ""
        return f"mysql+aiomysql://{mysql_user}{password_part}" f"@{mysql_host}:{mysql_port}/{mysql_database}"
    elif backend == "postgresql":
        password_part = f":{mysql_password}" if mysql_password else ""
        return f"postgresql+asyncpg://{mysql_user}{password_part}" f"@{mysql_host}:{mysql_port}/{mysql_database}"
    else:
        path = sqlite_path or str(Path(storage_path) / "users.db")
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        return f"sqlite+aiosqlite:///{path}"
