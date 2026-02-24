"""FastAPI server main entry point."""

import asyncio
import logging
from pathlib import Path
from typing import Any, Optional

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from etransfer import __version__
from etransfer.server.admin import create_admin_router
from etransfer.server.background import _get_config_mtime, cleanup_loop, config_watch_loop
from etransfer.server.config import ServerSettings, load_server_settings
from etransfer.server.middleware.auth import TokenAuthMiddleware
from etransfer.server.routes.auth import create_auth_router
from etransfer.server.routes.files import create_files_router
from etransfer.server.routes.info import create_info_router
from etransfer.server.services.ip_mgr import IPManager
from etransfer.server.services.state import BackendType, close_state_manager, get_state_manager
from etransfer.server.services.traffic import TrafficMonitor
from etransfer.server.tus.handler import TusHandler
from etransfer.server.tus.storage import TusStorage

logger = logging.getLogger("etransfer.server")

# Global singletons — initialised during the startup event.
_storage: Optional[TusStorage] = None
_traffic_monitor: Optional[TrafficMonitor] = None
_ip_manager: Optional[IPManager] = None
_user_db: Any = None
_oidc_provider: Any = None


# ── Helpers ──────────────────────────────────────────────────


def _create_user_db(settings: ServerSettings) -> Any:
    """Create UserDB with the appropriate SQLAlchemy async URL."""
    from etransfer.server.auth.db import UserDB, build_database_url

    url = build_database_url(
        backend=settings.user_db_backend,
        sqlite_path=settings.user_db_path,
        storage_path=str(settings.storage_path),
        mysql_host=settings.mysql_host,
        mysql_port=settings.mysql_port,
        mysql_user=settings.mysql_user,
        mysql_password=settings.mysql_password,
        mysql_database=settings.mysql_database,
    )
    return UserDB(url)


def _propagate_hot_changes(
    app: FastAPI,
    settings: ServerSettings,
    changes: dict[str, tuple],
) -> None:
    """Push hot-reloaded settings into live runtime objects."""
    if "max_storage_size" in changes and _storage:
        _storage.max_storage_size = settings.max_storage_size

    if "role_quotas" in changes:
        from etransfer.server.auth.models import RoleQuota

        parsed = {}
        for role_name, qdict in settings.role_quotas.items():
            if isinstance(qdict, dict):
                parsed[role_name] = RoleQuota(**qdict)
            else:
                parsed[role_name] = qdict
        app.state.parsed_role_quotas = parsed


# ── Lazy proxies ─────────────────────────────────────────────
# Proxies allow routers to be registered at import time while the
# actual service instances are created during the startup event.


class _StorageProxy:
    def __getattr__(self, name: str) -> Any:
        if _storage is None:
            raise RuntimeError("Storage not initialized")
        return getattr(_storage, name)


class _TrafficProxy:
    def __getattr__(self, name: str) -> Any:
        if _traffic_monitor is None:
            raise RuntimeError("Traffic monitor not initialized")
        return getattr(_traffic_monitor, name)


class _IPProxy:
    def __getattr__(self, name: str) -> Any:
        if _ip_manager is None:
            raise RuntimeError("IP manager not initialized")
        return getattr(_ip_manager, name)


class _UserDBProxy:
    def __getattr__(self, name: str) -> Any:
        if _user_db is None:
            raise RuntimeError("UserDB not initialized")
        return getattr(_user_db, name)


# ── Application factory ─────────────────────────────────────


def create_app(settings: Optional[ServerSettings] = None) -> FastAPI:
    """Create and configure FastAPI application.

    Args:
        settings: Server settings (None = load from env/config via auto-discovery)

    Returns:
        Configured FastAPI application
    """
    global _oidc_provider

    if settings is None:
        settings = load_server_settings()

    app = FastAPI(
        title="ETransfer",
        description="TUS-based file transfer server",
        version=__version__,
    )
    app.state.settings = settings
    app.state.config_mtime = _get_config_mtime(settings)

    # ── CORS ─────────────────────────────────────────────────
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=[
            "Tus-Resumable",
            "Tus-Version",
            "Tus-Extension",
            "Tus-Max-Size",
            "Upload-Offset",
            "Upload-Length",
            "Upload-Metadata",
            "Upload-Expires",
            "Location",
            "Content-Range",
            "X-Retention-Policy",
            "X-Retention-Expires",
            "X-Retention-Warning",
            "X-Download-Count",
        ],
    )

    # ── Auth middleware ───────────────────────────────────────
    if settings.auth_enabled and (settings.auth_tokens or settings.user_system_enabled):

        class _UserDBLazy:
            """Lazy proxy that resolves to _user_db at call time."""

            async def get_session(self, token: str) -> Any:
                return await _user_db.get_session(token) if _user_db else None

            async def get_user(self, user_id: int) -> Any:
                return await _user_db.get_user(user_id) if _user_db else None

        app.add_middleware(
            TokenAuthMiddleware,
            valid_tokens=settings.auth_tokens,
            exclude_paths=[
                "/docs",
                "/redoc",
                "/openapi.json",
                "/api/health",
                "/api/info",
                "/api/users/login",
                "/api/users/callback",
                "/api/auth/verify",
                "/l/",
                "/s/",
            ],
            user_db=_UserDBLazy() if settings.user_system_enabled else None,
        )

    # ── Lifecycle events ─────────────────────────────────────

    @app.on_event("startup")
    async def startup_event() -> None:
        """Initialize services on startup."""
        global _storage, _traffic_monitor, _ip_manager, _user_db

        backend_type = BackendType(settings.state_backend)

        # Guard: file/memory backend + multi-worker = data corruption
        if settings.workers > 1 and backend_type.value != "redis":
            raise RuntimeError(
                f"state_backend='{backend_type.value}' does not support "
                f"workers={settings.workers}. Use state_backend='redis' for multi-worker."
            )

        state_manager = await get_state_manager(
            backend_type=backend_type,
            storage_path=settings.storage_path,
            redis_url=settings.redis_url,
        )

        _storage = TusStorage(
            storage_path=settings.storage_path,
            state_manager=state_manager,
            chunk_size=settings.chunk_size,
            max_storage_size=settings.max_storage_size,
        )
        await _storage.initialize()

        _traffic_monitor = TrafficMonitor(
            interfaces=settings.interfaces or None,
        )
        _traffic_monitor.start()

        _ip_manager = IPManager(
            interfaces=settings.interfaces or None,
            prefer_ipv4=settings.prefer_ipv4,
        )

        # User system
        if settings.user_system_enabled:
            _user_db = _create_user_db(settings)
            await _user_db.connect()
            app.state.user_db = _user_db
            logger.info("User system enabled (OIDC + %s)", settings.user_db_backend)

        # Background tasks
        asyncio.create_task(cleanup_loop(settings.cleanup_interval, lambda: _storage, lambda: _user_db))
        if settings.config_watch:
            asyncio.create_task(config_watch_loop(app, _propagate_hot_changes))

        logger.info("Started on port %d", settings.port)
        logger.info("State backend: %s", backend_type.value)
        if settings.workers > 1:
            db_url = getattr(settings, "user_db_url", "") or ""
            if "sqlite" in db_url or (settings.user_system_enabled and settings.user_db_backend == "sqlite"):
                logger.warning(
                    "SQLite + %d workers — WAL mode enabled. " "For heavy write loads, consider MySQL.",
                    settings.workers,
                )
        if settings.max_storage_size:
            logger.info("Storage quota: %.0f MB", settings.max_storage_size / (1024 * 1024))
        else:
            logger.info("Storage quota: unlimited")
        if settings.config_watch:
            logger.info("Config watch enabled (interval: %ds)", settings.config_watch_interval)

    @app.on_event("shutdown")
    async def shutdown_event() -> None:
        """Cleanup on shutdown."""
        global _traffic_monitor, _user_db  # noqa: F824
        if _traffic_monitor:
            _traffic_monitor.stop()
        if _user_db:
            await _user_db.disconnect()
        await close_state_manager()
        logger.info("Shutdown complete")

    # ── Proxies ──────────────────────────────────────────────
    storage_proxy = _StorageProxy()

    # ── Register routes ──────────────────────────────────────
    tus_handler = TusHandler(storage_proxy, settings.max_upload_size)  # type: ignore[arg-type]
    app.include_router(tus_handler.get_router())

    app.include_router(create_files_router(storage_proxy))  # type: ignore[arg-type]

    app.include_router(
        create_info_router(
            storage_proxy,  # type: ignore[arg-type]
            _TrafficProxy(),  # type: ignore[arg-type]
            _IPProxy(),  # type: ignore[arg-type]
            max_upload_size=settings.max_upload_size,
            server_port=settings.port,
        )
    )

    app.include_router(create_auth_router(settings.auth_tokens))

    # User system routes (OIDC, roles, groups, quotas)
    if settings.user_system_enabled:
        from etransfer.server.auth.models import RoleQuota
        from etransfer.server.auth.oauth import OIDCProvider
        from etransfer.server.auth.routes import create_user_router

        parsed_role_quotas: dict[str, RoleQuota] = {}
        for role_name, qdict in settings.role_quotas.items():
            if isinstance(qdict, dict):
                parsed_role_quotas[role_name] = RoleQuota(**qdict)
            else:
                parsed_role_quotas[role_name] = qdict
        app.state.parsed_role_quotas = parsed_role_quotas

        callback_prefix = (
            settings.oidc_callback_url.rstrip("/")
            if settings.oidc_callback_url
            else (f"http://{settings.host}:{settings.port}")
        )
        callback_url = f"{callback_prefix}/api/users/callback"
        _oidc_provider = OIDCProvider(
            issuer_url=settings.oidc_issuer_url,
            client_id=settings.oidc_client_id,
            client_secret=settings.oidc_client_secret,
            callback_url=callback_url,
            scope=settings.oidc_scope,
        )

        app.include_router(
            create_user_router(_UserDBProxy(), _oidc_provider, parsed_role_quotas)  # type: ignore[arg-type]
        )

    # Admin routes
    app.include_router(create_admin_router(_propagate_hot_changes))

    return app


# ── Server runner ────────────────────────────────────────────


def run_server(
    host: Optional[str] = None,
    port: Optional[int] = None,
    workers: Optional[int] = None,
    config_path: Optional[Path] = None,
    storage_path: Optional[Path] = None,
    state_backend: Optional[str] = None,
    redis_url: Optional[str] = None,
) -> None:
    """Run the server.

    Config file values are used as defaults. CLI flags (non-None) override them.
    """
    settings = load_server_settings(config_path)

    # Only override settings when explicitly provided (not None)
    if host is not None:
        settings.host = host
    if port is not None:
        settings.port = port
    if workers is not None:
        settings.workers = workers
    if storage_path is not None:
        settings.storage_path = storage_path
    if state_backend is not None:
        settings.state_backend = state_backend  # type: ignore[assignment]
    if redis_url is not None:
        settings.redis_url = redis_url

    settings.storage_path.mkdir(parents=True, exist_ok=True)

    if settings.workers > 1 and settings.state_backend != "redis":
        logger.error(
            "state_backend='%s' does not support workers=%d. " "Multi-worker requires state_backend='redis'.",
            settings.state_backend,
            settings.workers,
        )
        raise SystemExit(1)

    app = create_app(settings)

    uvicorn.run(
        app,
        host=settings.host,
        port=settings.port,
        workers=settings.workers,
    )


# For uvicorn command line: uvicorn etransfer.server.main:app
app = create_app()


if __name__ == "__main__":
    run_server()
