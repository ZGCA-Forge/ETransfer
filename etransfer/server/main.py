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
from etransfer.server.middleware.traffic import TrafficCounterMiddleware
from etransfer.server.routes.auth import create_auth_router
from etransfer.server.routes.files import create_files_router
from etransfer.server.routes.info import create_info_router
from etransfer.server.services.instance_traffic import InstanceTrafficTracker
from etransfer.server.services.state import BackendType, close_state_manager, get_state_manager
from etransfer.server.tus.handler import TusHandler
from etransfer.server.tus.storage import TusStorage

logger = logging.getLogger("etransfer.server")

# Global singletons — initialised during the startup event.
_storage: Optional[TusStorage] = None
_instance_tracker: Optional[InstanceTrafficTracker] = None
_user_db: Any = None
_oidc_provider: Any = None


# ── Helpers ──────────────────────────────────────────────────


def _resolve_endpoint(settings: ServerSettings) -> str:
    """Determine the endpoint string to advertise to clients.

    Priority:
      1. ``settings.advertised_endpoints[0]`` — explicit config
      2. Auto-detect primary LAN IP via UDP socket trick
      3. Fall back to ``settings.host:settings.port``

    The returned string always includes the port, e.g. ``192.168.1.5:8765``.
    """
    import socket

    port = settings.port

    # 1. Explicit config
    if settings.advertised_endpoints:
        ep = settings.advertised_endpoints[0]
        # If the user wrote just an IP/hostname without port, append it
        if ":" not in ep or ep.count(":") > 1:  # IPv6 or plain hostname
            ep = f"{ep}:{port}"
        return ep

    # 2. Auto-detect via UDP connect (no traffic sent)
    host = settings.host
    if host in ("0.0.0.0", "::", ""):  # nosec B104
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            host = s.getsockname()[0]
            s.close()
        except Exception:
            host = "127.0.0.1"

    return f"{host}:{port}"


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


async def _reconcile_storage_quotas(storage: TusStorage, user_db: Any) -> None:
    """Reconcile SQL storage_used with actual files in state backend.

    Runs once at startup. Scans completed files, groups sizes by owner_id,
    and corrects any drift in the users table.
    Also clears stale quota reservations in Redis that may have been left
    behind by interrupted uploads or unclean shutdowns.
    """
    from etransfer.common.constants import RedisKeys

    try:
        files = await storage.list_files()
        owner_sizes: dict[int, int] = {}
        for f in files:
            oid = f.get("owner_id")
            if oid is not None:
                owner_sizes[oid] = owner_sizes.get(oid, 0) + f.get("size", 0)

        # Also account for in-flight (non-finalized) uploads — reservation
        # equals actual bytes received (offset), not the full file size.
        in_flight_reserved: dict[int, int] = {}
        uploads = await storage.list_uploads(include_completed=False, include_partial=True)
        for u in uploads:
            if u.owner_id is not None and not u.is_final:
                in_flight_reserved[u.owner_id] = in_flight_reserved.get(u.owner_id, 0) + u.offset

        users = await user_db.list_users()
        fixed_db = 0
        fixed_redis = 0
        for u in users:
            uid = u.id
            actual = owner_sizes.get(uid, 0)

            # Fix SQL storage_used
            if u.storage_used != actual:
                logger.info(
                    "Quota drift: user %s (%s) DB=%d actual=%d, correcting",
                    uid,
                    u.username,
                    u.storage_used,
                    actual,
                )
                await user_db.recalculate_storage(uid, actual)
                fixed_db += 1

            # Fix Redis reservation — should equal sum of in-flight upload offsets
            expected_reserved = in_flight_reserved.get(uid, 0)
            quota_key = f"{RedisKeys.QUOTA_PREFIX}{uid}"
            current_reserved_str = await storage.state.get(quota_key)
            current_reserved = int(current_reserved_str) if current_reserved_str else 0

            if current_reserved != expected_reserved:
                logger.info(
                    "Reservation drift: user %s (%s) redis=%d expected=%d, correcting",
                    uid,
                    u.username,
                    current_reserved,
                    expected_reserved,
                )
                if expected_reserved == 0:
                    await storage.state.delete(quota_key)
                else:
                    await storage.state.set(quota_key, str(expected_reserved))
                fixed_redis += 1

        if fixed_db or fixed_redis:
            logger.info(
                "Reconciled: %d user(s) storage_used, %d user(s) reservations",
                fixed_db,
                fixed_redis,
            )
        else:
            logger.info("Storage quotas consistent — no corrections needed")
    except Exception:
        logger.exception("Failed to reconcile storage quotas (non-fatal)")


# ── Lazy proxies ─────────────────────────────────────────────
# Proxies allow routers to be registered at import time while the
# actual service instances are created during the startup event.


class _StorageProxy:
    def __getattr__(self, name: str) -> Any:
        if _storage is None:
            raise RuntimeError("Storage not initialized")
        return getattr(_storage, name)


class _TrackerProxy:
    def __getattr__(self, name: str) -> Any:
        if _instance_tracker is None:
            raise RuntimeError("Instance tracker not initialized")
        return getattr(_instance_tracker, name)


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

    # ── Configure logging level ──────────────────────────────
    log_level = getattr(settings, "log_level", "INFO").upper()
    numeric_level = getattr(logging, log_level, logging.INFO)
    logging.getLogger("etransfer").setLevel(numeric_level)
    # Also set root handler if none configured
    if not logging.getLogger().handlers:
        logging.basicConfig(level=numeric_level, format="%(asctime)s %(name)s %(levelname)s %(message)s")

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
        global _storage, _instance_tracker, _user_db

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

        # Instance traffic tracker (application-level throughput)
        redis_client = None
        if backend_type.value == "redis":
            _backend = getattr(state_manager, "_backend", None)
            redis_client = getattr(_backend, "_client", None) if _backend else None
        resolved_ep = _resolve_endpoint(settings)
        _instance_tracker = InstanceTrafficTracker(
            host=settings.host,
            port=settings.port,
            redis_client=redis_client,
            endpoint=resolved_ep,
        )
        _instance_tracker.start()
        logger.info("Advertised endpoint: %s", resolved_ep)

        # User system
        if settings.user_system_enabled:
            _user_db = _create_user_db(settings)
            await _user_db.connect()
            app.state.user_db = _user_db
            logger.info("User system enabled (OIDC + %s)", settings.user_db_backend)

            # Reconcile storage_used with actual files on disk / state backend
            await _reconcile_storage_quotas(_storage, _user_db)

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
        global _instance_tracker, _user_db  # noqa: F824
        if _instance_tracker:
            await _instance_tracker.cleanup_redis()
            _instance_tracker.stop()
        if _user_db:
            await _user_db.disconnect()
        await close_state_manager()
        logger.info("Shutdown complete")

    # ── Proxies ──────────────────────────────────────────────
    storage_proxy = _StorageProxy()
    app.state.storage = storage_proxy
    tracker_proxy = _TrackerProxy()

    # ── Traffic counting middleware ──────────────────────────
    app.add_middleware(TrafficCounterMiddleware, tracker=tracker_proxy)

    # ── Register routes ──────────────────────────────────────
    tus_handler = TusHandler(storage_proxy, settings.max_upload_size)  # type: ignore[arg-type]
    app.include_router(tus_handler.get_router())

    app.include_router(create_files_router(storage_proxy))  # type: ignore[arg-type]

    app.include_router(
        create_info_router(
            storage_proxy,  # type: ignore[arg-type]
            tracker_proxy,  # type: ignore[arg-type]
            max_upload_size=settings.max_upload_size,
        )
    )

    app.include_router(create_auth_router(settings.auth_tokens))

    # ── Always parse role quotas (speed limits apply even without user system) ──
    from etransfer.server.auth.models import RoleQuota

    parsed_role_quotas: dict[str, RoleQuota] = {}
    for role_name, qdict in settings.role_quotas.items():
        if isinstance(qdict, dict):
            parsed_role_quotas[role_name] = RoleQuota(**qdict)
        else:
            parsed_role_quotas[role_name] = qdict
    app.state.parsed_role_quotas = parsed_role_quotas
    app.state.num_workers = settings.workers

    # User system routes (OIDC, roles, groups, quotas)
    if settings.user_system_enabled:
        from etransfer.server.auth.oauth import OIDCProvider
        from etransfer.server.auth.routes import create_user_router

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

    if settings.workers > 1:
        # Multi-worker mode: uvicorn needs an import string to fork workers.
        # Pass CLI overrides via env vars so each worker's create_app() picks
        # them up through load_server_settings() / pydantic env_prefix.
        import os

        if config_path is not None:
            os.environ["ETRANSFER_CONFIG"] = str(Path(config_path).resolve())
        if host is not None:
            os.environ["ETRANSFER_HOST"] = host
        if port is not None:
            os.environ["ETRANSFER_PORT"] = str(port)
        if storage_path is not None:
            os.environ["ETRANSFER_STORAGE_PATH"] = str(storage_path)
        if state_backend is not None:
            os.environ["ETRANSFER_STATE_BACKEND"] = state_backend
        if redis_url is not None:
            os.environ["ETRANSFER_REDIS_URL"] = redis_url

        uvicorn.run(
            "etransfer.server.main:app",
            host=settings.host,
            port=settings.port,
            workers=settings.workers,
        )
    else:
        app = create_app(settings)

        uvicorn.run(
            app,
            host=settings.host,
            port=settings.port,
        )


# For uvicorn command line: uvicorn etransfer.server.main:app
app = create_app()


if __name__ == "__main__":
    run_server()
