"""Admin API routes for config management."""

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request

from etransfer.common.constants import AUTH_HEADER
from etransfer.server.config import HOT_RELOADABLE_FIELDS, ServerSettings, reload_hot_settings

logger = logging.getLogger("etransfer.server")


def _safe_repr(value: Any) -> str:
    """Produce a safe string repr for change diffs (truncate long values)."""
    s = repr(value)
    return s[:200] + "..." if len(s) > 200 else s


def _require_admin_access(request: Request) -> None:
    """Check that the request has admin-level access."""
    settings: ServerSettings = request.app.state.settings

    # Accept any configured API token as admin
    api_token = request.headers.get(AUTH_HEADER, "")
    if api_token and api_token in settings.auth_tokens:
        return

    # Accept OIDC admin user
    user = getattr(request.state, "user", None)
    if user and (getattr(user, "is_admin", False) or getattr(user, "role", "") == "admin"):
        return

    raise HTTPException(403, "Admin access required")


def create_admin_router(propagate_fn: Any) -> APIRouter:
    """Create admin router for config management.

    Args:
        propagate_fn: Callable ``(app, settings, changes) -> None`` to apply hot-reloaded changes.

    Returns:
        FastAPI router with admin endpoints.
    """
    router = APIRouter(prefix="/api/admin", tags=["Admin"])

    @router.post("/reload-config")
    async def reload_config(request: Request) -> dict[str, Any]:
        """Reload hot-reloadable config fields from the config file.

        Requires admin authentication (API token or OIDC admin role).
        Returns a diff of changed fields and lists which fields are
        hot-reloadable vs require a restart.
        """
        _require_admin_access(request)

        settings: ServerSettings = request.app.state.settings
        changes = reload_hot_settings(settings)
        if changes:
            propagate_fn(request.app, settings, changes)
            from etransfer.server.background import _get_config_mtime

            request.app.state.config_mtime = _get_config_mtime(settings)
            change_summary = {k: {"old": _safe_repr(old), "new": _safe_repr(new)} for k, (old, new) in changes.items()}
            logger.info(
                "Hot-reloaded %d field(s): %s",
                len(changes),
                ", ".join(changes.keys()),
            )
        else:
            change_summary = {}

        return {  # type: ignore[no-any-return]
            "reloaded": bool(changes),
            "changes": change_summary,
            "hot_reloadable": sorted(HOT_RELOADABLE_FIELDS),
            "requires_restart": [
                "host",
                "port",
                "workers",
                "storage_path",
                "state_backend",
                "redis_url",
                "oidc_issuer_url",
                "oidc_client_id",
                "oidc_client_secret",
                "user_db_backend",
                "mysql_*",
            ],
            "note": (
                "host/port/workers 以及 OIDC、数据库等配置变更需要重启服务。"
                "无法在线增加新的监听 IP/端口——uvicorn 绑定在启动时确定。"
                "群组配额 (group quota) 存储在数据库中，通过 "
                "PUT /api/groups/{id}/quota 管理，天然即时生效。"
            ),
        }

    @router.get("/config-status")
    async def config_status(request: Request) -> dict[str, Any]:
        """Show which config file is loaded and watch status."""
        _require_admin_access(request)

        settings: ServerSettings = request.app.state.settings
        config_path = getattr(settings, "_config_path", None)
        return {
            "config_file": str(config_path.resolve()) if config_path else None,  # type: ignore[union-attr]
            "config_watch": settings.config_watch,
            "config_watch_interval": settings.config_watch_interval,
        }

    return router
