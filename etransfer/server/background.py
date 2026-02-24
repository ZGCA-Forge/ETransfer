"""Background tasks for the EasyTransfer server."""

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Optional

from fastapi import FastAPI

from etransfer.server.config import ServerSettings, reload_hot_settings

if TYPE_CHECKING:
    from etransfer.server.tus.storage import TusStorage

logger = logging.getLogger("etransfer.server")


async def cleanup_loop(
    interval: int,
    get_storage: Any,
    get_user_db: Any,
) -> None:
    """Background task to cleanup expired uploads.

    Args:
        interval: Seconds between cleanup runs.
        get_storage: Callable returning the current TusStorage (or None).
        get_user_db: Callable returning the current UserDB (or None).
    """
    while True:
        try:
            await asyncio.sleep(interval)
            storage = get_storage()
            if storage:
                cleaned = await storage.cleanup_expired(user_db=get_user_db())
                if cleaned > 0:
                    logger.info("Cleaned up %d expired uploads", cleaned)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Cleanup error: %s", e)


def _get_config_mtime(settings: ServerSettings) -> Optional[float]:
    config_path = getattr(settings, "_config_path", None)
    if config_path and config_path.exists():
        return config_path.stat().st_mtime  # type: ignore[no-any-return]
    return None


async def config_watch_loop(
    app: FastAPI,
    propagate_fn: Any,
) -> None:
    """Background task that watches the config file for changes.

    Args:
        app: FastAPI application (settings stored in ``app.state.settings``).
        propagate_fn: Callable ``(app, settings, changes) -> None`` to apply changes.
    """
    settings: ServerSettings = app.state.settings
    interval = settings.config_watch_interval

    while True:
        try:
            await asyncio.sleep(interval)
            new_mtime = _get_config_mtime(settings)
            old_mtime = app.state.config_mtime

            if new_mtime and old_mtime and new_mtime != old_mtime:
                logger.info("Config file change detected, hot-reloading...")
                changes = reload_hot_settings(settings)
                if changes:
                    propagate_fn(app, settings, changes)
                    logger.info(
                        "Auto-reloaded %d field(s): %s",
                        len(changes),
                        ", ".join(changes.keys()),
                    )
                app.state.config_mtime = new_mtime

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Config watch error: %s", e)
