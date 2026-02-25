"""Per-user async rate limiter for upload/download speed control.

Uses a token-bucket algorithm shared across all concurrent async tasks
for the same user within a single worker process.  The configured speed
limit is divided by the number of uvicorn workers so that the aggregate
throughput across all workers matches the configured limit.
"""

import asyncio
import logging
import time
from typing import Any, Dict, Optional

from fastapi import Request

from etransfer.common.constants import AUTH_HEADER

logger = logging.getLogger("etransfer.server.rate_limiter")


class AsyncRateLimiter:
    """Token-bucket rate limiter shared across concurrent async tasks."""

    __slots__ = ("_rate", "_tokens", "_last_refill", "_lock")

    def __init__(self, rate: float):
        """
        Args:
            rate: Maximum throughput in bytes per second.
        """
        self._rate = rate
        self._tokens = rate  # 1-second burst allowance
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def consume(self, nbytes: int) -> None:
        """Consume *nbytes* tokens, sleeping if the bucket is empty."""
        sleep_time = 0.0
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._last_refill = now
            # Refill tokens (cap at 1 second worth of burst)
            self._tokens = min(self._rate, self._tokens + elapsed * self._rate)
            self._tokens -= nbytes
            if self._tokens < 0:
                sleep_time = -self._tokens / self._rate
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)

    @property
    def rate(self) -> float:
        return self._rate


# Module-level stores — each worker process has its own copy.
_upload_limiters: Dict[Any, AsyncRateLimiter] = {}
_download_limiters: Dict[Any, AsyncRateLimiter] = {}


def get_user_key(request: Request) -> str:
    """Derive a rate-limiter key from the request context."""
    user = getattr(request.state, "user", None)
    if user and hasattr(user, "id"):
        return f"user:{user.id}"
    token = request.headers.get(AUTH_HEADER, "")
    if token:
        return f"token:{token[:16]}"
    return f"ip:{request.client.host}" if request.client else "anon"


def get_rate_limiter(
    direction: str,
    user_key: str,
    speed_limit: int,
    num_workers: int = 1,
) -> AsyncRateLimiter:
    """Get or create a shared rate limiter.

    The *speed_limit* is divided by *num_workers* so that the aggregate
    throughput across all worker processes matches the configured limit.

    Args:
        direction: ``"upload"`` or ``"download"``.
        user_key: Unique key identifying the user (from :func:`get_user_key`).
        speed_limit: Target speed limit in bytes/sec (total, before division).
        num_workers: Number of uvicorn worker processes.

    Returns:
        A shared :class:`AsyncRateLimiter` instance for the given user/direction.
    """
    store = _upload_limiters if direction == "upload" else _download_limiters
    if user_key not in store:
        per_worker = speed_limit // max(1, num_workers)
        store[user_key] = AsyncRateLimiter(per_worker)
        logger.debug(
            "Created %s rate limiter for %s: %.1f MB/s (total %.1f / %d workers)",
            direction,
            user_key,
            per_worker / 1024 / 1024,
            speed_limit / 1024 / 1024,
            num_workers,
        )
    return store[user_key]
