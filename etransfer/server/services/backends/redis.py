"""Redis state storage backend."""

import logging
from typing import Optional

from etransfer.server.services.backends.interface import StateBackend

logger = logging.getLogger("etransfer.server.state")

# Redis is optional
try:
    import redis.asyncio as redis

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None  # type: ignore[assignment]


class RedisStateBackend(StateBackend):
    """Redis state storage backend.

    Production-ready backend for multi-worker deployments.

    Suitable for:
    - Production deployments
    - Multi-worker/multi-process setups
    - High availability requirements

    Requirements:
    - Redis server running
    - `redis` package installed: pip install redis

    Usage:
        backend = RedisStateBackend("redis://localhost:6379/0")
        await backend.connect()
        await backend.set("key", "value", ex=3600)
        value = await backend.get("key")
    """

    def __init__(self, redis_url: str = "redis://localhost:6379/0") -> None:
        """Initialize Redis backend.

        Args:
            redis_url: Redis connection URL
        """
        if not REDIS_AVAILABLE:
            raise ImportError("Redis package not installed. " "Install with: pip install redis")

        self.redis_url = redis_url
        self._client: Optional[redis.Redis] = None
        self._pool: Optional[redis.ConnectionPool] = None

    async def connect(self) -> None:
        """Connect to Redis."""
        self._pool = redis.ConnectionPool.from_url(
            self.redis_url,
            decode_responses=True,
            max_connections=20,
        )
        self._client = redis.Redis(connection_pool=self._pool)

        # Test connection
        await self._client.ping()  # type: ignore[misc]
        logger.info("%s: Connected to %s", self.name, self.redis_url)

    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self._client:
            await self._client.close()
            self._client = None
        if self._pool:
            await self._pool.disconnect()
            self._pool = None

    async def get(self, key: str) -> Optional[str]:
        """Get value by key."""
        return await self._client.get(key)  # type: ignore[no-any-return, union-attr]

    async def set(
        self,
        key: str,
        value: str,
        ex: Optional[int] = None,
        nx: bool = False,
    ) -> bool:
        """Set a key-value pair."""
        result = await self._client.set(key, value, ex=ex, nx=nx)  # type: ignore[union-attr]
        return result is not None

    async def delete(self, key: str) -> int:
        """Delete a key."""
        return await self._client.delete(key)  # type: ignore[no-any-return, union-attr]

    async def exists(self, key: str) -> bool:
        """Check if key exists."""
        return await self._client.exists(key) > 0  # type: ignore[no-any-return, union-attr]

    async def scan(
        self,
        cursor: int,
        match: str = "*",
        count: int = 100,
    ) -> tuple[int, list[str]]:
        """Scan keys matching pattern."""
        return await self._client.scan(cursor, match=match, count=count)  # type: ignore[no-any-return, union-attr]

    async def incr(self, key: str, amount: int = 1) -> int:
        """Increment a numeric value."""
        return await self._client.incrby(key, amount)  # type: ignore[no-any-return, union-attr]

    async def expire(self, key: str, seconds: int) -> bool:
        """Set expiration on a key."""
        return await self._client.expire(key, seconds)  # type: ignore[no-any-return, union-attr]

    async def ping(self) -> bool:
        """Check if Redis is available."""
        try:
            await self._client.ping()  # type: ignore[misc, union-attr]
            return True
        except Exception:
            return False

    def get_stats(self) -> dict:
        """Get backend statistics."""
        return {
            "type": "redis",
            "url": self.redis_url,
        }
