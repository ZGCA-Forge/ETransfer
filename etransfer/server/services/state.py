"""State management service with pluggable backends."""

import asyncio
import logging
from contextlib import asynccontextmanager
from enum import Enum
from pathlib import Path
from typing import AsyncIterator, Optional

from etransfer.server.services.backends.file import FileStateBackend
from etransfer.server.services.backends.interface import StateBackend
from etransfer.server.services.backends.memory import MemoryStateBackend

logger = logging.getLogger("etransfer.server.state")


class BackendType(str, Enum):
    """Available state backend types."""

    MEMORY = "memory"
    FILE = "file"
    REDIS = "redis"


class StateManager:
    """State manager with pluggable backend support.

    Supports multiple backend types:
    - memory: In-memory storage (single process, dev/test)
    - file: File-based storage (persistence, single process)
    - redis: Redis storage (production, multi-process)

    Usage:
        # Using memory backend (default)
        manager = StateManager(BackendType.MEMORY)
        await manager.connect()

        # Using file backend
        manager = StateManager(BackendType.FILE, storage_path=Path("./data"))
        await manager.connect()

        # Using Redis backend
        manager = StateManager(BackendType.REDIS, redis_url="redis://localhost:6379")
        await manager.connect()
    """

    def __init__(
        self,
        backend_type: BackendType = BackendType.MEMORY,
        storage_path: Optional[Path] = None,
        redis_url: Optional[str] = None,
    ) -> None:
        """Initialize state manager.

        Args:
            backend_type: Type of backend to use
            storage_path: Path for file backend storage
            redis_url: URL for Redis backend
        """
        self.backend_type = backend_type
        self._backend: Optional[StateBackend] = None
        self._storage_path = storage_path or Path("./state_data")
        self._redis_url = redis_url or "redis://localhost:6379/0"

    def _create_backend(self) -> StateBackend:
        """Create backend instance based on type."""
        if self.backend_type == BackendType.MEMORY:
            return MemoryStateBackend()

        elif self.backend_type == BackendType.FILE:
            return FileStateBackend(self._storage_path / "state")

        elif self.backend_type == BackendType.REDIS:
            from etransfer.server.services.backends.redis import RedisStateBackend

            return RedisStateBackend(self._redis_url)

        else:
            raise ValueError(f"Unknown backend type: {self.backend_type}")

    async def connect(self) -> None:
        """Connect to the backend."""
        if self._backend is None:
            self._backend = self._create_backend()
            await self._backend.connect()
            logger.info("Using %s backend", self._backend.name)

    async def disconnect(self) -> None:
        """Disconnect from the backend."""
        if self._backend:
            await self._backend.disconnect()
            self._backend = None

    @property
    def backend(self) -> StateBackend:
        """Get backend instance."""
        if self._backend is None:
            raise RuntimeError("StateManager not connected. Call connect() first.")
        return self._backend

    @asynccontextmanager
    async def lock(self, key: str, timeout: int = 30) -> AsyncIterator[None]:
        """Acquire a distributed lock.

        Uses atomic set with nx=True for locking.

        Args:
            key: Lock key
            timeout: Lock timeout in seconds

        Yields:
            Lock context
        """
        lock_key = f"lock:{key}"
        acquired = False
        try:
            # Try to acquire lock with retries
            for _ in range(10):
                acquired = await self._backend.set(lock_key, "1", nx=True, ex=timeout)  # type: ignore[union-attr]
                if acquired:
                    break
                await asyncio.sleep(0.1)

            if not acquired:
                raise RuntimeError(f"Could not acquire lock: {key}")

            yield
        finally:
            if acquired:
                await self._backend.delete(lock_key)  # type: ignore[union-attr]

    async def get(self, key: str) -> Optional[str]:
        """Get value."""
        return await self._backend.get(key)  # type: ignore[union-attr]

    async def set(
        self,
        key: str,
        value: str,
        ex: Optional[int] = None,
        nx: bool = False,
    ) -> bool:
        """Set value."""
        return await self._backend.set(key, value, ex=ex, nx=nx)  # type: ignore[union-attr]

    async def delete(self, key: str) -> int:
        """Delete key."""
        return await self._backend.delete(key)  # type: ignore[union-attr]

    async def exists(self, key: str) -> bool:
        """Check if key exists."""
        return await self._backend.exists(key)  # type: ignore[union-attr]

    async def incr(self, key: str, amount: int = 1) -> int:
        """Increment value."""
        return await self._backend.incr(key, amount)  # type: ignore[union-attr]

    async def expire(self, key: str, seconds: int) -> bool:
        """Set key expiration."""
        return await self._backend.expire(key, seconds)  # type: ignore[union-attr]

    async def scan_keys(self, pattern: str) -> list[str]:
        """Scan keys matching pattern."""
        keys = []  # type: ignore[var-annotated]
        cursor = 0
        while True:
            cursor, batch = await self._backend.scan(cursor, match=pattern, count=100)  # type: ignore[union-attr]
            keys.extend(batch)
            if cursor == 0:
                break
        return keys

    async def ping(self) -> bool:
        """Check if backend is available."""
        return await self._backend.ping()  # type: ignore[union-attr]


# Global state manager instance
_state_manager: Optional[StateManager] = None


def create_state_manager(
    backend_type: BackendType = BackendType.FILE,
    storage_path: Optional[Path] = None,
    redis_url: Optional[str] = None,
) -> StateManager:
    """Create a new state manager instance.

    Args:
        backend_type: Type of backend to use
        storage_path: Path for file backend storage
        redis_url: URL for Redis backend

    Returns:
        StateManager instance
    """
    return StateManager(
        backend_type=backend_type,
        storage_path=storage_path,
        redis_url=redis_url,
    )


async def get_state_manager(
    backend_type: BackendType = BackendType.FILE,
    storage_path: Optional[Path] = None,
    redis_url: Optional[str] = None,
) -> StateManager:
    """Get or create global state manager instance.

    Args:
        backend_type: Type of backend to use
        storage_path: Path for file backend storage
        redis_url: URL for Redis backend

    Returns:
        StateManager instance
    """
    global _state_manager
    if _state_manager is None:
        _state_manager = create_state_manager(
            backend_type=backend_type,
            storage_path=storage_path,
            redis_url=redis_url,
        )
        await _state_manager.connect()
    return _state_manager


async def close_state_manager() -> None:
    """Close global state manager."""
    global _state_manager
    if _state_manager:
        await _state_manager.disconnect()
        _state_manager = None
