"""In-memory state storage backend."""

import asyncio
import fnmatch
import logging
import time
from dataclasses import dataclass
from typing import Optional

from etransfer.server.services.backends.interface import StateBackend

logger = logging.getLogger("etransfer.server.state")


@dataclass
class MemoryEntry:
    """Entry in memory storage with optional expiration."""

    value: str
    expires_at: Optional[float] = None

    def is_expired(self) -> bool:
        """Check if entry has expired."""
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at


class MemoryStateBackend(StateBackend):
    """In-memory state storage backend.

    Suitable for:
    - Unit testing
    - Quick prototyping
    - Temporary sessions

    Limitations:
    - Data is lost on restart
    - Not suitable for multi-worker deployments
    - No persistence

    Usage:
        backend = MemoryStateBackend()
        await backend.connect()
        await backend.set("key", "value", ex=3600)
        value = await backend.get("key")
    """

    def __init__(self) -> None:
        """Initialize memory backend."""
        self._data: dict[str, MemoryEntry] = {}
        self._lock = asyncio.Lock()
        self._cleanup_task: Optional[asyncio.Task] = None

    async def connect(self) -> None:
        """Initialize the backend."""
        # Start cleanup task for expired keys
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("%s: Initialized (in-memory storage)", self.name)

    async def disconnect(self) -> None:
        """Cleanup the backend."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        self._data.clear()

    async def _cleanup_loop(self) -> None:
        """Periodically clean up expired keys."""
        while True:
            try:
                await asyncio.sleep(60)  # Cleanup every minute
                await self._cleanup_expired()
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def _cleanup_expired(self) -> None:
        """Remove expired keys."""
        async with self._lock:
            expired = [key for key, entry in self._data.items() if entry.is_expired()]
            for key in expired:
                del self._data[key]

    async def get(self, key: str) -> Optional[str]:
        """Get value by key."""
        async with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            if entry.is_expired():
                del self._data[key]
                return None
            return entry.value

    async def set(
        self,
        key: str,
        value: str,
        ex: Optional[int] = None,
        nx: bool = False,
    ) -> bool:
        """Set a key-value pair."""
        async with self._lock:
            # Check nx condition
            if nx:
                existing = self._data.get(key)
                if existing and not existing.is_expired():
                    return False

            expires_at = None
            if ex is not None:
                expires_at = time.time() + ex

            self._data[key] = MemoryEntry(value=value, expires_at=expires_at)
            return True

    async def delete(self, key: str) -> int:
        """Delete a key."""
        async with self._lock:
            if key in self._data:
                del self._data[key]
                return 1
            return 0

    async def exists(self, key: str) -> bool:
        """Check if key exists."""
        async with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return False
            if entry.is_expired():
                del self._data[key]
                return False
            return True

    async def scan(
        self,
        cursor: int,
        match: str = "*",
        count: int = 100,
    ) -> tuple[int, list[str]]:
        """Scan keys matching pattern."""
        async with self._lock:
            # Get all non-expired keys matching pattern
            all_keys = [
                key for key, entry in self._data.items() if not entry.is_expired() and fnmatch.fnmatch(key, match)
            ]

            # Simulate cursor-based pagination
            start = cursor
            end = min(start + count, len(all_keys))
            keys = all_keys[start:end]

            # Return 0 cursor if done
            next_cursor = end if end < len(all_keys) else 0
            return next_cursor, keys

    async def incr(self, key: str, amount: int = 1) -> int:
        """Increment a numeric value."""
        async with self._lock:
            entry = self._data.get(key)
            if entry is None or entry.is_expired():
                new_value = amount
                self._data[key] = MemoryEntry(value=str(new_value))
            else:
                try:
                    current = int(entry.value)
                except ValueError:
                    raise ValueError(f"Value at {key} is not an integer")
                new_value = current + amount
                entry.value = str(new_value)
            return new_value

    async def expire(self, key: str, seconds: int) -> bool:
        """Set expiration on a key."""
        async with self._lock:
            entry = self._data.get(key)
            if entry is None or entry.is_expired():
                return False
            entry.expires_at = time.time() + seconds
            return True

    async def ping(self) -> bool:
        """Check if backend is available."""
        return True

    def get_stats(self) -> dict:
        """Get backend statistics."""
        return {
            "type": "memory",
            "keys": len(self._data),
            "expired": sum(1 for e in self._data.values() if e.is_expired()),
        }
