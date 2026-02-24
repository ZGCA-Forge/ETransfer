"""File-based state storage backend with persistence."""

import asyncio
import fnmatch
import json
import logging
import time
from pathlib import Path
from typing import Optional

import aiofiles  # type: ignore[import-untyped]

logger = logging.getLogger("etransfer.server.state")
import aiofiles.os  # type: ignore[import-untyped]

from etransfer.server.services.backends.interface import StateBackend


class FileStateBackend(StateBackend):
    """File-based state storage backend with persistence (DEFAULT).

    Stores state in JSON files for simple persistence across restarts.
    This is the default backend for EasyTransfer.

    Suitable for:
    - Default production deployments (single worker only)
    - Development with state preservation
    - Simple setups without external dependencies

    Limitations:
    - Single worker only — server refuses to start with workers>1
    - Slower than in-memory for high-frequency operations
    - For multi-worker, use RedisStateBackend instead

    Usage:
        backend = FileStateBackend(Path("/data/state"))
        await backend.connect()
        await backend.set("key", "value", ex=3600)
        value = await backend.get("key")
    """

    def __init__(self, storage_path: Path) -> None:
        """Initialize file backend.

        Args:
            storage_path: Directory to store state files
        """
        self.storage_path = Path(storage_path)
        self.state_file = self.storage_path / "state.json"
        self._data: dict[str, dict] = {}  # {key: {"value": str, "expires_at": float|None}}
        self._lock = asyncio.Lock()
        self._dirty = False
        self._save_task: Optional[asyncio.Task] = None

    async def connect(self) -> None:
        """Initialize the backend and load existing state."""
        self.storage_path.mkdir(parents=True, exist_ok=True)

        # Load existing state
        if self.state_file.exists():
            try:
                async with aiofiles.open(self.state_file, "r") as f:
                    content = await f.read()
                    self._data = json.loads(content) if content else {}
                logger.info("%s: Loaded %d keys from %s", self.name, len(self._data), self.state_file)
            except Exception as e:
                logger.warning("%s: Error loading state: %s, starting fresh", self.name, e)
                self._data = {}
        else:
            logger.info("%s: Initialized (file storage at %s)", self.name, self.storage_path)

        # Start periodic save task
        self._save_task = asyncio.create_task(self._save_loop())

    async def disconnect(self) -> None:
        """Save state and cleanup."""
        if self._save_task:
            self._save_task.cancel()
            try:
                await self._save_task
            except asyncio.CancelledError:
                pass

        # Final save
        await self._save_state()

    async def _save_loop(self) -> None:
        """Periodically save state to disk."""
        while True:
            try:
                await asyncio.sleep(5)  # Save every 5 seconds if dirty
                if self._dirty:
                    await self._save_state()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("%s: Error saving state: %s", self.name, e)

    async def _save_state(self) -> None:
        """Save current state to disk."""
        async with self._lock:
            if not self._dirty:
                return

            # Clean expired keys before saving
            now = time.time()
            self._data = {k: v for k, v in self._data.items() if v.get("expires_at") is None or v["expires_at"] > now}

            try:
                async with aiofiles.open(self.state_file, "w") as f:
                    await f.write(json.dumps(self._data, indent=2))
                self._dirty = False
            except Exception as e:
                logger.error("%s: Error writing state file: %s", self.name, e)

    def _is_expired(self, entry: dict) -> bool:
        """Check if an entry has expired."""
        expires_at = entry.get("expires_at")
        if expires_at is None:
            return False
        return time.time() > expires_at  # type: ignore[no-any-return]

    async def get(self, key: str) -> Optional[str]:
        """Get value by key."""
        async with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            if self._is_expired(entry):
                del self._data[key]
                self._dirty = True
                return None
            return entry["value"]  # type: ignore[no-any-return]

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
                if existing and not self._is_expired(existing):
                    return False

            entry = {"value": value, "expires_at": None}  # type: ignore[assignment]
            if ex is not None:
                entry["expires_at"] = time.time() + ex  # type: ignore[assignment]

            self._data[key] = entry
            self._dirty = True
            return True

    async def delete(self, key: str) -> int:
        """Delete a key."""
        async with self._lock:
            if key in self._data:
                del self._data[key]
                self._dirty = True
                return 1
            return 0

    async def exists(self, key: str) -> bool:
        """Check if key exists."""
        async with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return False
            if self._is_expired(entry):
                del self._data[key]
                self._dirty = True
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
                key for key, entry in self._data.items() if not self._is_expired(entry) and fnmatch.fnmatch(key, match)
            ]

            start = cursor
            end = min(start + count, len(all_keys))
            keys = all_keys[start:end]

            next_cursor = end if end < len(all_keys) else 0
            return next_cursor, keys

    async def incr(self, key: str, amount: int = 1) -> int:
        """Increment a numeric value."""
        async with self._lock:
            entry = self._data.get(key)
            if entry is None or self._is_expired(entry):
                new_value = amount
                self._data[key] = {"value": str(new_value), "expires_at": None}
            else:
                try:
                    current = int(entry["value"])
                except ValueError:
                    raise ValueError(f"Value at {key} is not an integer")
                new_value = current + amount
                entry["value"] = str(new_value)

            self._dirty = True
            return new_value

    async def expire(self, key: str, seconds: int) -> bool:
        """Set expiration on a key."""
        async with self._lock:
            entry = self._data.get(key)
            if entry is None or self._is_expired(entry):
                return False
            entry["expires_at"] = time.time() + seconds
            self._dirty = True
            return True

    async def ping(self) -> bool:
        """Check if backend is available."""
        return self.storage_path.exists()

    def get_stats(self) -> dict:
        """Get backend statistics."""
        return {
            "type": "file",
            "path": str(self.state_file),
            "keys": len(self._data),
        }
