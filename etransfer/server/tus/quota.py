"""Per-user quota reservation service backed by StateManager.

On CREATE: reserve full file size in Redis (INCRBY).
On PATCH:  no DB quota check — reservation already covers it.
On finalize: commit to DB (storage_used += size), release reservation.
On delete:   release reservation, revert DB if already committed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from etransfer.common.constants import RedisKeys, RedisTTL

if TYPE_CHECKING:
    from etransfer.server.services.state import StateManager


class QuotaService:
    """Manages per-user quota reservations via the state backend (Redis).

    Accepts a *storage* proxy so that the actual ``StateManager`` is resolved
    lazily at call time (the real storage is only available after startup).
    """

    def __init__(self, storage: Any) -> None:
        self._storage = storage

    @property
    def _state(self) -> "StateManager":
        return self._storage.state  # type: ignore[return-value]

    @staticmethod
    def _key(user_id: int) -> str:
        return f"{RedisKeys.QUOTA_PREFIX}{user_id}"

    async def get_reserved(self, user_id: int) -> int:
        """Get total bytes reserved in the state backend for *user_id*."""
        val = await self._state.get(self._key(user_id))
        return int(val) if val else 0

    async def reserve(self, user_id: int, size: int) -> None:
        """Reserve *size* bytes for an upload (atomic INCRBY)."""
        key = self._key(user_id)
        await self._state.incr(key, size)
        await self._state.expire(key, RedisTTL.QUOTA)

    async def release(self, user_id: int, size: int) -> None:
        """Release *size* reserved bytes (atomic DECRBY, floor at 0)."""
        key = self._key(user_id)
        await self._state.incr(key, -size)
        # Floor at 0 to avoid negative drift
        val = await self._state.get(key)
        if val and int(val) < 0:
            await self._state.set(key, "0", ex=RedisTTL.QUOTA)
        else:
            await self._state.expire(key, RedisTTL.QUOTA)
