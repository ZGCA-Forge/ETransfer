"""Application-level per-instance traffic tracker.

Counts actual ETransfer bytes (uploads via TUS PATCH, downloads via file
streaming) and computes rolling-average rates.  When a Redis backend is
available the tracker publishes its own stats and reads peer instances'
stats so that ``/api/info`` can expose a cluster-wide view.
"""

import asyncio
import json
import logging
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Optional

from etransfer.common.constants import TRAFFIC_MONITOR_INTERVAL, RedisKeys, RedisTTL

logger = logging.getLogger("etransfer.server.instance_traffic")

# Rolling-window parameters
_WINDOW_SECONDS = 10  # keep 10 s of samples
_SAMPLE_INTERVAL = TRAFFIC_MONITOR_INTERVAL  # 1 s between samples


@dataclass
class _Sample:
    """One second of accumulated byte counts."""

    timestamp: float
    upload_bytes: int = 0
    download_bytes: int = 0


class InstanceTrafficTracker:
    """Track application-level throughput for a single server instance.

    Thread-/task-safe: ``record_*`` may be called from any coroutine;
    the background loop publishes to Redis and rotates the sample window.
    """

    def __init__(
        self,
        host: str,
        port: int,
        redis_client: Any = None,
        endpoint: Optional[str] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.endpoint = endpoint or f"{host}:{port}"
        self._redis = redis_client
        self._pid = os.getpid()

        # Accumulator for the current second
        self._current_upload: int = 0
        self._current_download: int = 0

        # Rolling window of committed samples
        self._samples: deque[_Sample] = deque()

        # Lifetime counters
        self.total_bytes_sent: int = 0
        self.total_bytes_recv: int = 0

        self._running = False
        self._task: Optional[asyncio.Task] = None

    # ── Recording (called from middleware) ────────────────────

    def record_upload(self, nbytes: int) -> None:
        """Record *nbytes* received (upload / TUS PATCH)."""
        self._current_upload += nbytes
        self.total_bytes_recv += nbytes

    def record_download(self, nbytes: int) -> None:
        """Record *nbytes* sent (download)."""
        self._current_download += nbytes
        self.total_bytes_sent += nbytes

    # ── Rates ─────────────────────────────────────────────────

    def get_rates(self) -> tuple[float, float]:
        """Return (upload_rate, download_rate) in bytes/sec.

        Computed as the average over the rolling window.
        """
        now = time.monotonic()
        cutoff = now - _WINDOW_SECONDS
        total_up = 0
        total_down = 0
        count = 0
        for s in self._samples:
            if s.timestamp >= cutoff:
                total_up += s.upload_bytes
                total_down += s.download_bytes
                count += 1
        if count == 0:
            return 0.0, 0.0
        window = count * _SAMPLE_INTERVAL
        return total_up / window, total_down / window

    def get_snapshot(self) -> dict:
        """Return a dict suitable for JSON serialisation / API response."""
        up, down = self.get_rates()
        return {
            "endpoint": self.endpoint,
            "url": f"http://{self.endpoint}",
            "upload_rate": up,
            "download_rate": down,
            "bytes_sent": self.total_bytes_sent,
            "bytes_recv": self.total_bytes_recv,
        }

    # ── All endpoints (local + Redis peers) ───────────────────

    async def get_all_endpoints(self) -> list[dict]:
        """Return traffic snapshots for all known endpoints.

        If Redis is available, scans ``et:traffic:*`` keys.  Multiple
        workers sharing the same endpoint (same port, different PIDs)
        are **aggregated** into a single entry per endpoint.
        Otherwise returns only the local instance.
        """
        local = self.get_snapshot()
        if not self._redis:
            return [local]

        # Collect raw per-worker entries from Redis
        raw: list[dict] = []
        try:
            cursor = 0
            pattern = f"{RedisKeys.TRAFFIC_PREFIX}*"
            while True:
                cursor, keys = await self._redis.scan(cursor, match=pattern, count=100)
                if keys:
                    values = await self._redis.mget(*keys)
                    for val in values:
                        if val:
                            try:
                                raw.append(json.loads(val))
                            except (json.JSONDecodeError, TypeError):
                                pass
                if cursor == 0:
                    break
        except Exception:
            logger.debug("Redis scan failed, falling back to local only", exc_info=True)
            return [local]

        # Aggregate by endpoint (multiple workers → one entry)
        agg: dict[str, dict] = {}
        for entry in raw:
            ep = entry.get("endpoint", "")
            if not ep:
                continue
            if ep not in agg:
                agg[ep] = {
                    "endpoint": ep,
                    "url": entry.get("url", f"http://{ep}"),
                    "upload_rate": 0.0,
                    "download_rate": 0.0,
                    "bytes_sent": 0,
                    "bytes_recv": 0,
                }
            a = agg[ep]
            a["upload_rate"] += entry.get("upload_rate", 0)
            a["download_rate"] += entry.get("download_rate", 0)
            a["bytes_sent"] += entry.get("bytes_sent", 0)
            a["bytes_recv"] += entry.get("bytes_recv", 0)

        # Replace our own endpoint with the fresh local snapshot
        agg[self.endpoint] = local
        # Put self first
        results = [local]
        for ep, data in agg.items():
            if ep != self.endpoint:
                results.append(data)
        return results

    # ── Background loop ───────────────────────────────────────

    def start(self) -> None:
        """Start the background sample/publish loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())

    def stop(self) -> None:
        """Stop the background loop."""
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None

    async def _loop(self) -> None:
        while self._running:
            try:
                self._commit_sample()
                await self._publish_to_redis()
                await asyncio.sleep(_SAMPLE_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.debug("Instance traffic loop error", exc_info=True)
                await asyncio.sleep(_SAMPLE_INTERVAL)

    def _commit_sample(self) -> None:
        """Move current accumulators into a committed sample."""
        now = time.monotonic()
        self._samples.append(
            _Sample(
                timestamp=now,
                upload_bytes=self._current_upload,
                download_bytes=self._current_download,
            )
        )
        self._current_upload = 0
        self._current_download = 0

        # Trim old samples
        cutoff = now - _WINDOW_SECONDS - 1
        while self._samples and self._samples[0].timestamp < cutoff:
            self._samples.popleft()

    async def _publish_to_redis(self) -> None:
        """Write own snapshot to Redis with TTL.

        Key includes PID so each worker publishes independently.
        Multiple workers on the same endpoint are aggregated on read.
        """
        if not self._redis:
            return
        key = f"{RedisKeys.TRAFFIC_PREFIX}{self.endpoint}:{self._pid}"
        try:
            await self._redis.set(key, json.dumps(self.get_snapshot()), ex=RedisTTL.TRAFFIC)
        except Exception:
            logger.debug("Failed to publish traffic to Redis", exc_info=True)

    async def cleanup_redis(self) -> None:
        """Remove own key from Redis on shutdown."""
        if not self._redis:
            return
        key = f"{RedisKeys.TRAFFIC_PREFIX}{self.endpoint}:{self._pid}"
        try:
            await self._redis.delete(key)
        except Exception:
            pass
