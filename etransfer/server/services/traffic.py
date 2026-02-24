"""Traffic monitoring service."""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Optional

import psutil

from etransfer.common.constants import TRAFFIC_MONITOR_INTERVAL


@dataclass
class InterfaceTraffic:
    """Traffic data for a network interface."""

    name: str
    bytes_sent: int = 0
    bytes_recv: int = 0
    upload_rate: float = 0.0  # bytes/sec
    download_rate: float = 0.0  # bytes/sec
    speed_mbps: int = 0  # interface speed in Mbps
    last_update: float = field(default_factory=time.time)
    prev_bytes_sent: int = 0
    prev_bytes_recv: int = 0

    @property
    def speed_bytes(self) -> float:
        """Interface speed in bytes/sec."""
        return self.speed_mbps * 1024 * 1024 / 8 if self.speed_mbps > 0 else 0

    @property
    def upload_load_percent(self) -> float:
        """Upload load as percentage of interface speed."""
        if self.speed_bytes <= 0:
            return 0.0
        return min(100.0, (self.upload_rate / self.speed_bytes) * 100)

    @property
    def download_load_percent(self) -> float:
        """Download load as percentage of interface speed."""
        if self.speed_bytes <= 0:
            return 0.0
        return min(100.0, (self.download_rate / self.speed_bytes) * 100)

    @property
    def total_load_percent(self) -> float:
        """Total load (upload + download) as percentage."""
        if self.speed_bytes <= 0:
            return 0.0
        total_rate = self.upload_rate + self.download_rate
        return min(100.0, (total_rate / self.speed_bytes) * 100)


class TrafficMonitor:
    """Monitor network traffic across interfaces.

    Tracks bytes sent/received and calculates transfer rates.
    """

    def __init__(
        self,
        interfaces: Optional[list[str]] = None,
        interval: float = TRAFFIC_MONITOR_INTERVAL,
    ) -> None:
        """Initialize traffic monitor.

        Args:
            interfaces: List of interface names to monitor (None = all)
            interval: Update interval in seconds
        """
        self.interfaces = interfaces
        self.interval = interval
        self._traffic: dict[str, InterfaceTraffic] = {}
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        """Start traffic monitoring in background."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._monitor_loop())

    def stop(self) -> None:
        """Stop traffic monitoring."""
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None

    async def _monitor_loop(self) -> None:
        """Background monitoring loop."""
        while self._running:
            try:
                self._update_traffic()
                await asyncio.sleep(self.interval)
            except asyncio.CancelledError:
                break
            except Exception:
                # Log error but continue monitoring
                await asyncio.sleep(self.interval)

    def _update_traffic(self) -> None:
        """Update traffic statistics."""
        try:
            counters = psutil.net_io_counters(pernic=True)
            if_stats = psutil.net_if_stats()
        except Exception:
            return

        current_time = time.time()

        for name, stats in counters.items():
            # Filter interfaces if specified
            if self.interfaces and name not in self.interfaces:
                continue

            # Skip loopback
            if name.startswith("lo"):
                continue

            # Get interface speed
            speed_mbps = 0
            if name in if_stats:
                speed_mbps = if_stats[name].speed

            if name not in self._traffic:
                self._traffic[name] = InterfaceTraffic(
                    name=name,
                    bytes_sent=stats.bytes_sent,
                    bytes_recv=stats.bytes_recv,
                    speed_mbps=speed_mbps,
                    prev_bytes_sent=stats.bytes_sent,
                    prev_bytes_recv=stats.bytes_recv,
                    last_update=current_time,
                )
            else:
                traffic = self._traffic[name]
                time_delta = current_time - traffic.last_update

                if time_delta > 0:
                    # Calculate rates
                    sent_delta = stats.bytes_sent - traffic.prev_bytes_sent
                    recv_delta = stats.bytes_recv - traffic.prev_bytes_recv

                    traffic.upload_rate = sent_delta / time_delta
                    traffic.download_rate = recv_delta / time_delta

                # Update counters
                traffic.bytes_sent = stats.bytes_sent
                traffic.bytes_recv = stats.bytes_recv
                traffic.speed_mbps = speed_mbps
                traffic.prev_bytes_sent = stats.bytes_sent
                traffic.prev_bytes_recv = stats.bytes_recv
                traffic.last_update = current_time

    def get_interface_traffic(self, interface: str) -> dict:
        """Get traffic data for a specific interface.

        Args:
            interface: Interface name

        Returns:
            Dict with traffic data including load percentages
        """
        # Update first if not running
        if not self._running:
            self._update_traffic()

        traffic = self._traffic.get(interface)
        if not traffic:
            return {
                "bytes_sent": 0,
                "bytes_recv": 0,
                "upload_rate": 0.0,
                "download_rate": 0.0,
                "speed_mbps": 0,
                "upload_load_percent": 0.0,
                "download_load_percent": 0.0,
                "total_load_percent": 0.0,
            }

        return {
            "bytes_sent": traffic.bytes_sent,
            "bytes_recv": traffic.bytes_recv,
            "upload_rate": traffic.upload_rate,
            "download_rate": traffic.download_rate,
            "speed_mbps": traffic.speed_mbps,
            "upload_load_percent": traffic.upload_load_percent,
            "download_load_percent": traffic.download_load_percent,
            "total_load_percent": traffic.total_load_percent,
        }

    def get_all_traffic(self) -> dict[str, dict]:
        """Get traffic data for all monitored interfaces.

        Returns:
            Dict mapping interface names to traffic data with load info
        """
        if not self._running:
            self._update_traffic()

        return {
            name: {
                "bytes_sent": t.bytes_sent,
                "bytes_recv": t.bytes_recv,
                "upload_rate": t.upload_rate,
                "download_rate": t.download_rate,
                "speed_mbps": t.speed_mbps,
                "upload_load_percent": t.upload_load_percent,
                "download_load_percent": t.download_load_percent,
                "total_load_percent": t.total_load_percent,
            }
            for name, t in self._traffic.items()
        }

    def get_best_interface(self, for_upload: bool = True) -> Optional[str]:
        """Get the interface with lowest load.

        Args:
            for_upload: If True, consider upload load; else download load

        Returns:
            Interface name with lowest load, or None
        """
        if not self._running:
            self._update_traffic()

        if not self._traffic:
            return None

        def get_load(t: InterfaceTraffic) -> float:
            return t.upload_load_percent if for_upload else t.download_load_percent

        best = min(self._traffic.values(), key=get_load)
        return best.name

    def get_total_traffic(self) -> dict:
        """Get total traffic across all interfaces.

        Returns:
            Dict with aggregated traffic data
        """
        if not self._running:
            self._update_traffic()

        total = {
            "bytes_sent": 0,
            "bytes_recv": 0,
            "upload_rate": 0.0,
            "download_rate": 0.0,
        }

        for traffic in self._traffic.values():
            total["bytes_sent"] += traffic.bytes_sent
            total["bytes_recv"] += traffic.bytes_recv
            total["upload_rate"] += traffic.upload_rate
            total["download_rate"] += traffic.download_rate

        return total

    def format_rate(self, rate: float) -> str:
        """Format transfer rate for display.

        Args:
            rate: Rate in bytes/sec

        Returns:
            Formatted string (e.g., "1.5 MB/s")
        """
        if rate < 1024:
            return f"{rate:.0f} B/s"
        elif rate < 1024 * 1024:
            return f"{rate / 1024:.1f} KB/s"
        elif rate < 1024 * 1024 * 1024:
            return f"{rate / (1024 * 1024):.1f} MB/s"
        else:
            return f"{rate / (1024 * 1024 * 1024):.2f} GB/s"
