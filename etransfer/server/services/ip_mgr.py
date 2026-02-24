"""Network interface and IP address management."""

import socket
from dataclasses import dataclass
from typing import Optional

import psutil


@dataclass
class InterfaceInfo:
    """Information about a network interface."""

    name: str
    ip_address: str
    is_up: bool = True
    speed_mbps: Optional[int] = None
    mac_address: Optional[str] = None
    netmask: Optional[str] = None
    is_loopback: bool = False


class IPManager:
    """Manage network interfaces and IP addresses.

    Detects available network interfaces and their IP addresses.
    Used to advertise server endpoints to clients.
    """

    def __init__(
        self,
        interfaces: Optional[list[str]] = None,
        prefer_ipv4: bool = True,
    ) -> None:
        """Initialize IP manager.

        Args:
            interfaces: List of interface names to use (None = auto-detect)
            prefer_ipv4: Prefer IPv4 addresses over IPv6
        """
        self.interfaces = interfaces
        self.prefer_ipv4 = prefer_ipv4
        self._cache: list[InterfaceInfo] = []
        self._refresh()

    def _refresh(self) -> None:
        """Refresh interface information."""
        self._cache = []

        try:
            addrs = psutil.net_if_addrs()
            stats = psutil.net_if_stats()
        except Exception:
            return

        for iface_name, iface_addrs in addrs.items():
            # Skip if interface list is specified and this isn't in it
            if self.interfaces and iface_name not in self.interfaces:
                continue

            # Get interface stats
            iface_stats = stats.get(iface_name)
            if not iface_stats:
                continue

            # Skip interfaces that are down
            if not iface_stats.isup:
                continue

            # Find IP address
            ip_addr = None
            mac_addr = None
            netmask = None
            is_loopback = False

            for addr in iface_addrs:
                # Get MAC address
                if addr.family == psutil.AF_LINK:
                    mac_addr = addr.address

                # Get IPv4 address (preferred)
                if addr.family == socket.AF_INET:
                    ip_addr = addr.address
                    netmask = addr.netmask

                    # Check for loopback
                    if ip_addr.startswith("127."):
                        is_loopback = True

                # Get IPv6 address if no IPv4 and IPv6 is acceptable
                if addr.family == socket.AF_INET6 and not ip_addr and not self.prefer_ipv4:
                    ip_addr = addr.address
                    netmask = addr.netmask

            # Skip if no IP found
            if not ip_addr:
                continue

            # Skip loopback unless specifically requested
            if is_loopback and not self.interfaces:
                continue

            # Skip link-local addresses
            if ip_addr.startswith("169.254."):
                continue

            self._cache.append(
                InterfaceInfo(
                    name=iface_name,
                    ip_address=ip_addr,
                    is_up=iface_stats.isup,
                    speed_mbps=iface_stats.speed if iface_stats.speed > 0 else None,
                    mac_address=mac_addr,
                    netmask=netmask,
                    is_loopback=is_loopback,
                )
            )

    def get_interfaces(self) -> list[InterfaceInfo]:
        """Get list of available interfaces.

        Returns:
            List of InterfaceInfo objects
        """
        self._refresh()
        return self._cache.copy()

    def get_interface(self, name: str) -> Optional[InterfaceInfo]:
        """Get information about a specific interface.

        Args:
            name: Interface name

        Returns:
            InterfaceInfo or None if not found
        """
        self._refresh()
        for iface in self._cache:
            if iface.name == name:
                return iface
        return None

    def get_ip_addresses(self) -> list[str]:
        """Get list of available IP addresses.

        Returns:
            List of IP address strings
        """
        self._refresh()
        return [iface.ip_address for iface in self._cache]

    def get_primary_ip(self) -> Optional[str]:
        """Get primary IP address.

        Attempts to determine the main network interface's IP.

        Returns:
            Primary IP address or None
        """
        self._refresh()

        if not self._cache:
            return None

        # Prefer non-virtual interfaces
        for iface in self._cache:
            # Skip virtual interfaces
            if iface.name.startswith(("docker", "veth", "br-", "virbr")):
                continue
            # Prefer eth/ens interfaces
            if iface.name.startswith(("eth", "ens", "enp")):
                return iface.ip_address

        # Return first available
        return self._cache[0].ip_address if self._cache else None

    def get_endpoints(self, port: int) -> list[str]:
        """Get list of server endpoints (IP:port).

        Args:
            port: Server port

        Returns:
            List of endpoint URLs
        """
        return [f"http://{ip}:{port}" for ip in self.get_ip_addresses()]

    def is_local_ip(self, ip: str) -> bool:
        """Check if an IP address is local.

        Args:
            ip: IP address to check

        Returns:
            True if IP is local to this machine
        """
        return ip in self.get_ip_addresses()
