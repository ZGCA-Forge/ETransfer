"""Lazy proxy objects for late-bound server singletons.

These proxies allow routers to be registered at import time while the
actual service instances are created during the ``startup`` event.
"""

from typing import Any


class StorageProxy:
    """Proxy that resolves to the global TusStorage after startup."""

    def __init__(self, getter: Any) -> None:
        self._getter = getter

    def __getattr__(self, name: str) -> Any:
        target = self._getter()
        if target is None:
            raise RuntimeError("Storage not initialized")
        return getattr(target, name)


class TrafficProxy:
    """Proxy that resolves to the global TrafficMonitor after startup."""

    def __init__(self, getter: Any) -> None:
        self._getter = getter

    def __getattr__(self, name: str) -> Any:
        target = self._getter()
        if target is None:
            raise RuntimeError("Traffic monitor not initialized")
        return getattr(target, name)


class IPProxy:
    """Proxy that resolves to the global IPManager after startup."""

    def __init__(self, getter: Any) -> None:
        self._getter = getter

    def __getattr__(self, name: str) -> Any:
        target = self._getter()
        if target is None:
            raise RuntimeError("IP manager not initialized")
        return getattr(target, name)


class UserDBProxy:
    """Proxy that resolves to the global UserDB after startup."""

    def __init__(self, getter: Any) -> None:
        self._getter = getter

    def __getattr__(self, name: str) -> Any:
        target = self._getter()
        if target is None:
            raise RuntimeError("UserDB not initialized")
        return getattr(target, name)
