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


class TrackerProxy:
    """Proxy that resolves to the global InstanceTrafficTracker after startup."""

    def __init__(self, getter: Any) -> None:
        self._getter = getter

    def __getattr__(self, name: str) -> Any:
        target = self._getter()
        if target is None:
            raise RuntimeError("Instance tracker not initialized")
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
