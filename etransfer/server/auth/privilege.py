"""Shared privilege checks used across routes/handlers.

Centralises the logic for deciding whether a request was made by a
privileged caller (static API-token holder or an admin user).  Keeping
this in one place avoids subtle divergences between the TUS handler and
the REST routes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from etransfer.common.constants import AUTH_HEADER

if TYPE_CHECKING:
    from fastapi import Request


def is_privileged_caller(request: "Request") -> bool:
    """Return True if the request is from a static API token or admin user.

    Privileged callers bypass certain policy restrictions (for example the
    ``retention.allow_permanent`` flag).  Regular session-based users are
    never considered privileged unless their role is ``admin``.

    Precedence:
        1. Static API token matches ``settings.auth_tokens`` → privileged.
        2. ``request.state.user`` is admin / ``is_admin`` → privileged.
        3. Auth entirely disabled (no tokens configured and no user) →
           treat as privileged (dev/test environments).
    """
    settings = getattr(request.app.state, "settings", None)
    active_tokens = set(settings.auth_tokens) if settings else set()
    api_token = request.headers.get(AUTH_HEADER, "")
    if api_token and api_token in active_tokens:
        return True

    user = getattr(request.state, "user", None)
    if user is not None:
        if getattr(user, "is_admin", False):
            return True
        if getattr(user, "role", "") == "admin":
            return True
        return False

    return not active_tokens


def enforce_retention_policy(request: "Request", retention: str) -> None:
    """Raise ``HTTPException(403)`` if policy disallows the requested retention.

    Currently enforces ``settings.allow_permanent_retention``: when False,
    non-privileged callers cannot request ``retention=permanent``.
    """
    from fastapi import HTTPException

    if retention != "permanent":
        return
    settings = getattr(request.app.state, "settings", None)
    allow = True
    if settings is not None:
        allow = bool(getattr(settings, "allow_permanent_retention", True))
    if allow:
        return
    if is_privileged_caller(request):
        return
    raise HTTPException(
        status_code=403,
        detail="Permanent retention is disabled by server policy",
    )
