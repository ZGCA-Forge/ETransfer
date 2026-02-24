"""Authentication middleware for FastAPI."""

import logging
import time
from typing import Any, Callable, Optional

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from etransfer.common.constants import AUTH_HEADER

logger = logging.getLogger("etransfer.server.auth")

# ── In-process session cache (1 worker per port) ─────────
_SESSION_CACHE_TTL = 60  # seconds


class _SessionCache:
    """Simple TTL cache for session token → (session, user) lookups.

    Avoids hitting MySQL on every PATCH request.  Safe for single-worker
    processes; each worker maintains its own cache.
    """

    __slots__ = ("_store", "_ttl")

    def __init__(self, ttl: int = _SESSION_CACHE_TTL) -> None:
        self._store: dict[str, tuple[float, Any, Any]] = {}
        self._ttl = ttl

    def get(self, token: str) -> Optional[tuple[Any, Any]]:
        entry = self._store.get(token)
        if entry is None:
            return None
        ts, session, user = entry
        if time.monotonic() - ts > self._ttl:
            del self._store[token]
            return None
        return session, user

    def put(self, token: str, session: Any, user: Any) -> None:
        self._store[token] = (time.monotonic(), session, user)

    def invalidate(self, token: str) -> None:
        self._store.pop(token, None)


class _CachedSession:
    """Lightweight stand-in for a DB session row (from L2 cache)."""

    __slots__ = ("token", "user_id")

    def __init__(self, d: dict) -> None:
        self.token = d["token"]
        self.user_id = d["user_id"]


class _CachedUser:
    """Lightweight stand-in for a DB user row (from L2 cache)."""

    __slots__ = ("id", "username", "display_name", "email", "role", "is_active", "is_admin", "storage_used")

    def __init__(self, d: dict) -> None:
        self.id = d["id"]
        self.username = d["username"]
        self.display_name = d.get("display_name", "")
        self.email = d.get("email", "")
        self.role = d.get("role", "user")
        self.is_active = d.get("is_active", True)
        self.is_admin = d.get("is_admin", False)
        self.storage_used = d.get("storage_used", 0)


class TokenAuthMiddleware(BaseHTTPMiddleware):
    """Token-based authentication middleware.

    Validates API tokens in the X-API-Token header.
    Also accepts OAuth session tokens via Authorization: Bearer <token>
    or X-Session-Token header when user system is enabled.
    """

    def __init__(
        self,
        app: Any,
        valid_tokens: list[str],
        exclude_paths: Optional[list[str]] = None,
        user_db: Any = None,
    ) -> None:
        super().__init__(app)
        self.valid_tokens = set(valid_tokens)
        self.user_db = user_db
        self._cache = _SessionCache()
        self.exclude_paths = exclude_paths or [
            "/api/health",
            "/api/info",
            "/api/users/login",
            "/api/users/callback",
            "/l/",
            "/docs",
            "/openapi.json",
            "/redoc",
        ]
        # Always exclude login-info (clients need this unauthenticated)
        if "/api/users/login-info" not in self.exclude_paths:
            self.exclude_paths.append("/api/users/login-info")

    @property
    def _active_tokens(self) -> set[str]:
        """Return the current set of valid tokens.

        Reads from ``app.state.settings`` when available so that
        hot-reloaded tokens take effect immediately.
        """
        return self.valid_tokens

    async def dispatch(self, request: Request, call_next: Callable) -> JSONResponse:  # type: ignore[override]
        """Process request and validate token."""
        path = request.url.path

        # Skip authentication for excluded paths
        for exclude in self.exclude_paths:
            if path.startswith(exclude):
                return await call_next(request)

        # Skip authentication for OPTIONS requests (CORS preflight)
        if request.method == "OPTIONS":
            return await call_next(request)

        # Resolve current tokens (may have been hot-reloaded)
        _settings = getattr(request.app.state, "settings", None)
        active_tokens = set(_settings.auth_tokens) if _settings else self.valid_tokens

        # Skip if no tokens configured and no user_db (auth disabled)
        if not active_tokens and not self.user_db:
            return await call_next(request)

        # Try X-API-Token header first (static token auth)
        api_token = request.headers.get(AUTH_HEADER)
        if api_token and api_token in active_tokens:
            return await call_next(request)

        # Try session token (OAuth user system)
        session_token = None
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            session_token = auth_header[7:]
        if not session_token:
            session_token = request.headers.get("X-Session-Token")
        # Also try X-API-Token as session token (CLI sends session here)
        if not session_token and api_token:
            session_token = api_token

        if session_token and self.user_db:
            # Fast path: check in-process cache first
            cached = self._cache.get(session_token)
            if cached is not None:
                session, user = cached
                request.state.user = user
                request.state.session = session
                return await call_next(request)

            # Slow path: query DB, then cache result
            try:
                session = await self.user_db.get_session(session_token)
                if session:
                    user = await self.user_db.get_user(session.user_id)
                    if user and user.is_active:
                        self._cache.put(session_token, session, user)
                        request.state.user = user
                        request.state.session = session
                        return await call_next(request)
            except Exception as exc:
                logger.exception("Session lookup failed: %s", exc)

        # If we have valid_tokens configured, require one
        if active_tokens or self.user_db:
            return JSONResponse(
                status_code=401,
                content={
                    "error": "Unauthorized",
                    "message": (f"Provide {AUTH_HEADER} header or " "Authorization: Bearer <session_token>"),
                },
            )

        return await call_next(request)
