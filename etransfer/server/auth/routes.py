"""User system API routes with OIDC integration."""

import json
import secrets
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from etransfer.server.auth.db import UserDB
from etransfer.server.auth.models import GroupCreate, GroupPublic, GroupTable, Role, RoleQuota, UserPublic, UserTable
from etransfer.server.auth.oauth import OIDCProvider

# Load the login success HTML template once at import time.
# The template uses {username} and {token} as str.format() placeholders.
_TEMPLATE_DIR = Path(__file__).parent / "templates"
_LOGIN_SUCCESS_TEMPLATE = (_TEMPLATE_DIR / "login_success.html").read_text(encoding="utf-8")


def _group_to_public(g: GroupTable, member_count: int = 0) -> GroupPublic:
    """Convert GroupTable (with quota_json string) to GroupPublic."""
    quota_data = json.loads(g.quota_json) if g.quota_json else {}
    return GroupPublic(
        id=g.id,  # type: ignore[arg-type]
        name=g.name,
        description=g.description,
        quota=RoleQuota(**quota_data),
        member_count=member_count,
    )


def _derive_callback_url(request: Request) -> str:
    """Derive the OIDC callback URL from the incoming request's Host header.

    This is essential for SSH / remote scenarios where the configured
    callback_url might be ``localhost`` but the client accessed the
    server via a public IP or domain name.  The OIDC provider's
    redirect_uri must match, so the user should register the public
    URL in their OIDC provider settings.
    """
    host = request.headers.get("host") or request.url.netloc
    scheme = request.headers.get("x-forwarded-proto") or request.url.scheme
    return f"{scheme}://{host}/api/users/callback"


def create_user_router(
    user_db: UserDB,
    oidc: Optional[OIDCProvider],
    role_quotas: dict[str, RoleQuota],
) -> APIRouter:
    """Create user system API router.

    Args:
        user_db: User database instance
        oidc: OIDC provider (None if disabled)
        role_quotas: Role -> quota mapping from config
    """
    router = APIRouter(tags=["Users"])

    # ── Helpers ────────────────────────────────────────────────

    def _effective_callback_url(request: Request) -> str:
        """Return the callback URL to use for this request.

        If oidc.callback_url is explicitly configured (non-localhost prefix),
        use it.  Otherwise derive from the request Host header so that
        remote / SSH scenarios work out of the box.

        The fixed path ``/api/users/callback`` is always appended.
        """
        if oidc and oidc.callback_url:
            from urllib.parse import urlparse

            parsed = urlparse(oidc.callback_url)
            host = parsed.hostname or ""
            if host not in ("", "localhost", "127.0.0.1", "0.0.0.0"):  # nosec B104
                # callback_url already has the full path (set by main.py)
                return oidc.callback_url
        return _derive_callback_url(request)

    async def _get_current_user(request: Request) -> Optional[UserTable]:
        """Extract current user from session token.

        Accepts: Authorization: Bearer <token> or X-Session-Token header.
        """
        token = None
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
        if not token:
            token = request.headers.get("X-Session-Token")
        if not token:
            return None

        session = await user_db.get_session(token)
        if not session:
            return None

        user = await user_db.get_user(session.user_id)
        if user and not user.is_active:
            return None
        return user

    async def _require_user(request: Request) -> UserTable:
        user = await _get_current_user(request)
        if not user:
            raise HTTPException(401, "Authentication required. " "GET /api/users/login-info for login instructions.")
        return user

    async def _require_admin(request: Request) -> UserTable:
        user = await _require_user(request)
        if user.role != Role.ADMIN and not user.is_admin:
            raise HTTPException(403, "Admin access required")
        return user

    # ── Login info (server-driven config for clients) ──────────

    @router.get("/api/users/login-info")
    async def login_info() -> dict[str, Any]:
        """Return login configuration for clients.

        Clients call this to discover how to authenticate.
        The response includes the OIDC authorize URL that the client
        should open in a browser.
        """
        if not oidc:
            raise HTTPException(501, "OIDC authentication not configured")
        return oidc.get_login_info()

    # ── CLI login flow ─────────────────────────────────────────

    @router.post("/api/users/login/start")
    async def login_start(request: Request) -> dict[str, Any]:
        """Start a CLI login flow.

        Returns a unique state and the authorize URL.
        The CLI opens the URL in a browser, then polls
        GET /api/users/login/poll/{state} until login completes.

        The callback URL is auto-derived from the request Host header
        so that remote / SSH scenarios work without special config.
        """
        if not oidc:
            raise HTTPException(501, "OIDC authentication not configured")

        callback = _effective_callback_url(request)
        state = secrets.token_urlsafe(8)
        authorize_url = oidc.get_authorize_url(
            state=state,
            redirect_uri=callback,
        )
        await user_db.create_pending_login(
            state,
            redirect_uri=callback,
            authorize_url=authorize_url,
        )

        # Build a short redirect URL that 302s to the full authorize_url.
        # Use the callback_url's origin so the link is publicly reachable
        # (the nginx/frp that handles callback also proxies /l/*).
        from urllib.parse import urlparse

        cb_parsed = urlparse(callback)
        short_base = f"{cb_parsed.scheme}://{cb_parsed.netloc}"
        short_url = f"{short_base}/l/{state}"

        return {
            "state": state,
            "authorize_url": authorize_url,
            "short_url": short_url,
            "poll_endpoint": f"/api/users/login/poll/{state}",
            "callback_url": callback,
            "message": "Open authorize_url (or short_url) in your browser to complete login.",
        }

    @router.get("/api/users/login/poll/{state}")
    async def login_poll(state: str) -> dict[str, Any]:
        """Poll for CLI login completion.

        Returns completed=true and token once the OAuth callback fires.
        """
        pending = await user_db.get_pending_login(state)
        if not pending:
            raise HTTPException(404, "Login state not found or expired")

        if pending.completed and pending.session_token:
            session = await user_db.get_session(pending.session_token)
            if session:
                user = await user_db.get_user(session.user_id)
                return {
                    "completed": True,
                    "token": pending.session_token,
                    "expires_at": (session.expires_at.isoformat() if session.expires_at else None),
                    "user": (await _user_to_public(user)).model_dump() if user else None,
                }

        return {"completed": False}

    # ── Short-link redirect (avoids long OAuth URLs in CLI) ────

    @router.get("/l/{state}")
    async def login_go(state: str) -> RedirectResponse:
        """Redirect to the full OIDC authorize URL for a pending login.

        This provides a short, copy-friendly URL for CLI users.
        The link expires 10 minutes after creation.
        """
        pending = await user_db.get_pending_login(state)
        if not pending or not pending.authorize_url:
            raise HTTPException(404, "Login link not found or expired")
        if datetime.utcnow() - pending.created_at > timedelta(minutes=10):
            raise HTTPException(410, "Login link has expired (10 min TTL)")
        return RedirectResponse(pending.authorize_url)

    # ── OAuth redirect login (browser) ─────────────────────────

    @router.get("/api/users/login")
    async def login_redirect(request: Request) -> RedirectResponse:
        """Redirect browser to OIDC provider login page."""
        if not oidc:
            raise HTTPException(501, "OIDC authentication not configured")
        callback = _effective_callback_url(request)
        state = secrets.token_urlsafe(24)
        await user_db.create_pending_login(state, redirect_uri=callback)
        url = oidc.get_authorize_url(state=state, redirect_uri=callback)
        return RedirectResponse(url)

    # ── OAuth callback ─────────────────────────────────────────

    @router.get("/api/users/callback")
    async def oauth_callback(
        request: Request,
        code: str,
        state: Optional[str] = None,
    ) -> HTMLResponse:
        """Handle OIDC callback.

        Exchanges code for token, fetches user profile + groups,
        creates/updates local user, returns session token.

        If there's a matching pending_login (CLI flow), it
        completes it so the CLI poll picks up the token.
        """
        if not oidc:
            raise HTTPException(501, "OIDC authentication not configured")

        # Determine the redirect_uri used in the authorize request.
        # Must match exactly for code exchange to succeed.
        redirect_uri = None
        if state:
            pending = await user_db.get_pending_login(state)
            if pending and pending.redirect_uri:
                redirect_uri = pending.redirect_uri
        if not redirect_uri:
            redirect_uri = _effective_callback_url(request)

        # Exchange code for access token
        try:
            token_data = await oidc.exchange_code(code, redirect_uri=redirect_uri)
        except Exception as e:
            raise HTTPException(400, f"OIDC token exchange failed: {e}")

        access_token = token_data.get("access_token")
        if not access_token:
            raise HTTPException(400, "No access_token in OIDC response")

        # Fetch user profile
        try:
            userinfo = await oidc.get_user_info(access_token)
        except Exception as e:
            raise HTTPException(400, f"Failed to fetch user profile: {e}")

        # Extract standard OIDC claims
        oidc_sub = str(userinfo.get("sub") or userinfo.get("id") or "")
        if not oidc_sub:
            raise HTTPException(400, "No user ID (sub) in OIDC response")

        username = userinfo.get("preferred_username") or userinfo.get("name") or userinfo.get("displayName") or oidc_sub
        display_name = userinfo.get("displayName") or userinfo.get("name")
        email = userinfo.get("email")
        avatar_url = userinfo.get("picture") or userinfo.get("avatar") or userinfo.get("permanentAvatar")
        is_admin = bool(userinfo.get("isAdmin", False))

        # Extract groups (provider-specific: array of strings or objects)
        groups_raw = userinfo.get("groups") or []
        if isinstance(groups_raw, list):
            group_names = []
            for g in groups_raw:
                if isinstance(g, str):
                    group_names.append(g)
                elif isinstance(g, dict):
                    group_names.append(g.get("name", str(g.get("id", ""))))
        else:
            group_names = []

        # Upsert user and sync groups
        user = await user_db.upsert_user(
            oidc_sub=oidc_sub,
            username=username,
            display_name=display_name,
            email=email,
            avatar_url=avatar_url,
            is_admin=is_admin,
            groups=group_names,
        )

        # Create session
        session = await user_db.create_session(user.id)  # type: ignore[arg-type]

        # If this is a CLI-initiated login, complete the pending login.
        # Re-fetch pending (may have been fetched above for redirect_uri,
        # but we need a fresh check for the completion flag).
        if state:
            pending_login = await user_db.get_pending_login(state)
            if pending_login and not pending_login.completed:
                await user_db.complete_pending_login(state, session.token)

        # Return HTML page with token (for browser-based flow)
        html = _LOGIN_SUCCESS_TEMPLATE.replace("{username}", user.username).replace("{token}", session.token)
        return HTMLResponse(html)

    # ── Session / profile ──────────────────────────────────────

    @router.get("/api/users/me")
    async def get_me(request: Request) -> UserPublic:
        """Get current user's profile and effective quota."""
        user = await _require_user(request)
        _rq = getattr(request.app.state, "parsed_role_quotas", role_quotas)
        quota = await user_db.get_effective_quota(user, _rq)
        pub = await _user_to_public(user)
        pub.effective_quota = quota
        return pub

    @router.post("/api/users/logout")
    async def logout(request: Request) -> dict[str, Any]:
        """Invalidate current session."""
        token = _extract_token(request)
        if token:
            await user_db.delete_session(token)
        return {"message": "Logged out"}

    # ── Admin: user management ─────────────────────────────────

    @router.get("/api/users")
    async def list_users(request: Request) -> dict[str, Any]:
        """List all users (admin only)."""
        await _require_admin(request)
        users = await user_db.list_users()
        pub_list = []
        for u in users:
            pub_list.append((await _user_to_public(u)).model_dump())
        return {"users": pub_list}

    @router.put("/api/users/{user_id}/role")
    async def set_role(user_id: int, role: str, request: Request) -> dict[str, Any]:
        """Set a user's role (admin only)."""
        await _require_admin(request)
        if role not in [r.value for r in Role]:
            raise HTTPException(400, f"Invalid role: {role}. Use: guest, user, admin")
        user = await user_db.set_user_role(user_id, role)
        if not user:
            raise HTTPException(404, "User not found")
        return {"message": f"Role set to {role}", "user_id": user_id}

    @router.put("/api/users/{user_id}/active")
    async def set_active(user_id: int, active: bool, request: Request) -> dict[str, Any]:
        """Enable/disable a user (admin only)."""
        await _require_admin(request)
        user = await user_db.set_user_active(user_id, active)
        if not user:
            raise HTTPException(404, "User not found")
        if not active:
            await user_db.delete_user_sessions(user_id)
        return {
            "message": f"User {'enabled' if active else 'disabled'}",
            "user_id": user_id,
        }

    # ── Admin: group management ────────────────────────────────

    @router.get("/api/groups")
    async def list_groups(request: Request) -> dict[str, Any]:
        """List all groups and their quotas."""
        await _require_user(request)
        groups = await user_db.list_groups()
        result = []
        for g in groups:
            count = await user_db.get_group_member_count(g.id)  # type: ignore[arg-type]
            result.append(_group_to_public(g, count))
        return {"groups": [g.model_dump() for g in result]}

    @router.post("/api/groups")
    async def create_group(body: GroupCreate, request: Request) -> dict[str, Any]:
        """Create a group with quota config (admin only).

        Groups are normally synced from the OIDC provider on login.
        This endpoint allows admins to create groups ahead of time
        or set quotas for groups that don't exist yet.
        """
        await _require_admin(request)
        existing = await user_db.get_group_by_name(body.name)
        if existing:
            raise HTTPException(409, f"Group '{body.name}' already exists")
        quota = RoleQuota(
            max_storage_size=body.max_storage_size,
            max_upload_size=body.max_upload_size,
            upload_speed_limit=body.upload_speed_limit,
            download_speed_limit=body.download_speed_limit,
            default_retention=body.default_retention,
            default_retention_ttl=body.default_retention_ttl,
        )
        group = await user_db.ensure_group(body.name, body.description)
        group = await user_db.update_group_quota(group.id, quota)  # type: ignore[assignment, arg-type]
        return {"message": "Group created", "group": _group_to_public(group).model_dump()}

    @router.put("/api/groups/{group_id}/quota")
    async def update_group_quota(group_id: int, body: GroupCreate, request: Request) -> dict[str, Any]:
        """Update a group's quota (admin only)."""
        await _require_admin(request)
        quota = RoleQuota(
            max_storage_size=body.max_storage_size,
            max_upload_size=body.max_upload_size,
            upload_speed_limit=body.upload_speed_limit,
            download_speed_limit=body.download_speed_limit,
            default_retention=body.default_retention,
            default_retention_ttl=body.default_retention_ttl,
        )
        group = await user_db.update_group_quota(group_id, quota)
        if not group:
            raise HTTPException(404, "Group not found")
        return {"message": "Quota updated", "group": _group_to_public(group).model_dump()}

    @router.delete("/api/groups/{group_id}")
    async def delete_group(group_id: int, request: Request) -> dict[str, Any]:
        """Delete a group (admin only)."""
        await _require_admin(request)
        await user_db.delete_group(group_id)
        return {"message": "Group deleted"}

    @router.post("/api/groups/{group_id}/members/{user_id}")
    async def add_member(group_id: int, user_id: int, request: Request) -> dict[str, Any]:
        """Add a user to a group (admin only)."""
        await _require_admin(request)
        group = await user_db.get_group(group_id)
        if not group:
            raise HTTPException(404, "Group not found")
        user = await user_db.get_user(user_id)
        if not user:
            raise HTTPException(404, "User not found")
        await user_db.add_user_to_group(user_id, group_id)
        return {"message": f"User {user.username} added to group {group.name}"}

    @router.delete("/api/groups/{group_id}/members/{user_id}")
    async def remove_member(group_id: int, user_id: int, request: Request) -> dict[str, Any]:
        """Remove a user from a group (admin only)."""
        await _require_admin(request)
        await user_db.remove_user_from_group(user_id, group_id)
        return {"message": "User removed from group"}

    # ── Quota check endpoint ───────────────────────────────────

    @router.get("/api/users/me/quota")
    async def get_my_quota(request: Request) -> dict[str, Any]:
        """Get current user's effective quota and usage."""
        user = await _require_user(request)
        _rq = getattr(request.app.state, "parsed_role_quotas", role_quotas)
        quota = await user_db.get_effective_quota(user, _rq)

        usage_pct = None
        if quota.max_storage_size:
            usage_pct = round((user.storage_used / quota.max_storage_size) * 100, 2)

        return {
            "storage_used": user.storage_used,
            "quota": quota.model_dump(),
            "usage_percent": usage_pct,
            "is_over_quota": (user.storage_used >= quota.max_storage_size if quota.max_storage_size else False),
        }

    # ── Internal helpers ───────────────────────────────────────

    async def _user_to_public(user: UserTable, groups: Optional[list[str]] = None) -> UserPublic:
        if groups is None:
            groups = await user_db.get_user_group_names(user.id)  # type: ignore[arg-type]
        role_val = user.role.value if hasattr(user.role, "value") else user.role
        return UserPublic(
            id=user.id,  # type: ignore[arg-type]
            username=user.username,
            display_name=user.display_name,
            email=user.email,
            avatar_url=user.avatar_url,
            role=role_val,
            is_active=user.is_active,
            is_admin=user.is_admin,
            storage_used=user.storage_used,
            groups=groups,
        )

    def _extract_token(request: Request) -> Optional[str]:
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            return auth_header[7:]
        return request.headers.get("X-Session-Token")

    return router
