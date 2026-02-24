#!/usr/bin/env python3
"""Test script for the user system (OIDC + roles + groups + quotas).

Usage:
    python scripts/test_user_system.py

This tests the user system WITHOUT requiring a real OIDC provider:
  1. Starts a server with user_system_enabled
  2. Directly creates users/sessions in the DB via SQLModel for testing
  3. Tests role-based quotas, group quotas, admin endpoints
  4. Tests that session tokens work for TUS upload authentication
  5. Tests the login-info endpoint (server-driven config)
  6. Tests the CLI login flow (pending login / poll)

For real OIDC testing (e.g. with Casdoor, Keycloak, Auth0):
  - Configure OIDC credentials in config.yaml
  - Start server: etransfer server start --config config.yaml
  - Run: etransfer setup localhost:8765 && etransfer login
"""

import asyncio
import base64
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

SERVER_PORT = 8765
BASE_URL = f"http://127.0.0.1:{SERVER_PORT}"
API_TOKEN = "admin-static-token"
OIDC_ISSUER = "https://ca.auth.test.example.com"


def format_size(n):
    for u in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} TB"


# ── Server lifecycle ──────────────────────────────────────────


def start_server(storage_path):
    env = os.environ.copy()
    env.update(
        {
            "ETRANSFER_PORT": str(SERVER_PORT),
            "ETRANSFER_STATE_BACKEND": "memory",
            "ETRANSFER_AUTH_TOKENS": json.dumps([API_TOKEN]),
            "ETRANSFER_STORAGE_PATH": storage_path,
            "ETRANSFER_USER_SYSTEM_ENABLED": "true",
            "ETRANSFER_OIDC_ISSUER_URL": OIDC_ISSUER,
            "ETRANSFER_OIDC_CLIENT_ID": "test-client-id",
            "ETRANSFER_OIDC_CLIENT_SECRET": "test-secret",
            "ETRANSFER_OIDC_CALLBACK_URL": f"{BASE_URL}/api/users/callback",
            "ETRANSFER_USER_DB_PATH": str(Path(storage_path) / "users.db"),
            "ETRANSFER_ROLE_QUOTAS": json.dumps(
                {
                    "admin": {
                        "max_storage_size": None,
                        "max_upload_size": None,
                        "upload_speed_limit": None,
                        "download_speed_limit": None,
                    },
                    "user": {
                        "max_storage_size": 50 * 1024 * 1024,  # 50 MB
                        "max_upload_size": 20 * 1024 * 1024,  # 20 MB
                        "upload_speed_limit": None,
                        "download_speed_limit": None,
                    },
                    "guest": {
                        "max_storage_size": 10 * 1024 * 1024,  # 10 MB
                        "max_upload_size": 5 * 1024 * 1024,  # 5 MB
                        "upload_speed_limit": 1 * 1024 * 1024,  # 1 MB/s
                        "download_speed_limit": 1 * 1024 * 1024,
                    },
                }
            ),
        }
    )
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "etransfer.server.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(SERVER_PORT),
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    for _ in range(30):
        time.sleep(0.5)
        try:
            r = httpx.get(f"{BASE_URL}/api/health", timeout=2)
            if r.status_code == 200:
                return proc
        except Exception:
            pass
    proc.kill()
    raise RuntimeError("Server failed to start")


def stop_server(proc):
    if proc:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


# ── Direct DB helpers (simulate OIDC without real provider) ───


async def create_test_user(db_path, oidc_sub, username, role="user", groups=None):
    """Create a user and session directly in the DB for testing."""
    from etransfer.server.auth.db import UserDB

    db = UserDB(f"sqlite+aiosqlite:///{db_path}")
    await db.connect()

    user = await db.upsert_user(
        oidc_sub=oidc_sub,
        username=username,
        display_name=f"Test {username}",
        email=f"{username}@test.com",
        avatar_url=f"https://example.com/{username}.png",
        is_admin=(role == "admin"),
        groups=groups,
    )

    if role != "user" and role != "admin":
        await db.set_user_role(user.id, role)
        user = await db.get_user(user.id)

    session = await db.create_session(user.id)
    await db.disconnect()

    return user, session.token


# ── Test functions ────────────────────────────────────────────


def test_login_info():
    """Test that login-info endpoint returns OIDC config."""
    print("\n" + "=" * 60)
    print("  TEST: Login info (server-driven config)")
    print("=" * 60)

    r = httpx.get(f"{BASE_URL}/api/users/login-info")
    assert r.status_code == 200, f"Expected 200, got {r.status_code}: {r.text}"
    data = r.json()
    assert data["provider"] == "oidc"
    assert data["issuer"] == OIDC_ISSUER
    assert "client_id" in data
    assert "authorize_url" in data
    assert "callback_url" in data
    print(f"  Provider: {data['provider']}")
    print(f"  Issuer: {data['issuer']}")
    print(f"  Authorize URL: {data['authorize_url'][:60]}...")
    print("\n  PASSED: Login info")


def test_cli_login_flow():
    """Test the CLI login start/poll flow."""
    print("\n" + "=" * 60)
    print("  TEST: CLI login flow (start + poll)")
    print("=" * 60)

    # Start login
    r = httpx.post(f"{BASE_URL}/api/users/login/start")
    assert r.status_code == 200, f"Start failed: {r.status_code} {r.text}"
    data = r.json()
    state = data["state"]
    assert "authorize_url" in data
    assert "poll_endpoint" in data
    print(f"  Login started: state={state[:16]}...")
    print(f"  Authorize URL: {data['authorize_url'][:60]}...")

    # Poll (should be incomplete)
    r = httpx.get(f"{BASE_URL}/api/users/login/poll/{state}")
    assert r.status_code == 200
    assert r.json()["completed"] is False
    print("  Poll (before callback): completed=false")

    # Invalid state
    r = httpx.get(f"{BASE_URL}/api/users/login/poll/nonexistent-state")
    assert r.status_code == 404
    print("  Invalid state: 404")

    print("\n  PASSED: CLI login flow")


def test_user_endpoints(admin_token, user_token, guest_token):
    """Test user API endpoints."""
    print("\n" + "=" * 60)
    print("  TEST: User API endpoints")
    print("=" * 60)

    ha = {"Authorization": f"Bearer {admin_token}"}
    hu = {"Authorization": f"Bearer {user_token}"}
    hg = {"Authorization": f"Bearer {guest_token}"}

    # /api/users/me
    r = httpx.get(f"{BASE_URL}/api/users/me", headers=ha)
    assert r.status_code == 200, f"GET /me failed: {r.status_code} {r.text}"
    me = r.json()
    assert me["role"] == "admin"
    assert me["is_admin"] is True
    assert me["effective_quota"]["max_storage_size"] is None
    print(f"  Admin: {me['username']}, role={me['role']}, quota=unlimited")

    r = httpx.get(f"{BASE_URL}/api/users/me", headers=hu)
    me = r.json()
    assert me["role"] == "user"
    assert me["effective_quota"]["max_storage_size"] == 50 * 1024 * 1024
    print(
        f"  User:  {me['username']}, role={me['role']}, "
        f"quota={format_size(me['effective_quota']['max_storage_size'])}"
    )

    r = httpx.get(f"{BASE_URL}/api/users/me", headers=hg)
    me = r.json()
    assert me["role"] == "guest"
    assert me["effective_quota"]["max_storage_size"] == 10 * 1024 * 1024
    print(
        f"  Guest: {me['username']}, role={me['role']}, "
        f"quota={format_size(me['effective_quota']['max_storage_size'])}"
    )

    # Unauthenticated
    r = httpx.get(f"{BASE_URL}/api/users/me")
    assert r.status_code == 401
    print("  Unauthenticated -> 401")

    # Admin lists users
    r = httpx.get(f"{BASE_URL}/api/users", headers=ha)
    assert r.status_code == 200
    users = r.json()["users"]
    assert len(users) >= 3, f"Expected at least 3 users, got {len(users)}"
    print(f"  Admin lists users: {len(users)} users")

    # Non-admin cannot list users
    r = httpx.get(f"{BASE_URL}/api/users", headers=hu)
    assert r.status_code == 403
    print("  Non-admin list users -> 403")

    print("\n  PASSED: User endpoints")


def test_group_management(admin_token, user_token):
    """Test group CRUD and membership."""
    print("\n" + "=" * 60)
    print("  TEST: Group management")
    print("=" * 60)

    ha = {
        "Authorization": f"Bearer {admin_token}",
        "Content-Type": "application/json",
    }
    hu = {"Authorization": f"Bearer {user_token}"}

    # Create a group with generous quota (admin pre-configures quota)
    r = httpx.post(
        f"{BASE_URL}/api/groups",
        headers=ha,
        json={
            "name": "vip",
            "description": "VIP users with extra quota",
            "max_storage_size": 200 * 1024 * 1024,  # 200 MB
            "max_upload_size": 100 * 1024 * 1024,  # 100 MB
        },
    )
    assert r.status_code == 200, f"Create group failed: {r.text}"
    group = r.json()["group"]
    group_id = group["id"]
    print(f"  Created group 'vip' (id={group_id}), quota={format_size(200*1024*1024)}")

    # Create a restricted group
    r = httpx.post(
        f"{BASE_URL}/api/groups",
        headers=ha,
        json={
            "name": "restricted",
            "description": "Restricted access",
            "max_storage_size": 5 * 1024 * 1024,
        },
    )
    assert r.status_code == 200

    # List groups
    r = httpx.get(f"{BASE_URL}/api/groups", headers=hu)
    assert r.status_code == 200
    groups = r.json()["groups"]
    assert len(groups) >= 2
    print(f"  Listed {len(groups)} groups")

    # Get user ID
    r = httpx.get(f"{BASE_URL}/api/users/me", headers=hu)
    user_id = r.json()["id"]

    # Add user to VIP group
    r = httpx.post(f"{BASE_URL}/api/groups/{group_id}/members/{user_id}", headers=ha)
    assert r.status_code == 200
    print(f"  Added user (id={user_id}) to 'vip' group")

    # Check effective quota (should use group's 200MB, not role's 50MB)
    r = httpx.get(f"{BASE_URL}/api/users/me/quota", headers=hu)
    assert r.status_code == 200
    quota = r.json()
    effective_max = quota["quota"]["max_storage_size"]
    assert effective_max == 200 * 1024 * 1024, f"Expected 200MB from VIP group, got {format_size(effective_max or 0)}"
    print(f"  Effective quota after VIP group: {format_size(effective_max)}")

    # Remove from group
    r = httpx.delete(f"{BASE_URL}/api/groups/{group_id}/members/{user_id}", headers=ha)
    assert r.status_code == 200

    # Verify quota reverts to role default
    r = httpx.get(f"{BASE_URL}/api/users/me/quota", headers=hu)
    effective_max = r.json()["quota"]["max_storage_size"]
    assert effective_max == 50 * 1024 * 1024
    print(f"  Quota after leaving group: {format_size(effective_max)} (role default)")

    print("\n  PASSED: Group management")
    return group_id


def test_oidc_group_sync(db_path):
    """Test that user groups are synced from OIDC provider profile data."""
    print("\n" + "=" * 60)
    print("  TEST: OIDC group sync on login")
    print("=" * 60)

    async def _test():
        from etransfer.server.auth.db import UserDB

        db = UserDB(f"sqlite+aiosqlite:///{db_path}")
        await db.connect()

        # Simulate an OIDC login with groups
        user = await db.upsert_user(
            oidc_sub="oidc-sync-test",
            username="sync-user",
            groups=["engineering", "beta-testers"],
        )
        user_groups = await db.get_user_group_names(user.id)
        assert "engineering" in user_groups
        assert "beta-testers" in user_groups
        print(f"  Created user with groups: {user_groups}")

        # Verify groups were auto-created
        eng = await db.get_group_by_name("engineering")
        assert eng is not None
        print(f"  Group 'engineering' auto-created (id={eng.id})")

        # Simulate another login with different groups (sync)
        user = await db.upsert_user(
            oidc_sub="oidc-sync-test",
            username="sync-user",
            groups=["engineering", "managers"],
        )
        user_groups = await db.get_user_group_names(user.id)
        assert "engineering" in user_groups
        assert "managers" in user_groups
        assert "beta-testers" not in user_groups
        print(f"  After re-login with new groups: {user_groups}")
        print("  'beta-testers' removed, 'managers' added")

        await db.disconnect()

    asyncio.run(_test())
    print("\n  PASSED: OIDC group sync")


def test_session_auth_for_upload(user_token, storage_path):
    """Test that session tokens work with TUS upload."""
    print("\n" + "=" * 60)
    print("  TEST: Session token works for TUS upload")
    print("=" * 60)

    test_data = os.urandom(1024 * 100)  # 100 KB

    headers = {
        "Authorization": f"Bearer {user_token}",
        "Tus-Resumable": "1.0.0",
    }

    with httpx.Client(base_url=BASE_URL, headers=headers, timeout=30) as client:
        r = client.options("/tus")
        assert r.status_code in (200, 204), f"TUS OPTIONS failed: {r.status_code}"
        print(f"  TUS OPTIONS: {r.status_code}")

        filename_b64 = base64.b64encode(b"session_test.bin").decode()
        r = client.post(
            "/tus",
            headers={
                "Upload-Length": str(len(test_data)),
                "Upload-Metadata": f"filename {filename_b64}",
            },
        )
        assert r.status_code == 201, f"TUS POST failed: {r.status_code} {r.text}"
        location = r.headers["Location"]
        file_id = location.split("/")[-1]
        print(f"  TUS CREATE: file_id={file_id}")

        r = client.patch(
            location,
            content=test_data,
            headers={
                "Content-Type": "application/offset+octet-stream",
                "Upload-Offset": "0",
            },
        )
        assert r.status_code == 204, f"TUS PATCH failed: {r.status_code} {r.text}"
        print(f"  TUS UPLOAD: {len(test_data)} bytes")

        r = client.get(f"/api/files/{file_id}/download")
        assert r.status_code == 200
        assert len(r.content) == len(test_data)
        print(f"  Download verified: {len(r.content)} bytes")

    print("\n  PASSED: Session auth for TUS upload")


def test_role_change(admin_token, guest_token, user_id_guest):
    """Test admin changing user roles."""
    print("\n" + "=" * 60)
    print("  TEST: Role management")
    print("=" * 60)

    ha = {"Authorization": f"Bearer {admin_token}"}
    hg = {"Authorization": f"Bearer {guest_token}"}

    r = httpx.get(f"{BASE_URL}/api/users/me/quota", headers=hg)
    q = r.json()["quota"]
    assert q["max_storage_size"] == 10 * 1024 * 1024
    print(f"  Guest quota: {format_size(q['max_storage_size'])}")

    r = httpx.put(
        f"{BASE_URL}/api/users/{user_id_guest}/role?role=user",
        headers=ha,
    )
    assert r.status_code == 200
    print("  Admin set guest -> user role")

    r = httpx.get(f"{BASE_URL}/api/users/me/quota", headers=hg)
    q = r.json()["quota"]
    assert q["max_storage_size"] == 50 * 1024 * 1024
    print(f"  New quota: {format_size(q['max_storage_size'])}")

    # Revert
    r = httpx.put(
        f"{BASE_URL}/api/users/{user_id_guest}/role?role=guest",
        headers=ha,
    )
    assert r.status_code == 200

    print("\n  PASSED: Role management")


def test_disable_user(admin_token, guest_token, user_id_guest):
    """Test admin disabling a user."""
    print("\n" + "=" * 60)
    print("  TEST: Disable/enable user")
    print("=" * 60)

    ha = {"Authorization": f"Bearer {admin_token}"}
    hg = {"Authorization": f"Bearer {guest_token}"}

    r = httpx.put(
        f"{BASE_URL}/api/users/{user_id_guest}/active?active=false",
        headers=ha,
    )
    assert r.status_code == 200
    print("  Admin disabled guest user")

    r = httpx.get(f"{BASE_URL}/api/users/me", headers=hg)
    assert r.status_code == 401, f"Expected 401, got {r.status_code}"
    print("  Disabled user -> 401")

    r = httpx.put(
        f"{BASE_URL}/api/users/{user_id_guest}/active?active=true",
        headers=ha,
    )
    assert r.status_code == 200
    print("  Admin re-enabled guest user")

    print("\n  PASSED: Disable/enable user")


def test_logout(user_token):
    """Test logout invalidates session."""
    print("\n" + "=" * 60)
    print("  TEST: Logout")
    print("=" * 60)

    hu = {"Authorization": f"Bearer {user_token}"}

    r = httpx.post(f"{BASE_URL}/api/users/logout", headers=hu)
    assert r.status_code == 200
    print("  Logout success")

    r = httpx.get(f"{BASE_URL}/api/users/me", headers=hu)
    assert r.status_code == 401
    print("  Token invalidated -> 401")

    print("\n  PASSED: Logout")


# ── Main ──────────────────────────────────────────────────────


def main():
    os.system(f"lsof -ti:{SERVER_PORT} | xargs -r kill -9 2>/dev/null")
    time.sleep(1)

    storage_path = tempfile.mkdtemp(prefix="et_usersys_")
    db_path = str(Path(storage_path) / "users.db")
    print("Starting server with OIDC user system...")
    print(f"  Storage: {storage_path}")

    proc = start_server(storage_path)
    print("  Server ready!")

    results = []

    try:
        # Test login-info endpoint (no auth needed)
        try:
            test_login_info()
            results.append(("Login info", True))
        except AssertionError as e:
            results.append(("Login info", False))
            print(f"\n  FAILED: {e}")

        # Test CLI login flow
        try:
            test_cli_login_flow()
            results.append(("CLI login flow", True))
        except AssertionError as e:
            results.append(("CLI login flow", False))
            print(f"\n  FAILED: {e}")

        # Test OIDC group sync
        try:
            test_oidc_group_sync(db_path)
            results.append(("OIDC group sync", True))
        except AssertionError as e:
            results.append(("OIDC group sync", False))
            print(f"\n  FAILED: {e}")

        # Create test users directly in DB (simulates OIDC login)
        admin_user, admin_token = asyncio.run(create_test_user(db_path, "oidc-admin-001", "admin-user", role="admin"))
        normal_user, user_token = asyncio.run(create_test_user(db_path, "oidc-user-001", "normal-user", role="user"))
        guest_user, guest_token = asyncio.run(create_test_user(db_path, "oidc-guest-001", "guest-user", role="guest"))
        print("\n  Created test users:")
        print(f"    admin: {admin_user.username} (token: {admin_token[:16]}...)")
        print(f"    user:  {normal_user.username} (token: {user_token[:16]}...)")
        print(f"    guest: {guest_user.username} (token: {guest_token[:16]}...)")

        # Run tests
        try:
            test_user_endpoints(admin_token, user_token, guest_token)
            results.append(("User endpoints", True))
        except AssertionError as e:
            results.append(("User endpoints", False))
            print(f"\n  FAILED: {e}")

        try:
            test_group_management(admin_token, user_token)
            results.append(("Group management", True))
        except AssertionError as e:
            results.append(("Group management", False))
            print(f"\n  FAILED: {e}")

        try:
            test_session_auth_for_upload(user_token, storage_path)
            results.append(("Session auth + TUS", True))
        except AssertionError as e:
            results.append(("Session auth + TUS", False))
            print(f"\n  FAILED: {e}")

        try:
            test_role_change(admin_token, guest_token, guest_user.id)
            results.append(("Role management", True))
        except AssertionError as e:
            results.append(("Role management", False))
            print(f"\n  FAILED: {e}")

        try:
            test_disable_user(admin_token, guest_token, guest_user.id)
            results.append(("Disable/enable user", True))
        except AssertionError as e:
            results.append(("Disable/enable user", False))
            print(f"\n  FAILED: {e}")

        # Re-create user token for logout test
        _, user_token2 = asyncio.run(create_test_user(db_path, "oidc-user-001", "normal-user"))
        try:
            test_logout(user_token2)
            results.append(("Logout", True))
        except AssertionError as e:
            results.append(("Logout", False))
            print(f"\n  FAILED: {e}")

    finally:
        print("\nStopping server...")
        stop_server(proc)

    # Summary
    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    passed = sum(1 for _, ok in results if ok)
    total = len(results)
    for name, ok in results:
        mark = "PASS" if ok else "FAIL"
        print(f"  {mark}: {name}")
    print(f"\nTotal: {passed} passed, {total - passed} failed out of {total}")

    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
