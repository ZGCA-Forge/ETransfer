#!/usr/bin/env python3
"""
集成测试 — 配置自动发现 + 热重载 + et server reload 命令。

场景:
1. 使用临时 config.yaml 启动服务端
2. 验证自动发现加载了正确的配置
3. 通过 HTTP API 触发热重载并验证变更生效
4. 修改 YAML 中的热重载字段 (tokens, quota, retention, endpoints, role_quotas)
5. 验证静态字段 (port) 不会被热重载改变
6. 测试 config-status 端点
7. 测试 et server reload CLI 命令
8. 验证变更后上传/鉴权行为与新配置一致

运行方式:
    python scripts/test_hot_reload.py
    python scripts/test_hot_reload.py --keep-server  # 测试后保留服务端

自动启动服务端，测试完毕后自动停止。
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import httpx  # noqa: E402

PORT = 8767
TOKEN_V1 = "test-token-v1"
TOKEN_V2 = "test-token-v2"
TOKEN_V3 = "test-token-v3"
SERVER_URL = f"http://127.0.0.1:{PORT}"


def banner(title: str):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def ok(msg: str):
    print(f"  ✓ {msg}")


def fail(msg: str):
    print(f"  ✗ {msg}")


def fmt_size(n):
    if n is None:
        return "unlimited"
    for u in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} TB"


class HotReloadTest:
    """Integration test for config auto-discovery and hot-reload."""

    def __init__(self):
        self.tmpdir = Path(tempfile.mkdtemp(prefix="et_hotreload_"))
        self.config_path = self.tmpdir / "config.yaml"
        self.storage_path = self.tmpdir / "storage"
        self.storage_path.mkdir()
        self.server_process = None
        self.results: dict[str, bool] = {}

    def _write_config(self, yaml_text: str):
        """Write YAML config to the temp config file."""
        self.config_path.write_text(textwrap.dedent(yaml_text))

    def _initial_config(self) -> str:
        return f"""\
            server:
              host: 0.0.0.0
              port: {PORT}
              workers: 1
            storage:
              path: {self.storage_path}
              max_storage_size: 50MB
            state:
              backend: file
            auth:
              enabled: true
              tokens:
                - "{TOKEN_V1}"
            retention:
              default: permanent
            network:
              advertised_endpoints:
                - "10.0.0.1"
        """

    def _start_server(self) -> bool:
        banner("Starting Server")
        self._write_config(self._initial_config())

        env = os.environ.copy()
        env["ETRANSFER_CONFIG"] = str(self.config_path)

        self.server_process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "etransfer.server.main:app",
                "--host",
                "0.0.0.0",
                "--port",
                str(PORT),
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=str(Path(__file__).parent.parent),
        )

        for _ in range(30):
            try:
                r = httpx.get(f"{SERVER_URL}/api/health", timeout=2)
                if r.status_code == 200:
                    ok(f"Server started (PID {self.server_process.pid})")
                    ok(f"Config: {self.config_path}")
                    return True
            except Exception:
                pass
            time.sleep(0.5)
        fail("Server failed to start within 15s")
        return False

    def _stop_server(self):
        if self.server_process:
            self.server_process.terminate()
            try:
                self.server_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.server_process.kill()
            print("\n  Server stopped")

    def _cleanup(self):
        self._stop_server()
        if self.tmpdir.exists():
            shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _http(self, token: str = TOKEN_V1) -> httpx.Client:
        return httpx.Client(
            base_url=SERVER_URL,
            headers={"X-API-Token": token},
            timeout=15,
        )

    def _run_test(self, name: str, fn):
        try:
            fn()
            self.results[name] = True
        except Exception as e:
            self.results[name] = False
            fail(f"EXCEPTION: {e}")
            import traceback

            traceback.print_exc()

    # ── Tests ─────────────────────────────────────────────────

    def test_01_initial_config_loaded(self):
        """Verify server loaded the initial config correctly."""
        banner("TEST 1: Initial Config Verification")
        with self._http() as h:
            # Should authenticate with TOKEN_V1
            r = h.get("/api/storage")
            assert r.status_code == 200, f"Expected 200, got {r.status_code}"
            info = r.json()
            ok("Auth with TOKEN_V1 works")

            assert info["max"] == 50 * 1024 * 1024, f"Unexpected max: {info['max']}"
            ok(f"Storage quota: {info['max_formatted']}")

            # Check endpoints
            r = h.get("/api/endpoints")
            assert r.status_code == 200
            eps = r.json()
            ep_ips = [e["ip_address"] for e in eps.get("endpoints", [])]
            assert "10.0.0.1" in ep_ips, f"10.0.0.1 not in endpoints: {ep_ips}"
            ok(f"Advertised endpoints: {ep_ips}")

    def test_02_config_status(self):
        """Test GET /api/admin/config-status."""
        banner("TEST 2: Config Status Endpoint")
        with self._http() as h:
            r = h.get("/api/admin/config-status")
            assert r.status_code == 200, f"Expected 200, got {r.status_code}"
            data = r.json()
            assert data["config_file"] is not None
            ok(f"Config file: {data['config_file']}")
            ok(f"Config watch: {data['config_watch']}")

    def test_03_reload_no_changes(self):
        """Reload when config hasn't changed should report no changes."""
        banner("TEST 3: Reload With No Changes")
        with self._http() as h:
            r = h.post("/api/admin/reload-config")
            assert r.status_code == 200
            data = r.json()
            assert data["reloaded"] is False
            assert data["changes"] == {}
            assert "hot_reloadable" in data
            assert "requires_restart" in data
            ok("No changes detected (as expected)")
            ok(f"Hot-reloadable fields: {data['hot_reloadable']}")

    def test_04_reload_auth_tokens(self):
        """Change auth tokens via hot-reload."""
        banner("TEST 4: Hot-Reload Auth Tokens")

        # Verify TOKEN_V2 is rejected before reload
        with self._http(TOKEN_V2) as h:
            r = h.get("/api/storage")
            assert r.status_code == 401, f"TOKEN_V2 should be rejected, got {r.status_code}"
            ok("TOKEN_V2 correctly rejected before reload")

        # Update config to add TOKEN_V2
        self._write_config(f"""\
            server:
              host: 0.0.0.0
              port: {PORT}
            storage:
              path: {self.storage_path}
              max_storage_size: 50MB
            state:
              backend: file
            auth:
              enabled: true
              tokens:
                - "{TOKEN_V1}"
                - "{TOKEN_V2}"
            retention:
              default: permanent
            network:
              advertised_endpoints:
                - "10.0.0.1"
        """)

        # Trigger reload using TOKEN_V1
        with self._http(TOKEN_V1) as h:
            r = h.post("/api/admin/reload-config")
            assert r.status_code == 200
            data = r.json()
            assert data["reloaded"] is True
            assert "auth_tokens" in data["changes"]
            ok(f"Reloaded: {list(data['changes'].keys())}")

        # Verify TOKEN_V2 now works
        with self._http(TOKEN_V2) as h:
            r = h.get("/api/storage")
            assert r.status_code == 200, f"TOKEN_V2 should work now, got {r.status_code}"
            ok("TOKEN_V2 accepted after reload")

    def test_05_reload_storage_quota(self):
        """Change storage quota via hot-reload."""
        banner("TEST 5: Hot-Reload Storage Quota")

        # Update config to change quota
        self._write_config(f"""\
            server:
              host: 0.0.0.0
              port: {PORT}
            storage:
              path: {self.storage_path}
              max_storage_size: 200MB
            state:
              backend: file
            auth:
              enabled: true
              tokens:
                - "{TOKEN_V1}"
                - "{TOKEN_V2}"
            retention:
              default: permanent
            network:
              advertised_endpoints:
                - "10.0.0.1"
        """)

        with self._http() as h:
            r = h.post("/api/admin/reload-config")
            assert r.status_code == 200
            data = r.json()
            assert "max_storage_size" in data["changes"]
            ok("Quota change detected")

            # Verify new quota
            r = h.get("/api/storage")
            assert r.status_code == 200
            info = r.json()
            assert info["max"] == 200 * 1024 * 1024
            ok(f"New quota: {info['max_formatted']}")

    def test_06_reload_retention(self):
        """Change default retention policy via hot-reload."""
        banner("TEST 6: Hot-Reload Retention Policy")

        self._write_config(f"""\
            server:
              host: 0.0.0.0
              port: {PORT}
            storage:
              path: {self.storage_path}
              max_storage_size: 200MB
            state:
              backend: file
            auth:
              enabled: true
              tokens:
                - "{TOKEN_V1}"
                - "{TOKEN_V2}"
            retention:
              default: download_once
              default_ttl: 7200
            network:
              advertised_endpoints:
                - "10.0.0.1"
        """)

        with self._http() as h:
            r = h.post("/api/admin/reload-config")
            assert r.status_code == 200
            data = r.json()
            assert "default_retention" in data["changes"]
            ok("default_retention changed to download_once")
            if "default_retention_ttl" in data["changes"]:
                ok("default_retention_ttl changed to 7200")

    def test_07_reload_advertised_endpoints(self):
        """Change advertised endpoints via hot-reload."""
        banner("TEST 7: Hot-Reload Advertised Endpoints")

        self._write_config(f"""\
            server:
              host: 0.0.0.0
              port: {PORT}
            storage:
              path: {self.storage_path}
              max_storage_size: 200MB
            state:
              backend: file
            auth:
              enabled: true
              tokens:
                - "{TOKEN_V1}"
                - "{TOKEN_V2}"
            retention:
              default: download_once
              default_ttl: 7200
            network:
              advertised_endpoints:
                - "10.0.0.1"
                - "10.0.0.2"
                - "10.0.0.3"
        """)

        with self._http() as h:
            r = h.post("/api/admin/reload-config")
            assert r.status_code == 200
            data = r.json()
            assert "advertised_endpoints" in data["changes"]
            ok("Endpoints change detected")

            # Verify new endpoints are served
            r = h.get("/api/endpoints")
            assert r.status_code == 200
            eps = r.json()
            ep_ips = [e["ip_address"] for e in eps.get("endpoints", [])]
            assert "10.0.0.2" in ep_ips
            assert "10.0.0.3" in ep_ips
            ok(f"New endpoints: {ep_ips}")

    def test_08_static_fields_unchanged(self):
        """Verify static fields (port) are not affected by reload."""
        banner("TEST 8: Static Fields Remain Unchanged")

        # Change port in config (static — should not take effect)
        self._write_config(f"""\
            server:
              host: 0.0.0.0
              port: 9999
            storage:
              path: {self.storage_path}
              max_storage_size: 200MB
            state:
              backend: file
            auth:
              enabled: true
              tokens:
                - "{TOKEN_V1}"
                - "{TOKEN_V2}"
            retention:
              default: download_once
            network:
              advertised_endpoints:
                - "10.0.0.1"
                - "10.0.0.2"
                - "10.0.0.3"
        """)

        with self._http() as h:
            r = h.post("/api/admin/reload-config")
            assert r.status_code == 200
            ok("Reload succeeded (port change ignored)")

            # Server still responds on original port
            r = h.get("/api/health")
            assert r.status_code == 200
            ok(f"Server still listening on port {PORT}")

        # Port 9999 should NOT respond
        try:
            r = httpx.get("http://127.0.0.1:9999/api/health", timeout=2)
            fail("Port 9999 should not be listening!")
            assert False
        except (httpx.ConnectError, httpx.ConnectTimeout):
            ok("Port 9999 not listening (static field not changed)")

    def test_09_reload_requires_auth(self):
        """Reload endpoint requires admin auth."""
        banner("TEST 9: Reload Requires Authentication")

        # No token
        r = httpx.post(f"{SERVER_URL}/api/admin/reload-config", timeout=10)
        assert r.status_code == 401, f"Expected 401, got {r.status_code}"
        ok("Unauthenticated request rejected (401)")

        # Invalid token
        r = httpx.post(
            f"{SERVER_URL}/api/admin/reload-config",
            headers={"X-API-Token": "invalid-token"},
            timeout=10,
        )
        assert r.status_code in (401, 403), f"Expected 401/403, got {r.status_code}"
        ok("Invalid token rejected")

    def test_10_reload_remove_own_token(self):
        """Removing the token used for reload from config should work.

        The reload itself uses the old token (still valid during the request).
        After reload, only the new tokens are valid.
        """
        banner("TEST 10: Reload With Token Replacement")

        # Config with only TOKEN_V3 (removing V1 and V2)
        self._write_config(f"""\
            server:
              host: 0.0.0.0
              port: {PORT}
            storage:
              path: {self.storage_path}
              max_storage_size: 200MB
            state:
              backend: file
            auth:
              enabled: true
              tokens:
                - "{TOKEN_V3}"
            retention:
              default: permanent
            network:
              advertised_endpoints:
                - "10.0.0.1"
        """)

        # Use TOKEN_V2 to trigger reload (it will remove itself)
        with self._http(TOKEN_V2) as h:
            r = h.post("/api/admin/reload-config")
            assert r.status_code == 200
            data = r.json()
            assert "auth_tokens" in data["changes"]
            ok("Reload succeeded (self-removing token)")

        # TOKEN_V1 and TOKEN_V2 should no longer work
        with self._http(TOKEN_V1) as h:
            r = h.get("/api/storage")
            assert r.status_code == 401
            ok("TOKEN_V1 rejected after reload")

        with self._http(TOKEN_V2) as h:
            r = h.get("/api/storage")
            assert r.status_code == 401
            ok("TOKEN_V2 rejected after reload")

        # TOKEN_V3 should work
        with self._http(TOKEN_V3) as h:
            r = h.get("/api/storage")
            assert r.status_code == 200
            ok("TOKEN_V3 accepted after reload")

    def test_11_reload_response_fields(self):
        """Verify reload response includes hot/static field lists and note."""
        banner("TEST 11: Reload Response Metadata")

        with self._http(TOKEN_V3) as h:
            r = h.post("/api/admin/reload-config")
            assert r.status_code == 200
            data = r.json()

            assert "hot_reloadable" in data
            assert isinstance(data["hot_reloadable"], list)
            assert "auth_tokens" in data["hot_reloadable"]
            assert "max_storage_size" in data["hot_reloadable"]
            assert "role_quotas" in data["hot_reloadable"]
            ok(f"hot_reloadable: {data['hot_reloadable']}")

            assert "requires_restart" in data
            assert isinstance(data["requires_restart"], list)
            assert "host" in data["requires_restart"]
            assert "port" in data["requires_restart"]
            ok(f"requires_restart: {data['requires_restart']}")

            assert "note" in data
            assert len(data["note"]) > 10
            ok(f"Note: {data['note'][:60]}...")

    def test_12_cli_server_reload(self):
        """Test the 'et server reload' CLI command (via subprocess)."""
        banner("TEST 12: CLI 'et server reload' Command")

        env = os.environ.copy()
        env["ETRANSFER_CONFIG"] = str(self.config_path)

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "etransfer.client.cli",
                "server",
                "reload",
                f"127.0.0.1:{PORT}",
                "--token",
                TOKEN_V3,
            ],
            capture_output=True,
            text=True,
            timeout=15,
            env=env,
            cwd=str(Path(__file__).parent.parent),
        )

        print(f"  stdout: {result.stdout[:200]}")
        if result.stderr:
            print(f"  stderr: {result.stderr[:200]}")

        assert result.returncode == 0, f"CLI returned {result.returncode}"
        ok("CLI command succeeded (exit code 0)")

        # The output should mention "No changes" or "Reloaded"
        assert (
            "changes" in result.stdout.lower()
            or "no changes" in result.stdout.lower()
            or "reloaded" in result.stdout.lower()
            or "unchanged" in result.stdout.lower()
        ), f"Unexpected output: {result.stdout[:300]}"
        ok("CLI output contains expected reload info")

    def test_13_cli_reload_reads_yaml_token(self):
        """Test that CLI reads token from YAML automatically."""
        banner("TEST 13: CLI Reads Token From Config YAML")

        env = os.environ.copy()
        env["ETRANSFER_CONFIG"] = str(self.config_path)

        # No --token flag, should auto-read from config YAML
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "etransfer.client.cli",
                "server",
                "reload",
            ],
            capture_output=True,
            text=True,
            timeout=15,
            env=env,
            cwd=str(Path(__file__).parent.parent),
        )

        print(f"  stdout: {result.stdout[:300]}")
        if result.stderr:
            print(f"  stderr: {result.stderr[:200]}")

        assert result.returncode == 0, (
            f"CLI returned {result.returncode}. " f"Output: {result.stdout[:200]} {result.stderr[:200]}"
        )
        ok("CLI auto-read token from YAML and succeeded")

    # ── Runner ────────────────────────────────────────────────

    def run_all(self, keep_server: bool = False):
        print("=" * 70)
        print("  EasyTransfer — Config Hot-Reload 集成测试")
        print("=" * 70)
        print(f"  Temp dir: {self.tmpdir}")
        print(f"  Port:     {PORT}")

        try:
            if not self._start_server():
                return False

            tests = [
                ("01_initial_config", self.test_01_initial_config_loaded),
                ("02_config_status", self.test_02_config_status),
                ("03_reload_no_changes", self.test_03_reload_no_changes),
                ("04_reload_auth_tokens", self.test_04_reload_auth_tokens),
                ("05_reload_storage_quota", self.test_05_reload_storage_quota),
                ("06_reload_retention", self.test_06_reload_retention),
                ("07_reload_endpoints", self.test_07_reload_advertised_endpoints),
                ("08_static_unchanged", self.test_08_static_fields_unchanged),
                ("09_reload_auth_required", self.test_09_reload_requires_auth),
                ("10_token_replacement", self.test_10_reload_remove_own_token),
                ("11_response_metadata", self.test_11_reload_response_fields),
                ("12_cli_reload", self.test_12_cli_server_reload),
                ("13_cli_yaml_token", self.test_13_cli_reload_reads_yaml_token),
            ]

            for name, fn in tests:
                self._run_test(name, fn)

        finally:
            if not keep_server:
                self._cleanup()
            else:
                print(f"\n  [keep-server] Server still running on {SERVER_URL}")
                print(f"  [keep-server] Config: {self.config_path}")
                print(f"  [keep-server] PID: {self.server_process.pid if self.server_process else 'N/A'}")

        # Summary
        banner("Test Results")
        total = len(self.results)
        passed = sum(1 for v in self.results.values() if v)
        failed = total - passed
        for name, success in self.results.items():
            mark = "✓" if success else "✗"
            print(f"  {mark} {name}")
        print(f"\n  Total: {total}  Passed: {passed}  Failed: {failed}")

        return failed == 0


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Config hot-reload integration test")
    parser.add_argument("--keep-server", action="store_true", help="Don't stop server after tests")
    args = parser.parse_args()

    test = HotReloadTest()
    success = test.run_all(keep_server=args.keep_server)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
