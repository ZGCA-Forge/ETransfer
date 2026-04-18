"""Smoke test that verifies the test harness (memory server + CLI) works."""

from __future__ import annotations


def test_memory_server_health(memory_server):
    import httpx

    r = httpx.get(f"{memory_server['url']}/api/health", timeout=5)
    r.raise_for_status()
    body = r.json()
    assert body.get("status") in ("ok", "healthy")


def test_cli_info(cli_env, run_cli):
    result = run_cli("info")
    assert result.exit_code == 0, result.output
    assert "Server" in result.output or "server" in result.output
