"""End-to-end test for ``et server reload`` against the in-process memory server.

Covers the full hot-reload pipeline:

    YAML on disk → /api/admin/reload-config → ServerSettings live update
    → /api/info reflects the change
"""

from __future__ import annotations

from pathlib import Path

import httpx
import yaml


def _write_config(path: Path, *, allow_permanent: bool, token: str) -> None:
    cfg = {
        "server": {"host": "127.0.0.1", "port": 0},
        "auth": {"enabled": True, "tokens": [token]},
        "retention": {
            "default": "download_once",
            "allow_permanent": allow_permanent,
        },
    }
    path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")


def test_server_reload_hot_toggles_allow_permanent(tmp_path, cli_env, run_cli, memory_server):
    cfg_path = tmp_path / "server.yaml"
    _write_config(cfg_path, allow_permanent=True, token=memory_server["token"])

    settings = memory_server["settings"]
    settings._config_path = cfg_path  # noqa: SLF001  expose for hot reload
    assert settings.allow_permanent_retention is True

    # Sanity: API exposes "permanent".
    body = httpx.get(f"{memory_server['url']}/api/info", timeout=5).json()
    assert "permanent" in body["retention_policies"]

    _write_config(cfg_path, allow_permanent=False, token=memory_server["token"])

    result = run_cli(
        "server",
        "reload",
        memory_server["url"],
        "--token",
        memory_server["token"],
    )
    assert result.exit_code == 0, result.output

    assert settings.allow_permanent_retention is False
    body = httpx.get(f"{memory_server['url']}/api/info", timeout=5).json()
    assert "permanent" not in body["retention_policies"]

    # Flip back to keep test isolation strict.
    _write_config(cfg_path, allow_permanent=True, token=memory_server["token"])
    result = run_cli(
        "server",
        "reload",
        memory_server["url"],
        "--token",
        memory_server["token"],
    )
    assert result.exit_code == 0, result.output
    assert settings.allow_permanent_retention is True
