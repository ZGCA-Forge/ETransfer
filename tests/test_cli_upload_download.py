"""End-to-end CLI tests for upload / list / download / delete flows."""

from __future__ import annotations

import hashlib
from pathlib import Path

import httpx


def _make_file(tmp_path: Path, name: str, size: int = 200 * 1024) -> tuple[Path, str]:
    """Create a deterministic file and return ``(path, md5_hex)``."""
    data = (b"ETransfer-CLI-test!" * ((size // 19) + 1))[:size]
    path = tmp_path / name
    path.write_bytes(data)
    return path, hashlib.md5(data, usedforsecurity=False).hexdigest()


def _api_files(server: dict) -> list[dict]:
    r = httpx.get(
        f"{server['url']}/api/files",
        headers={"X-API-Token": server["token"]},
        params={"per_page": 200},
        timeout=10,
    )
    r.raise_for_status()
    return list(r.json()["files"])


def test_upload_list_download_delete_cycle(tmp_path, cli_env, run_cli, memory_server):
    src, md5 = _make_file(tmp_path, "hello.bin", 128 * 1024)

    # --- upload ---
    result = run_cli("upload", str(src), "--retention", "permanent")
    assert result.exit_code == 0, result.output

    files = _api_files(memory_server)
    match = [f for f in files if f["filename"] == "hello.bin"]
    assert match, f"uploaded file not in server list: {files}"
    file_id = match[0]["file_id"]

    # --- list (smoke) ---
    result = run_cli("list")
    assert result.exit_code == 0, result.output

    # --- download ---
    out_dir = tmp_path / "downloaded"
    out_dir.mkdir()
    result = run_cli("download", file_id, "-p", str(out_dir))
    assert result.exit_code == 0, result.output

    downloaded = out_dir / "hello.bin"
    assert downloaded.exists(), f"downloaded file missing: listing = {list(out_dir.iterdir())}"
    assert hashlib.md5(downloaded.read_bytes(), usedforsecurity=False).hexdigest() == md5

    # --- delete ---
    result = run_cli("delete", file_id, "-f")
    assert result.exit_code == 0, result.output

    remaining = [f for f in _api_files(memory_server) if f["filename"] == "hello.bin"]
    assert remaining == []


def test_retention_policies_exposed_in_api_info(memory_server):
    """/api/info should always report retention_policies; default includes permanent."""
    r = httpx.get(f"{memory_server['url']}/api/info", timeout=5)
    r.raise_for_status()
    body = r.json()
    assert "retention_policies" in body
    assert "default_retention" in body
    assert set(body["retention_policies"]) == {"download_once", "ttl", "permanent"}
    assert body["default_retention"] == "download_once"


def test_retention_policy_filtered_when_permanent_disabled(memory_server):
    """Flip the hot flag; the public /api/info must drop 'permanent'."""
    memory_server["settings"].allow_permanent_retention = False
    r = httpx.get(f"{memory_server['url']}/api/info", timeout=5)
    r.raise_for_status()
    body = r.json()
    assert "permanent" not in body["retention_policies"]
    # Reset for next test isolation.
    memory_server["settings"].allow_permanent_retention = True
