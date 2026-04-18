"""Sink integration tests using the in-process ``fake_bucket`` plugin.

Three end-to-end paths are exercised:

1. ``et upload <file> --sink fake_bucket`` — streamed multipart upload
   directly to the sink during TUS PATCH.
2. ``et push <file_id> --sink fake_bucket`` — push of an already-uploaded
   server-side file.
3. ``et remote-download <url> --sink fake_bucket --wait`` — server-side
   offline download then sink push.

For each path we assert that:

* the bucket received the file with the correct MD5 + total size;
* the multipart session produced ``parts > 0`` (chunked) for files larger
   than the chunk size;
* the CLI reports a non-failing exit code.
"""

from __future__ import annotations

import hashlib
import time
from pathlib import Path

import httpx
import pytest

from tests.conftest import TEST_BUCKET


def _make_file(tmp_path: Path, name: str, size: int) -> tuple[Path, str]:
    data = (b"sink-fake-bucket-payload-" * (size // 25 + 1))[:size]
    path = tmp_path / name
    path.write_bytes(data)
    return path, hashlib.md5(data, usedforsecurity=False).hexdigest()


def _bucket_object(server: dict, key: str) -> dict:
    """Look up an object in the fake bucket via its introspect endpoint."""
    r = httpx.get(f"{server['fake_bucket_url']}/objects/{TEST_BUCKET}/{key}", timeout=5)
    r.raise_for_status()
    return r.json()


def _introspect(server: dict) -> dict:
    r = httpx.get(f"{server['fake_bucket_url']}/_introspect", timeout=5)
    r.raise_for_status()
    return r.json()


def _wait_bucket_object(server: dict, name_suffix: str, timeout: float = 15.0) -> tuple[str, dict]:
    """Wait until *some* object whose key ends with ``name_suffix`` exists."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        objects = _introspect(server)["objects"]
        for key, info in objects.items():
            if key.endswith(name_suffix):
                return key, info
        time.sleep(0.1)
    raise TimeoutError(f"no bucket object ending in {name_suffix} after {timeout}s")


def _wait_all_tasks_done(server: dict, timeout: float = 15.0) -> list[dict]:
    """Wait for every task on the server to reach a terminal status."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = httpx.get(
            f"{server['url']}/api/tasks",
            headers={"X-API-Token": server["token"]},
            timeout=5,
        )
        r.raise_for_status()
        tasks = r.json()
        if tasks and all(t["status"] in ("completed", "failed", "cancelled") for t in tasks):
            return tasks
        time.sleep(0.2)
    raise TimeoutError("not all tasks reached terminal status")


# ── 1) upload --sink fake_bucket ─────────────────────────────────


def test_upload_streams_to_fake_bucket(tmp_path, cli_env, run_cli, memory_server):
    src, md5_hex = _make_file(tmp_path, "stream-up.bin", 200 * 1024)

    result = run_cli("upload", str(src), "--sink", "fake_bucket", "-r", "permanent")
    assert result.exit_code == 0, result.output

    key, info = _wait_bucket_object(memory_server, "stream-up.bin")
    assert info["size"] == src.stat().st_size
    assert info["md5"] == md5_hex
    assert info["parts"] >= 1


# ── 2) push <file_id> --sink fake_bucket ─────────────────────────


def test_push_existing_file_to_fake_bucket(tmp_path, cli_env, run_cli, memory_server):
    src, md5_hex = _make_file(tmp_path, "to-push.bin", 96 * 1024)

    result = run_cli("upload", str(src), "-r", "permanent")
    assert result.exit_code == 0, result.output

    files = httpx.get(
        f"{memory_server['url']}/api/files",
        headers={"X-API-Token": memory_server["token"]},
        timeout=10,
    ).json()["files"]
    file_id = next(f["file_id"] for f in files if f["filename"] == "to-push.bin")

    result = run_cli("push", file_id, "--sink", "fake_bucket")
    assert result.exit_code == 0, result.output

    _wait_all_tasks_done(memory_server)
    key, info = _wait_bucket_object(memory_server, "to-push.bin")
    assert info["size"] == src.stat().st_size
    assert info["md5"] == md5_hex


# ── 3) remote-download --sink fake_bucket ────────────────────────


def test_remote_download_streams_to_fake_bucket(
    tmp_path, cli_env, run_cli, memory_server, host_file
):
    payload = b"remote-sink-bytes" * 4000
    md5_hex = hashlib.md5(payload, usedforsecurity=False).hexdigest()
    url = host_file("rd-sink/file.bin", payload)

    result = run_cli(
        "remote-download",
        url,
        "--sink", "fake_bucket",
        "-r", "permanent",
        "--wait",
    )
    assert result.exit_code == 0, result.output

    _wait_all_tasks_done(memory_server)
    key, info = _wait_bucket_object(memory_server, "file.bin")
    assert info["size"] == len(payload)
    assert info["md5"] == md5_hex
    assert info["parts"] >= 1
