"""CLI tests for `et remote-download` (offline download) and `et tasks *`."""

from __future__ import annotations

import hashlib
import io
import time
from pathlib import Path

import httpx


def _api_tasks(server: dict) -> list[dict]:
    r = httpx.get(
        f"{server['url']}/api/tasks",
        headers={"X-API-Token": server["token"]},
        timeout=10,
    )
    r.raise_for_status()
    body = r.json()
    return list(body if isinstance(body, list) else body.get("tasks", []))


def _wait_task_done(server: dict, task_id: str, timeout: float = 15.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = httpx.get(
            f"{server['url']}/api/tasks/{task_id}",
            headers={"X-API-Token": server["token"]},
            timeout=5,
        )
        if r.status_code == 200:
            body = r.json()
            if body["status"] in ("completed", "failed", "cancelled"):
                return body
        time.sleep(0.2)
    raise TimeoutError(f"task {task_id} did not finish in {timeout}s")


def _wait_all_tasks(server: dict, task_ids: list[str], timeout: float = 30.0) -> list[dict]:
    deadline = time.monotonic() + timeout
    results: dict[str, dict] = {}
    while time.monotonic() < deadline and len(results) < len(task_ids):
        for tid in task_ids:
            if tid in results:
                continue
            r = httpx.get(
                f"{server['url']}/api/tasks/{tid}",
                headers={"X-API-Token": server["token"]},
                timeout=5,
            )
            if r.status_code == 200:
                body = r.json()
                if body["status"] in ("completed", "failed", "cancelled"):
                    results[tid] = body
        time.sleep(0.2)
    return [results[tid] for tid in task_ids if tid in results]


def test_remote_download_single_url(tmp_path, cli_env, run_cli, memory_server, host_file):
    data = b"hello-remote-download\n" * 200
    md5 = hashlib.md5(data, usedforsecurity=False).hexdigest()
    url = host_file("remote-one.bin", data)

    result = run_cli("remote-download", url, "--retention", "permanent", "--wait")
    assert result.exit_code == 0, result.output

    tasks = _api_tasks(memory_server)
    assert len(tasks) == 1
    t = tasks[0]
    assert t["status"] == "completed"
    assert t["source_plugin"] == "direct"

    # file should be saved on the server
    r = httpx.get(
        f"{memory_server['url']}/api/files",
        headers={"X-API-Token": memory_server["token"]},
        timeout=10,
    )
    r.raise_for_status()
    files = r.json()["files"]
    saved = next(f for f in files if f["filename"].endswith("remote-one.bin"))
    assert saved["size"] == len(data)
    # Pull the bytes back and compare
    r = httpx.get(
        f"{memory_server['url']}/api/files/{saved['file_id']}/download",
        headers={"X-API-Token": memory_server["token"]},
        timeout=15,
    )
    r.raise_for_status()
    assert hashlib.md5(r.content, usedforsecurity=False).hexdigest() == md5


def test_remote_download_batch_file(tmp_path, cli_env, run_cli, memory_server, host_file):
    urls = []
    for i in range(3):
        data = f"batch-file-{i}".encode() * 50
        urls.append(host_file(f"batch/file-{i}.bin", data))

    urls_file = tmp_path / "urls.txt"
    urls_file.write_text("\n".join(["# header comment", *urls, ""]))

    result = run_cli("remote-download", "--urls-file", str(urls_file), "--wait")
    assert result.exit_code == 0, result.output

    tasks = _api_tasks(memory_server)
    assert len(tasks) == 3
    assert all(t["status"] == "completed" for t in tasks)


def test_remote_download_batch_stdin(cli_env, run_cli, memory_server, host_file):
    data = b"stdin-payload"
    url_a = host_file("std/a.bin", data)
    url_b = host_file("std/b.bin", data * 2)

    stdin = f"{url_a}\n{url_b}\n"
    result = run_cli("remote-download", "-f", "-", "--wait", input=stdin)
    assert result.exit_code == 0, result.output

    tasks = _api_tasks(memory_server)
    assert len(tasks) == 2
    assert all(t["status"] == "completed" for t in tasks)


def test_tasks_list_and_retry_failed(tmp_path, cli_env, run_cli, memory_server):
    """``et tasks list`` + ``et tasks retry`` against a failed task."""
    bad_url = f"{memory_server['url']}/__definitely_missing__/file.bin"
    r = httpx.post(
        f"{memory_server['url']}/api/tasks",
        json={"source_url": bad_url, "retention": "permanent"},
        headers={"X-API-Token": memory_server["token"]},
        timeout=15,
    )
    r.raise_for_status()
    tid = r.json()["task_id"]

    body = _wait_task_done(memory_server, tid, timeout=20)
    assert body["status"] == "failed", body

    # `tasks list` must show the failed task.
    result = run_cli("tasks", "list")
    assert result.exit_code == 0, result.output

    # `tasks retry` must succeed for a failed task and return a new task id.
    result = run_cli("tasks", "retry", tid[:12])
    assert result.exit_code == 0, result.output

    # The original task should now be marked superseded.
    tasks = _api_tasks(memory_server)
    original = next(t for t in tasks if t["task_id"] == tid)
    assert original.get("superseded_by"), original
