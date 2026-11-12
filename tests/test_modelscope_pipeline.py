"""End-to-end tests: ModelScope repo → batch expansion → sink, with dedup.

These run the real FastAPI app in-process with ``state_backend="memory"`` and a
fake S3-style bucket, replacing only the ModelScope HTTP calls with a mock
transport. Everything else (task manager, pipeline, sink, deterministic keys)
is the production code path.
"""

from __future__ import annotations

from typing import Any, Callable

import httpx
import pytest
from fastapi.testclient import TestClient

from etransfer.common import modelscope_hub as ms
from etransfer.plugins.registry import plugin_registry
from etransfer.plugins.sources.modelscope import ModelScopeSource
from etransfer.server.config import ServerSettings
from etransfer.server.main import create_app
from etransfer.server.routes import modelscope as ms_routes
from tests._fake_bucket_sink import FakeBucketSink

MODEL_PAGE = "https://modelscope.cn/models/Qwen/Qwen3.5-0.8B"

_FILES = [
    {"Type": "blob", "Path": "config.json", "Size": 12},
    {"Type": "blob", "Path": "tokenizer.json", "Size": 20},
]


@pytest.fixture()
def ms_client(tmp_path: Any, fake_bucket_server: dict) -> Any:
    # Register explicitly: entry-point metadata is only refreshed on reinstall,
    # so tests must not depend on the editable install having been rebuilt.
    plugin_registry.register_source(ModelScopeSource)
    plugin_registry.register_sink(FakeBucketSink)
    settings = ServerSettings(
        host="127.0.0.1",
        state_backend="memory",
        storage_path=tmp_path / "storage",
        auth_enabled=False,
        cleanup_interval=999_999,
        config_watch=False,
        sink_presets={
            "fake_bucket": {
                "default": {"endpoint": fake_bucket_server["url"], "bucket": "bkt"},
            }
        },
    )
    app = create_app(settings)
    ms_routes.clear_cache()
    with TestClient(app) as c:
        yield c


def _patch_hub(monkeypatch: pytest.MonkeyPatch, files: list[dict] | None = None) -> dict:
    """Serve repo listings + file bytes through a mock transport."""
    calls: dict[str, int] = {"listing": 0, "download": 0}
    payload = files if files is not None else _FILES
    bodies = {f["Path"]: (f"data-{f['Path']}".encode() * max(1, f["Size"] // 8)) for f in payload}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if "/repo/files" in path or "/repo/tree" in path:
            calls["listing"] += 1
            return httpx.Response(200, json={"Code": 200, "Data": {"Files": payload}})
        if "/resolve/" in path:
            calls["download"] += 1
            rel = path.split("/resolve/", 1)[1].split("/", 1)[1]
            return httpx.Response(
                200,
                content=bodies.get(rel, b"x"),
                headers={"content-length": str(len(bodies.get(rel, b"x")))},
            )
        return httpx.Response(404)

    def fake_client(token: str | None = None) -> httpx.AsyncClient:
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        return httpx.AsyncClient(transport=httpx.MockTransport(handler), headers=headers)

    monkeypatch.setattr(ms, "_make_client", fake_client)

    # The plugin's streaming download has its own client factory; route it
    # too. Patching httpx.AsyncClient itself would leak into the fake bucket.
    monkeypatch.setattr(
        "etransfer.plugins.sources.modelscope._http_client",
        lambda token=None, timeout=None: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    monkeypatch.setattr("etransfer.plugins.aria2.is_available", lambda: False)
    return calls


def _wait_for_tasks(client: Any, expected: int, timeout: float = 20.0) -> list[dict]:
    """Poll /api/tasks until *expected* tasks reach a terminal state."""
    import time

    end = time.monotonic() + timeout
    last: list[dict] = []
    while time.monotonic() < end:
        last = client.get("/api/tasks").json()
        terminal = [t for t in last if t["status"] in ("completed", "failed", "cancelled")]
        if len(terminal) >= expected:
            return last
        time.sleep(0.05)
    return last


class TestBatchExpansion:
    def test_repo_url_expands_into_one_task_per_file(self, ms_client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_hub(monkeypatch)

        resp = ms_client.post(
            "/api/tasks",
            json={"source_url": MODEL_PAGE, "sink_plugin": "fake_bucket", "retention": "permanent"},
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()

        assert body["batch"] is True
        assert body["repo"] == "Qwen/Qwen3.5-0.8B"
        assert body["total_files"] == 2
        assert body["created_tasks"] == 2
        assert body["skipped_files"] == 0
        assert body["total_size"] == 32

    def test_expansion_records_deterministic_object_keys(self, ms_client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_hub(monkeypatch)

        body = ms_client.post(
            "/api/tasks",
            json={"source_url": MODEL_PAGE, "sink_plugin": "fake_bucket", "retention": "permanent"},
        ).json()

        keys = sorted(t["sink_object_key"] for t in body["tasks"])
        assert keys == [
            "modelscope/models/Qwen/Qwen3.5-0.8B/master/config.json",
            "modelscope/models/Qwen/Qwen3.5-0.8B/master/tokenizer.json",
        ]

    def test_dataset_url_expands_via_tree_endpoint(self, ms_client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        seen: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["path"] = request.url.path
            return httpx.Response(
                200,
                json={
                    "Code": 200,
                    "Data": {
                        "Files": [
                            {"Type": "tree", "Path": "main", "Size": 0},
                            {"Type": "blob", "Path": "main/train.parquet", "Size": 8},
                        ]
                    },
                },
            )

        def fake_client(token: str | None = None) -> httpx.AsyncClient:
            return httpx.AsyncClient(transport=httpx.MockTransport(handler))

        monkeypatch.setattr(ms, "_make_client", fake_client)

        body = ms_client.post(
            "/api/tasks",
            json={"source_url": "https://modelscope.cn/datasets/AI-ModelScope/gsm8k"},
        ).json()

        assert "/api/v1/datasets/AI-ModelScope/gsm8k/repo/tree" in seen["path"]
        # The directory node is filtered; only the parquet becomes a task.
        assert body["total_files"] == 1
        assert body["tasks"][0]["filename"] == "main/train.parquet"

    def test_single_file_url_stays_a_single_task(self, ms_client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_hub(monkeypatch)

        body = ms_client.post(
            "/api/tasks",
            json={"source_url": MODEL_PAGE + "/resolve/master/config.json"},
        ).json()

        assert "batch" not in body
        assert body["source_plugin"] == "modelscope"


class TestSkipExisting:
    def test_skip_existing_creates_no_task_when_object_present(
        self, ms_client: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_hub(monkeypatch)

        class _Sink(FakeBucketSink):
            async def object_exists(self, object_key: str) -> bool | None:
                # Pretend everything is already in the bucket.
                return True

        plugin_registry.register_sink(_Sink)

        body = ms_client.post(
            "/api/tasks",
            json={
                "source_url": MODEL_PAGE,
                "sink_plugin": "fake_bucket",
                "skip_existing": True,
            },
        ).json()

        assert body["created_tasks"] == 0
        assert body["skipped_files"] == 2
        assert sorted(body["skipped"]) == ["config.json", "tokenizer.json"]

    def test_skip_existing_downloads_when_absent(self, ms_client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_hub(monkeypatch)

        class _Sink(FakeBucketSink):
            async def object_exists(self, object_key: str) -> bool | None:
                return None  # unknown → must not skip

        plugin_registry.register_sink(_Sink)

        body = ms_client.post(
            "/api/tasks",
            json={"source_url": MODEL_PAGE, "sink_plugin": "fake_bucket", "skip_existing": True},
        ).json()

        assert body["created_tasks"] == 2
        assert body["skipped_files"] == 0

    def test_skip_existing_only_skips_matching_files(self, ms_client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_hub(monkeypatch)

        class _Sink(FakeBucketSink):
            async def object_exists(self, object_key: str) -> bool | None:
                return object_key.endswith("config.json")

        plugin_registry.register_sink(_Sink)

        body = ms_client.post(
            "/api/tasks",
            json={"source_url": MODEL_PAGE, "sink_plugin": "fake_bucket", "skip_existing": True},
        ).json()

        assert body["created_tasks"] == 1
        assert body["skipped"] == ["config.json"]

    def test_skip_existing_without_sink_does_not_skip(self, ms_client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        """No sink → no object keys → nothing to compare against."""
        _patch_hub(monkeypatch)

        body = ms_client.post("/api/tasks", json={"source_url": MODEL_PAGE, "skip_existing": True}).json()

        assert body["created_tasks"] == 2
        assert body["skipped_files"] == 0
        assert all(t["sink_object_key"] == "" for t in body["tasks"])


class TestPipelineEndToEnd:
    def test_download_and_push_completes(self, ms_client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_hub(monkeypatch)

        body = ms_client.post(
            "/api/tasks",
            json={"source_url": MODEL_PAGE, "sink_plugin": "fake_bucket", "retention": "permanent"},
        ).json()
        assert body["created_tasks"] == 2

        tasks = _wait_for_tasks(ms_client, expected=2)
        statuses = {t["filename"]: t["status"] for t in tasks}
        assert statuses.get("config.json") == "completed", tasks
        assert statuses.get("tokenizer.json") == "completed", tasks

        # The sink received the deterministic keys, not timestamped ones.
        for task in tasks:
            assert task["sink_object_key"].startswith("modelscope/models/Qwen/Qwen3.5-0.8B/master/")
            assert task["sink_result_url"].startswith("fake://")

    def test_dedup_reflects_completed_tasks_after_run(self, ms_client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_hub(monkeypatch)
        ms_client.post(
            "/api/tasks",
            json={"source_url": MODEL_PAGE, "sink_plugin": "fake_bucket", "retention": "permanent"},
        )
        _wait_for_tasks(ms_client, expected=2)

        downloaded = ms_client.get("/api/sources/modelscope/downloaded").json()
        entry = next(i for i in downloaded["items"] if i["repo_id"] == "Qwen/Qwen3.5-0.8B")
        assert entry["files"] == 2
        assert entry["sink_plugin"] == "fake_bucket"

    def test_object_key_is_stable_across_two_runs(self, ms_client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        """A second run must target the same objects (no duplicate 15 GB copies)."""
        _patch_hub(monkeypatch)

        first = ms_client.post("/api/tasks", json={"source_url": MODEL_PAGE, "sink_plugin": "fake_bucket"}).json()
        keys_first = sorted(t["sink_object_key"] for t in first["tasks"])

        second = ms_client.post("/api/tasks", json={"source_url": MODEL_PAGE, "sink_plugin": "fake_bucket"}).json()
        keys_second = sorted(t["sink_object_key"] for t in second["tasks"])

        assert keys_first == keys_second
        assert all(keys_first), "keys must not be empty"
