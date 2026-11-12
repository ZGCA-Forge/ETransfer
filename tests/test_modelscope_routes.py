"""Tests for the ModelScope discovery REST routes and dedup logic."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

import httpx
import pytest
from fastapi.testclient import TestClient

from etransfer.common import modelscope_hub as ms
from etransfer.server.config import ServerSettings
from etransfer.server.main import create_app
from etransfer.server.routes import modelscope as ms_routes
from etransfer.server.tasks.models import TaskStatus, TransferTask


def _app(settings: ServerSettings) -> Any:
    return create_app(settings)


@pytest.fixture()
def client(tmp_path: Any) -> Any:
    settings = ServerSettings(
        host="127.0.0.1",
        state_backend="memory",
        storage_path=tmp_path / "storage",
        auth_enabled=False,
        cleanup_interval=999_999,
        config_watch=False,
    )
    app = _app(settings)
    ms_routes.clear_cache()
    with TestClient(app) as c:
        yield c


def _patch(monkeypatch: pytest.MonkeyPatch, handler: Callable[[httpx.Request], httpx.Response]) -> None:
    def fake_client(token: str | None = None) -> httpx.AsyncClient:
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        return httpx.AsyncClient(transport=httpx.MockTransport(handler), headers=headers)

    monkeypatch.setattr(ms, "_make_client", fake_client)


def _models_payload(items: list[dict[str, Any]], total: int = 2) -> dict[str, Any]:
    return {
        "success": True,
        "data": {"models": items, "total_count": total, "page_number": 1, "page_size": len(items)},
    }


class TestDiscoveryRoutes:
    def test_list_models(self, client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/openapi/v1/models"
            return httpx.Response(
                200,
                json=_models_payload(
                    [
                        {"id": "Qwen/Qwen3.5-0.8B", "file_size": 1769980952, "downloads": 2515986},
                        {"id": "Qwen/Qwen3.5-0.8B-Base", "file_size": 1769914437, "downloads": 21041},
                    ]
                ),
            )

        _patch(monkeypatch, handler)
        resp = client.get("/api/sources/modelscope/models", params={"search": "qwen3.5"})

        assert resp.status_code == 200
        body = resp.json()
        assert body["total_count"] == 2
        assert [i["id"] for i in body["items"]] == ["Qwen/Qwen3.5-0.8B", "Qwen/Qwen3.5-0.8B-Base"]
        # Not downloaded yet → explicit false, not missing.
        assert all(i["downloaded"] is False for i in body["items"])
        assert all(i["artifact"] is None for i in body["items"])

    def test_list_datasets(self, client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/openapi/v1/datasets"
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "data": {
                        "datasets": [{"id": "AI-ModelScope/gsm8k", "file_size": 5900352}],
                        "total_count": 47308,
                        "page_number": 1,
                        "page_size": 1,
                    },
                },
            )

        _patch(monkeypatch, handler)
        resp = client.get("/api/sources/modelscope/datasets")

        assert resp.status_code == 200
        assert resp.json()["total_count"] == 47308

    def test_list_collections(self, client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "data": {
                        "collection_list": [{"slug": "a/b", "title": "音色1", "item_count": 1}],
                        "total_count": 1,
                        "page_number": 1,
                        "page_size": 1,
                    },
                },
            )

        _patch(monkeypatch, handler)
        resp = client.get("/api/sources/modelscope/collections")

        assert resp.status_code == 200
        assert resp.json()["items"][0]["slug"] == "a/b"

    def test_collection_detail_resolves_members(self, client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "data": {
                        "slug": "Bigcat19/Krea2-Lora",
                        "items": [{"item_type": "model", "item_object_id": "yan/krea2"}],
                    },
                },
            )

        _patch(monkeypatch, handler)
        resp = client.get("/api/sources/modelscope/collections/Bigcat19/Krea2-Lora")

        assert resp.status_code == 200
        assert resp.json()["members"] == [{"kind": "model", "repo_id": "yan/krea2"}]

    def test_repo_files_preview_sums_size(self, client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            assert "/api/v1/models/Qwen/Qwen3.5-0.8B/repo/files" in request.url.path
            return httpx.Response(
                200,
                json={
                    "Code": 200,
                    "Data": {
                        "Files": [
                            {"Type": "blob", "Path": "config.json", "Size": 2907},
                            {"Type": "blob", "Path": "model.safetensors", "Size": 1746942600},
                        ]
                    },
                },
            )

        _patch(monkeypatch, handler)
        resp = client.get("/api/sources/modelscope/model/Qwen/Qwen3.5-0.8B/files")

        assert resp.status_code == 200
        body = resp.json()
        assert body["total_files"] == 2
        assert body["total_size"] == 1746945507
        assert body["kind"] == "model"
        # Preview must expose deterministic keys so the UI can flag existing files.
        assert body["object_keys"]["config.json"] == "modelscope/models/Qwen/Qwen3.5-0.8B/master/config.json"

    def test_repo_files_rejects_bad_kind(self, client: Any) -> None:
        resp = client.get("/api/sources/modelscope/space/a/b/files")
        assert resp.status_code == 400

    def test_repo_info(self, client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"success": True, "data": {"id": "a/b", "gated": False}})

        _patch(monkeypatch, handler)
        resp = client.get("/api/sources/modelscope/model/a/b")

        assert resp.status_code == 200
        assert resp.json()["info"]["gated"] is False


class TestUpstreamFailureHandling:
    def test_upstream_error_becomes_502(self, client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404, text="nope")

        _patch(monkeypatch, handler)
        resp = client.get("/api/sources/modelscope/models")
        assert resp.status_code == 502
        assert "ModelScope" in resp.json()["detail"]

    def test_bad_sort_becomes_400(self, client: Any) -> None:
        resp = client.get("/api/sources/modelscope/models", params={"sort": "stars"})
        assert resp.status_code == 400

    def test_connection_error_becomes_502(self, client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("no route to host")

        _patch(monkeypatch, handler)
        resp = client.get("/api/sources/modelscope/models")
        assert resp.status_code == 502


class TestCaching:
    def test_second_request_is_served_from_cache(self, client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return httpx.Response(200, json=_models_payload([{"id": "a/b"}]))

        _patch(monkeypatch, handler)
        client.get("/api/sources/modelscope/models", params={"search": "x"})
        client.get("/api/sources/modelscope/models", params={"search": "x"})

        assert calls["n"] == 1, "identical queries must not hammer the hub"

    def test_different_params_bypass_cache(self, client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return httpx.Response(200, json=_models_payload([{"id": "a/b"}]))

        _patch(monkeypatch, handler)
        client.get("/api/sources/modelscope/models", params={"search": "x"})
        client.get("/api/sources/modelscope/models", params={"search": "y"})

        assert calls["n"] == 2

    def test_cache_expires(self, client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return httpx.Response(200, json=_models_payload([{"id": "a/b"}]))

        _patch(monkeypatch, handler)
        client.get("/api/sources/modelscope/models", params={"search": "x"})

        # Age the entry past the TTL.
        key = next(iter(ms_routes._cache))
        ts, value = ms_routes._cache[key]
        ms_routes._cache[key] = (ts - ms_routes._CACHE_TTL_SECONDS - 1, value)

        client.get("/api/sources/modelscope/models", params={"search": "x"})
        assert calls["n"] == 2


class TestDownloadedAnnotation:
    """The dedup index is derived from persisted task state."""

    @staticmethod
    def _task(**overrides: Any) -> TransferTask:
        defaults: dict[str, Any] = {
            "task_id": "t1",
            "source_url": "https://modelscope.cn/models/Qwen/Qwen3.5-0.8B/resolve/master/config.json",
            "source_plugin": "modelscope",
            "sink_plugin": "tos",
            "status": TaskStatus.COMPLETED,
            "filename": "config.json",
            "file_size": 2907,
            "sink_result_url": "https://bucket.example/modelscope/models/Qwen/Qwen3.5-0.8B/master/config.json",
            "completed_at": datetime(2026, 11, 12, 1, 2, 3),
        }
        defaults.update(overrides)
        return TransferTask(**defaults)

    def test_downloaded_is_true_when_task_completed(self, client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        task = self._task()

        async def fake_list(self: Any) -> list[TransferTask]:  # noqa: ANN001
            return [task]

        monkeypatch.setattr("etransfer.server.tasks.manager.TaskManager.list_tasks", fake_list)

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=_models_payload([{"id": "Qwen/Qwen3.5-0.8B"}]))

        _patch(monkeypatch, handler)
        resp = client.get("/api/sources/modelscope/models")

        item = resp.json()["items"][0]
        assert item["downloaded"] is True
        assert item["artifact"]["repo_id"] == "Qwen/Qwen3.5-0.8B"
        assert item["artifact"]["sink_plugin"] == "tos"
        assert item["artifact"]["files"] == 1
        assert item["artifact"]["bytes"] == 2907

    def test_incomplete_push_does_not_count_as_downloaded(self, client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        task = self._task(status=TaskStatus.PUSHING)

        async def fake_list(self: Any) -> list[TransferTask]:  # noqa: ANN001
            return [task]

        monkeypatch.setattr("etransfer.server.tasks.manager.TaskManager.list_tasks", fake_list)
        _patch(monkeypatch, lambda r: httpx.Response(200, json=_models_payload([{"id": "Qwen/Qwen3.5-0.8B"}])))

        resp = client.get("/api/sources/modelscope/models")
        assert resp.json()["items"][0]["downloaded"] is False

    def test_multiple_files_are_aggregated(self, client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        tasks = [
            self._task(task_id="t1", file_size=100),
            self._task(task_id="t2", file_size=900),
        ]

        async def fake_list(self: Any) -> list[TransferTask]:  # noqa: ANN001
            return tasks

        monkeypatch.setattr("etransfer.server.tasks.manager.TaskManager.list_tasks", fake_list)
        _patch(monkeypatch, lambda r: httpx.Response(200, json=_models_payload([{"id": "Qwen/Qwen3.5-0.8B"}])))

        artifact = client.get("/api/sources/modelscope/models").json()["items"][0]["artifact"]
        assert artifact["files"] == 2
        assert artifact["bytes"] == 1000
        assert len(artifact["task_ids"]) == 2

    def test_dataset_and_model_with_same_id_do_not_collide(self, client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        model_task = self._task(task_id="m", source_url="https://modelscope.cn/models/o/n/resolve/master/f")
        ds_task = self._task(task_id="d", source_url="https://modelscope.cn/datasets/o/n/resolve/master/f")

        async def fake_list(self: Any) -> list[TransferTask]:  # noqa: ANN001
            return [model_task, ds_task]

        monkeypatch.setattr("etransfer.server.tasks.manager.TaskManager.list_tasks", fake_list)

        downloaded = client.get("/api/sources/modelscope/downloaded").json()
        kinds = {(i["kind"], i["repo_id"]) for i in downloaded["items"]}
        assert kinds == {("model", "o/n"), ("dataset", "o/n")}

    def test_downloaded_endpoint_totals_bytes(self, client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        tasks = [self._task(task_id="t1", file_size=500)]

        async def fake_list(self: Any) -> list[TransferTask]:  # noqa: ANN001
            return tasks

        monkeypatch.setattr("etransfer.server.tasks.manager.TaskManager.list_tasks", fake_list)

        body = client.get("/api/sources/modelscope/downloaded").json()
        assert body["total"] == 1
        assert body["total_bytes"] == 500

    def test_unparsable_task_urls_are_ignored(self, client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        task = self._task(source_url="https://example.com/whatever.bin")

        async def fake_list(self: Any) -> list[TransferTask]:  # noqa: ANN001
            return [task]

        monkeypatch.setattr("etransfer.server.tasks.manager.TaskManager.list_tasks", fake_list)
        assert client.get("/api/sources/modelscope/downloaded").json()["total"] == 0

    def test_include_downloaded_false_skips_annotation(self, client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        async def fake_list(self: Any) -> list[TransferTask]:  # noqa: ANN001
            return [self._task()]

        monkeypatch.setattr("etransfer.server.tasks.manager.TaskManager.list_tasks", fake_list)
        _patch(monkeypatch, lambda r: httpx.Response(200, json=_models_payload([{"id": "Qwen/Qwen3.5-0.8B"}])))

        item = client.get("/api/sources/modelscope/models", params={"include_downloaded": "false"}).json()["items"][0]
        assert "downloaded" not in item
