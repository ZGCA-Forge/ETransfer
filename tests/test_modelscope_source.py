"""Unit tests for ``ModelScopeSource`` (the ``modelscope`` Source plugin)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import httpx
import pytest

from etransfer.common import modelscope_hub as ms
from etransfer.plugins.sources.modelscope import ModelScopeSource

MODEL_URL = "https://modelscope.cn/models/Qwen/Qwen3.5-0.8B"
DATASET_URL = "https://modelscope.cn/datasets/AI-ModelScope/gsm8k"
FILE_URL = MODEL_URL + "/resolve/master/config.json"


@pytest.fixture()
def source() -> ModelScopeSource:
    return ModelScopeSource()


def _patch_transport(monkeypatch: pytest.MonkeyPatch, handler: Callable[[httpx.Request], httpx.Response]) -> None:
    """Route the hub's HTTP traffic (and the plugin's own) through a mock."""

    def fake_client(token: str | None = None) -> httpx.AsyncClient:
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        return httpx.AsyncClient(transport=httpx.MockTransport(handler), headers=headers)

    monkeypatch.setattr(ms, "_make_client", fake_client)


# ── Matching & classification ───────────────────────────────


class TestMatching:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "url",
        [
            MODEL_URL,
            MODEL_URL + "/summary",
            DATASET_URL,
            FILE_URL,
            "https://www.modelscope.cn/models/Qwen/Qwen3.5-0.8B",
        ],
    )
    async def test_can_handle_modelscope_urls(self, source: ModelScopeSource, url: str) -> None:
        assert await source.can_handle(url) is True
        assert source.can_handle_sync(url) is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "url",
        [
            "https://huggingface.co/Qwen/Qwen2.5-7B/resolve/main/config.json",
            "https://example.com/file.zip",
            "https://modelscope.cn/studios/foo/bar",
        ],
    )
    async def test_rejects_other_urls(self, source: ModelScopeSource, url: str) -> None:
        assert await source.can_handle(url) is False

    @pytest.mark.parametrize(
        "url,expected",
        [
            (MODEL_URL, True),
            (MODEL_URL + "/", True),
            (MODEL_URL + "/summary", True),
            (DATASET_URL, True),
            (FILE_URL, False),
            (DATASET_URL + "/resolve/master/README.md", False),
        ],
    )
    def test_is_repo_url(self, source: ModelScopeSource, url: str, expected: bool) -> None:
        assert source.is_repo_url(url) is expected

    def test_registered_metadata(self, source: ModelScopeSource) -> None:
        assert source.name == "modelscope"
        assert source.priority > 0, "must outrank DirectLinkSource (-1)"
        assert "modelscope.cn" in source.supported_hosts
        assert source.matches_host("https://modelscope.cn/models/a/b")

    def test_auth_headers(self, source: ModelScopeSource) -> None:
        assert source.auth_headers("ms-abc") == {"Authorization": "Bearer ms-abc"}
        assert source.auth_headers(None) == {}
        assert source.auth_headers("") == {}

    def test_beats_direct_link_fallback(self) -> None:
        """The registry sorts by priority; ModelScope must win over ``direct``."""
        from etransfer.plugins.sources.direct import DirectLinkSource

        assert ModelScopeSource.priority > DirectLinkSource.priority


# ── Repo listing ────────────────────────────────────────────


def _files_response(entries: list[dict[str, Any]]) -> httpx.Response:
    return httpx.Response(200, json={"Code": 200, "Data": {"Files": entries}})


class TestListRepoFiles:
    @pytest.mark.asyncio
    async def test_returns_files_and_meta(self, source: ModelScopeSource, monkeypatch: pytest.MonkeyPatch) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return _files_response(
                [
                    {"Type": "blob", "Path": "config.json", "Size": 663},
                    {"Type": "blob", "Path": "model.safetensors", "Size": 1746942600},
                ]
            )

        _patch_transport(monkeypatch, handler)
        files, meta = await source.list_repo_files(MODEL_URL)

        assert [f["path"] for f in files] == ["config.json", "model.safetensors"]
        assert meta["repo"] == "Qwen/Qwen3.5-0.8B"
        assert meta["owner"] == "Qwen"
        assert meta["name"] == "Qwen3.5-0.8B"
        assert meta["kind"] == "model"
        assert meta["revision"] == "master"
        assert meta["folder_name"] == "Qwen3.5-0.8B"
        assert meta["web_prefix"] == "models"

    @pytest.mark.asyncio
    async def test_dataset_meta_uses_datasets_prefix(
        self, source: ModelScopeSource, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        seen: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["path"] = request.url.path
            return _files_response([{"Type": "blob", "Path": "main/train.parquet", "Size": 10}])

        _patch_transport(monkeypatch, handler)
        files, meta = await source.list_repo_files(DATASET_URL)

        assert "/api/v1/datasets/AI-ModelScope/gsm8k/repo/tree" in seen["path"]
        assert meta["kind"] == "dataset"
        assert meta["web_prefix"] == "datasets"
        assert files[0]["path"] == "main/train.parquet"

    @pytest.mark.asyncio
    async def test_empty_repo_raises_actionable_error(
        self, source: ModelScopeSource, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_transport(monkeypatch, lambda request: _files_response([]))
        with pytest.raises(ValueError, match="没有可下载文件"):
            await source.list_repo_files(MODEL_URL)

    @pytest.mark.asyncio
    async def test_non_dataset_url_raises(self, source: ModelScopeSource) -> None:
        with pytest.raises(ValueError, match="Not a ModelScope"):
            await source.list_repo_files("https://example.com/x")

    @pytest.mark.asyncio
    async def test_only_directories_yields_actionable_error(
        self, source: ModelScopeSource, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_transport(
            monkeypatch,
            lambda request: _files_response([{"Type": "tree", "Path": "main", "Size": 0}]),
        )
        with pytest.raises(ValueError, match="没有可下载文件"):
            await source.list_repo_files(DATASET_URL)

    @pytest.mark.asyncio
    async def test_token_is_passed_to_listing(self, source: ModelScopeSource, monkeypatch: pytest.MonkeyPatch) -> None:
        seen: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["auth"] = request.headers.get("authorization")
            return _files_response([{"Type": "blob", "Path": "a.bin", "Size": 1}])

        _patch_transport(monkeypatch, handler)
        await source.list_repo_files(MODEL_URL, token="ms-secret")
        assert seen["auth"] == "Bearer ms-secret"


# ── URL building & object keys ──────────────────────────────


class TestBuildFileUrl:
    @pytest.mark.parametrize(
        "meta,path,expected",
        [
            (
                {"kind": "model", "owner": "Qwen", "name": "Qwen3.5-0.8B", "revision": "master"},
                "config.json",
                "https://modelscope.cn/models/Qwen/Qwen3.5-0.8B/resolve/master/config.json",
            ),
            (
                {"kind": "dataset", "owner": "AI-ModelScope", "name": "gsm8k", "revision": "master"},
                "main/train.parquet",
                "https://modelscope.cn/datasets/AI-ModelScope/gsm8k/resolve/master/main/train.parquet",
            ),
            (
                {"kind": "model", "owner": "o", "name": "n", "revision": "v1"},
                "a/b.bin",
                "https://modelscope.cn/models/o/n/resolve/v1/a/b.bin",
            ),
        ],
    )
    def test_builds_resolve_urls(self, source: ModelScopeSource, meta: dict, path: str, expected: str) -> None:
        assert source.build_file_url(meta, path) == expected

    def test_missing_revision_defaults_to_master(self, source: ModelScopeSource) -> None:
        meta = {"kind": "model", "owner": "o", "name": "n"}
        assert "/resolve/master/" in source.build_file_url(meta, "f.bin")

    def test_derive_object_key(self, source: ModelScopeSource) -> None:
        meta = {"kind": "model", "owner": "Qwen", "name": "Qwen3.5-0.8B", "revision": "master"}
        assert source.derive_object_key(meta, "config.json") == "modelscope/models/Qwen/Qwen3.5-0.8B/master/config.json"

    def test_derive_object_key_dataset(self, source: ModelScopeSource) -> None:
        meta = {"kind": "dataset", "owner": "AI-ModelScope", "name": "gsm8k", "revision": "master"}
        assert (
            source.derive_object_key(meta, "main/train.parquet")
            == "modelscope/datasets/AI-ModelScope/gsm8k/master/main/train.parquet"
        )

    def test_object_key_is_stable_across_calls(self, source: ModelScopeSource) -> None:
        """Repeated downloads must hit the same key, else dedup is meaningless."""
        meta = {"kind": "model", "owner": "o", "name": "n", "revision": "master"}
        assert source.derive_object_key(meta, "f.bin") == source.derive_object_key(meta, "f.bin")


# ── File metadata ───────────────────────────────────────────


class TestGetFileInfo:
    @pytest.mark.asyncio
    async def test_head_reports_size(self, source: ModelScopeSource, monkeypatch: pytest.MonkeyPatch) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                headers={"content-length": "663", "content-type": "application/json", "etag": '"abc"'},
            )

        monkeypatch.setattr(
            "etransfer.plugins.sources.modelscope._http_client",
            lambda token=None, timeout=None: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        info = await source.get_file_info(FILE_URL)

        assert info.size == 663
        assert info.filename == "config.json"
        assert info.extra["repo"] == "Qwen/Qwen3.5-0.8B"
        assert info.extra["kind"] == "model"

    @pytest.mark.asyncio
    async def test_content_range_gives_size_when_head_is_ranged(
        self, source: ModelScopeSource, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Some CDN edges answer HEAD with 403; we fall back to a ranged GET."""

        calls: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request.method)
            if request.method == "HEAD":
                return httpx.Response(403, text="forbidden")
            return httpx.Response(206, headers={"content-range": "bytes 0-0/1746942600"})

        monkeypatch.setattr(
            "etransfer.plugins.sources.modelscope._http_client",
            lambda token=None, timeout=None: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        info = await source.get_file_info(MODEL_URL + "/resolve/master/model.safetensors")

        assert calls == ["HEAD", "GET"]
        assert info.size == 1746942600

    @pytest.mark.asyncio
    async def test_repo_page_url_raises_helpful_error(self, source: ModelScopeSource) -> None:
        with pytest.raises(ValueError, match="仓库页面"):
            await source.get_file_info(MODEL_URL)


# ── Download ────────────────────────────────────────────────


class TestDownload:
    @pytest.mark.asyncio
    async def test_streams_file_to_disk(
        self, source: ModelScopeSource, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        payload = b"hello-modelscope" * 100

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path.endswith("/resolve/master/data.bin")
            return httpx.Response(200, content=payload, headers={"content-length": str(len(payload))})

        monkeypatch.setattr(
            "etransfer.plugins.sources.modelscope._http_client",
            lambda token=None, timeout=None: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        monkeypatch.setattr("etransfer.plugins.aria2.is_available", lambda: False)

        progress: list[tuple[int, Any]] = []
        out = await source.download(
            MODEL_URL + "/resolve/master/data.bin",
            tmp_path,
            on_progress=lambda done, total: progress.append((done, total)),
        )

        assert out == tmp_path / "data.bin"
        assert out.read_bytes() == payload
        assert progress, "progress callback should fire at least once"
        assert progress[-1][0] == len(payload)
        assert progress[-1][1] == len(payload)

    @pytest.mark.asyncio
    async def test_repo_url_cannot_be_downloaded_directly(self, source: ModelScopeSource, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="仓库页面"):
            await source.download(MODEL_URL, tmp_path)

    @pytest.mark.asyncio
    async def test_non_modelscope_url_raises(self, source: ModelScopeSource, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Not a ModelScope"):
            await source.download("https://example.com/f.bin", tmp_path)


# ── Registry integration ────────────────────────────────────


class TestRegistryIntegration:
    @pytest.mark.asyncio
    async def test_registry_resolves_modelscope_before_direct(self) -> None:
        from etransfer.plugins.registry import PluginRegistry

        registry = PluginRegistry()
        # Register explicitly so the test does not depend on installed metadata.
        registry.register_source(ModelScopeSource)
        resolved = await registry.resolve_source(MODEL_URL)
        assert resolved.name == "modelscope"

    @pytest.mark.asyncio
    async def test_registry_still_falls_back_to_direct_for_plain_urls(self) -> None:
        from etransfer.plugins.registry import PluginRegistry
        from etransfer.plugins.sources.direct import DirectLinkSource

        registry = PluginRegistry()
        registry.register_source(ModelScopeSource)
        registry.register_source(DirectLinkSource)
        resolved = await registry.resolve_source("https://example.com/plain.bin")
        assert resolved.name == "direct"

    def test_entry_point_declared(self) -> None:
        """The plugin must be discoverable via packaging metadata."""
        import pathlib
        import tomllib

        root = pathlib.Path(__file__).resolve().parent.parent
        data = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
        sources = data["project"]["entry-points"]["etransfer.sources"]
        assert sources["modelscope"] == "etransfer.plugins.sources.modelscope:ModelScopeSource"
