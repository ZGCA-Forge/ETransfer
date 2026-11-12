"""Unit tests for the ModelScope hub client (``etransfer.common.modelscope_hub``).

Everything here runs against ``httpx.MockTransport`` — no network. The live
hub is only exercised by ``test_modelscope_live.py``, which is opt-in.
"""

from __future__ import annotations

import json
from typing import Any, Callable

import httpx
import pytest

from etransfer.common import modelscope_hub as ms


def _transport(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.MockTransport:
    return httpx.MockTransport(handler)


def _client(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=_transport(handler), follow_redirects=True)


# ── URL parsing ─────────────────────────────────────────────


class TestUrlParsing:
    @pytest.mark.parametrize(
        "url,kind,repo,path",
        [
            ("https://modelscope.cn/models/Qwen/Qwen3.5-0.8B", "model", "Qwen/Qwen3.5-0.8B", ""),
            ("https://modelscope.cn/models/Qwen/Qwen3.5-0.8B/", "model", "Qwen/Qwen3.5-0.8B", ""),
            ("https://modelscope.cn/models/Qwen/Qwen3.5-0.8B/summary", "model", "Qwen/Qwen3.5-0.8B", ""),
            ("https://www.modelscope.cn/models/Qwen/Qwen3.5-0.8B", "model", "Qwen/Qwen3.5-0.8B", ""),
            ("https://modelscope.cn/datasets/AI-ModelScope/gsm8k", "dataset", "AI-ModelScope/gsm8k", ""),
            (
                "https://modelscope.cn/models/Qwen/Qwen3.5-0.8B/resolve/master/config.json",
                "model",
                "Qwen/Qwen3.5-0.8B",
                "config.json",
            ),
            (
                "https://modelscope.cn/datasets/AI-ModelScope/gsm8k/resolve/master/main/train.parquet",
                "dataset",
                "AI-ModelScope/gsm8k",
                "main/train.parquet",
            ),
            (
                "https://modelscope.cn/models/Qwen/Qwen3.5-0.8B/blob/master/model.safetensors",
                "model",
                "Qwen/Qwen3.5-0.8B",
                "model.safetensors",
            ),
            (
                "https://modelscope.cn/models/Qwen/Qwen3.5-0.8B/file/v1.0/README.md",
                "model",
                "Qwen/Qwen3.5-0.8B",
                "README.md",
            ),
            # Web UI file-browser path for datasets.
            (
                "https://modelscope.cn/datasets/AI-ModelScope/gsm8k/tree/master/main",
                "dataset",
                "AI-ModelScope/gsm8k",
                "main",
            ),
        ],
    )
    def test_parses_known_shapes(self, url: str, kind: str, repo: str, path: str) -> None:
        parsed = ms.parse_url(url)
        assert parsed is not None, url
        ref, file_path = parsed
        assert ref.kind == kind
        assert ref.repo_id == repo
        assert file_path == path

    @pytest.mark.parametrize(
        "url",
        [
            "https://example.com/models/Qwen/Qwen3.5-0.8B",
            "https://modelscope.cn/",
            "https://modelscope.cn/studios/foo/bar",
            "https://modelscope.cn/models/onlyowner",
            "not-a-url",
            "",
        ],
    )
    def test_rejects_non_repo_urls(self, url: str) -> None:
        assert ms.parse_url(url) is None

    def test_unknown_subpage_is_rejected(self) -> None:
        # A path segment that is not a known file action is not a file URL.
        assert ms.parse_url("https://modelscope.cn/models/Qwen/Qwen3.5-0.8B/whatever") is None

    def test_revision_is_captured(self) -> None:
        ref, path = ms.parse_url("https://modelscope.cn/models/o/n/resolve/v1.2.3/a/b.bin")  # type: ignore[misc]
        assert ref.revision == "v1.2.3"
        assert path == "a/b.bin"

    def test_default_revision_is_master(self) -> None:
        # ModelScope's default branch is ``master``, unlike Hugging Face.
        ref, _ = ms.parse_url("https://modelscope.cn/models/o/n")  # type: ignore[misc]
        assert ref.revision == "master"

    def test_url_encoded_path_is_decoded(self) -> None:
        ref, path = ms.parse_url("https://modelscope.cn/models/o/n/resolve/master/dir/a%20b.bin")  # type: ignore[misc]
        assert path == "dir/a b.bin"

    def test_is_modelscope_host(self) -> None:
        assert ms.is_modelscope_url("https://modelscope.cn/x")
        assert ms.is_modelscope_url("https://www.modelscope.cn/x")
        assert not ms.is_modelscope_url("https://modelscope.cn.evil.com/x")
        assert not ms.is_modelscope_url("https://notmodelscope.cn/x")


# ── URL / key construction ──────────────────────────────────


class TestUrlAndKeyBuilding:
    def test_resolve_url_model(self) -> None:
        ref = ms.RepoRef(kind="model", owner="Qwen", name="Qwen3.5-0.8B")
        assert (
            ms.build_resolve_url(ref, "config.json")
            == "https://modelscope.cn/models/Qwen/Qwen3.5-0.8B/resolve/master/config.json"
        )

    def test_resolve_url_dataset(self) -> None:
        ref = ms.RepoRef(kind="dataset", owner="AI-ModelScope", name="gsm8k")
        assert (
            ms.build_resolve_url(ref, "main/train.parquet")
            == "https://modelscope.cn/datasets/AI-ModelScope/gsm8k/resolve/master/main/train.parquet"
        )

    def test_resolve_url_strips_leading_slash(self) -> None:
        ref = ms.RepoRef(kind="model", owner="o", name="n")
        assert ms.build_resolve_url(ref, "/a.bin").endswith("/resolve/master/a.bin")

    def test_object_key_is_deterministic_and_namespaced(self) -> None:
        ref = ms.RepoRef(kind="model", owner="Qwen", name="Qwen3.5-0.8B")
        key = ms.derive_repo_object_key(ref, "config.json")
        assert key == "modelscope/models/Qwen/Qwen3.5-0.8B/master/config.json"
        # Determinism is the whole point: same input, same key, forever.
        assert key == ms.derive_repo_object_key(ref, "config.json")

    def test_object_key_carries_revision(self) -> None:
        a = ms.derive_repo_object_key(ms.RepoRef("model", "o", "n", "master"), "f")
        b = ms.derive_repo_object_key(ms.RepoRef("model", "o", "n", "v2"), "f")
        assert a != b, "different revisions must not collide"

    def test_object_key_respects_config_prefix(self) -> None:
        ref = ms.RepoRef(kind="model", owner="o", name="n")
        key = ms.derive_repo_object_key(ref, "f.bin", prefix="/team-a/")
        assert key == "team-a/modelscope/models/o/n/master/f.bin"

    def test_object_key_dataset_namespace_differs_from_model(self) -> None:
        model_key = ms.derive_repo_object_key(ms.RepoRef("model", "o", "n"), "f")
        dataset_key = ms.derive_repo_object_key(ms.RepoRef("dataset", "o", "n"), "f")
        assert model_key != dataset_key


# ── Repo file listing ───────────────────────────────────────


def _files_payload(paths: list[tuple[str, int, str]]) -> dict[str, Any]:
    return {
        "Code": 200,
        "Data": {"Files": [{"Type": "blob", "Path": p, "Size": s, "Sha256": sh, "IsLFS": True} for p, s, sh in paths]},
    }


class TestListRepoFiles:
    @pytest.mark.asyncio
    async def test_uses_files_endpoint_for_models(self) -> None:
        seen: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["url"] = str(request.url)
            seen["params"] = dict(request.url.params)
            return httpx.Response(200, json=_files_payload([("config.json", 663, "abc")]))

        ref = ms.RepoRef(kind="model", owner="Qwen", name="Qwen3.5-0.8B")
        async with _client(handler) as client:
            files = await ms.list_repo_files(ref, client=client)

        assert "/api/v1/models/Qwen/Qwen3.5-0.8B/repo/files" in seen["url"]
        assert "repo/tree" not in seen["url"]
        assert seen["params"]["Revision"] == "master"
        assert seen["params"]["Recursive"] == "True"
        assert files == [{"path": "config.json", "size": 663, "sha256": "abc", "lfs": True}]

    @pytest.mark.asyncio
    async def test_uses_tree_endpoint_for_datasets(self) -> None:
        seen: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["url"] = str(request.url)
            return httpx.Response(200, json=_files_payload([("main/train.parquet", 100, "s")]))

        ref = ms.RepoRef(kind="dataset", owner="AI-ModelScope", name="gsm8k")
        async with _client(handler) as client:
            await ms.list_repo_files(ref, client=client)

        assert "/api/v1/datasets/AI-ModelScope/gsm8k/repo/tree" in seen["url"]
        assert "repo/files" not in seen["url"]

    @pytest.mark.asyncio
    async def test_filters_directory_entries(self) -> None:
        payload = {
            "Code": 200,
            "Data": {
                "Files": [
                    {"Type": "tree", "Path": "main", "Size": 0},
                    {"Type": "tree", "Path": "socratic", "Size": 0},
                    {"Type": "blob", "Path": "main/train.parquet", "Size": 10},
                    {"Type": "blob", "Path": "README.md", "Size": 5},
                ]
            },
        }

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=payload)

        async with _client(handler) as client:
            files = await ms.list_repo_files(ms.RepoRef("dataset", "o", "n"), client=client)

        # Directory nodes must never become download tasks.
        assert [f["path"] for f in files] == ["README.md", "main/train.parquet"]

    @pytest.mark.asyncio
    async def test_dedupes_repeated_paths_and_sorts(self) -> None:
        payload = _files_payload([("b.bin", 1, ""), ("a.bin", 2, ""), ("b.bin", 1, "")])

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=payload)

        async with _client(handler) as client:
            files = await ms.list_repo_files(ms.RepoRef("model", "o", "n"), client=client)

        assert [f["path"] for f in files] == ["a.bin", "b.bin"]

    @pytest.mark.asyncio
    async def test_missing_size_defaults_to_zero(self) -> None:
        payload = {"Code": 200, "Data": {"Files": [{"Type": "blob", "Path": "x"}]}}

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=payload)

        async with _client(handler) as client:
            files = await ms.list_repo_files(ms.RepoRef("model", "o", "n"), client=client)

        assert files[0]["size"] == 0
        assert files[0]["sha256"] == ""
        assert files[0]["lfs"] is False

    @pytest.mark.asyncio
    async def test_404_raises_modelscope_error(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404, text="404 page not found")

        async with _client(handler) as client:
            with pytest.raises(ms.ModelScopeError, match="not found"):
                await ms.list_repo_files(ms.RepoRef("model", "o", "n"), client=client)

    @pytest.mark.asyncio
    async def test_405_mentions_endpoint_mismatch(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(405, json={"Code": 10020000400})

        async with _client(handler) as client:
            with pytest.raises(ms.ModelScopeError, match="mismatch"):
                await ms.list_repo_files(ms.RepoRef("dataset", "o", "n"), client=client)

    @pytest.mark.asyncio
    async def test_upstream_error_code_explains_itself(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"Code": 10016801000, "Message": "参数错误"})

        async with _client(handler) as client:
            with pytest.raises(ms.ModelScopeError, match="参数错误"):
                await ms.list_repo_files(ms.RepoRef("model", "o", "n"), client=client)

    @pytest.mark.asyncio
    async def test_malformed_payload_raises(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"Code": 200, "Data": {}})

        async with _client(handler) as client:
            with pytest.raises(ms.ModelScopeError, match="Files"):
                await ms.list_repo_files(ms.RepoRef("model", "o", "n"), client=client)

    @pytest.mark.asyncio
    async def test_non_dict_payload_raises(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=[1, 2, 3])

        async with _client(handler) as client:
            with pytest.raises(ms.ModelScopeError, match="payload type"):
                await ms.list_repo_files(ms.RepoRef("model", "o", "n"), client=client)

    @pytest.mark.asyncio
    async def test_root_param_is_forwarded(self) -> None:
        seen: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["params"] = dict(request.url.params)
            return httpx.Response(200, json=_files_payload([("d/x.bin", 1, "")]))

        async with _client(handler) as client:
            await ms.list_repo_files(ms.RepoRef("model", "o", "n"), root="d", client=client)

        assert seen["params"]["Root"] == "d"


# ── Discovery ───────────────────────────────────────────────


def _discovery_payload(key: str, items: list[dict], total: int = 1) -> dict[str, Any]:
    return {
        "success": True,
        "request_id": "rid",
        "data": {key: items, "total_count": total, "page_number": 1, "page_size": len(items)},
    }


class TestDiscovery:
    @pytest.mark.asyncio
    async def test_list_models_envelope(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json=_discovery_payload(
                    "models",
                    [{"id": "Qwen/Qwen3.5-0.8B", "file_size": 1769980952, "downloads": 2515986}],
                    total=288411,
                ),
            )

        async with _client(handler) as client:
            result = await ms.list_models(client=client)

        assert result["total_count"] == 288411
        assert result["items"][0]["id"] == "Qwen/Qwen3.5-0.8B"
        assert result["page_size"] == 1

    @pytest.mark.asyncio
    async def test_list_models_uses_search_param_only(self) -> None:
        seen: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["params"] = dict(request.url.params)
            seen["path"] = request.url.path
            return httpx.Response(200, json=_discovery_payload("models", []))

        async with _client(handler) as client:
            await ms.list_models(search="qwen", client=client)

        # Upstream silently ignores name/query/keyword, so we must not send them.
        assert seen["params"]["search"] == "qwen"
        for ignored in ("name", "query", "keyword"):
            assert ignored not in seen["params"]
        assert seen["path"] == "/openapi/v1/models"

    @pytest.mark.asyncio
    async def test_sort_is_lowercased(self) -> None:
        seen: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["params"] = dict(request.url.params)
            return httpx.Response(200, json=_discovery_payload("models", []))

        async with _client(handler) as client:
            await ms.list_models(sort="Downloads", client=client)

        # Upstream errors on "Downloads"; only the lowercase form is valid.
        assert seen["params"]["sort"] == "downloads"

    @pytest.mark.asyncio
    async def test_invalid_sort_is_rejected_locally(self) -> None:
        with pytest.raises(ValueError, match="sort must be one of"):
            await ms.list_models(sort="stars")

    @pytest.mark.asyncio
    async def test_default_sort_is_not_sent(self) -> None:
        seen: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["params"] = dict(request.url.params)
            return httpx.Response(200, json=_discovery_payload("models", []))

        async with _client(handler) as client:
            await ms.list_models(sort="default", client=client)

        assert "sort" not in seen["params"]

    @pytest.mark.asyncio
    async def test_page_size_is_clamped(self) -> None:
        seen: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["params"] = dict(request.url.params)
            return httpx.Response(200, json=_discovery_payload("models", []))

        async with _client(handler) as client:
            await ms.list_models(page_size=5000, page_number=0, client=client)

        assert seen["params"]["page_size"] == "100"
        assert seen["params"]["page_number"] == "1"

    @pytest.mark.asyncio
    async def test_list_datasets_hits_datasets_endpoint(self) -> None:
        seen: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["path"] = request.url.path
            return httpx.Response(200, json=_discovery_payload("datasets", [{"id": "a/b"}]))

        async with _client(handler) as client:
            result = await ms.list_datasets(client=client)

        assert seen["path"] == "/openapi/v1/datasets"
        assert result["items"][0]["id"] == "a/b"

    @pytest.mark.asyncio
    async def test_collections_use_collection_list_key(self) -> None:
        seen: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["path"] = request.url.path
            return httpx.Response(
                200,
                json=_discovery_payload("collection_list", [{"slug": "Bigcat19/Krea2-Lora", "item_count": 1}]),
            )

        async with _client(handler) as client:
            result = await ms.list_collections(client=client)

        # Endpoint segment is ``collections`` but the data key is ``collection_list``.
        assert seen["path"] == "/openapi/v1/collections"
        assert result["items"][0]["slug"] == "Bigcat19/Krea2-Lora"

    @pytest.mark.asyncio
    async def test_success_false_raises(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"success": False, "message": "rate limited"})

        async with _client(handler) as client:
            with pytest.raises(ms.ModelScopeError, match="rate limited"):
                await ms.list_models(client=client)


class TestCollections:
    @pytest.mark.asyncio
    async def test_get_collection_returns_items(self) -> None:
        payload = {
            "success": True,
            "data": {
                "slug": "Bigcat19/Krea2-Lora",
                "items": [
                    {
                        "item_type": "model",
                        "item_object_id": "yan303145427/krea2-Cc",
                        "position": 1,
                    }
                ],
            },
        }

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/openapi/v1/collections/Bigcat19/Krea2-Lora"
            return httpx.Response(200, json=payload)

        async with _client(handler) as client:
            data = await ms.get_collection("Bigcat19", "Krea2-Lora", client=client)

        members = ms.collection_members(data)
        assert members == [{"kind": "model", "repo_id": "yan303145427/krea2-Cc"}]

    def test_collection_members_skips_unknown_types(self) -> None:
        data = {
            "items": [
                {"item_type": "model", "item_object_id": "a/b"},
                {"item_type": "dataset", "item_object_id": "c/d"},
                {"item_type": "studio", "item_object_id": "e/f"},
                {"item_type": "model", "item_object_id": "no-slash"},
                {"item_type": "model"},
                "not-a-dict",
            ]
        }
        assert ms.collection_members(data) == [
            {"kind": "model", "repo_id": "a/b"},
            {"kind": "dataset", "repo_id": "c/d"},
        ]

    def test_collection_members_handles_empty(self) -> None:
        assert ms.collection_members({}) == []
        assert ms.collection_members({"items": None}) == []


class TestRepoInfo:
    @pytest.mark.asyncio
    async def test_get_repo_info_unwraps_data(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/openapi/v1/models/Qwen/Qwen3.5-0.8B"
            return httpx.Response(
                200,
                json={"success": True, "data": {"id": "Qwen/Qwen3.5-0.8B", "gated": False, "downloads": 5}},
            )

        async with _client(handler) as client:
            info = await ms.get_repo_info(ms.RepoRef("model", "Qwen", "Qwen3.5-0.8B"), client=client)

        assert info["gated"] is False
        assert info["downloads"] == 5

    @pytest.mark.asyncio
    async def test_get_repo_info_failure_raises(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(502, text="bad gateway")

        async with _client(handler) as client:
            with pytest.raises(ms.ModelScopeError):
                await ms.get_repo_info(ms.RepoRef("model", "o", "n"), client=client)


class TestAuth:
    def test_make_client_sets_bearer_header(self) -> None:
        client = ms._make_client("ms-secret")
        try:
            assert client.headers.get("authorization") == "Bearer ms-secret"
        finally:
            # The factory is sync but returns an AsyncClient; close it without
            # awaiting so the test stays synchronous.
            client._transport = None  # type: ignore[attr-defined]

    def test_make_client_omits_header_without_token(self) -> None:
        client = ms._make_client(None)
        assert "authorization" not in {k.lower() for k in client.headers.keys()}

    @pytest.mark.asyncio
    async def test_token_is_threaded_through_listing(self) -> None:
        captured: dict[str, Any] = {}
        real_make_client = ms._make_client

        def fake_make_client(token: str | None = None) -> httpx.AsyncClient:
            captured["token"] = token

            def handler(request: httpx.Request) -> httpx.Response:
                captured["auth"] = request.headers.get("authorization")
                return httpx.Response(200, json=_discovery_payload("models", []))

            # Mirror the real factory: attach the auth header to the client.
            headers = {"Authorization": f"Bearer {token}"} if token else {}
            return httpx.AsyncClient(transport=_transport(handler), headers=headers)

        ms._make_client = fake_make_client  # type: ignore[assignment]
        try:
            await ms.list_models(token="ms-secret")
        finally:
            ms._make_client = real_make_client  # type: ignore[assignment]

        assert captured["token"] == "ms-secret"
        assert captured["auth"] == "Bearer ms-secret"
