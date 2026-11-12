"""Tests for bucket introspection hooks used by the ModelScope dedup flow."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from etransfer.plugins.base_sink import BaseSink, PartResult
from etransfer.plugins.sinks.local import LocalSink
from etransfer.plugins.sinks.tos_multipart import TosSink
from etransfer.plugins.sinks.zos import ZosSink


class _BareSink(BaseSink):
    """A sink with no introspection support (the default)."""

    name = "bare"

    async def initialize_upload(self, object_key: str, metadata: dict) -> str:
        return "sid"

    async def upload_part(self, session_id: str, part_number: int, data: bytes) -> PartResult:
        return PartResult(part_number=part_number, etag="e")

    async def complete_upload(self, session_id: str, parts: list[PartResult]) -> str:
        return "url"

    async def abort_upload(self, session_id: str) -> None:
        return None


class TestBaseSinkDefaults:
    @pytest.mark.asyncio
    async def test_object_exists_returns_none(self) -> None:
        sink = _BareSink()
        assert await sink.object_exists("any/key") is None

    @pytest.mark.asyncio
    async def test_list_objects_returns_none(self) -> None:
        assert await _BareSink().list_objects("prefix/") is None

    def test_supports_introspection_false_by_default(self) -> None:
        assert _BareSink.supports_introspection() is False


class TestLocalSinkIntrospection:
    @pytest.mark.asyncio
    async def test_object_exists_after_completion(self, tmp_path: Path) -> None:
        sink = LocalSink({"base_path": str(tmp_path)})
        key = "modelscope/models/Qwen/Qwen3.5-0.8B/master/config.json"

        assert await sink.object_exists(key) is False

        sid = await sink.initialize_upload(key, {})
        await sink.upload_part(sid, 1, b"hello ")
        await sink.upload_part(sid, 2, b"world")
        await sink.complete_upload(sid, [PartResult(part_number=1, etag="a"), PartResult(part_number=2, etag="b")])

        assert await sink.object_exists(key) is True

    @pytest.mark.asyncio
    async def test_completed_content_is_reassembled(self, tmp_path: Path) -> None:
        sink = LocalSink({"base_path": str(tmp_path)})
        sid = await sink.initialize_upload("a/b.txt", {})
        await sink.upload_part(sid, 1, b"AB")
        await sink.upload_part(sid, 2, b"CD")
        url = await sink.complete_upload(
            sid, [PartResult(part_number=2, etag="2"), PartResult(part_number=1, etag="1")]
        )

        assert Path(url).read_bytes() == b"ABCD"

    @pytest.mark.asyncio
    async def test_list_objects_filters_by_prefix(self, tmp_path: Path) -> None:
        sink = LocalSink({"base_path": str(tmp_path)})
        for key in (
            "modelscope/models/Qwen/Qwen3.5-0.8B/master/config.json",
            "modelscope/models/Qwen/Qwen3.5-0.8B/master/README.md",
            "modelscope/models/other/repo/master/config.json",
        ):
            sid = await sink.initialize_upload(key, {})
            await sink.upload_part(sid, 1, b"x")
            await sink.complete_upload(sid, [PartResult(part_number=1, etag="e")])

        listed = await sink.list_objects(prefix="modelscope/models/Qwen/Qwen3.5-0.8B/")
        assert listed is not None
        keys = sorted(item["key"] for item in listed)
        assert keys == [
            "modelscope/models/Qwen/Qwen3.5-0.8B/master/README.md",
            "modelscope/models/Qwen/Qwen3.5-0.8B/master/config.json",
        ]
        assert all(item["size"] == 1 for item in listed)

    @pytest.mark.asyncio
    async def test_list_objects_honours_limit(self, tmp_path: Path) -> None:
        sink = LocalSink({"base_path": str(tmp_path)})
        for i in range(5):
            sid = await sink.initialize_upload(f"p/f{i}.bin", {})
            await sink.upload_part(sid, 1, b"x")
            await sink.complete_upload(sid, [PartResult(part_number=1, etag="e")])

        listed = await sink.list_objects(prefix="p/", limit=3)
        assert listed is not None
        assert len(listed) == 3

    @pytest.mark.asyncio
    async def test_list_objects_on_empty_root_returns_empty(self, tmp_path: Path) -> None:
        sink = LocalSink({"base_path": str(tmp_path / "missing")})
        assert await sink.list_objects() == []

    def test_local_supports_introspection(self) -> None:
        assert LocalSink.supports_introspection() is True


class TestZosSinkIntrospection:
    """ZOS uses boto3; we stub the client to keep the test offline."""

    @staticmethod
    def _sink() -> ZosSink:
        sink = ZosSink({"bucket": "bkt", "endpoint": "https://zos.example.com", "ak": "a", "sk": "s"})
        return sink

    @pytest.mark.asyncio
    async def test_object_exists_true_on_head_success(self) -> None:
        sink = self._sink()

        class _Client:
            def head_object(self, **kwargs: Any) -> dict:
                assert kwargs["Key"] == "key.json"
                return {}

        sink._client = _Client()
        assert await sink.object_exists("key.json") is True

    @pytest.mark.asyncio
    async def test_object_exists_false_on_404(self) -> None:
        sink = self._sink()

        class _Err(Exception):
            response = {"Error": {"Code": "404"}, "ResponseMetadata": {"HTTPStatusCode": 404}}

        class _Client:
            def head_object(self, **kwargs: Any) -> dict:
                raise _Err()

        sink._client = _Client()
        assert await sink.object_exists("key.json") is False

    @pytest.mark.asyncio
    async def test_object_exists_none_on_other_errors(self) -> None:
        """Any non-404 error is 'unknown', never 'absent'."""
        sink = self._sink()

        class _Client:
            def head_object(self, **kwargs: Any) -> dict:
                raise RuntimeError("network down")

        sink._client = _Client()
        assert await sink.object_exists("key.json") is None

    @pytest.mark.asyncio
    async def test_prefix_is_applied(self) -> None:
        sink = ZosSink({"bucket": "bkt", "endpoint": "https://x", "ak": "a", "sk": "s", "prefix": "pre/fix"})
        seen: dict[str, Any] = {}

        class _Client:
            def head_object(self, **kwargs: Any) -> dict:
                seen.update(kwargs)
                return {}

        sink._client = _Client()
        await sink.object_exists("sub/key.json")
        assert seen["Key"] == "pre/fix/sub/key.json"

    @pytest.mark.asyncio
    async def test_list_objects_paginates(self) -> None:
        sink = self._sink()
        pages = [
            {
                "Contents": [{"Key": "p/a", "Size": 1}, {"Key": "p/b", "Size": 2}],
                "IsTruncated": True,
                "NextContinuationToken": "tok",
            },
            {"Contents": [{"Key": "p/c", "Size": 3}], "IsTruncated": False},
        ]
        calls: list[dict] = []

        class _Client:
            def list_objects_v2(self, **kwargs: Any) -> dict:
                calls.append(kwargs)
                return pages[len(calls) - 1]

        sink._client = _Client()
        listed = await sink.list_objects(prefix="p/")

        assert listed == [
            {"key": "p/a", "size": 1},
            {"key": "p/b", "size": 2},
            {"key": "p/c", "size": 3},
        ]
        assert calls[1]["ContinuationToken"] == "tok"

    @pytest.mark.asyncio
    async def test_list_objects_returns_none_on_error(self) -> None:
        sink = self._sink()

        class _Client:
            def list_objects_v2(self, **kwargs: Any) -> dict:
                raise RuntimeError("boom")

        sink._client = _Client()
        assert await sink.list_objects() is None

    def test_zos_supports_introspection(self) -> None:
        assert ZosSink.supports_introspection() is True


class TestTosSinkIntrospection:
    @staticmethod
    def _sink() -> TosSink:
        return TosSink({"bucket": "bkt", "endpoint": "tos.example.com", "region": "r", "ak": "a", "sk": "s"})

    @pytest.mark.asyncio
    async def test_object_exists_true(self) -> None:
        sink = self._sink()
        seen: dict[str, Any] = {}

        class _Client:
            def head_object(self, bucket: str, key: str) -> None:
                seen["bucket"] = bucket
                seen["key"] = key

        sink._client = _Client()
        assert await sink.object_exists("k.json") is True
        assert seen == {"bucket": "bkt", "key": "k.json"}

    @pytest.mark.asyncio
    async def test_object_exists_false_on_status_404(self) -> None:
        sink = self._sink()

        class _Err(Exception):
            status_code = 404

        class _Client:
            def head_object(self, bucket: str, key: str) -> None:
                raise _Err()

        sink._client = _Client()
        assert await sink.object_exists("k.json") is False

    @pytest.mark.asyncio
    async def test_object_exists_none_on_other_errors(self) -> None:
        sink = self._sink()

        class _Client:
            def head_object(self, bucket: str, key: str) -> None:
                raise RuntimeError("nope")

        sink._client = _Client()
        assert await sink.object_exists("k.json") is None

    @pytest.mark.asyncio
    async def test_list_objects_reads_contents(self) -> None:
        sink = self._sink()

        class _Item:
            def __init__(self, key: str, size: int) -> None:
                self.key = key
                self.size = size

        class _Resp:
            contents = [_Item("p/a", 10)]
            is_truncated = False

        class _Client:
            def list_objects(self, bucket: str, **kwargs: Any) -> _Resp:
                return _Resp()

        sink._client = _Client()
        assert await sink.list_objects(prefix="p/") == [{"key": "p/a", "size": 10}]

    def test_tos_supports_introspection(self) -> None:
        assert TosSink.supports_introspection() is True
