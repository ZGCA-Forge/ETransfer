from __future__ import annotations

import types

import pytest

from etransfer.plugins.base_sink import SinkContext
from etransfer.plugins.registry import PluginRegistry
from etransfer.plugins.sinks.zos import ZosSink, _normalize_zos_endpoint


def test_sink_config_overrides_are_merged_over_preset() -> None:
    config = ZosSink.resolve_config(
        SinkContext(client_metadata={"sink_preset": "default", "sink_config": {"bucket": "override", "prefix": "u1"}}),
        {
            "default": {
                "endpoint": "storage.example.com",
                "bucket": "base",
                "ak": "ak",
                "sk": "sk",
            }
        },
    )

    assert config == {
        "endpoint": "storage.example.com",
        "bucket": "override",
        "ak": "ak",
        "sk": "sk",
        "prefix": "u1",
    }


def test_plugin_registry_includes_builtin_zos_sink() -> None:
    sinks = {info.name for info in PluginRegistry().list_sinks()}

    assert "zos" in sinks


def test_normalize_zos_endpoint_accepts_bucket_prefixed_host() -> None:
    endpoint = _normalize_zos_endpoint(
        "https://source-bucket.storage.example.com",
        "source-bucket",
    )

    assert endpoint == "https://storage.example.com"


def test_normalize_zos_endpoint_adds_scheme_from_secure_flag() -> None:
    endpoint = _normalize_zos_endpoint("storage.example.com", "source-bucket", secure=False)

    assert endpoint == "http://storage.example.com"


@pytest.mark.asyncio
async def test_zos_sink_uses_boto3_multipart_lifecycle(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, dict]] = []

    class FakeConfig:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

    class FakeClient:
        def create_multipart_upload(self, **kwargs: object) -> dict[str, str]:
            calls.append(("create", kwargs))
            return {"UploadId": "upload-1"}

        def upload_part(self, **kwargs: object) -> dict[str, str]:
            calls.append(("part", kwargs))
            return {"ETag": '"etag-1"'}

        def complete_multipart_upload(self, **kwargs: object) -> dict[str, object]:
            calls.append(("complete", kwargs))
            return {}

        def abort_multipart_upload(self, **kwargs: object) -> dict[str, object]:
            calls.append(("abort", kwargs))
            return {}

    fake_client = FakeClient()

    def fake_boto3_client(service: str, **kwargs: object) -> FakeClient:
        calls.append(("client", {"service": service, **kwargs}))
        return fake_client

    boto3_mod = types.ModuleType("boto3")
    boto3_mod.client = fake_boto3_client  # type: ignore[attr-defined]
    botocore_mod = types.ModuleType("botocore")
    botocore_config_mod = types.ModuleType("botocore.config")
    botocore_config_mod.Config = FakeConfig  # type: ignore[attr-defined]
    monkeypatch.setitem(__import__("sys").modules, "boto3", boto3_mod)
    monkeypatch.setitem(__import__("sys").modules, "botocore", botocore_mod)
    monkeypatch.setitem(__import__("sys").modules, "botocore.config", botocore_config_mod)

    sink = ZosSink(
        {
            "endpoint": "https://bucket.storage.example.com",
            "bucket": "bucket",
            "ak": "ak",
            "sk": "sk",
            "prefix": "backup",
        }
    )

    session_id = await sink.initialize_upload("dir/file.bin", {"task_id": "task-1"})
    part = await sink.upload_part(session_id, 1, b"payload")
    result_url = await sink.complete_upload(session_id, [part])

    assert result_url == "https://storage.example.com/bucket/backup/dir/file.bin"
    assert calls[0][0] == "client"
    assert calls[0][1]["service"] == "s3"
    assert calls[0][1]["endpoint_url"] == "https://storage.example.com"
    assert calls[1] == (
        "create",
        {"Bucket": "bucket", "Key": "backup/dir/file.bin", "Metadata": {"task_id": "task-1"}},
    )
    assert calls[2][0] == "part"
    assert calls[2][1]["UploadId"] == "upload-1"
    assert calls[3][0] == "complete"
    assert calls[3][1]["MultipartUpload"] == {"Parts": [{"PartNumber": 1, "ETag": '"etag-1"'}]}
