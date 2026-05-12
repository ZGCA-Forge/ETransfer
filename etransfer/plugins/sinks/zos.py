"""ZosSink - push files to China Telecom ZOS via S3-compatible multipart upload."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
from typing import Any, Optional
from urllib.parse import urlparse, urlunparse

from etransfer.plugins.base_sink import BaseSink, PartResult

logger = logging.getLogger("etransfer.plugins.sinks.zos")

_DEFAULT_REGION = "us-east-1"
_DEFAULT_ADDRESSING_STYLE = "path"


def _as_bool(value: Any, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() not in {"0", "false", "no", "off"}
    return bool(value)


def _normalize_zos_endpoint(endpoint: str, bucket: str, secure: bool = True) -> str:
    """Return a boto3 endpoint URL for ZOS/S3-compatible APIs.

    Bucket migration accepts both regional endpoints and bucket-prefixed
    endpoints (``<bucket>.<region-endpoint>``).  The sink follows the same rule
    and strips the bucket prefix because boto3 is configured for path-style
    addressing.
    """

    value = endpoint.strip().rstrip("/")
    if not value:
        raise ValueError("ZOS endpoint is required")

    parsed = urlparse(value)
    if not parsed.scheme:
        scheme = "https" if secure else "http"
        value = f"{scheme}://{value}"
        parsed = urlparse(value)

    if parsed.scheme not in {"http", "https"}:
        raise ValueError("ZOS endpoint must use http or https")
    if not parsed.netloc:
        raise ValueError("ZOS endpoint must include a host")

    host = parsed.hostname or ""
    bucket_prefix = f"{bucket}."
    if bucket and host.startswith(bucket_prefix):
        bare_host = host[len(bucket_prefix) :]
        netloc = f"{bare_host}:{parsed.port}" if parsed.port else bare_host
        parsed = parsed._replace(netloc=netloc, path="", params="", query="", fragment="")

    return urlunparse(parsed).rstrip("/")


class ZosSink(BaseSink):
    """China Telecom ZOS sink using boto3's S3-compatible multipart API."""

    name = "zos"
    display_name = "China Telecom ZOS"
    supports_multipart = True

    def __init__(self, config: Optional[dict] = None) -> None:
        super().__init__(config)
        self._client: Any = None

    @property
    def _bucket(self) -> str:
        bucket = str(self._config.get("bucket", "")).strip()
        if not bucket:
            raise ValueError("ZosSink requires 'bucket' in config")
        return bucket

    @property
    def _region(self) -> str:
        return str(self._config.get("region") or _DEFAULT_REGION).strip() or _DEFAULT_REGION

    def _object_key(self, key: str) -> str:
        prefix = str(self._config.get("prefix", "")).strip("/")
        return f"{prefix}/{key}" if prefix else key

    def _endpoint_url(self) -> str:
        secure = _as_bool(self._config.get("secure"), default=True)
        return _normalize_zos_endpoint(str(self._config.get("endpoint", "")), self._bucket, secure=secure)

    def _ensure_client(self) -> None:
        if self._client is not None:
            return

        try:
            import boto3  # type: ignore[import-untyped]
            from botocore.config import Config  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError("The 'boto3' package is required for ZosSink. Install with: pip install boto3") from exc

        ak = str(self._config.get("ak", "")).strip()
        sk = str(self._config.get("sk", "")).strip()
        if not ak or not sk:
            raise ValueError("ZosSink requires 'ak' and 'sk' in config")

        addressing_style = str(self._config.get("addressing_style") or _DEFAULT_ADDRESSING_STYLE)
        self._client = boto3.client(
            "s3",
            endpoint_url=self._endpoint_url(),
            aws_access_key_id=ak,
            aws_secret_access_key=sk,
            region_name=self._region,
            config=Config(signature_version="s3v4", s3={"addressing_style": addressing_style}),
        )

    @staticmethod
    def _encode_session(upload_id: str, object_key: str) -> str:
        payload = json.dumps({"upload_id": upload_id, "object_key": object_key}, separators=(",", ":")).encode()
        return "zos:" + base64.urlsafe_b64encode(payload).decode().rstrip("=")

    @staticmethod
    def _decode_session(session_id: str) -> tuple[str, str]:
        if not session_id.startswith("zos:"):
            raise ValueError("Invalid ZOS session id")
        raw = session_id[4:]
        raw += "=" * (-len(raw) % 4)
        data = json.loads(base64.urlsafe_b64decode(raw.encode()).decode())
        return str(data["upload_id"]), str(data["object_key"])

    async def initialize_upload(self, object_key: str, metadata: dict) -> str:
        self._ensure_client()
        full_key = self._object_key(object_key)
        clean_metadata = {str(k): str(v) for k, v in metadata.items() if v is not None}
        loop = asyncio.get_running_loop()

        def _create() -> str:
            kwargs: dict[str, Any] = {"Bucket": self._bucket, "Key": full_key}
            if clean_metadata:
                kwargs["Metadata"] = clean_metadata
            resp = self._client.create_multipart_upload(**kwargs)
            return str(resp["UploadId"])

        upload_id = await loop.run_in_executor(None, _create)
        logger.info("ZOS multipart init: bucket=%s key=%s upload_id=%s", self._bucket, full_key, upload_id[:12])
        return self._encode_session(upload_id, full_key)

    async def upload_part(self, session_id: str, part_number: int, data: bytes) -> PartResult:
        self._ensure_client()
        upload_id, full_key = self._decode_session(session_id)
        loop = asyncio.get_running_loop()

        def _upload() -> str:
            resp = self._client.upload_part(
                Bucket=self._bucket,
                Key=full_key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=data,
            )
            return str(resp["ETag"])

        etag = await loop.run_in_executor(None, _upload)
        logger.debug("ZOS upload_part: upload_id=%s part=%d etag=%s", upload_id[:12], part_number, etag[:16])
        return PartResult(part_number=part_number, etag=etag)

    async def complete_upload(self, session_id: str, parts: list[PartResult]) -> str:
        self._ensure_client()
        upload_id, full_key = self._decode_session(session_id)
        sorted_parts = [
            {"PartNumber": p.part_number, "ETag": p.etag} for p in sorted(parts, key=lambda part: part.part_number)
        ]
        loop = asyncio.get_running_loop()

        await loop.run_in_executor(
            None,
            lambda: self._client.complete_multipart_upload(
                Bucket=self._bucket,
                Key=full_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": sorted_parts},
            ),
        )
        url = f"{self._endpoint_url()}/{self._bucket}/{full_key}"
        logger.info("ZOS multipart complete: %s (%d parts)", url, len(parts))
        return url

    async def abort_upload(self, session_id: str) -> None:
        self._ensure_client()
        upload_id, full_key = self._decode_session(session_id)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: self._client.abort_multipart_upload(Bucket=self._bucket, Key=full_key, UploadId=upload_id),
        )
        logger.info("ZOS multipart aborted: %s", upload_id[:12])

    @classmethod
    def get_config_schema(cls) -> dict:
        return {
            "endpoint": {"type": "string", "required": True, "description": "ZOS/S3 endpoint"},
            "bucket": {"type": "string", "required": True, "description": "Bucket name"},
            "ak": {"type": "string", "required": True, "secret": True, "description": "Access Key"},  # nosec B105
            "sk": {"type": "string", "required": True, "secret": True, "description": "Secret Key"},  # nosec B105
            "region": {
                "type": "string",
                "required": False,
                "description": f"Signing region (default: {_DEFAULT_REGION})",
            },
            "prefix": {"type": "string", "required": False, "description": "Object key prefix"},
            "secure": {"type": "boolean", "required": False, "description": "Use HTTPS when endpoint has no scheme"},
            "addressing_style": {
                "type": "select",
                "required": False,
                "description": "S3 addressing style",
                "options": ["path", "virtual"],
            },
        }
