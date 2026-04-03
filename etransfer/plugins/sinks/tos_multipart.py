"""TosSink — push files to Volcengine TOS via multipart upload."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from etransfer.plugins.base_sink import BaseSink, PartResult

logger = logging.getLogger("etransfer.plugins.sinks.tos")


class TosSink(BaseSink):
    """Volcengine TOS (Torch Object Storage) multipart upload sink.

    Requires the optional ``tos`` package (``pip install tos``).

    Config keys (via ``resolve_config`` or explicit):
        - endpoint, region, bucket, ak, sk
        - prefix (optional): object key prefix
    """

    name = "tos"
    display_name = "Volcengine TOS"
    supports_multipart = True

    def __init__(self, config: Optional[dict] = None) -> None:
        super().__init__(config)
        self._client: Any = None
        self._object_keys: dict[str, str] = {}

    def _ensure_client(self) -> None:
        if self._client is not None:
            return
        try:
            import tos  # type: ignore[import-untyped]
        except ImportError:
            raise ImportError("The 'tos' package is required for TosSink. Install with: pip install tos")

        self._client = tos.TosClientV2(
            ak=self._config["ak"],
            sk=self._config["sk"],
            endpoint=self._config["endpoint"],
            region=self._config["region"],
        )

    @property
    def _bucket(self) -> str:
        return self._config["bucket"]

    def _object_key(self, key: str) -> str:
        prefix = self._config.get("prefix", "").strip("/")
        return f"{prefix}/{key}" if prefix else key

    async def initialize_upload(self, object_key: str, metadata: dict) -> str:
        self._ensure_client()
        full_key = self._object_key(object_key)
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(None, self._client.create_multipart_upload, self._bucket, full_key)
        session_id = resp.upload_id
        self._object_keys[session_id] = full_key
        logger.info("TOS multipart init: bucket=%s key=%s upload_id=%s", self._bucket, full_key, session_id[:12])
        return session_id

    async def upload_part(self, session_id: str, part_number: int, data: bytes) -> PartResult:
        self._ensure_client()
        full_key = self._object_keys[session_id]
        loop = asyncio.get_running_loop()

        def _do_upload() -> str:
            resp = self._client.upload_part(
                self._bucket,
                full_key,
                session_id,
                part_number=part_number,
                content=data,
            )
            return resp.etag

        etag = await loop.run_in_executor(None, _do_upload)
        logger.debug("TOS upload_part: upload_id=%s part=%d etag=%s", session_id[:12], part_number, etag[:16])
        return PartResult(part_number=part_number, etag=etag)

    async def complete_upload(self, session_id: str, parts: list[PartResult]) -> str:
        self._ensure_client()
        import tos  # type: ignore[import-untyped]

        full_key = self._object_keys.pop(session_id, "")
        loop = asyncio.get_running_loop()

        uploaded_parts = [
            tos.models2.UploadedPart(p.part_number, p.etag) for p in sorted(parts, key=lambda p: p.part_number)
        ]

        await loop.run_in_executor(
            None,
            lambda: self._client.complete_multipart_upload(self._bucket, full_key, session_id, parts=uploaded_parts),
        )
        url = f"https://{self._bucket}.{self._config['endpoint']}/{full_key}"
        logger.info("TOS multipart complete: %s (%d parts)", url, len(parts))
        return url

    async def abort_upload(self, session_id: str) -> None:
        self._ensure_client()
        full_key = self._object_keys.pop(session_id, "")
        if full_key:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                lambda: self._client.abort_multipart_upload(self._bucket, full_key, session_id),
            )
        logger.info("TOS multipart aborted: %s", session_id[:12])

    @classmethod
    def get_config_schema(cls) -> dict:
        return {
            "endpoint": {"type": "string", "required": True, "description": "TOS endpoint"},
            "region": {"type": "string", "required": True, "description": "TOS region"},
            "bucket": {"type": "string", "required": True, "description": "Bucket name"},
            "ak": {"type": "string", "required": True, "secret": True, "description": "Access Key"},  # nosec B105
            "sk": {"type": "string", "required": True, "secret": True, "description": "Secret Key"},  # nosec B105
            "prefix": {"type": "string", "required": False, "description": "Object key prefix"},
        }
