"""FakeBucketSink — talks to the ``tests._fake_bucket`` test server.

Registered programmatically (not via entry_points) inside ``conftest.py``.
"""

from __future__ import annotations

import httpx

from etransfer.plugins.base_sink import BaseSink, PartResult


class FakeBucketSink(BaseSink):
    """S3-compatible mock sink used solely by the CLI test suite."""

    name = "fake_bucket"
    display_name = "Fake Bucket (test)"
    supports_multipart = True

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        cfg = self._config or {}
        self._endpoint: str = str(cfg.get("endpoint", "")).rstrip("/")
        self._bucket: str = str(cfg.get("bucket", "test-bucket"))
        if not self._endpoint:
            raise ValueError("FakeBucketSink requires 'endpoint' in config")

    def _url(self, path: str) -> str:
        return f"{self._endpoint}{path}"

    async def initialize_upload(self, object_key: str, metadata: dict) -> str:
        async with httpx.AsyncClient(timeout=15.0) as http:
            r = await http.post(self._url(f"/objects/{self._bucket}/{object_key}/init"))
            r.raise_for_status()
            return str(r.json()["session_id"])

    async def upload_part(self, session_id: str, part_number: int, data: bytes) -> PartResult:
        async with httpx.AsyncClient(timeout=60.0) as http:
            r = await http.put(
                self._url(f"/sessions/{session_id}/parts/{part_number}"),
                content=data,
            )
            r.raise_for_status()
            body = r.json()
        return PartResult(
            part_number=part_number,
            etag=body["etag"],
            extra={"md5": body["md5"], "size": body["size"]},
        )

    async def complete_upload(self, session_id: str, parts: list[PartResult]) -> str:
        async with httpx.AsyncClient(timeout=30.0) as http:
            r = await http.post(self._url(f"/sessions/{session_id}/complete"))
            r.raise_for_status()
            return str(r.json()["url"])

    async def abort_upload(self, session_id: str) -> None:
        try:
            async with httpx.AsyncClient(timeout=10.0) as http:
                await http.delete(self._url(f"/sessions/{session_id}"))
        except Exception:
            pass

    @classmethod
    def get_config_schema(cls) -> dict:
        return {
            "endpoint": {"type": "string", "required": True, "description": "Fake bucket base URL"},
            "bucket": {"type": "string", "required": False, "description": "Fake bucket name"},
        }
