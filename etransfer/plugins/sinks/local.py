"""LocalSink — stores files in the server's local filesystem."""

from __future__ import annotations

import logging
import os
import shutil
import uuid
from pathlib import Path
from typing import Optional

from etransfer.plugins.base_sink import BaseSink, PartResult

logger = logging.getLogger("etransfer.plugins.sinks.local")


class LocalSink(BaseSink):
    """Write uploaded parts directly to a local directory.

    ``complete_upload`` moves the assembled file to the final destination.
    This sink is mainly useful as a pass-through / default — it mirrors
    what ``TusStorage`` already does, but exposes it through the Sink
    interface so the pipeline treats *local* the same as *remote*.
    """

    name = "local"
    display_name = "Local Storage"
    supports_multipart = True

    def __init__(self, config: Optional[dict] = None) -> None:
        super().__init__(config)
        base = self._config.get("base_path", "./storage/sink_local")
        self._base_path = Path(base)
        self._base_path.mkdir(parents=True, exist_ok=True)

    async def initialize_upload(self, object_key: str, metadata: dict) -> str:
        session_id = uuid.uuid4().hex
        session_dir = self._base_path / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        # Store the intended final object key
        (session_dir / ".object_key").write_text(object_key, encoding="utf-8")
        return session_id

    async def upload_part(self, session_id: str, part_number: int, data: bytes) -> PartResult:
        part_path = self._base_path / session_id / f"part_{part_number:06d}"
        part_path.write_bytes(data)
        return PartResult(part_number=part_number, etag=f"local-{part_number}")

    async def complete_upload(self, session_id: str, parts: list[PartResult]) -> str:
        session_dir = self._base_path / session_id
        object_key = (session_dir / ".object_key").read_text(encoding="utf-8").strip()
        dest = self._base_path / "completed" / object_key
        dest.parent.mkdir(parents=True, exist_ok=True)

        sorted_parts = sorted(parts, key=lambda p: p.part_number)
        with open(dest, "wb") as out:
            for p in sorted_parts:
                part_path = session_dir / f"part_{p.part_number:06d}"
                out.write(part_path.read_bytes())

        shutil.rmtree(session_dir, ignore_errors=True)
        logger.info("LocalSink completed: %s -> %s", session_id[:8], dest)
        return str(dest)

    async def abort_upload(self, session_id: str) -> None:
        session_dir = self._base_path / session_id
        shutil.rmtree(session_dir, ignore_errors=True)
        logger.info("LocalSink aborted: %s", session_id[:8])

    @classmethod
    def get_config_schema(cls) -> dict:
        return {
            "base_path": {"type": "string", "required": False, "description": "Local directory for sink output"},
        }
