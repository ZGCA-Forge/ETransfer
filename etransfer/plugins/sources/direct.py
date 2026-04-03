"""Direct HTTP/HTTPS link downloader — the default fallback source."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import unquote, urlparse

import httpx

from etransfer.plugins.base_source import BaseSource, RemoteFileInfo

logger = logging.getLogger("etransfer.plugins.sources.direct")

_DEFAULT_CHUNK = 4 * 1024 * 1024  # 4 MB streaming buffer


class DirectLinkSource(BaseSource):
    """Download files from any HTTP/HTTPS direct link.

    This is the lowest-priority source (``priority = -1``) so that
    specialised plugins always match first.  It accepts every
    ``http(s)`` URL that returns a downloadable response.
    """

    name = "direct"
    display_name = "Direct Link"
    supported_hosts: list[str] = []
    priority = -1

    async def can_handle(self, url: str) -> bool:
        scheme = urlparse(url).scheme.lower()
        return scheme in ("http", "https")

    async def get_file_info(self, url: str) -> RemoteFileInfo:
        async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
            resp = await client.head(url)
            resp.raise_for_status()

        content_length = resp.headers.get("content-length")
        content_type = resp.headers.get("content-type")
        etag = resp.headers.get("etag")

        filename = _extract_filename(url, resp.headers)

        return RemoteFileInfo(
            filename=filename,
            size=int(content_length) if content_length else None,
            mime_type=content_type,
            etag=etag,
        )

    async def download(
        self,
        url: str,
        dest: Path,
        on_progress: Optional[Callable[[int, Optional[int]], Any]] = None,
    ) -> Path:
        dest.mkdir(parents=True, exist_ok=True)

        async with httpx.AsyncClient(follow_redirects=True, timeout=httpx.Timeout(30, read=300)) as client:
            async with client.stream("GET", url) as resp:
                resp.raise_for_status()
                filename = _extract_filename(url, resp.headers)
                file_path = dest / filename

                total_str = resp.headers.get("content-length")
                total = int(total_str) if total_str else None
                downloaded = 0

                with open(file_path, "wb") as f:
                    async for chunk in resp.aiter_bytes(chunk_size=_DEFAULT_CHUNK):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if on_progress:
                            on_progress(downloaded, total)

        logger.info("Downloaded %s -> %s (%d bytes)", url, file_path, downloaded)
        return file_path


def _extract_filename(url: str, headers: httpx.Headers) -> str:
    """Best-effort filename extraction from Content-Disposition or URL path."""
    cd = headers.get("content-disposition", "")
    if "filename=" in cd:
        for part in cd.split(";"):
            part = part.strip()
            if part.startswith("filename="):
                name = part[len("filename=") :].strip().strip('"').strip("'")
                if name:
                    return name

    path = urlparse(url).path
    name = unquote(path.rsplit("/", 1)[-1]) if "/" in path else ""
    return name or "download"
