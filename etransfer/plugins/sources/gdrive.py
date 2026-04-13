"""Google Drive source plugin — download files from Google Drive share links."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import parse_qs, urlparse

import httpx

from etransfer.plugins.base_source import BaseSource, RemoteFileInfo

logger = logging.getLogger("etransfer.plugins.sources.gdrive")

_CHUNK = 4 * 1024 * 1024
_DOWNLOAD_URL = "https://drive.google.com/uc?export=download&id={file_id}"
_CONFIRM_PATTERN = re.compile(r'name="uuid" value="([^"]+)"')
_DRIVE_API_FILES = "https://www.googleapis.com/drive/v3/files"
_FOLDER_MIME = "application/vnd.google-apps.folder"


def _extract_file_id(url: str) -> Optional[str]:
    """Extract Google Drive file ID from various URL formats."""
    parsed = urlparse(url)
    # /file/d/FILE_ID/...
    m = re.search(r"/file/d/([a-zA-Z0-9_-]+)", parsed.path)
    if m:
        return m.group(1)
    # /open?id=FILE_ID
    # /uc?id=FILE_ID
    qs = parse_qs(parsed.query)
    if "id" in qs:
        return qs["id"][0]
    return None


def _extract_folder_id(url: str) -> Optional[str]:
    """Extract Google Drive folder ID from folder URLs."""
    parsed = urlparse(url)
    m = re.search(r"/folders/([a-zA-Z0-9_-]+)", parsed.path)
    if m:
        return m.group(1)
    return None


class GoogleDriveSource(BaseSource):
    """Download files from Google Drive share links.

    Handles the large-file virus-scan confirmation automatically.
    """

    name = "gdrive"
    display_name = "Google Drive"
    supported_hosts = ["drive.google.com", "docs.google.com"]
    priority = 10

    google_api_key: Optional[str] = None

    async def can_handle(self, url: str) -> bool:
        return _extract_file_id(url) is not None or _extract_folder_id(url) is not None

    def is_repo_url(self, url: str) -> bool:
        """Check if this is a folder URL (not a single file)."""
        return _extract_file_id(url) is None and _extract_folder_id(url) is not None

    async def list_repo_files(self, url: str) -> tuple[list[dict], dict]:
        """List all files in a Google Drive folder (requires google_api_key)."""
        folder_id = _extract_folder_id(url)
        if not folder_id:
            raise ValueError(f"Not a Google Drive folder URL: {url}")

        if not self.google_api_key:
            raise ValueError(
                "Google Drive 文件夹下载需要配置 Google API Key。\n"
                "1. 在 Google Cloud Console 创建 API Key 并启用 Drive API\n"
                "2. 在 config.yaml 中添加:\n"
                "   google_api_key: \"your-key-here\""
            )

        all_files: list[dict] = []
        page_token: Optional[str] = None

        async with httpx.AsyncClient(timeout=30) as client:
            while True:
                params: dict[str, str] = {
                    "q": f"'{folder_id}' in parents and trashed=false",
                    "key": self.google_api_key,
                    "fields": "nextPageToken,files(id,name,mimeType,size)",
                    "pageSize": "1000",
                }
                if page_token:
                    params["pageToken"] = page_token

                resp = await client.get(_DRIVE_API_FILES, params=params)
                resp.raise_for_status()
                data = resp.json()

                for f in data.get("files", []):
                    if f.get("mimeType") == _FOLDER_MIME:
                        continue
                    all_files.append({
                        "path": f["name"],
                        "size": int(f.get("size", 0)),
                        "gdrive_id": f["id"],
                    })

                page_token = data.get("nextPageToken")
                if not page_token:
                    break

        # Get folder name
        folder_name = folder_id[:8]
        try:
            resp = await client.get(
                f"{_DRIVE_API_FILES}/{folder_id}",
                params={"key": self.google_api_key, "fields": "name"},
            )
            if resp.status_code == 200:
                folder_name = resp.json().get("name", folder_name)
        except Exception:
            pass

        meta = {"folder_id": folder_id, "folder_name": folder_name}
        return all_files, meta

    def build_file_url(self, meta: dict, file_path: str, gdrive_id: str = "") -> str:
        """Build a download URL for a file in a Google Drive folder."""
        if gdrive_id:
            return f"https://drive.google.com/file/d/{gdrive_id}/view"
        return f"https://drive.google.com/file/d/{file_path}/view"

    async def get_file_info(self, url: str) -> RemoteFileInfo:
        file_id = _extract_file_id(url)
        if not file_id:
            raise ValueError(f"Cannot extract file ID from: {url}")

        dl_url = _DOWNLOAD_URL.format(file_id=file_id)
        async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
            resp = await client.head(dl_url)
            cd = resp.headers.get("content-disposition", "")
            filename = _filename_from_cd(cd) or f"gdrive_{file_id}"
            size_str = resp.headers.get("content-length")

        return RemoteFileInfo(
            filename=filename,
            size=int(size_str) if size_str else 0,
            mime_type=resp.headers.get("content-type") or "application/octet-stream",
        )

    async def download(
        self,
        url: str,
        dest: Path,
        on_progress: Optional[Callable[[int, Optional[int]], Any]] = None,
    ) -> Path:
        file_id = _extract_file_id(url)
        if not file_id:
            raise ValueError(f"Cannot extract file ID from: {url}")

        dest.mkdir(parents=True, exist_ok=True)
        dl_url = _DOWNLOAD_URL.format(file_id=file_id)

        async with httpx.AsyncClient(follow_redirects=True, timeout=httpx.Timeout(30, read=600), cookies={}) as client:
            resp = await client.get(dl_url)

            # Large files require confirmation (virus scan warning)
            if b"download_warning" in resp.content or b"confirm" in resp.content:
                # Extract confirm token and retry
                confirm_match = re.search(rb'confirm=([a-zA-Z0-9_-]+)', resp.content)
                uuid_match = _CONFIRM_PATTERN.search(resp.text)
                if confirm_match:
                    token = confirm_match.group(1).decode()
                    dl_url = f"{dl_url}&confirm={token}"
                elif uuid_match:
                    dl_url = f"{dl_url}&confirm=t&uuid={uuid_match.group(1)}"
                else:
                    dl_url = f"{dl_url}&confirm=t"

            # Use aria2c if available (after resolving confirm URL)
            from etransfer.plugins import aria2

            if aria2.is_available():
                logger.info("Using aria2c for Google Drive download: %s", dl_url)
                return await aria2.download(dl_url, dest, on_progress=on_progress)

            filename = "download"
            downloaded = 0
            total: Optional[int] = None

            async with client.stream("GET", dl_url) as stream:
                stream.raise_for_status()
                cd = stream.headers.get("content-disposition", "")
                filename = _filename_from_cd(cd) or f"gdrive_{file_id}"
                total_str = stream.headers.get("content-length")
                total = int(total_str) if total_str else None

                file_path = dest / filename
                with open(file_path, "wb") as f:
                    async for chunk in stream.aiter_bytes(chunk_size=_CHUNK):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if on_progress:
                            on_progress(downloaded, total)

        logger.info("Downloaded Google Drive %s -> %s (%d bytes)", file_id, file_path, downloaded)
        return file_path


def _filename_from_cd(cd: str) -> Optional[str]:
    if not cd:
        return None
    for part in cd.split(";"):
        part = part.strip()
        if part.startswith("filename*="):
            name = part.split("''", 1)[-1] if "''" in part else part[len("filename*="):]
            from urllib.parse import unquote
            return unquote(name.strip('"'))
        if part.startswith("filename="):
            return part[len("filename="):].strip().strip('"').strip("'")
    return None
