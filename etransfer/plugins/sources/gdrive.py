"""Google Drive source plugin — download files from Google Drive share links.

File download flow replicates gdown (https://github.com/wkentaro/gdown):
  1. GET  uc?id={file_id}  (session keeps cookies)
  2. If response has Content-Disposition → it's the file → done.
  3. If HTML → parse confirmation page for the real download URL → goto 1.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlsplit, urlunsplit

import httpx

from etransfer.plugins.base_source import BaseSource, RemoteFileInfo

logger = logging.getLogger("etransfer.plugins.sources.gdrive")

_CHUNK = 4 * 1024 * 1024
_UC_DOWNLOAD_URL = "https://drive.google.com/uc?export=download&id={file_id}"
_DRIVE_API_FILES = "https://www.googleapis.com/drive/v3/files"
_FOLDER_MIME = "application/vnd.google-apps.folder"
_EMBEDDED_FOLDER_URL = "https://drive.google.com/embeddedfolderview?id={folder_id}"

# The embeddedfolderview HTML wraps each item in a div.flip-entry with
# id="entry-{item_id}", a href to the file/folder, and the name inside
# <div class="flip-entry-title">{name}</div>.
_RE_ENTRY = re.compile(
    r'id="entry-([-\w]+)"[^>]*>.*?'
    r'href="([^"]+)".*?'
    r'class="flip-entry-title">([^<]+)</div>',
)
_RE_TITLE = re.compile(r"<title>([^<]+)</title>")

_UA_FOLDER = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/98.0.4758.102 Safari/537.36"
)
_UA_FILE = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/39.0.2171.95 Safari/537.36"
)
_MAX_FOLDER_DEPTH = 5


def _get_url_from_gdrive_confirmation(html: str) -> Optional[str]:
    """Parse Google Drive virus-scan confirmation page (replicates gdown).

    Tries three strategies in order:
      1. Direct ``/uc?export=download`` link in an <a> tag.
      2. ``#download-form`` <form> with hidden inputs.
      3. ``"downloadUrl":"..."`` embedded in JavaScript.
    """
    # Strategy 1: href="/uc?export=download..." link
    m = re.search(r'href="(/uc\?export=download[^"]+)"', html)
    if m:
        return "https://docs.google.com" + m.group(1).replace("&amp;", "&")

    # Strategy 2: <form id="download-form" action="URL"> + hidden inputs
    form_m = re.search(
        r'<form[^>]+id="download-form"[^>]+action="([^"]+)"', html,
    )
    if form_m:
        action = form_m.group(1).replace("&amp;", "&")
        params: dict[str, list[str]] = {}
        parts = urlsplit(action)
        params.update(parse_qs(parts.query))
        for inp in re.finditer(
            r'<input[^>]+type="hidden"[^>]*'
            r'name="([^"]+)"[^>]*value="([^"]*)"',
            html,
        ):
            params[inp.group(1)] = [inp.group(2)]
        # Also handle value before name (attribute order varies)
        for inp in re.finditer(
            r'<input[^>]+value="([^"]*)"[^>]*'
            r'name="([^"]+)"[^>]*type="hidden"',
            html,
        ):
            params[inp.group(2)] = [inp.group(1)]
        return urlunsplit(parts._replace(query=urlencode(params, doseq=True)))

    # Strategy 3: downloadUrl in JS
    m = re.search(r'"downloadUrl"\s*:\s*"([^"]+)"', html)
    if m:
        url = m.group(1)
        return url.replace("\\u003d", "=").replace("\\u0026", "&")

    return None


def _filename_from_cd(cd: str) -> Optional[str]:
    """Extract filename from Content-Disposition header (gdown-compatible)."""
    if not cd:
        return None
    from urllib.parse import unquote

    m = re.search(r"filename\*=UTF-8''(.*)", cd)
    if m:
        return unquote(m.group(1)).strip('"')
    m = re.search(r'filename="(.+?)"', cd)
    if m:
        return m.group(1)
    for part in cd.split(";"):
        part = part.strip()
        if part.startswith("filename="):
            return part[len("filename="):].strip().strip('"').strip("'")
    return None


def _extract_file_id(url: str) -> Optional[str]:
    """Extract Google Drive file ID from various URL formats."""
    parsed = urlparse(url)
    m = re.search(r"/file/d/([a-zA-Z0-9_-]+)", parsed.path)
    if m:
        return m.group(1)
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


async def _parse_embedded_folder(
    client: httpx.AsyncClient,
    folder_id: str,
) -> tuple[str, list[tuple[str, str, bool]]]:
    """Fetch embeddedfolderview and parse <a> tags (gdown approach).

    Returns (folder_name, [(child_id, child_name, is_folder), ...]).
    """
    url = _EMBEDDED_FOLDER_URL.format(folder_id=folder_id)
    resp = await client.get(url)
    resp.raise_for_status()
    html = resp.text

    folder_name = folder_id[:8]
    title_m = _RE_TITLE.search(html)
    if title_m:
        raw = title_m.group(1).strip()
        if raw:
            folder_name = raw

    children: list[tuple[str, str, bool]] = []
    seen: set[str] = set()

    for m in _RE_ENTRY.finditer(html):
        entry_id, href, title = m.group(1), m.group(2), m.group(3).strip()
        if entry_id in seen:
            continue
        seen.add(entry_id)
        is_folder = "/drive/folders/" in href
        children.append((entry_id, title, is_folder))

    return folder_name, children


async def _scrape_folder_recursive(
    client: httpx.AsyncClient,
    folder_id: str,
    prefix: str = "",
    depth: int = 0,
) -> tuple[list[dict], str]:
    """Recursively scrape a Google Drive folder via embeddedfolderview.

    Returns (flat_file_list, root_folder_name).
    """
    folder_name, children = await _parse_embedded_folder(client, folder_id)
    files: list[dict] = []

    for cid, cname, is_folder in children:
        if is_folder:
            if depth >= _MAX_FOLDER_DEPTH:
                logger.warning("Skipping subfolder %s/%s — max depth reached", prefix, cname)
                continue
            sub_files, _ = await _scrape_folder_recursive(
                client, cid,
                prefix=f"{prefix}{cname}/" if prefix else f"{cname}/",
                depth=depth + 1,
            )
            files.extend(sub_files)
        else:
            files.append({
                "path": f"{prefix}{cname}" if prefix else cname,
                "size": 0,
                "gdrive_id": cid,
            })

    return files, folder_name


async def _scrape_folder(folder_id: str) -> tuple[list[dict], str]:
    """Scrape Google Drive folder file list without API key (gdown approach).

    Uses the embeddedfolderview endpoint which returns lightweight HTML
    with simple <a> tags — much more stable than parsing the main folder page.
    """
    async with httpx.AsyncClient(
        follow_redirects=True, timeout=30,
        headers={"User-Agent": _UA_FOLDER},
    ) as client:
        return await _scrape_folder_recursive(client, folder_id)


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
        """List all files in a Google Drive folder.

        Uses Google Drive API v3 when an API key is configured; otherwise
        falls back to scraping the folder HTML page (no key required).
        """
        folder_id = _extract_folder_id(url)
        if not folder_id:
            raise ValueError(f"Not a Google Drive folder URL: {url}")

        if self.google_api_key:
            return await self._list_via_api(folder_id)

        logger.info("No google_api_key configured — scraping folder page for %s", folder_id)
        files, folder_name = await _scrape_folder(folder_id)
        if not files:
            raise ValueError(
                f"无法从 Google Drive 文件夹页面提取文件列表 (folder_id={folder_id})。\n"
                "可能原因：文件夹非公开共享，或 Google 页面结构已变更。\n"
                "可配置 google_api_key 使用 API 方式：\n"
                "  1. 在 Google Cloud Console 创建 API Key 并启用 Drive API\n"
                '  2. 在 config.yaml 中添加: google_api_key: "your-key-here"'
            )
        meta = {"folder_id": folder_id, "folder_name": folder_name}
        return files, meta

    async def _list_via_api(self, folder_id: str) -> tuple[list[dict], dict]:
        """List folder files using Google Drive API v3 (requires API key)."""
        all_files: list[dict] = []
        page_token: Optional[str] = None
        folder_name = folder_id[:8]

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
        fid = gdrive_id or file_path
        folder_id = meta.get("folder_id", "")
        base = f"https://drive.google.com/file/d/{fid}/view"
        if folder_id:
            return f"{base}?folder={folder_id}"
        return base

    async def resolve_download_url(self, url: str) -> tuple[str, dict[str, str]]:
        """Resolve the direct download URL following gdown's confirmation flow.

        Returns (direct_url, cookies).  The caller MUST pass the cookies to
        the subsequent download request — Google requires them for large files.

        Uses follow_redirects=False to avoid downloading file content during
        resolution.
        """
        file_id = _extract_file_id(url)
        if not file_id:
            raise ValueError(f"Cannot extract file ID from: {url}")

        base_url = _UC_DOWNLOAD_URL.format(file_id=file_id)

        async with httpx.AsyncClient(
            follow_redirects=False, timeout=30,
            headers={"User-Agent": _UA_FILE},
        ) as client:
            resp = await client.get(base_url)
            cookies = {k: v for k, v in client.cookies.items()}

            # Small files: Google redirects directly to the file
            if resp.status_code in (301, 302, 303, 307, 308):
                return base_url, cookies
            if "Content-Disposition" in resp.headers:
                return base_url, cookies

            # Large files: confirmation page (may have HTML or be empty)
            if resp.text:
                new_url = _get_url_from_gdrive_confirmation(resp.text)
                if new_url:
                    return new_url, cookies

            # Fallback: confirm=t bypass (works for current Google Drive)
            return f"{base_url}&confirm=t", cookies

    async def get_file_info(self, url: str) -> RemoteFileInfo:
        """Get filename and size via a Range request to the resolved URL."""
        file_id = _extract_file_id(url)
        if not file_id:
            raise ValueError(f"Cannot extract file ID from: {url}")

        dl_url, cookies = await self.resolve_download_url(url)
        async with httpx.AsyncClient(
            follow_redirects=True, timeout=30,
            headers={"User-Agent": _UA_FILE}, cookies=cookies,
        ) as client:
            resp = await client.get(dl_url, headers={"Range": "bytes=0-0"})

        cd = resp.headers.get("content-disposition", "")
        filename = _filename_from_cd(cd) or f"gdrive_{file_id}"

        size = 0
        cr = resp.headers.get("content-range", "")
        if "/" in cr:
            try:
                size = int(cr.rsplit("/", 1)[1])
            except (ValueError, IndexError):
                pass
        if not size:
            cl = resp.headers.get("content-length")
            if cl:
                size = int(cl)

        return RemoteFileInfo(
            filename=filename,
            size=size,
            mime_type=resp.headers.get("content-type") or "application/octet-stream",
        )

    async def download(
        self,
        url: str,
        dest: Path,
        on_progress: Optional[Callable[[int, Optional[int]], Any]] = None,
    ) -> Path:
        """Download a Google Drive file to local disk."""
        file_id = _extract_file_id(url)
        if not file_id:
            raise ValueError(f"Cannot extract file ID from: {url}")

        dest.mkdir(parents=True, exist_ok=True)
        dl_url, cookies = await self.resolve_download_url(url)

        from etransfer.plugins import aria2

        if aria2.is_available() and not cookies:
            logger.info("Using aria2c for Google Drive download: %s", dl_url)
            return await aria2.download(dl_url, dest, on_progress=on_progress)

        filename = "download"
        downloaded = 0
        total: Optional[int] = None

        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(30, read=600),
            headers={"User-Agent": _UA_FILE},
            cookies=cookies,
        ) as client:
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
