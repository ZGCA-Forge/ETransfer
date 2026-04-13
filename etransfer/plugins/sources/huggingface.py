"""Hugging Face source plugin — download files from HF Hub."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import unquote, urlparse

import httpx

from etransfer.plugins.base_source import BaseSource, RemoteFileInfo

logger = logging.getLogger("etransfer.plugins.sources.huggingface")

_CHUNK = 4 * 1024 * 1024
_HF_PATTERN = re.compile(
    r"^/(?:datasets/|spaces/)?"
    r"(?P<repo>[^/]+/[^/]+)"
    r"/(?P<action>resolve|blob|raw)"
    r"/(?P<revision>[^/]+)"
    r"/(?P<path>.+)$"
)

_HF_REPO_PATTERN = re.compile(
    r"^/(?:datasets/|spaces/)?(?P<repo>[^/]+/[^/]+)(?:/tree/(?P<revision>[^/]+)(?:/(?P<subdir>.*))?)?/?$"
)


def _parse_hf_url(url: str) -> Optional[dict[str, str]]:
    """Parse a Hugging Face URL into components."""
    parsed = urlparse(url)
    m = _HF_PATTERN.match(parsed.path)
    if not m:
        return None
    path = parsed.path
    if path.startswith("/datasets/"):
        prefix = "datasets/"
    elif path.startswith("/spaces/"):
        prefix = "spaces/"
    else:
        prefix = ""
    return {
        "repo": m.group("repo"),
        "action": m.group("action"),
        "revision": m.group("revision"),
        "path": m.group("path"),
        "host": parsed.hostname or "huggingface.co",
        "prefix": prefix,
    }


def _to_resolve_url(parts: dict[str, str]) -> str:
    """Convert any HF URL to a /resolve/ (direct download) URL."""
    prefix = parts.get("prefix", "")
    return f"https://{parts['host']}/{prefix}{parts['repo']}/resolve/{parts['revision']}/{parts['path']}"


class HuggingFaceSource(BaseSource):
    """Download files from Hugging Face Hub.

    Supports URLs like:
      - https://huggingface.co/user/repo/resolve/main/file.bin
      - https://huggingface.co/user/repo/blob/main/file.bin
      - https://hf-mirror.com/user/repo/resolve/main/file.bin
      - https://hf.xwall.us.kg/user/repo/resolve/main/file.bin
      - https://huggingface.org.cn/user/repo/resolve/main/file.bin
    """

    name = "huggingface"
    display_name = "Hugging Face"
    supported_hosts = [
        "huggingface.co",
        "hf.co",
        "hf-mirror.com",
        "hf-api.gitee.com",
        "huggingface.org.cn",
        "hf.xwall.us.kg",
    ]
    priority = 10

    async def can_handle(self, url: str) -> bool:
        if _parse_hf_url(url) is not None:
            return True
        parsed = urlparse(url)
        if parsed.hostname in self.supported_hosts and _HF_REPO_PATTERN.match(parsed.path):
            return True
        return False

    async def get_file_info(self, url: str) -> RemoteFileInfo:
        parts = _parse_hf_url(url)
        if not parts:
            self._raise_repo_hint(url)

        dl_url = _to_resolve_url(parts)
        async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
            resp = await client.head(dl_url)
            resp.raise_for_status()

        rel_path = unquote(parts["path"])
        size_str = resp.headers.get("content-length")

        return RemoteFileInfo(
            filename=rel_path,
            size=int(size_str) if size_str else 0,
            mime_type=resp.headers.get("content-type") or "application/octet-stream",
            extra={"repo": parts["repo"], "revision": parts["revision"]},
        )

    def _raise_repo_hint(self, url: str) -> None:
        """Raise a helpful error for repo/dataset page URLs."""
        parsed = urlparse(url)
        m = _HF_REPO_PATTERN.match(parsed.path)
        repo = m.group("repo") if m else "?"
        host = parsed.hostname or "huggingface.co"
        raise ValueError(
            f"这是 Hugging Face 仓库页面，不是文件下载链接。\n"
            f"请使用具体文件的 URL，例如:\n"
            f"  https://{host}/{repo}/resolve/main/<filename>"
        )

    def _parse_repo_url(self, url: str) -> Optional[dict[str, str]]:
        """Parse a HF repo/dataset page URL into components."""
        parsed = urlparse(url)
        m = _HF_REPO_PATTERN.match(parsed.path)
        if not m:
            return None
        path = parsed.path
        if path.startswith("/datasets/"):
            api_prefix = "datasets"
        elif path.startswith("/spaces/"):
            api_prefix = "spaces"
        else:
            api_prefix = "models"
        return {
            "repo": m.group("repo"),
            "revision": m.group("revision") or "main",
            "subdir": m.group("subdir") or "",
            "host": parsed.hostname or "huggingface.co",
            "api_prefix": api_prefix,
        }

    def is_repo_url(self, url: str) -> bool:
        """Check if this is a repo/dataset page URL (not a single file)."""
        return _parse_hf_url(url) is None and self._parse_repo_url(url) is not None

    async def list_repo_files(self, url: str) -> tuple[list[dict], dict]:
        """List all files in a HF repo.

        Returns:
            (files, meta) where files is [{path, size, type}, ...]
            and meta is {repo, revision, host, api_prefix, folder_name}.
        """
        info = self._parse_repo_url(url)
        if not info:
            raise ValueError(f"Not a HF repo URL: {url}")

        api_url = (
            f"https://{info['host']}/api/{info['api_prefix']}"
            f"/{info['repo']}/tree/{info['revision']}"
        )
        params: dict[str, str] = {"recursive": "true"}
        if info["subdir"]:
            api_url += f"/{info['subdir']}"

        all_files: list[dict] = []
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.get(api_url, params=params)
            resp.raise_for_status()
            entries = resp.json()

        for entry in entries:
            if entry.get("type") == "file":
                all_files.append({
                    "path": entry["path"],
                    "size": entry.get("size", 0),
                })

        folder_name = info["repo"].split("/")[-1]
        meta = {**info, "folder_name": folder_name}
        return all_files, meta

    def build_file_url(self, meta: dict, file_path: str, **kwargs: Any) -> str:
        """Build a resolve URL for a specific file in a repo."""
        prefix = meta.get("api_prefix", "models")
        path_prefix = "" if prefix == "models" else f"{prefix}/"
        return (
            f"https://{meta['host']}/{path_prefix}{meta['repo']}"
            f"/resolve/{meta['revision']}/{file_path}"
        )

    async def download(
        self,
        url: str,
        dest: Path,
        on_progress: Optional[Callable[[int, Optional[int]], Any]] = None,
    ) -> Path:
        parts = _parse_hf_url(url)
        if not parts:
            self._raise_repo_hint(url)

        dest.mkdir(parents=True, exist_ok=True)
        dl_url = _to_resolve_url(parts)
        rel_path = unquote(parts["path"])
        filename = rel_path.rsplit("/", 1)[-1]

        # Prefer aria2c for multi-connection download
        from etransfer.plugins import aria2

        if aria2.is_available():
            logger.info("Using aria2c for HF download: %s", dl_url)
            return await aria2.download(dl_url, dest, filename=filename, on_progress=on_progress)

        async with httpx.AsyncClient(follow_redirects=True, timeout=httpx.Timeout(30, read=600)) as client:
            async with client.stream("GET", dl_url) as resp:
                resp.raise_for_status()

                cd = resp.headers.get("content-disposition", "")
                if "filename=" in cd:
                    for p in cd.split(";"):
                        p = p.strip()
                        if p.startswith("filename="):
                            filename = p[len("filename="):].strip().strip('"')

                total_str = resp.headers.get("content-length")
                total = int(total_str) if total_str else None
                downloaded = 0

                file_path = dest / filename
                with open(file_path, "wb") as f:
                    async for chunk in resp.aiter_bytes(chunk_size=_CHUNK):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if on_progress:
                            on_progress(downloaded, total)

        logger.info("Downloaded HF %s/%s -> %s (%d bytes)", parts["repo"], filename, file_path, downloaded)
        return file_path
