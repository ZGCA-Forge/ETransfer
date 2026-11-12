"""ModelScope source plugin — download models/datasets from modelscope.cn."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable, Optional

import httpx

from etransfer.common import modelscope_hub as ms
from etransfer.plugins.base_source import BaseSource, RemoteFileInfo

logger = logging.getLogger("etransfer.plugins.sources.modelscope")

_CHUNK = 4 * 1024 * 1024


def _http_client(token: Optional[str] = None, timeout: Optional[float] = 30.0) -> httpx.AsyncClient:
    """Build the HTTP client used for file access.

    Kept as a module-level factory so tests can substitute a mock transport
    without monkeypatching ``httpx.AsyncClient`` — that attribute lives on the
    shared ``httpx`` module and patching it would leak into unrelated sinks.
    """
    headers = {"User-Agent": "etransfer"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return httpx.AsyncClient(timeout=timeout or 30.0, follow_redirects=True, headers=headers)


def _size_from_headers(headers: "httpx.Headers") -> int:
    """Extract the total object size from response headers, or 0 if unknown.

    ``Content-Range`` (``bytes 0-0/1746942600``) is authoritative when present
    because a ranged GET reports the *full* size there; ``Content-Length`` on
    such a response is only the length of the returned range.
    """
    content_range = headers.get("content-range", "")
    if "/" in content_range:
        total = content_range.rsplit("/", 1)[-1].strip()
        if total.isdigit():
            return int(total)
    try:
        return int(headers.get("content-length") or 0)
    except ValueError:
        return 0


class ModelScopeSource(BaseSource):
    """Download models and datasets from ModelScope.

    Handles both single-file URLs and repository pages. A repository page
    URL (``https://modelscope.cn/models/{owner}/{name}``) makes
    ``is_repo_url`` return True, which triggers the server's batch expansion
    (``routes/tasks.py::_create_batch_from_repo``) — every file in the repo
    becomes its own download task, all pushed to the configured sink.

    Because ModelScope's ``/resolve/`` URLs honour HTTP Range requests, the
    existing chunked downloader, TUS pipeline and aria2 acceleration all work
    unchanged.

    Examples::

        https://modelscope.cn/models/Qwen/Qwen3.5-0.8B
        https://modelscope.cn/models/Qwen/Qwen3.5-0.8B/resolve/master/config.json
        https://modelscope.cn/datasets/AI-ModelScope/gsm8k
        https://www.modelscope.cn/models/Qwen/Qwen3.5-0.8B/summary
    """

    name = "modelscope"
    display_name = "ModelScope"
    supported_hosts = list(ms.MS_HOSTS)
    priority = 10

    # ── Matching ─────────────────────────────────────────────

    async def can_handle(self, url: str) -> bool:
        return ms.parse_url(url) is not None

    def can_handle_sync(self, url: str) -> bool:
        """Cheap synchronous variant (used by callers that only need matching)."""
        return ms.parse_url(url) is not None

    def auth_headers(self, token: Optional[str] = None) -> dict[str, str]:
        """Bearer auth for gated/private repos (``--token ms_***``).

        ModelScope ignores the header on public resources, so it is safe to
        send unconditionally when a token is configured.
        """
        if token:
            return {"Authorization": f"Bearer {token}"}
        return {}

    # ── Repo helpers ─────────────────────────────────────────

    def is_repo_url(self, url: str) -> bool:
        """True when *url* is a repository page (not a single file)."""
        parsed = ms.parse_url(url)
        if parsed is None:
            return False
        _ref, file_path = parsed
        return file_path == ""

    async def list_repo_files(self, url: str, *, token: Optional[str] = None) -> tuple[list[dict], dict]:
        """List every file in the repo referenced by *url*.

        Returns:
            ``(files, meta)`` where ``files`` is ``[{"path", "size", ...}]``
            and ``meta`` carries everything ``build_file_url`` needs.
        """
        parsed = ms.parse_url(url)
        if parsed is None:
            raise ValueError(f"Not a ModelScope model/dataset URL: {url}")
        ref, _ = parsed

        files = await ms.list_repo_files(ref, token=token)
        if not files:
            raise ValueError(
                f"仓库 {ref.repo_id} 在 revision '{ref.revision}' 下没有可下载文件"
                f"（请确认仓库存在且分支正确，默认分支为 master）"
            )

        meta: dict[str, Any] = {
            "repo": ref.repo_id,
            "owner": ref.owner,
            "name": ref.name,
            "kind": ref.kind,
            "revision": ref.revision,
            "folder_name": ref.name,
            "web_prefix": ref.web_prefix,
        }
        return files, meta

    def build_file_url(self, meta: dict, file_path: str, **kwargs: Any) -> str:
        """Build a direct-download URL for one file of a repo."""
        ref = ms.RepoRef(
            kind=meta.get("kind", "model"),
            owner=meta["owner"],
            name=meta["name"],
            revision=meta.get("revision") or ms.DEFAULT_REVISION,
        )
        return ms.build_resolve_url(ref, file_path)

    def derive_object_key(self, meta: dict, file_path: str, prefix: str = "") -> str:
        """Deterministic sink key for a repo file (used by dedup / skipping)."""
        ref = ms.RepoRef(
            kind=meta.get("kind", "model"),
            owner=meta["owner"],
            name=meta["name"],
            revision=meta.get("revision") or ms.DEFAULT_REVISION,
        )
        return ms.derive_repo_object_key(ref, file_path, prefix)

    async def get_repo_info(self, url: str, *, token: Optional[str] = None) -> dict:
        """Fetch repo metadata (downloads, likes, file_size, gated …)."""
        parsed = ms.parse_url(url)
        if parsed is None:
            raise ValueError(f"Not a ModelScope model/dataset URL: {url}")
        ref, _ = parsed
        return await ms.get_repo_info(ref, token=token)

    # ── File access ──────────────────────────────────────────

    async def get_file_info(self, url: str, *, token: Optional[str] = None) -> RemoteFileInfo:
        parsed = ms.parse_url(url)
        if parsed is None:
            raise ValueError(f"Not a ModelScope model/dataset URL: {url}")
        ref, file_path = parsed
        if not file_path:
            raise ValueError(
                f"这是 ModelScope 仓库页面，不是文件下载链接。\n"
                f"请使用具体文件的 URL，例如:\n"
                f"  {ref.web_url()}/resolve/{ref.revision}/config.json"
            )

        dl_url = ms.build_resolve_url(ref, file_path)
        headers = self.auth_headers(token)
        async with _http_client(token) as client:
            resp = await client.head(dl_url)
            size = _size_from_headers(resp.headers)
            # ModelScope's HEAD returns 200 with no Content-Length at all
            # (verified live against Qwen/Qwen3.5-0.8B), so "no size" must
            # trigger the ranged-GET fallback rather than being taken as zero.
            if size == 0 or resp.status_code in (403, 405, 501):
                # A one-byte ranged GET answers Content-Range: bytes 0-0/<total>.
                ranged = await client.get(dl_url, headers={**headers, "Range": "bytes=0-0"})
                if ranged.status_code < 400:
                    resp = ranged
                    ranged_size = _size_from_headers(ranged.headers)
                    if ranged_size:
                        size = ranged_size
            resp.raise_for_status()

        return RemoteFileInfo(
            filename=file_path,
            size=size,
            mime_type=resp.headers.get("content-type") or "application/octet-stream",
            etag=resp.headers.get("etag", ""),
            extra={"repo": ref.repo_id, "revision": ref.revision, "kind": ref.kind},
        )

    async def download(
        self,
        url: str,
        dest: Path,
        on_progress: Optional[Callable[[int, Optional[int]], Any]] = None,
        *,
        token: Optional[str] = None,
    ) -> Path:
        parsed = ms.parse_url(url)
        if parsed is None:
            raise ValueError(f"Not a ModelScope model/dataset URL: {url}")
        ref, file_path = parsed
        if not file_path:
            raise ValueError(
                f"这是 ModelScope 仓库页面，不能作为单个文件下载；" f"服务端会自动展开为逐文件任务。仓库: {ref.repo_id}"
            )

        dest.mkdir(parents=True, exist_ok=True)
        dl_url = ms.build_resolve_url(ref, file_path)
        filename = file_path.rsplit("/", 1)[-1]
        headers = self.auth_headers(token)

        # Prefer aria2c for multi-connection download when available.
        from etransfer.plugins import aria2

        if aria2.is_available():
            logger.info("Using aria2c for ModelScope download: %s", dl_url)
            return await aria2.download(
                dl_url, dest, filename=filename, headers=headers or None, on_progress=on_progress
            )

        async with _http_client(token, timeout=600) as client:
            async with client.stream("GET", dl_url) as resp:
                resp.raise_for_status()
                total_str = resp.headers.get("content-length")
                total = int(total_str) if total_str else None
                downloaded = 0
                file_path_local = dest / filename
                with open(file_path_local, "wb") as f:
                    async for chunk in resp.aiter_bytes(chunk_size=_CHUNK):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if on_progress:
                            on_progress(downloaded, total)

        logger.info("Downloaded ModelScope %s/%s -> %s (%d bytes)", ref.repo_id, filename, file_path_local, downloaded)
        return file_path_local
