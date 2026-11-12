"""REST routes for ModelScope discovery.

Exposes a server-side proxy over ModelScope's public hub APIs so the browser
never talks to modelscope.cn directly (CORS, internal-network egress and
credential handling all stay server-side).

Endpoints are registered under ``/api/sources/modelscope``. They degrade
gracefully: when the upstream hub is unreachable from the server (common for
air-gapped deployments) the route returns HTTP 502 with a readable message
instead of a stack trace.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from etransfer.common import modelscope_hub as ms
from etransfer.plugins.sources.modelscope import ModelScopeSource

logger = logging.getLogger("etransfer.server.routes.modelscope")

_SOURCE = ModelScopeSource()

# Small in-process cache so paging through the UI does not hammer the hub.
_CACHE_TTL_SECONDS = 60.0
_CACHE_MAX_ENTRIES = 256
_cache: dict[str, tuple[float, Any]] = {}


def _cache_get(key: str) -> Optional[Any]:
    entry = _cache.get(key)
    if entry is None:
        return None
    ts, value = entry
    if time.monotonic() - ts > _CACHE_TTL_SECONDS:
        _cache.pop(key, None)
        return None
    return value


def _cache_put(key: str, value: Any) -> Any:
    if len(_cache) >= _CACHE_MAX_ENTRIES:
        # Drop the oldest entry; the cache is a pure optimisation.
        oldest = min(_cache.items(), key=lambda kv: kv[1][0])[0]
        _cache.pop(oldest, None)
    _cache[key] = (time.monotonic(), value)
    return value


def clear_cache() -> None:
    """Reset the discovery cache (used by tests)."""
    _cache.clear()


def _wrap_error(exc: Exception) -> HTTPException:
    if isinstance(exc, ValueError):
        return HTTPException(400, str(exc))
    if isinstance(exc, ms.ModelScopeError):
        return HTTPException(502, f"ModelScope 上游返回错误: {exc}")
    return HTTPException(502, f"ModelScope 请求失败: {exc}")


def _paging(page_number: int, page_size: int) -> tuple[int, int]:
    return max(1, int(page_number)), max(1, min(int(page_size), 100))


# ── Dedup: which repos are already in the bucket ────────────


async def _downloaded_repos(request: Request) -> dict[tuple[str, str], dict[str, Any]]:
    """Index completed tasks by ``(kind, repo_id)``.

    Task state (``et:task:*``) never expires and already records
    ``source_url`` / ``filename`` / ``sink_result_url``, so this needs no new
    storage. It answers "was this pushed at least once?", **not** "is it
    still there" — bucket verification lives behind a separate call.
    """
    mgr = getattr(request.app.state, "task_manager", None)
    if mgr is None:
        return {}

    try:
        tasks = await mgr.list_tasks()
    except Exception:  # pragma: no cover - state backend failure
        logger.debug("Could not list tasks for dedup index", exc_info=True)
        return {}

    index: dict[tuple[str, str], dict[str, Any]] = {}
    for task in tasks:
        if task.source_url.rstrip("/") == "":
            continue
        parsed = ms.parse_url(task.source_url)
        if parsed is None:
            continue
        ref, _file_path = parsed
        # Only fully pushed tasks count as "downloaded".
        if task.sink_plugin and task.status.value != "completed":
            continue
        key = (ref.kind, ref.repo_id)
        entry = index.setdefault(
            key,
            {
                "kind": ref.kind,
                "repo_id": ref.repo_id,
                "sink_plugin": task.sink_plugin,
                "bucket_ref": "",
                "files": 0,
                "bytes": 0,
                "pushed_at": task.completed_at.isoformat() if task.completed_at else "",
                "task_ids": [],
            },
        )
        entry["files"] += 1
        entry["bytes"] += int(task.file_size or 0)
        entry["task_ids"].append(task.task_id)
        if task.sink_result_url and not entry["bucket_ref"]:
            entry["bucket_ref"] = task.sink_result_url
        completed = task.completed_at.isoformat() if task.completed_at else ""
        if completed and completed > entry["pushed_at"]:
            entry["pushed_at"] = completed
    return index


def _annotate(
    items: list[dict[str, Any]], kind: str, index: dict[tuple[str, str], dict[str, Any]]
) -> list[dict[str, Any]]:
    """Attach ``downloaded`` / ``artifact`` to each discovery item."""
    for item in items:
        repo_id = str(item.get("id") or "")
        entry = index.get((kind, repo_id))
        item["downloaded"] = entry is not None
        item["artifact"] = entry
    return items


def create_modelscope_router() -> APIRouter:
    router = APIRouter(prefix="/api/sources/modelscope", tags=["ModelScope"])

    @router.get("/models")
    async def list_models(
        request: Request,
        search: str = Query("", description="Keyword search"),
        sort: str = Query("default", description="default | downloads | likes | last_modified"),
        page_number: int = Query(1, ge=1),
        page_size: int = Query(20, ge=1, le=100),
        task: str = Query("", description="Filter by task type, e.g. text-generation"),
        include_downloaded: bool = Query(True, description="Annotate items already pushed"),
    ) -> dict[str, Any]:
        """List/search ModelScope models."""
        page, size = _paging(page_number, page_size)
        cache_key = f"models:{search}:{sort}:{task}:{page}:{size}"
        try:
            result = _cache_get(cache_key)
            if result is None:
                result = _cache_put(
                    cache_key,
                    await ms.list_models(search=search, sort=sort, page_number=page, page_size=size, task=task),
                )
        except Exception as exc:
            raise _wrap_error(exc)

        items = list(result["items"])
        if include_downloaded:
            items = _annotate(items, "model", await _downloaded_repos(request))
        return {**result, "items": items}

    @router.get("/datasets")
    async def list_datasets(
        request: Request,
        search: str = Query(""),
        sort: str = Query("default"),
        page_number: int = Query(1, ge=1),
        page_size: int = Query(20, ge=1, le=100),
        task: str = Query(""),
        include_downloaded: bool = Query(True),
    ) -> dict[str, Any]:
        """List/search ModelScope datasets."""
        page, size = _paging(page_number, page_size)
        cache_key = f"datasets:{search}:{sort}:{task}:{page}:{size}"
        try:
            result = _cache_get(cache_key)
            if result is None:
                result = _cache_put(
                    cache_key,
                    await ms.list_datasets(search=search, sort=sort, page_number=page, page_size=size, task=task),
                )
        except Exception as exc:
            raise _wrap_error(exc)

        items = list(result["items"])
        if include_downloaded:
            items = _annotate(items, "dataset", await _downloaded_repos(request))
        return {**result, "items": items}

    @router.get("/collections")
    async def list_collections(
        search: str = Query(""),
        page_number: int = Query(1, ge=1),
        page_size: int = Query(20, ge=1, le=100),
    ) -> dict[str, Any]:
        """List public collections (合集)."""
        page, size = _paging(page_number, page_size)
        cache_key = f"collections:{search}:{page}:{size}"
        try:
            result = _cache_get(cache_key)
            if result is None:
                result = _cache_put(
                    cache_key, await ms.list_collections(search=search, page_number=page, page_size=size)
                )
        except Exception as exc:
            raise _wrap_error(exc)
        return result

    @router.get("/collections/{owner}/{slug}")
    async def get_collection(owner: str, slug: str) -> dict[str, Any]:
        """Fetch one collection with its member list resolved."""
        cache_key = f"collection:{owner}/{slug}"
        try:
            data = _cache_get(cache_key)
            if data is None:
                data = _cache_put(cache_key, await ms.get_collection(owner, slug))
        except Exception as exc:
            raise _wrap_error(exc)
        return {
            "collection": data,
            "members": ms.collection_members(data),
        }

    @router.get("/{kind}/{owner}/{name}/files")
    async def list_repo_files_endpoint(
        kind: str,
        owner: str,
        name: str,
        request: Request,
        revision: str = Query(ms.DEFAULT_REVISION),
        path: str = Query("", description="Sub-directory to list (empty = repo root)"),
    ) -> dict[str, Any]:
        """Dry-run preview: files and total size before committing to a download.

        This is what the UI calls to render "将下载 14 个文件 / 1.77 GB".
        """
        if kind not in ("model", "dataset"):
            raise HTTPException(400, "kind must be 'model' or 'dataset'")

        ref = ms.RepoRef(kind=kind, owner=owner, name=name, revision=revision or ms.DEFAULT_REVISION)
        cache_key = f"files:{kind}:{owner}/{name}:{revision}:{path}"
        try:
            cached = _cache_get(cache_key)
            if cached is None:
                files = await ms.list_repo_files(ref, root=path)
                cached = {
                    "files": files,
                    "total_files": len(files),
                    "total_size": sum(f["size"] for f in files),
                }
                _cache_put(cache_key, cached)
        except Exception as exc:
            raise _wrap_error(exc)

        result = dict(cached)
        # Preview object keys so the UI can show "已存在" per file later.
        result["object_keys"] = (
            {f["path"]: ms.derive_repo_object_key(ref, f["path"]) for f in result["files"]}
            if len(result["files"]) <= 200
            else {}
        )
        result.update(
            {
                "kind": kind,
                "repo_id": ref.repo_id,
                "revision": ref.revision,
                "url": ref.web_url(),
            }
        )
        return result

    @router.get("/{kind}/{owner}/{name}")
    async def get_repo_info(kind: str, owner: str, name: str) -> dict[str, Any]:
        """Fetch repo metadata (downloads, likes, gated, file_size …)."""
        if kind not in ("model", "dataset"):
            raise HTTPException(400, "kind must be 'model' or 'dataset'")
        ref = ms.RepoRef(kind=kind, owner=owner, name=name)
        cache_key = f"info:{kind}:{owner}/{name}"
        try:
            data = _cache_get(cache_key)
            if data is None:
                data = _cache_put(cache_key, await ms.get_repo_info(ref))
        except Exception as exc:
            raise _wrap_error(exc)
        return {"repo_id": ref.repo_id, "kind": kind, "url": ref.web_url(), "info": data}

    @router.get("/downloaded")
    async def list_downloaded(request: Request) -> dict[str, Any]:
        """Everything already pushed to a bucket, grouped by repo."""
        index = await _downloaded_repos(request)
        items = sorted(index.values(), key=lambda e: e["pushed_at"], reverse=True)
        return {
            "total": len(items),
            "items": items,
            "total_bytes": sum(int(i["bytes"] or 0) for i in items),
        }

    return router
