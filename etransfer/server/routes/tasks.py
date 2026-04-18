"""REST API routes for transfer tasks."""

from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from etransfer.server.auth.privilege import enforce_retention_policy
from etransfer.server.tasks.models import CreateTaskRequest, TaskResponse, TaskStatus, TransferTask

logger = logging.getLogger("etransfer.server.tasks")


class QueueItem(BaseModel):
    """Public-safe view of a task for the global queue."""

    task_id: str
    owner_name: str
    source_plugin: str
    sink_plugin: str
    filename: str
    file_size: int
    status: TaskStatus
    progress: float
    downloaded_bytes: int
    pushed_parts: int
    created_at: str
    updated_at: str


def create_tasks_router() -> APIRouter:
    router = APIRouter(prefix="/api/tasks", tags=["Tasks"])

    def _get_manager(request: Request):  # type: ignore[no-untyped-def]
        mgr = getattr(request.app.state, "task_manager", None)
        if mgr is None:
            raise HTTPException(503, "Task manager not initialized")
        return mgr

    @router.post("", status_code=201)
    async def create_task(body: CreateTaskRequest, request: Request) -> Any:
        """Create a download task.

        If the URL is a HuggingFace repo/dataset page, automatically
        expands into one task per file and returns a batch response.
        """
        mgr = _get_manager(request)
        user = getattr(request.state, "user", None)
        owner_id = getattr(user, "id", None) if user else None

        # Enforce server policy on requested retention.  Raises 403 for
        # non-privileged callers when permanent retention is disabled.
        enforce_retention_policy(request, body.retention)

        # Check if this is a repo/folder URL → batch expand
        try:
            from etransfer.plugins.registry import plugin_registry

            plugin_registry.discover()
            source = await plugin_registry.resolve_source(body.source_url)
            if hasattr(source, "is_repo_url") and source.is_repo_url(body.source_url):
                # Pass google_api_key to GDrive source if configured
                if source.name == "gdrive":
                    _settings = getattr(request.app.state, "settings", None)
                    api_key = getattr(_settings, "google_api_key", None)
                    if api_key:
                        setattr(source, "google_api_key", api_key)
                return await _create_batch_from_repo(
                    mgr,
                    source,
                    body,
                    owner_id,
                    user,
                )
        except HTTPException:
            raise
        except ValueError:
            pass
        except Exception:
            logger.debug("Repo detection failed, falling back to single task", exc_info=True)

        try:
            task = await mgr.create_task(
                source_url=body.source_url,
                sink_plugin=body.sink_plugin,
                sink_config=body.sink_config,
                sink_preset=body.sink_preset,
                retention=body.retention,
                retention_ttl=body.retention_ttl,
                owner_id=owner_id,
                user=user,
                filename=body.filename,
            )
        except KeyError as e:
            # e.g. unknown preset name from resolve_config
            raise HTTPException(400, str(e))
        except ValueError as e:
            raise HTTPException(400, str(e))
        except Exception as e:
            logger.exception("Failed to create task")
            raise HTTPException(500, f"Failed to create task: {e}")

        return TaskResponse(**task.model_dump())

    async def _create_batch_from_repo(
        mgr: Any,
        source: Any,
        body: CreateTaskRequest,
        owner_id: Optional[int],
        user: Any,
    ) -> dict:
        """Expand a repo/folder URL into multiple download tasks."""
        try:
            files, meta = await source.list_repo_files(body.source_url)
        except ValueError as e:
            raise HTTPException(400, str(e))
        except Exception as e:
            raise HTTPException(400, f"Failed to list files: {e}")

        if not files:
            raise HTTPException(400, "No files found in repository/folder")

        tasks: list[TaskResponse] = []
        total_size = 0
        for f in files:
            file_url = source.build_file_url(
                meta,
                f["path"],
                gdrive_id=f.get("gdrive_id", ""),
            )
            total_size += f.get("size", 0)
            try:
                task = await mgr.create_task(
                    source_url=file_url,
                    sink_plugin=body.sink_plugin,
                    sink_config=body.sink_config,
                    sink_preset=body.sink_preset,
                    retention=body.retention,
                    retention_ttl=body.retention_ttl,
                    owner_id=owner_id,
                    user=user,
                    filename=f["path"],
                )
                tasks.append(TaskResponse(**task.model_dump()))
            except Exception as e:
                logger.warning("Failed to create task for %s: %s", f["path"], e)

        return {
            "batch": True,
            "folder_name": meta.get("folder_name", ""),
            "repo": meta.get("repo", meta.get("folder_id", "")),
            "total_files": len(files),
            "created_tasks": len(tasks),
            "total_size": total_size,
            "tasks": [t.model_dump() for t in tasks],
        }

    @router.get("", response_model=list[TaskResponse])
    async def list_tasks(request: Request) -> list[TaskResponse]:
        mgr = _get_manager(request)
        tasks = await mgr.list_tasks()

        # Filter by owner: session users see only their own tasks;
        # API token users and admins see all.
        user = getattr(request.state, "user", None)
        if user:
            is_admin = getattr(user, "is_admin", False) or getattr(user, "role", "") == "admin"
            if not is_admin:
                uid = getattr(user, "id", None)
                tasks = [t for t in tasks if t.owner_id == uid]

        return [TaskResponse(**t.model_dump()) for t in tasks]

    # ── Global queue (sanitized, all users) ───────────────────

    @router.get("/queue/global", response_model=list[QueueItem])
    async def global_queue(request: Request) -> list[QueueItem]:
        """All tasks across all users, with sensitive info stripped."""
        mgr = _get_manager(request)
        tasks = await mgr.list_tasks()

        user_db = getattr(request.app.state, "user_db", None)
        name_cache: dict[int, str] = {}

        def _mask_email(email: str) -> str:
            """Mask email: zoumeng@zgci.ac.cn → z***@zgci.ac.cn"""
            if "@" not in email:
                return email
            local, domain = email.rsplit("@", 1)
            if len(local) <= 1:
                masked = local + "***"
            else:
                masked = local[0] + "***"
            return f"{masked}@{domain}"

        async def _resolve_name(owner_id: Optional[int]) -> str:
            if owner_id is None:
                return "API"
            if owner_id in name_cache:
                return name_cache[owner_id]
            name = f"#{owner_id}"
            if user_db:
                try:
                    u = await user_db.get_user(owner_id)
                    if u:
                        if u.email:
                            name = _mask_email(u.email)
                        else:
                            name = u.username or u.display_name or f"#{owner_id}"
                except Exception:
                    pass
            name_cache[owner_id] = name
            return name

        items: list[QueueItem] = []
        for t in tasks:
            items.append(
                QueueItem(
                    task_id=t.task_id,
                    owner_name=await _resolve_name(t.owner_id),
                    source_plugin=t.source_plugin,
                    sink_plugin=t.sink_plugin,
                    filename=t.filename,
                    file_size=t.file_size,
                    status=t.status,
                    progress=t.progress,
                    downloaded_bytes=t.downloaded_bytes,
                    pushed_parts=t.pushed_parts,
                    created_at=t.created_at.isoformat(),
                    updated_at=t.updated_at.isoformat(),
                )
            )
        items.sort(key=lambda i: i.updated_at, reverse=True)
        return items

    def _check_task_access(task: TransferTask, request: Request) -> None:
        """Raise 404 if non-admin session user doesn't own the task."""
        user = getattr(request.state, "user", None)
        if not user:
            return
        is_admin = getattr(user, "is_admin", False) or getattr(user, "role", "") == "admin"
        if is_admin:
            return
        if task.owner_id != getattr(user, "id", None):
            raise HTTPException(404, "Task not found")

    @router.get("/{task_id}", response_model=TaskResponse)
    async def get_task(task_id: str, request: Request) -> TaskResponse:
        mgr = _get_manager(request)
        task = await mgr.get_task(task_id)
        if task is None:
            raise HTTPException(404, "Task not found")
        _check_task_access(task, request)
        return TaskResponse(**task.model_dump())

    @router.delete("/{task_id}")
    async def cancel_task(task_id: str, request: Request) -> dict:
        mgr = _get_manager(request)
        task = await mgr.get_task(task_id)
        if task is None:
            raise HTTPException(404, "Task not found")
        _check_task_access(task, request)
        ok = await mgr.cancel_task(task_id)
        if not ok:
            raise HTTPException(404, "Task not found or already finished")
        return {"status": "cancelled", "task_id": task_id}

    @router.post("/{task_id}/retry", response_model=TaskResponse, status_code=201)
    async def retry_task(task_id: str, request: Request) -> TaskResponse:
        """Retry a failed/cancelled task by creating a new one with the same parameters."""
        mgr = _get_manager(request)
        old = await mgr.get_task(task_id)
        if old is None:
            raise HTTPException(404, "Task not found")
        _check_task_access(old, request)
        if old.status not in (TaskStatus.FAILED, TaskStatus.CANCELLED):
            raise HTTPException(400, "Only failed or cancelled tasks can be retried")

        user = getattr(request.state, "user", None)
        owner_id = getattr(user, "id", None) if user else old.owner_id
        task = await mgr.create_task(
            source_url=old.source_url,
            sink_plugin=old.sink_plugin,
            sink_config=old.sink_config if old.sink_config else None,
            sink_preset=old.sink_preset,
            retention=old.retention,
            retention_ttl=old.retention_ttl,
            owner_id=owner_id,
            user=user,
            filename=old.filename,
        )

        # Mark ALL old tasks with same source_url that are failed/cancelled as superseded
        all_tasks = await mgr.list_tasks()
        for t in all_tasks:
            if (
                t.source_url == old.source_url
                and t.task_id != task.task_id
                and not t.superseded_by
                and t.status in (TaskStatus.FAILED, TaskStatus.CANCELLED)
            ):
                t.superseded_by = task.task_id
                await mgr._save(t)

        return TaskResponse(**task.model_dump())

    return router
