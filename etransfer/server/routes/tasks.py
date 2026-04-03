"""REST API routes for transfer tasks."""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request

from etransfer.server.tasks.models import CreateTaskRequest, TaskResponse

logger = logging.getLogger("etransfer.server.tasks")


def create_tasks_router() -> APIRouter:
    router = APIRouter(prefix="/api/tasks", tags=["Tasks"])

    def _get_manager(request: Request):  # type: ignore[no-untyped-def]
        mgr = getattr(request.app.state, "task_manager", None)
        if mgr is None:
            raise HTTPException(503, "Task manager not initialized")
        return mgr

    @router.post("", response_model=TaskResponse, status_code=201)
    async def create_task(body: CreateTaskRequest, request: Request) -> TaskResponse:
        mgr = _get_manager(request)
        user = getattr(request.state, "user", None)
        owner_id = getattr(user, "id", None) if user else None

        try:
            task = await mgr.create_task(
                source_url=body.source_url,
                sink_plugin=body.sink_plugin,
                sink_config=body.sink_config,
                retention=body.retention,
                retention_ttl=body.retention_ttl,
                owner_id=owner_id,
                user=user,
            )
        except ValueError as e:
            raise HTTPException(400, str(e))
        except Exception as e:
            logger.exception("Failed to create task")
            raise HTTPException(500, f"Failed to create task: {e}")

        return TaskResponse(**task.model_dump())

    @router.get("", response_model=list[TaskResponse])
    async def list_tasks(request: Request) -> list[TaskResponse]:
        mgr = _get_manager(request)
        tasks = await mgr.list_tasks()
        return [TaskResponse(**t.model_dump()) for t in tasks]

    @router.get("/{task_id}", response_model=TaskResponse)
    async def get_task(task_id: str, request: Request) -> TaskResponse:
        mgr = _get_manager(request)
        task = await mgr.get_task(task_id)
        if task is None:
            raise HTTPException(404, "Task not found")
        return TaskResponse(**task.model_dump())

    @router.delete("/{task_id}")
    async def cancel_task(task_id: str, request: Request) -> dict:
        mgr = _get_manager(request)
        ok = await mgr.cancel_task(task_id)
        if not ok:
            raise HTTPException(404, "Task not found or already finished")
        return {"status": "cancelled", "task_id": task_id}

    return router
