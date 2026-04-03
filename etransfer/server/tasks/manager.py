"""TaskManager — orchestrates Source -> local -> Sink pipelines."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from etransfer.plugins.base_sink import BaseSink, PartResult, SinkContext
from etransfer.plugins.registry import PluginRegistry, plugin_registry
from etransfer.server.tasks.models import TaskStatus, TransferTask

logger = logging.getLogger("etransfer.server.tasks")

_STATE_PREFIX = "et:task:"


class TaskManager:
    """Manages the lifecycle of TransferTasks.

    Each task runs as a background ``asyncio.Task``.  State is persisted
    to the same ``StateManager`` used by ``TusStorage``.
    """

    def __init__(
        self,
        state_manager: Any,
        storage: Any,
        registry: Optional[PluginRegistry] = None,
        download_dir: Optional[Path] = None,
        sink_presets: Optional[dict] = None,
    ) -> None:
        self._state = state_manager
        self._storage = storage
        self._registry = registry or plugin_registry
        self._download_dir = download_dir or Path("./storage/downloads")
        self._download_dir.mkdir(parents=True, exist_ok=True)
        self._sink_presets: dict = sink_presets or {}
        self._running: dict[str, asyncio.Task] = {}  # type: ignore[type-arg]
        self._tasks: dict[str, TransferTask] = {}

    # ── Persistence helpers ───────────────────────────────────

    def _key(self, task_id: str) -> str:
        return f"{_STATE_PREFIX}{task_id}"

    async def _save(self, task: TransferTask) -> None:
        task.updated_at = datetime.utcnow()
        self._tasks[task.task_id] = task
        data = task.model_dump_json()
        await self._state.set(self._key(task.task_id), data)

    async def _load(self, task_id: str) -> Optional[TransferTask]:
        if task_id in self._tasks:
            return self._tasks[task_id]
        raw = await self._state.get(self._key(task_id))
        if raw is None:
            return None
        task = TransferTask.model_validate_json(raw)
        self._tasks[task_id] = task
        return task

    # ── Public API ────────────────────────────────────────────

    async def create_task(
        self,
        source_url: str,
        sink_plugin: Optional[str] = None,
        sink_config: Optional[dict] = None,
        retention: str = "permanent",
        retention_ttl: Optional[int] = None,
        owner_id: Optional[int] = None,
        user: Any = None,
    ) -> TransferTask:
        source = await self._registry.resolve_source(source_url)

        # Resolve sink config dynamically
        resolved_sink_config: dict = {}
        if sink_plugin:
            ctx = SinkContext(
                user=user,
                client_metadata={"sink_config": sink_config} if sink_config else {},
                retention=retention,
                filename="",
                file_size=None,
            )
            presets_for_sink = self._sink_presets.get(sink_plugin, {})
            resolved_sink_config = self._registry.resolve_sink_config(sink_plugin, ctx, presets_for_sink)

        task = TransferTask(  # type: ignore[call-arg]
            task_id=uuid.uuid4().hex,
            source_url=source_url,
            source_plugin=source.name,
            sink_plugin=sink_plugin,
            sink_config=resolved_sink_config,
            retention=retention,
            retention_ttl=retention_ttl,
            owner_id=owner_id,
        )
        await self._save(task)

        bg = asyncio.create_task(self._run_pipeline(task))
        self._running[task.task_id] = bg
        return task

    async def get_task(self, task_id: str) -> Optional[TransferTask]:
        return await self._load(task_id)

    async def list_tasks(self) -> list[TransferTask]:
        pattern = f"{_STATE_PREFIX}*"
        keys = await self._state.keys(pattern)
        tasks: list[TransferTask] = []
        for k in keys:
            raw = await self._state.get(k)
            if raw:
                tasks.append(TransferTask.model_validate_json(raw))
        tasks.sort(key=lambda t: t.created_at)
        return tasks

    async def cancel_task(self, task_id: str) -> bool:
        task = await self._load(task_id)
        if task is None:
            return False
        if task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED):
            return False

        bg = self._running.pop(task_id, None)
        if bg and not bg.done():
            bg.cancel()

        task.status = TaskStatus.CANCELLED
        task.error = "Cancelled by user"
        await self._save(task)
        return True

    # ── Pipeline ──────────────────────────────────────────────

    async def _run_pipeline(self, task: TransferTask) -> None:
        try:
            await self._phase_download(task)
            if task.status == TaskStatus.CANCELLED:
                return
            if task.sink_plugin:
                await self._phase_push(task)
            await self._phase_register(task)
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.utcnow()
            task.progress = 1.0
            await self._save(task)
            logger.info("Task %s completed: %s", task.task_id[:8], task.filename)
        except asyncio.CancelledError:
            task.status = TaskStatus.CANCELLED
            task.error = "Cancelled"
            await self._save(task)
        except Exception as exc:
            task.status = TaskStatus.FAILED
            task.error = str(exc)
            await self._save(task)
            logger.exception("Task %s failed", task.task_id[:8])
        finally:
            self._running.pop(task.task_id, None)

    async def _phase_download(self, task: TransferTask) -> None:
        task.status = TaskStatus.DOWNLOADING
        await self._save(task)

        source = self._registry.get_source(task.source_plugin)

        info = await source.get_file_info(task.source_url)
        task.filename = info.filename
        task.file_size = info.size
        await self._save(task)

        dest_dir = self._download_dir / task.task_id
        dest_dir.mkdir(parents=True, exist_ok=True)

        def _progress(downloaded: int, total: Optional[int]) -> None:
            task.downloaded_bytes = downloaded
            if total and total > 0:
                ratio = downloaded / total
                task.progress = ratio * (0.5 if task.sink_plugin else 1.0)

        file_path = await source.download(task.source_url, dest_dir, on_progress=_progress)
        task.filename = file_path.name
        task.file_size = file_path.stat().st_size
        task.downloaded_bytes = task.file_size
        await self._save(task)

    async def _phase_push(self, task: TransferTask) -> None:
        task.status = TaskStatus.PUSHING
        await self._save(task)

        sink = self._registry.create_sink(task.sink_plugin, config=task.sink_config)  # type: ignore[arg-type]
        file_path = self._download_dir / task.task_id / (task.filename or "download")
        chunk_size = 32 * 1024 * 1024  # 32 MB
        file_size = file_path.stat().st_size
        total_parts = math.ceil(file_size / chunk_size)

        session_id = await sink.initialize_upload(task.filename or task.task_id, {"task_id": task.task_id})
        task.sink_session_id = session_id
        await self._save(task)

        parts: list[PartResult] = []
        with open(file_path, "rb") as f:
            for part_num in range(1, total_parts + 1):
                data = f.read(chunk_size)
                if not data:
                    break
                result = await sink.upload_part(session_id, part_num, data)
                parts.append(result)
                task.pushed_parts = part_num
                task.progress = 0.5 + 0.5 * (part_num / total_parts)
                await self._save(task)

        result_url = await sink.complete_upload(session_id, parts)
        task.sink_result_url = result_url
        await self._save(task)

    async def _phase_register(self, task: TransferTask) -> None:
        """Register downloaded file in TusStorage with the chosen retention."""
        file_path = self._download_dir / task.task_id / (task.filename or "download")
        if not file_path.exists():
            return

        from etransfer.server.tus.models import TusUpload

        file_id = uuid.uuid4().hex
        file_size = file_path.stat().st_size

        use_chunked = task.retention == "download_once"
        chunk_size = 32 * 1024 * 1024

        if use_chunked:
            chunk_dir = self._storage.get_chunk_dir(file_id)
            chunk_dir.mkdir(parents=True, exist_ok=True)
            idx = 0
            with open(file_path, "rb") as f:
                while True:
                    data = f.read(chunk_size)
                    if not data:
                        break
                    chunk_path = chunk_dir / f"chunk_{idx:06d}"
                    chunk_path.write_bytes(data)
                    await self._storage.mark_chunk_available(file_id, idx)
                    idx += 1
            storage_path = str(chunk_dir)
            total_chunks = idx
        else:
            final_path = self._storage.get_file_path(file_id)
            final_path.parent.mkdir(parents=True, exist_ok=True)
            import shutil

            shutil.move(str(file_path), str(final_path))
            storage_path = str(final_path)
            total_chunks = 0

        upload = TusUpload(  # type: ignore[call-arg]
            file_id=file_id,
            filename=task.filename or "download",
            size=file_size,
            offset=file_size,
            is_final=True,
            storage_path=storage_path,
            retention=task.retention,
            retention_ttl=task.retention_ttl,
            owner_id=task.owner_id,
            chunked_storage=use_chunked,
            chunk_size=chunk_size,
            available_size=file_size,
            total_chunks=total_chunks,
            received_ranges=[[0, file_size]],
        )
        await self._storage.create_upload(upload)
        await self._storage.finalize_upload(file_id)

        task.file_id = file_id
        await self._save(task)
