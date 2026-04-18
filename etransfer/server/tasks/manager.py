"""TaskManager — orchestrates Source -> local -> Sink pipelines."""

from __future__ import annotations

import asyncio
import io
import logging
import math
import shutil
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import httpx

from etransfer.common.fileutil import derive_sink_object_key
from etransfer.plugins.base_sink import PartResult, SinkContext
from etransfer.plugins.registry import PluginRegistry, plugin_registry
from etransfer.server.tasks.models import TaskStatus, TransferTask

logger = logging.getLogger("etransfer.server.tasks")

_STATE_PREFIX = "et:task:"
_CHUNK_SIZE = 128 * 1024 * 1024  # 128 MB — larger chunks reduce multipart overhead
_DEFAULT_MAX_CONCURRENT_TASKS = 50
_MAX_RETRIES = 3
_RETRY_BACKOFF = (5, 15, 30)  # seconds between retries


class _RangeNotSupportedError(Exception):
    """Internal signal: the source ignored our Range header, restart fresh."""
    pass


class _DynamicTaskGate:
    """Async concurrency gate whose limit can be changed at runtime.

    A plain ``asyncio.Semaphore`` cannot be safely resized while waiters are
    parked on it.  We instead hand out slots based on an integer ``limit``
    that is consulted on every acquire/release through a Condition.
    Increasing the limit immediately wakes parked waiters; decreasing it
    only blocks future acquires (in-flight tasks finish naturally and then
    the new lower cap takes effect).
    """

    def __init__(self, limit: int) -> None:
        self._limit = max(1, int(limit))
        self._active = 0
        self._cond = asyncio.Condition()

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def active(self) -> int:
        return self._active

    async def acquire(self) -> None:
        async with self._cond:
            await self._cond.wait_for(lambda: self._active < self._limit)
            self._active += 1

    async def release(self) -> None:
        async with self._cond:
            if self._active > 0:
                self._active -= 1
            self._cond.notify_all()

    async def __aenter__(self) -> "_DynamicTaskGate":
        await self.acquire()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.release()

    def set_limit(self, new_limit: int) -> None:
        new_limit = max(1, int(new_limit))
        if new_limit == self._limit:
            return
        old = self._limit
        self._limit = new_limit
        if new_limit > old:
            # Wake parked waiters so they can re-check the new cap.
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                return

            async def _notify() -> None:
                async with self._cond:
                    self._cond.notify_all()

            loop.create_task(_notify())


class TaskManager:
    """Manages the lifecycle of TransferTasks.

    Two pipeline modes:
      - **Sink mode** (streaming): Source → HTTP stream → buffer chunks →
        Sink multipart parts. Only one chunk in memory at a time.
        Local disk usage: zero.
      - **Local mode**: Source → aria2c/httpx download to disk →
        register in TusStorage.
    """

    def __init__(
        self,
        state_manager: Any,
        storage: Any,
        registry: Optional[PluginRegistry] = None,
        download_dir: Optional[Path] = None,
        sink_presets: Optional[dict] = None,
        max_concurrent_tasks: int = _DEFAULT_MAX_CONCURRENT_TASKS,
    ) -> None:
        self._state = state_manager
        self._storage = storage
        self._registry = registry or plugin_registry
        self._download_dir = download_dir or Path("./storage/downloads")
        self._download_dir.mkdir(parents=True, exist_ok=True)
        self._sink_presets: dict = sink_presets or {}
        self._running: dict[str, asyncio.Task] = {}  # type: ignore[type-arg]
        self._tasks: dict[str, TransferTask] = {}
        self._prev_dl: dict[str, int] = {}
        self._prev_push: dict[str, int] = {}
        self._prev_time: dict[str, datetime] = {}
        self._shutting_down = False
        self._task_gate = _DynamicTaskGate(max_concurrent_tasks)

    # ── Concurrency control ───────────────────────────────────

    @property
    def max_concurrent_tasks(self) -> int:
        return self._task_gate.limit

    def set_max_concurrent_tasks(self, limit: int) -> None:
        """Update the global task concurrency cap on the fly."""
        self._task_gate.set_limit(limit)
        logger.info("Task concurrency cap set to %d (active=%d)", self._task_gate.limit, self._task_gate.active)

    # ── Persistence helpers ───────────────────────────────────

    def _key(self, task_id: str) -> str:
        return f"{_STATE_PREFIX}{task_id}"

    async def _save(self, task: TransferTask) -> None:
        now = datetime.utcnow()
        prev_t = self._prev_time.get(task.task_id)
        if prev_t:
            elapsed = (now - prev_t).total_seconds()
            if elapsed > 0.3:
                # Download speed: delta-based (downloaded_bytes updates frequently)
                dl_prev = self._prev_dl.get(task.task_id, 0)
                dl_delta = task.downloaded_bytes - dl_prev
                task.download_speed = max(0.0, dl_delta / elapsed) if dl_delta > 0 else task.download_speed * 0.5

                # Push speed: set directly by pipeline via upload_part timing;
                # here we only decay when no push activity.
                push_bytes = task.pushed_parts * _CHUNK_SIZE
                push_prev = self._prev_push.get(task.task_id, 0)
                if push_bytes == push_prev:
                    task.push_speed *= 0.8

                task.speed = max(task.download_speed, task.push_speed)
                self._prev_dl[task.task_id] = task.downloaded_bytes
                self._prev_push[task.task_id] = push_bytes
                self._prev_time[task.task_id] = now
        else:
            self._prev_time[task.task_id] = now
            self._prev_dl[task.task_id] = 0
            self._prev_push[task.task_id] = 0

        task.updated_at = now
        self._tasks[task.task_id] = task
        data = task.model_dump_json()
        await self._state.set(self._key(task.task_id), data)

    @staticmethod
    def _sanitize_task_dict(d: dict) -> dict:
        """Convert legacy None values to protocol-standard defaults."""
        _str_defaults = {
            "sink_plugin": "", "error": "", "filename": "", "file_id": "",
            "sink_session_id": "", "sink_result_url": "", "source_plugin": "",
            "superseded_by": "",
        }
        _int_defaults = {"file_size": 0, "retention_ttl": 0}
        _float_defaults = {"speed": 0.0, "download_speed": 0.0, "push_speed": 0.0, "download_progress": 0.0, "push_progress": 0.0}
        for k, v in _float_defaults.items():
            if k in d and d[k] is None:
                d[k] = v
        for k, v in _str_defaults.items():
            if k in d and d[k] is None:
                d[k] = v
        for k, v in _int_defaults.items():
            if k in d and d[k] is None:
                d[k] = v
        return d

    async def _load(self, task_id: str) -> Optional[TransferTask]:
        if task_id in self._tasks:
            return self._tasks[task_id]
        raw = await self._state.get(self._key(task_id))
        if raw is None:
            return None
        import json as _json
        d = _json.loads(raw)
        self._sanitize_task_dict(d)
        task = TransferTask.model_validate(d)
        self._tasks[task_id] = task
        return task

    # ── Public API ────────────────────────────────────────────

    async def create_task(
        self,
        source_url: str,
        sink_plugin: str = "",
        sink_config: Optional[dict] = None,
        sink_preset: str = "",
        retention: str = "download_once",
        retention_ttl: int = 0,
        owner_id: Optional[int] = None,
        user: Any = None,
        filename: str = "",
    ) -> TransferTask:
        source = await self._registry.resolve_source(source_url)

        resolved_sink_config: dict = {}
        if sink_plugin:
            cm: dict = {}
            if sink_config:
                cm["sink_config"] = sink_config
            if sink_preset:
                cm["sink_preset"] = sink_preset
            ctx = SinkContext(
                user=user,
                client_metadata=cm,
                retention=retention,
                filename=filename,
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
            sink_preset=sink_preset,
            retention=retention,
            retention_ttl=retention_ttl,
            owner_id=owner_id,
            filename=filename,
        )
        task._user = user  # transient, not serialized
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
                import json as _json
                d = _json.loads(raw)
                self._sanitize_task_dict(d)
                tasks.append(TransferTask.model_validate(d))
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

        # Best-effort abort of any open sink multipart session so we do not
        # leak parts on the remote bucket (e.g. TOS keeps them for ~7 days).
        session_id = getattr(task, "sink_session_id", "") or ""
        if session_id and task.sink_plugin:
            try:
                sink = self._registry.create_sink(task.sink_plugin, config=task.sink_config)  # type: ignore[arg-type]
                await sink.abort_upload(session_id)
            except Exception:
                logger.debug("Failed to abort sink session %s on cancel", session_id[:12])

        task.status = TaskStatus.CANCELLED
        task.error = "Cancelled by user"
        task.sink_session_id = ""
        await self._save(task)
        return True

    # ── Graceful shutdown ─────────────────────────────────────

    async def shutdown(self) -> int:
        """Gracefully stop all running tasks.

        - Downloading tasks: cancel immediately (can restart from scratch)
        - Pushing tasks: wait for current chunk to finish, then stop
        - All interrupted tasks saved as PENDING for restart recovery
        """
        self._shutting_down = True
        running_ids = list(self._running.keys())
        count = len(running_ids)
        if not count:
            return 0

        # Categorize: which are pushing (need to wait) vs downloading (can cancel now)
        pushing = []
        downloading = []
        for tid in running_ids:
            task = self._tasks.get(tid) or await self._load(tid)
            if task and task.status.value == "pushing":
                pushing.append(tid)
            else:
                downloading.append(tid)

        logger.info(
            "Shutdown: %d downloading (cancel now), %d pushing (wait for current chunk)",
            len(downloading), len(pushing),
        )

        # Cancel downloading tasks immediately
        for tid in downloading:
            bg = self._running.get(tid)
            if bg and not bg.done():
                bg.cancel()

        # Wait for pushing tasks to notice _shutting_down flag and finish current chunk
        if pushing:
            for _ in range(30):  # max 30s wait
                still_running = [tid for tid in pushing if tid in self._running and not self._running[tid].done()]
                if not still_running:
                    break
                await asyncio.sleep(1)
            # Force-cancel any stragglers
            for tid in pushing:
                bg = self._running.get(tid)
                if bg and not bg.done():
                    bg.cancel()

        await asyncio.sleep(1)

        # Mark all interrupted tasks as PENDING for restart recovery
        for tid in running_ids:
            task = self._tasks.get(tid) or await self._load(tid)
            if task and task.status.value in ("downloading", "pushing", "pending", "cancelled"):
                task.status = TaskStatus.PENDING
                task.error = "interrupted by shutdown"
                task.speed = 0.0
                task.download_speed = 0.0
                task.push_speed = 0.0
                await self._save(task)

        logger.info("Shutdown complete: %d task(s) saved for restart recovery", count)
        return count

    # ── Startup recovery ──────────────────────────────────────

    async def resume_interrupted(self) -> int:
        """Resume tasks left over from a previous run.

        At process startup ``self._running`` is always empty, so any task whose
        persisted status is still in a "running" state (PENDING / DOWNLOADING /
        PUSHING) is by definition a zombie that must be revived.  This is
        broader than the historical "PENDING + interrupted-by-shutdown"
        signature, which missed tasks left behind by hard-kill / OOM / crash
        scenarios where ``shutdown()`` never had the chance to rewrite their
        status.

        Resume semantics (sink / stream-to-sink tasks):
            ``downloaded_bytes``, ``pushed_parts``, ``sink_session_id`` and
            ``uploaded_parts`` are **preserved** so the pipeline can continue
            the in-flight multipart upload where it left off instead of
            re-downloading from zero.  Only transient fields (error, speeds)
            are cleared.
        """
        all_tasks = await self.list_tasks()
        zombie_states = {TaskStatus.PENDING, TaskStatus.DOWNLOADING, TaskStatus.PUSHING}
        zombies = [t for t in all_tasks if t.status in zombie_states]
        if not zombies:
            return 0

        for task in zombies:
            task.status = TaskStatus.PENDING
            task.error = ""
            task.speed = 0.0
            task.download_speed = 0.0
            task.push_speed = 0.0
            await self._save(task)
            bg = asyncio.create_task(self._run_pipeline(task))
            self._running[task.task_id] = bg

        logger.info("Resumed %d interrupted task(s)", len(zombies))
        return len(zombies)

    async def push_file(
        self,
        file_path: Path,
        filename: str,
        file_size: int,
        sink_plugin: str,
        sink_config: Optional[dict] = None,
        sink_preset: str = "",
        owner_id: Optional[int] = None,
        user: Any = None,
    ) -> TransferTask:
        """Push an already-uploaded local file to a sink."""
        resolved_sink_config: dict = {}
        cm: dict = {}
        if sink_config:
            cm["sink_config"] = sink_config
        if sink_preset:
            cm["sink_preset"] = sink_preset
        ctx = SinkContext(
            user=user,
            client_metadata=cm,
            retention="permanent",
            filename=filename,
            file_size=file_size,
        )
        presets_for_sink = self._sink_presets.get(sink_plugin, {})
        resolved_sink_config = self._registry.resolve_sink_config(sink_plugin, ctx, presets_for_sink)

        task = TransferTask(  # type: ignore[call-arg]
            task_id=uuid.uuid4().hex,
            source_url=f"local://{file_path}",
            source_plugin="upload",
            sink_plugin=sink_plugin,
            sink_config=resolved_sink_config,
            sink_preset=sink_preset,
            filename=filename,
            file_size=file_size,
            owner_id=owner_id,
        )
        task._user = user  # transient, not serialized
        await self._save(task)

        bg = asyncio.create_task(self._pipeline_file_to_sink(task, file_path))
        self._running[task.task_id] = bg
        return task

    # ── Pipeline dispatcher ───────────────────────────────────

    async def _run_pipeline(self, task: TransferTask) -> None:
        try:
            async with self._task_gate:
                if task.sink_plugin:
                    await self._pipeline_stream_to_sink(task)
                else:
                    await self._pipeline_download_local(task)

            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.utcnow()
            task.progress = 1.0
            task.download_progress = 1.0
            if task.sink_plugin:
                task.push_progress = 1.0
            await self._save(task)
            logger.info("Task %s completed: %s", task.task_id[:8], task.filename)
        except asyncio.CancelledError:
            if self._shutting_down:
                task.status = TaskStatus.PENDING
                task.error = "interrupted by shutdown"
                task.speed = 0.0
                task.download_speed = 0.0
                task.push_speed = 0.0
                logger.info("Task %s interrupted by shutdown, saved for resume", task.task_id[:8])
            else:
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

    # ── Local file → Sink pipeline ──────────────────────────────

    async def _pipeline_file_to_sink(self, task: TransferTask, file_path: Path) -> None:
        """Read a local file and push to sink via multipart upload."""
        try:
            sink = self._registry.create_sink(task.sink_plugin, config=task.sink_config)  # type: ignore[arg-type]
            total_size = task.file_size or file_path.stat().st_size
            task.status = TaskStatus.PUSHING
            await self._save(task)

            _user = getattr(task, "_user", None)
            sink_key = derive_sink_object_key(task.filename or file_path.name, _user)
            session_id = await sink.initialize_upload(
                sink_key, {"task_id": task.task_id}
            )
            task.sink_session_id = session_id
            await self._save(task)

            parts: list[PartResult] = []
            part_num = 0
            pushed = 0

            with open(file_path, "rb") as f:
                while True:
                    data = f.read(_CHUNK_SIZE)
                    if not data:
                        break
                    part_num += 1
                    result = await sink.upload_part(session_id, part_num, data)
                    parts.append(result)
                    pushed += len(data)
                    task.pushed_parts = part_num
                    task.downloaded_bytes = pushed
                    task.download_progress = 1.0
                    task.push_progress = pushed / total_size if total_size else 1.0
                    task.progress = task.push_progress
                    await self._save(task)

                    if self._shutting_down:
                        logger.info("Shutdown: task %s stopping after push part %d", task.task_id[:8], part_num)
                        raise asyncio.CancelledError()

            result_url = await sink.complete_upload(session_id, parts)
            task.sink_result_url = result_url
            task.progress = 1.0
            task.download_progress = 1.0
            task.push_progress = 1.0
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.utcnow()
            await self._save(task)
            logger.info("File push complete: %s -> %s (%d parts)", task.filename, result_url or "?", part_num)

        except asyncio.CancelledError:
            if self._shutting_down:
                task.status = TaskStatus.PENDING
                task.error = "interrupted by shutdown"
                task.speed = 0.0
                task.download_speed = 0.0
                task.push_speed = 0.0
            else:
                task.status = TaskStatus.CANCELLED
                task.error = "Cancelled"
            await self._save(task)
        except Exception as exc:
            task.status = TaskStatus.FAILED
            task.error = str(exc)
            await self._save(task)
            logger.exception("File push %s failed", task.task_id[:8])
            try:
                await sink.abort_upload(session_id)
            except Exception:
                pass
        finally:
            self._running.pop(task.task_id, None)

    # ── Streaming pipeline: Source → HTTP stream → Sink ───────
    # Only one chunk buffer in memory at a time — zero local disk.

    async def _pipeline_stream_to_sink(self, task: TransferTask) -> None:
        """Stream from source URL directly to sink via multipart upload.

        Memory usage: ~1 chunk buffer (_CHUNK_SIZE, default 32 MB).
        Disk usage: zero.

        Resume: if ``task.sink_session_id`` and ``task.pushed_parts > 0`` are
        set (e.g. restored by :meth:`resume_interrupted`), the pipeline tries
        to continue the existing multipart upload at part
        ``pushed_parts + 1`` using a ranged source GET starting at
        ``pushed_parts * _CHUNK_SIZE``.  If the source rejects the Range, we
        abort the stale session and fall back to a fresh upload.
        """
        source = self._registry.get_source(task.source_plugin)
        sink = self._registry.create_sink(task.sink_plugin, config=task.sink_config)  # type: ignore[arg-type]

        # Get file info (filename, size) — skip if already pre-set from batch
        task.status = TaskStatus.DOWNLOADING
        await self._save(task)
        info = await self._retry(source.get_file_info, task.source_url, label=f"get_file_info({task.task_id[:8]})")
        if not task.filename:
            task.filename = info.filename
        if info.size:
            task.file_size = info.size
        await self._save(task)

        total_size = info.size or task.file_size or 0
        total_parts = math.ceil(total_size / _CHUNK_SIZE) if total_size else 0

        # Resolve the actual download URL (handle GDrive confirm, HF blob→resolve)
        dl_url, dl_cookies = await self._retry(
            self._resolve_download_url, task.source_url, task.source_plugin,
            label=f"resolve_url({task.task_id[:8]})",
        )

        # ── Decide: fresh start or resume ─────────────────────────
        resume_possible = bool(task.sink_session_id) and task.pushed_parts > 0
        parts: list[PartResult] = []
        part_num = 0
        downloaded = 0
        resume_offset = 0

        if resume_possible:
            try:
                parts = [PartResult(**p) for p in task.uploaded_parts]
            except Exception:
                parts = []
            if len(parts) == task.pushed_parts:
                session_id = task.sink_session_id
                part_num = task.pushed_parts
                resume_offset = part_num * _CHUNK_SIZE
                downloaded = resume_offset
                logger.info(
                    "Resuming stream %s -> sink '%s' (session %s, from byte %d / part %d)",
                    task.source_url[:60], task.sink_plugin, session_id[:12],
                    resume_offset, part_num,
                )
                task.status = TaskStatus.PUSHING
                await self._save(task)

                # Fast-path: everything already uploaded, just needs to be
                # committed on the sink side.
                if total_size and downloaded >= total_size:
                    result_url = await sink.complete_upload(session_id, parts)
                    task.sink_result_url = result_url
                    task.file_size = total_size
                    task.progress = 1.0
                    task.download_progress = 1.0
                    task.push_progress = 1.0
                    task.uploaded_parts = []  # freed after completion
                    await self._save(task)
                    logger.info(
                        "Resume completed (all parts pre-uploaded): %s -> %s",
                        task.filename, result_url[:80] if result_url else "?",
                    )
                    return
            else:
                # Persisted parts list is inconsistent with counter — safer to
                # abandon the old session and re-upload.
                logger.warning(
                    "Task %s has pushed_parts=%d but %d persisted parts — restarting",
                    task.task_id[:8], task.pushed_parts, len(parts),
                )
                try:
                    await sink.abort_upload(task.sink_session_id)
                except Exception:
                    pass
                resume_possible = False
                parts = []
                task.sink_session_id = ""
                task.uploaded_parts = []
                task.pushed_parts = 0
                task.downloaded_bytes = 0

        if not resume_possible:
            _user = getattr(task, "_user", None)
            sink_key = derive_sink_object_key(task.filename or task.task_id, _user)
            session_id = await sink.initialize_upload(sink_key, {"task_id": task.task_id})
            task.sink_session_id = session_id
            task.uploaded_parts = []
            task.pushed_parts = 0
            task.downloaded_bytes = 0
            task.status = TaskStatus.PUSHING
            await self._save(task)
            logger.info(
                "Streaming %s -> sink '%s' (session %s, ~%d parts)",
                task.source_url[:60], task.sink_plugin, session_id[:12],
                total_parts or -1,
            )

        buf = bytearray()

        # Google Drive requires a browser-like User-Agent, otherwise it
        # returns a small HTML error page instead of the actual file.
        stream_headers: dict[str, str] = {}
        if task.source_plugin == "gdrive":
            stream_headers["User-Agent"] = (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/39.0.2171.95 Safari/537.36"
            )
        if resume_offset > 0:
            stream_headers["Range"] = f"bytes={resume_offset}-"

        try:
            async with httpx.AsyncClient(
                follow_redirects=True, timeout=httpx.Timeout(30, read=600),
                cookies=dl_cookies or None, headers=stream_headers or None,
            ) as client:
                async with client.stream("GET", dl_url) as resp:
                    # Detect source ignoring our Range header: status 200 + full length
                    # means resume failed. Abort stale session, restart from zero.
                    if resume_offset > 0 and resp.status_code == 200:
                        logger.warning(
                            "Task %s: source ignored Range (got 200), restarting from scratch",
                            task.task_id[:8],
                        )
                        await resp.aclose()
                        try:
                            await sink.abort_upload(session_id)
                        except Exception:
                            pass
                        task.sink_session_id = ""
                        task.uploaded_parts = []
                        task.pushed_parts = 0
                        task.downloaded_bytes = 0
                        await self._save(task)
                        raise _RangeNotSupportedError()
                    resp.raise_for_status()

                    # Update filename from Content-Disposition if available
                    cd = resp.headers.get("content-disposition", "")
                    if "filename=" in cd:
                        for p in cd.split(";"):
                            p = p.strip()
                            if p.startswith("filename="):
                                task.filename = p[len("filename="):].strip().strip('"')

                    # Only trust Content-Length for total size on a *fresh*
                    # (non-Range) request.  For 206 responses Content-Length
                    # is the remaining bytes, which would corrupt total_size.
                    if not total_size and resume_offset == 0:
                        cl = resp.headers.get("content-length")
                        if cl:
                            total_size = int(cl)
                            task.file_size = total_size
                            total_parts = math.ceil(total_size / _CHUNK_SIZE)

                    _last_save = downloaded
                    async for chunk in resp.aiter_bytes(chunk_size=1024 * 1024):
                        buf.extend(chunk)
                        downloaded += len(chunk)
                        task.downloaded_bytes = downloaded

                        # Update download progress every ~8MB without waiting for push
                        if downloaded - _last_save >= 8 * 1024 * 1024:
                            _last_save = downloaded
                            if total_size > 0:
                                task.download_progress = downloaded / total_size
                                task.progress = downloaded / total_size
                            await self._save(task)

                        # Flush full chunks to sink
                        while len(buf) >= _CHUNK_SIZE:
                            part_num += 1
                            data = bytes(buf[:_CHUNK_SIZE])
                            del buf[:_CHUNK_SIZE]

                            t0 = time.monotonic()
                            result = await sink.upload_part(session_id, part_num, data)
                            push_elapsed = time.monotonic() - t0
                            if push_elapsed > 0:
                                task.push_speed = len(data) / push_elapsed
                            parts.append(result)
                            task.pushed_parts = part_num
                            task.uploaded_parts = [p.model_dump() for p in parts]

                            if total_size > 0:
                                task.download_progress = downloaded / total_size
                                pushed_bytes = part_num * _CHUNK_SIZE
                                task.push_progress = pushed_bytes / total_size
                                task.progress = downloaded / total_size
                            await self._save(task)

                            if self._shutting_down:
                                logger.info("Shutdown: task %s stopping after push part %d", task.task_id[:8], part_num)
                                raise asyncio.CancelledError()

                # Flush remaining buffer as final part
                if buf:
                    part_num += 1
                    final_data = bytes(buf)
                    t0 = time.monotonic()
                    result = await sink.upload_part(session_id, part_num, final_data)
                    push_elapsed = time.monotonic() - t0
                    if push_elapsed > 0:
                        task.push_speed = len(final_data) / push_elapsed
                    parts.append(result)
                    task.pushed_parts = part_num
                    task.uploaded_parts = [p.model_dump() for p in parts]
                    buf.clear()

            result_url = await sink.complete_upload(session_id, parts)
            task.sink_result_url = result_url
            task.file_size = downloaded
            task.progress = 1.0
            task.download_progress = 1.0
            task.push_progress = 1.0
            task.uploaded_parts = []  # freed after successful completion
            await self._save(task)

            logger.info(
                "Stream complete: %s -> %s (%d parts, %d bytes)",
                task.filename, result_url[:80] if result_url else "?",
                part_num, downloaded,
            )

        except _RangeNotSupportedError:
            # Source cannot resume — session already aborted and state cleared.
            # Restart the pipeline so it creates a fresh session.
            return await self._pipeline_stream_to_sink(task)
        except asyncio.CancelledError:
            # Cancelled (shutdown or user cancel): keep the sink session alive
            # so we can resume on restart. The caller's handler decides status.
            raise
        except Exception:
            # Real failure: abort sink upload so we do not leak parts.
            try:
                await sink.abort_upload(session_id)
            except Exception:
                logger.debug("Failed to abort sink session %s", session_id[:12])
            task.sink_session_id = ""
            task.uploaded_parts = []
            raise

    async def _retry(self, fn, *args, label: str = "", **kwargs):
        """Retry an async callable with exponential backoff on transient errors."""
        last_exc: Exception | None = None
        for attempt in range(_MAX_RETRIES):
            try:
                return await fn(*args, **kwargs)
            except (httpx.ConnectTimeout, httpx.ConnectError, httpx.ReadTimeout,
                    httpx.PoolTimeout, ConnectionError, OSError) as exc:
                last_exc = exc
                delay = _RETRY_BACKOFF[min(attempt, len(_RETRY_BACKOFF) - 1)]
                logger.warning(
                    "Retry %d/%d for %s: %s — waiting %ds",
                    attempt + 1, _MAX_RETRIES, label or fn.__name__,
                    type(exc).__name__, delay,
                )
                await asyncio.sleep(delay)
        raise last_exc  # type: ignore[misc]

    async def _resolve_download_url(
        self, url: str, source_plugin: str,
    ) -> tuple[str, dict[str, str]]:
        """Convert a user-facing URL to a direct download URL.

        Returns (resolved_url, cookies). Cookies are required for sources
        like Google Drive that gate downloads behind a confirmation page.
        """
        if source_plugin == "gdrive":
            source = self._registry.get_source("gdrive")
            return await source.resolve_download_url(url)
        elif source_plugin == "huggingface":
            from etransfer.plugins.sources.huggingface import _parse_hf_url, _to_resolve_url
            parts = _parse_hf_url(url)
            if parts:
                return _to_resolve_url(parts), {}
        return url, {}

    # ── Local pipeline: download to disk → register ───────────

    async def _pipeline_download_local(self, task: TransferTask) -> None:
        """Download file to local storage using aria2c/httpx, then register."""
        await self._phase_download(task)
        if task.status == TaskStatus.CANCELLED:
            return
        await self._phase_register(task)

    async def _phase_download(self, task: TransferTask) -> None:
        task.status = TaskStatus.DOWNLOADING
        await self._save(task)

        source = self._registry.get_source(task.source_plugin)
        info = await self._retry(source.get_file_info, task.source_url, label=f"get_file_info({task.task_id[:8]})")
        if not task.filename:
            task.filename = info.filename
        if info.size:
            task.file_size = info.size
        await self._save(task)

        dest_dir = self._download_dir / task.task_id
        dest_dir.mkdir(parents=True, exist_ok=True)

        def _progress(downloaded: int, total: Optional[int]) -> None:
            task.downloaded_bytes = downloaded
            if total and total > 0:
                task.download_progress = downloaded / total
                task.progress = downloaded / total

        file_path = await self._retry(source.download, task.source_url, dest_dir, on_progress=_progress, label=f"download({task.task_id[:8]})")
        task.filename = file_path.name
        task.file_size = file_path.stat().st_size
        task.downloaded_bytes = task.file_size
        await self._save(task)

    async def _phase_register(self, task: TransferTask) -> None:
        """Register downloaded file in TusStorage."""
        file_path = self._download_dir / task.task_id / (task.filename or "download")
        if not file_path.exists():
            return

        from etransfer.server.tus.models import TusUpload

        file_id = uuid.uuid4().hex
        file_size = file_path.stat().st_size
        use_chunked = task.retention == "download_once"

        if use_chunked:
            chunk_dir = self._storage.get_chunk_dir(file_id)
            chunk_dir.mkdir(parents=True, exist_ok=True)
            idx = 0
            with open(file_path, "rb") as f:
                while True:
                    data = f.read(_CHUNK_SIZE)
                    if not data:
                        break
                    (chunk_dir / f"chunk_{idx:06d}").write_bytes(data)
                    await self._storage.mark_chunk_available(file_id, idx)
                    idx += 1
            storage_path = str(chunk_dir)
            total_chunks = idx
        else:
            storage_path = str(self._storage.get_file_path(file_id))
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
            chunk_size=_CHUNK_SIZE,
            available_size=file_size,
            total_chunks=total_chunks,
            received_ranges=[[0, file_size]],
        )
        await self._storage.create_upload(upload)

        if not use_chunked:
            shutil.move(str(file_path), str(self._storage.get_file_path(file_id)))

        await self._storage.finalize_upload(file_id)
        task.file_id = file_id
        await self._save(task)
