"""REST API routes for bucket-to-bucket migrations."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import uuid
from datetime import datetime
from enum import Enum
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Optional
from urllib.parse import urlparse, urlunparse

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field, SecretStr, field_validator

from etransfer.common.fileutil import derive_sink_object_key
from etransfer.plugins.sinks.tos_multipart import TosSink

logger = logging.getLogger("etransfer.server.migrations")

_DEFAULT_REGION = "us-east-1"
_DEFAULT_PRESIGN_EXPIRES = 24 * 60 * 60
_MAX_PRESIGN_EXPIRES = 7 * 24 * 60 * 60
_DEFAULT_MAX_OBJECTS = 1000
_MAX_OBJECTS = 10000
_DEFAULT_CONTROLLER_PAGE_SIZE = 100
_MAX_CONTROLLER_PAGE_SIZE = 1000
_DEFAULT_CONTROLLER_MAX_RUNNING_PER_PAGE = 20
_MAX_CONTROLLER_MAX_RUNNING_PER_PAGE = 20
_MIGRATION_STATE_PREFIX = "et:migration:"
_COPY_CHUNK_SIZE = 16 * 1024 * 1024
_SHUTDOWN_PAUSE_ERROR = "interrupted by shutdown"
_SHUTDOWN_DRAIN_TIMEOUT_SECONDS = 10 * 60
_SHUTDOWN_LOG_INTERVAL_SECONDS = 10
_MIGRATION_PROGRESS_LOG_INTERVAL_SECONDS = 10
_MIGRATION_OBJECT_LOG_PATH = Path("local_deploy/bucket_migration_objects.jsonl")
_MIGRATION_FAILED_OBJECT_LOG_PATH = Path("local_deploy/bucket_migration_failed_objects.jsonl")
_MIGRATION_OBJECT_MAX_ATTEMPTS = 3
_SOURCE_RANGE_MAX_ATTEMPTS = 3
_SOURCE_RANGE_CONCURRENCY = 4
_SOURCE_RANGE_GLOBAL_CONCURRENCY = 64
_SOURCE_CONNECT_TIMEOUT_SECONDS = 5
_SOURCE_READ_TIMEOUT_SECONDS = 10
_SOURCE_WRITE_TIMEOUT_SECONDS = 10
_SOURCE_POOL_TIMEOUT_SECONDS = 5
_SOURCE_STREAM_CHUNK_SIZE = 1024 * 1024


class BucketSourceConfig(BaseModel):
    """S3-compatible source bucket settings for one migration request."""

    endpoint: str = Field(..., min_length=1, description="ZOS/S3 endpoint")
    bucket: str = Field(..., min_length=1, description="Source bucket name")
    ak: SecretStr = Field(..., min_length=1, description="Access key")
    sk: SecretStr = Field(..., min_length=1, description="Secret key")
    region: str = Field(_DEFAULT_REGION, description="Signing region")
    prefix: str = Field("", description="Only migrate objects under this prefix")
    secure: bool = Field(True, description="Use HTTPS when endpoint has no scheme")

    @field_validator("endpoint", "bucket", "region", "prefix", mode="before")
    @classmethod
    def _strip_text(cls, value: Any) -> Any:
        return value.strip() if isinstance(value, str) else value


def _migration_object_log_paths(log_path: Path = _MIGRATION_OBJECT_LOG_PATH) -> list[Path]:
    if log_path.name != _MIGRATION_OBJECT_LOG_PATH.name:
        return [log_path]
    paths = sorted(log_path.parent.glob(f"{log_path.stem}*.jsonl"))
    return [path for path in paths if not path.name.startswith(_MIGRATION_FAILED_OBJECT_LOG_PATH.stem)]


def _empty_migration_object_log_stats() -> dict[str, int]:
    return {
        "migration_object_count": 0,
        "migration_download_bytes": 0,
        "migration_push_bytes": 0,
    }


def _scan_migration_object_log_stats(log_path: Path = _MIGRATION_OBJECT_LOG_PATH) -> dict[str, int]:
    """Fully summarize small or test-only completed object-copy logs."""

    stats = _empty_migration_object_log_stats()
    for path in _migration_object_log_paths(log_path):
        if not path.exists():
            continue
        try:
            with path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        logger.warning("Skipping malformed migration object log line")
                        continue
                    size = int(record.get("size") or 0)
                    stats["migration_object_count"] += 1
                    stats["migration_download_bytes"] += size
                    stats["migration_push_bytes"] += size
        except OSError:
            logger.warning("Failed to read migration object log: %s", path, exc_info=True)
    return stats


def read_migration_object_log_stats(log_path: Path = _MIGRATION_OBJECT_LOG_PATH) -> dict[str, int]:
    """Summarize completed object-copy records from the append-only JSONL log."""

    if log_path == _MIGRATION_OBJECT_LOG_PATH:
        # Production stats must stay cheap.  JSONL migration logs are imported
        # into durable aggregate storage out of band after migrations finish.
        return _empty_migration_object_log_stats()
    return _scan_migration_object_log_stats(log_path)


def _source_download_timeout() -> httpx.Timeout:
    return httpx.Timeout(
        connect=_SOURCE_CONNECT_TIMEOUT_SECONDS,
        read=_SOURCE_READ_TIMEOUT_SECONDS,
        write=_SOURCE_WRITE_TIMEOUT_SECONDS,
        pool=_SOURCE_POOL_TIMEOUT_SECONDS,
    )


class TosTargetConfig(BaseModel):
    """Volcengine TOS sink settings for one migration request."""

    endpoint: str = Field(..., min_length=1, description="TOS endpoint")
    region: str = Field(..., min_length=1, description="TOS region")
    bucket: str = Field(..., min_length=1, description="Target bucket name")
    ak: SecretStr = Field(..., min_length=1, description="Access key")
    sk: SecretStr = Field(..., min_length=1, description="Secret key")
    prefix: str = Field("", description="Target object key prefix")

    @field_validator("endpoint", "region", "bucket", "prefix", mode="before")
    @classmethod
    def _strip_text(cls, value: Any) -> Any:
        return value.strip() if isinstance(value, str) else value


class CreateBucketMigrationRequest(BaseModel):
    """Create a batch of existing transfer tasks from an S3-compatible bucket."""

    source: BucketSourceConfig
    target: Optional[TosTargetConfig] = None
    dry_run: bool = Field(False, description="List source objects without creating transfer tasks")
    marker: str = Field("", description="Continue listing after this object key")
    max_objects: int = Field(_DEFAULT_MAX_OBJECTS, ge=1, le=_MAX_OBJECTS)
    presign_expires_seconds: int = Field(
        _DEFAULT_PRESIGN_EXPIRES,
        ge=60,
        le=_MAX_PRESIGN_EXPIRES,
        description="Signed source URL TTL, in seconds",
    )


class MigrationObjectError(BaseModel):
    key: str
    error: str


class MigrationTaskItem(BaseModel):
    task_id: str
    key: str
    size: int
    filename: str


class CreateBucketMigrationResponse(BaseModel):
    batch: bool = True
    dry_run: bool = False
    source_bucket: str
    target_bucket: str = ""
    source_prefix: str
    total_objects: int
    created_tasks: int
    total_size: int
    truncated: bool
    next_marker: str = ""
    tasks: list[MigrationTaskItem]
    errors: list[MigrationObjectError]


class MigrationJobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    FAILED = "failed"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class CreateMigrationJobRequest(BaseModel):
    """Create a durable, page-by-page bucket migration controller job."""

    source: BucketSourceConfig
    target: TosTargetConfig
    owner_email: str = Field("", description="Optional owner email for sink key derivation and ownership")
    marker: str = Field("", description="Initial ZOS ListBucketObjects marker")
    page_size: int = Field(_DEFAULT_CONTROLLER_PAGE_SIZE, ge=1, le=_MAX_CONTROLLER_PAGE_SIZE)
    max_running_per_page: int = Field(
        _DEFAULT_CONTROLLER_MAX_RUNNING_PER_PAGE,
        ge=1,
        le=_MAX_CONTROLLER_MAX_RUNNING_PER_PAGE,
    )
    presign_expires_seconds: int = Field(
        _MAX_PRESIGN_EXPIRES,
        ge=60,
        le=_MAX_PRESIGN_EXPIRES,
        description="Signed source URL TTL for each page of child tasks",
    )
    cleanup_completed_tasks: bool = Field(True, description="Delete terminal child task states after each page")

    @field_validator("owner_email", "marker", mode="before")
    @classmethod
    def _strip_text(cls, value: Any) -> Any:
        return value.strip() if isinstance(value, str) else value


class MigrationJob(BaseModel):
    """Internal persisted controller state.

    Only one page of child task IDs is stored at a time.  Source/target
    credentials are stored once per controller job so interrupted jobs can
    resume without expanding millions of per-object records into the state
    backend.
    """

    job_id: str
    status: MigrationJobStatus = MigrationJobStatus.PENDING
    source_config: dict[str, Any]
    target_config: dict[str, Any]
    owner_id: Optional[int] = None
    owner_email: str = ""
    marker: str = ""
    next_marker: str = ""
    page_size: int = _DEFAULT_CONTROLLER_PAGE_SIZE
    max_running_per_page: int = _DEFAULT_CONTROLLER_MAX_RUNNING_PER_PAGE
    presign_expires_seconds: int = _MAX_PRESIGN_EXPIRES
    cleanup_completed_tasks: bool = True
    active_task_ids: list[str] = Field(default_factory=list)
    current_page_objects: int = 0
    current_page_bytes: int = 0
    current_page_started_at: Optional[datetime] = None
    processed_objects: int = 0
    processed_bytes: int = 0
    pages_completed: int = 0
    failed_tasks: list[str] = Field(default_factory=list)
    error: str = ""
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None


class MigrationJobResponse(BaseModel):
    job_id: str
    status: MigrationJobStatus
    owner_id: Optional[int]
    owner_email: str
    source_bucket: str
    target_bucket: str
    source_prefix: str
    marker: str
    next_marker: str
    page_size: int
    max_running_per_page: int
    active_task_count: int
    current_page_objects: int
    current_page_bytes: int
    processed_objects: int
    processed_bytes: int
    pages_completed: int
    failed_tasks: list[str]
    error: str
    cleanup_completed_tasks: bool
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime]


def _normalize_source_endpoint(source: BucketSourceConfig) -> str:
    endpoint = source.endpoint
    value = endpoint.strip().rstrip("/")
    if not value:
        raise ValueError("source endpoint is required")
    parsed = urlparse(value)
    if not parsed.scheme:
        scheme = "https" if source.secure else "http"
        value = f"{scheme}://{value}"
        parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("source endpoint must use http or https")
    if not parsed.netloc:
        raise ValueError("source endpoint must include a host")
    host = parsed.hostname or ""
    bucket_prefix = f"{source.bucket}."
    if host.startswith(bucket_prefix):
        bare_host = host[len(bucket_prefix) :]
        netloc = bare_host
        if parsed.port:
            netloc = f"{bare_host}:{parsed.port}"
        parsed = parsed._replace(netloc=netloc, path="", params="", query="", fragment="")
        value = urlunparse(parsed)
    return value.rstrip("/")


def _normalize_tos_endpoint(endpoint: str) -> str:
    value = endpoint.strip().rstrip("/")
    if not value:
        raise ValueError("target endpoint is required")
    parsed = urlparse(value)
    if parsed.scheme:
        return parsed.netloc.rstrip("/")
    return value


def _build_s3_client(source: BucketSourceConfig) -> Any:
    """Create a boto3 S3 client lazily so normal server startup has no hard dependency."""

    try:
        import boto3  # type: ignore[import-untyped]
        from botocore.config import Config  # type: ignore[import-untyped]
    except ImportError as exc:
        raise RuntimeError(
            "Bucket migration requires boto3. Install the server with S3 support or install boto3."
        ) from exc

    endpoint_url = _normalize_source_endpoint(source)
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=source.ak.get_secret_value(),
        aws_secret_access_key=source.sk.get_secret_value(),
        region_name=source.region or _DEFAULT_REGION,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _target_sink_config(target: TosTargetConfig) -> dict[str, str]:
    cfg = {
        "endpoint": _normalize_tos_endpoint(target.endpoint),
        "region": target.region,
        "bucket": target.bucket,
        "ak": target.ak.get_secret_value(),
        "sk": target.sk.get_secret_value(),
    }
    prefix = target.prefix.strip("/")
    if prefix:
        cfg["prefix"] = prefix
    return cfg


def _is_directory_placeholder(key: str, size: int) -> bool:
    return key.endswith("/") and size == 0


def _list_source_objects(
    client: Any,
    source: BucketSourceConfig,
    max_objects: int,
    marker: str = "",
) -> tuple[list[dict[str, Any]], bool, str]:
    objects: list[dict[str, Any]] = []
    current_marker = marker
    next_marker = ""
    truncated = False
    prefix = source.prefix or ""

    while len(objects) < max_objects:
        limit = min(1000, max_objects - len(objects))
        params: dict[str, Any] = {
            "Bucket": source.bucket,
            "Prefix": prefix,
            "MaxKeys": limit,
        }
        if current_marker:
            params["Marker"] = current_marker
        response = client.list_objects(**params)
        for item in response.get("Contents", []) or []:
            key = item.get("Key", "")
            size = int(item.get("Size") or 0)
            if not key or _is_directory_placeholder(key, size):
                continue
            objects.append({"key": key, "size": size})
            if len(objects) >= max_objects:
                break

        if len(objects) >= max_objects:
            truncated = bool(response.get("IsTruncated")) or bool(response.get("NextMarker"))
            next_marker = response.get("NextMarker") or (objects[-1]["key"] if truncated and objects else "")
            break
        if not response.get("IsTruncated"):
            break
        current_marker = response.get("NextMarker") or ""
        next_marker = current_marker
        if not current_marker:
            break

    return objects, truncated, next_marker


def _presign_get_object(client: Any, bucket: str, key: str, expires: int) -> str:
    return str(
        client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires,
        )
    )


def _source_config_to_plain(source: BucketSourceConfig) -> dict[str, Any]:
    return {
        "endpoint": source.endpoint,
        "bucket": source.bucket,
        "ak": source.ak.get_secret_value(),
        "sk": source.sk.get_secret_value(),
        "region": source.region,
        "prefix": source.prefix,
        "secure": source.secure,
    }


def _target_config_to_plain(target: TosTargetConfig) -> dict[str, Any]:
    return {
        "endpoint": target.endpoint,
        "region": target.region,
        "bucket": target.bucket,
        "ak": target.ak.get_secret_value(),
        "sk": target.sk.get_secret_value(),
        "prefix": target.prefix,
    }


def _job_response(job: MigrationJob) -> MigrationJobResponse:
    source = job.source_config
    target = job.target_config
    return MigrationJobResponse(
        job_id=job.job_id,
        status=job.status,
        owner_id=job.owner_id,
        owner_email=job.owner_email,
        source_bucket=str(source.get("bucket", "")),
        target_bucket=str(target.get("bucket", "")),
        source_prefix=str(source.get("prefix", "")),
        marker=job.marker,
        next_marker=job.next_marker,
        page_size=job.page_size,
        max_running_per_page=job.max_running_per_page,
        active_task_count=len(job.active_task_ids),
        current_page_objects=job.current_page_objects,
        current_page_bytes=job.current_page_bytes,
        processed_objects=job.processed_objects,
        processed_bytes=job.processed_bytes,
        pages_completed=job.pages_completed,
        failed_tasks=job.failed_tasks,
        error=job.error,
        cleanup_completed_tasks=job.cleanup_completed_tasks,
        created_at=job.created_at,
        updated_at=job.updated_at,
        completed_at=job.completed_at,
    )


class BucketMigrationController:
    """Page-by-page migration orchestrator.

    The controller advances a single persisted marker after each page of child
    tasks completes.  This keeps state bounded for buckets with millions of
    objects and lets process restarts resume from the last saved marker.
    """

    def __init__(
        self,
        state_manager: Any,
        task_manager: Any,
        user_db: Any = None,
        poll_interval: float = 5.0,
        shutdown_drain_timeout: float = _SHUTDOWN_DRAIN_TIMEOUT_SECONDS,
        shutdown_log_interval: float = _SHUTDOWN_LOG_INTERVAL_SECONDS,
        object_log_path: Optional[Path] = None,
        failed_object_log_path: Optional[Path] = None,
    ) -> None:
        self._state = state_manager
        self._task_manager = task_manager
        self._user_db = user_db
        self._poll_interval = poll_interval
        self._shutdown_drain_timeout = shutdown_drain_timeout
        self._shutdown_log_interval = shutdown_log_interval
        self._object_log_path = object_log_path or _MIGRATION_OBJECT_LOG_PATH
        self._failed_object_log_path = failed_object_log_path or _MIGRATION_FAILED_OBJECT_LOG_PATH
        self._object_log_lock = asyncio.Lock()
        self._failed_object_log_lock = asyncio.Lock()
        self._range_gate = asyncio.Semaphore(_SOURCE_RANGE_GLOBAL_CONCURRENCY)
        self._running: dict[str, asyncio.Task] = {}
        self._stopping = False

    def _key(self, job_id: str) -> str:
        return f"{_MIGRATION_STATE_PREFIX}{job_id}"

    async def _save(self, job: MigrationJob) -> None:
        job.updated_at = datetime.utcnow()
        await self._state.set(self._key(job.job_id), job.model_dump_json())

    async def _load(self, job_id: str) -> Optional[MigrationJob]:
        raw = await self._state.get(self._key(job_id))
        if not raw:
            return None
        return MigrationJob.model_validate_json(raw)

    async def list_jobs(self) -> list[MigrationJob]:
        keys = await self._state.keys(f"{_MIGRATION_STATE_PREFIX}*")
        jobs: list[MigrationJob] = []
        for key in keys:
            raw = await self._state.get(key)
            if raw:
                jobs.append(MigrationJob.model_validate_json(raw))
        jobs.sort(key=lambda item: item.created_at, reverse=True)
        return jobs

    async def get_job(self, job_id: str) -> Optional[MigrationJob]:
        return await self._load(job_id)

    async def create_job(self, body: CreateMigrationJobRequest, user: Any = None) -> MigrationJob:
        owner_id = getattr(user, "id", None) if user else None
        owner_email = body.owner_email or getattr(user, "email", "") or ""

        if body.owner_email and self._user_db is not None:
            owner = await self._resolve_user_by_email(body.owner_email)
            if owner is not None:
                owner_id = getattr(owner, "id", None)
                owner_email = getattr(owner, "email", body.owner_email) or body.owner_email
            else:
                logger.warning("Migration owner email not found; recording email without owner_id: %s", body.owner_email)

        job = MigrationJob(
            job_id=uuid.uuid4().hex,
            status=MigrationJobStatus.RUNNING,
            source_config=_source_config_to_plain(body.source),
            target_config=_target_config_to_plain(body.target),
            owner_id=owner_id,
            owner_email=owner_email,
            marker=body.marker,
            page_size=body.page_size,
            max_running_per_page=body.max_running_per_page,
            presign_expires_seconds=body.presign_expires_seconds,
            cleanup_completed_tasks=body.cleanup_completed_tasks,
        )
        await self._save(job)
        self._ensure_running(job.job_id)
        return job

    async def _resolve_user_by_email(self, email: str) -> Any:
        users = await self._user_db.list_users()
        target = email.lower()
        for user in users:
            if (getattr(user, "email", "") or "").lower() == target:
                return user
        return None

    def _ensure_running(self, job_id: str) -> None:
        task = self._running.get(job_id)
        if task and not task.done():
            return
        self._running[job_id] = asyncio.create_task(self._run_job(job_id))

    async def resume_interrupted(self) -> int:
        resumed = 0
        for job in await self.list_jobs():
            if job.status == MigrationJobStatus.PAUSED and job.error == _SHUTDOWN_PAUSE_ERROR:
                job.status = MigrationJobStatus.RUNNING
                job.error = ""
                job.active_task_ids = []
                job.current_page_objects = 0
                job.current_page_bytes = 0
                job.current_page_started_at = None
                await self._save(job)
                self._ensure_running(job.job_id)
                resumed += 1
            elif job.status == MigrationJobStatus.RUNNING:
                self._ensure_running(job.job_id)
                resumed += 1
        return resumed

    async def shutdown(self) -> None:
        self._stopping = True
        for job_id in list(self._running.keys()):
            job = await self._load(job_id)
            if job is not None and job.status == MigrationJobStatus.RUNNING:
                logger.info("Migration shutdown pause requested: %s", self._shutdown_job_detail(job))
                job.status = MigrationJobStatus.PAUSED
                job.error = _SHUTDOWN_PAUSE_ERROR
                await self._save(job)

        deadline = asyncio.get_running_loop().time() + self._shutdown_drain_timeout
        next_log_at = 0.0
        while True:
            running = [(job_id, task) for job_id, task in self._running.items() if not task.done()]
            if not running:
                break
            now = asyncio.get_running_loop().time()
            if now >= next_log_at:
                logger.info(
                    "Migration shutdown waiting for %d job(s), remaining timeout=%ds",
                    len(running),
                    max(0, int(deadline - now)),
                )
                for job_id, _task in running:
                    job = await self._load(job_id)
                    if job is not None:
                        logger.info("Migration shutdown waiting detail: %s", self._shutdown_job_detail(job))
                next_log_at = now + self._shutdown_log_interval
            if now >= deadline:
                logger.warning("Migration shutdown drain timeout; cancelling %d job runner(s)", len(running))
                break
            await asyncio.sleep(0.5)

        for task in list(self._running.values()):
            if not task.done():
                task.cancel()
        for task in list(self._running.values()):
            with contextlib.suppress(asyncio.CancelledError):
                await task
        for job_id in list(self._running.keys()):
            job = await self._load(job_id)
            if job is not None and job.status == MigrationJobStatus.PAUSED and job.error == _SHUTDOWN_PAUSE_ERROR:
                job.active_task_ids = []
                job.current_page_objects = 0
                job.current_page_bytes = 0
                job.current_page_started_at = None
                await self._save(job)
        self._running.clear()

    @staticmethod
    def _shutdown_job_detail(job: MigrationJob) -> str:
        first_key = job.active_task_ids[0] if job.active_task_ids else "-"
        last_key = job.active_task_ids[-1] if job.active_task_ids else "-"
        return (
            f"id={job.job_id[:8]} status={job.status.value} owner={job.owner_email or '-'} "
            f"page_size={job.page_size} page_running_limit={job.max_running_per_page} "
            f"current_page_objects={job.current_page_objects} current_page_bytes={job.current_page_bytes} "
            f"processed_objects={job.processed_objects} processed_bytes={job.processed_bytes} "
            f"pages_completed={job.pages_completed} marker={job.marker or '-'} next_marker={job.next_marker or '-'} "
            f"first_key={first_key} last_key={last_key}"
        )

    async def pause_job(self, job_id: str) -> Optional[MigrationJob]:
        job = await self._load(job_id)
        if job is None:
            return None
        if job.status == MigrationJobStatus.RUNNING:
            job.status = MigrationJobStatus.PAUSED
            await self._save(job)
        return job

    async def resume_job(self, job_id: str) -> Optional[MigrationJob]:
        job = await self._load(job_id)
        if job is None:
            return None
        if job.status in (MigrationJobStatus.PAUSED, MigrationJobStatus.FAILED, MigrationJobStatus.PENDING):
            job.status = MigrationJobStatus.RUNNING
            job.error = ""
            await self._save(job)
            self._ensure_running(job.job_id)
        return job

    async def cancel_job(self, job_id: str) -> Optional[MigrationJob]:
        job = await self._load(job_id)
        if job is None:
            return None
        job.status = MigrationJobStatus.CANCELLED
        job.completed_at = datetime.utcnow()
        for task_id in list(job.active_task_ids):
            with contextlib.suppress(Exception):
                await self._task_manager.cancel_task(task_id)
        await self._save(job)
        return job

    async def _run_job(self, job_id: str) -> None:
        while not self._stopping:
            job = await self._load(job_id)
            if job is None or job.status != MigrationJobStatus.RUNNING:
                return
            try:
                await self._start_next_page(job)
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Migration job %s failed", job_id[:8])
                job = await self._load(job_id) or job
                job.status = MigrationJobStatus.FAILED
                job.error = str(exc)
                await self._save(job)
                return

    async def _start_next_page(self, job: MigrationJob) -> None:
        source = BucketSourceConfig(**job.source_config)
        target = TosTargetConfig(**job.target_config)
        client = _build_s3_client(source)
        objects, truncated, next_marker = await asyncio.to_thread(
            _list_source_objects,
            client,
            source,
            job.page_size,
            job.marker,
        )
        if not objects:
            if truncated and next_marker:
                job.marker = next_marker
                job.next_marker = next_marker
                await self._save(job)
                return
            job.status = MigrationJobStatus.COMPLETED
            job.next_marker = ""
            job.completed_at = datetime.utcnow()
            await self._save(job)
            return

        owner_user = self._owner_user(job)
        page_bytes = 0
        job.active_task_ids = [str(obj["key"]) for obj in objects]
        job.current_page_objects = len(objects)
        job.current_page_bytes = sum(int(obj.get("size") or 0) for obj in objects)
        job.current_page_started_at = datetime.utcnow()
        job.next_marker = next_marker if truncated else ""
        await self._save(job)
        logger.info(
            "Migration page started: job=%s objects=%d bytes=%d marker=%s next_marker=%s first_key=%s last_key=%s object_log=%s",
            job.job_id[:8],
            job.current_page_objects,
            job.current_page_bytes,
            job.marker or "-",
            job.next_marker or "-",
            job.active_task_ids[0] if job.active_task_ids else "-",
            job.active_task_ids[-1] if job.active_task_ids else "-",
            self._object_log_path,
        )

        sink_config = _target_sink_config(target)
        sem = asyncio.Semaphore(job.max_running_per_page)
        latest = job

        async def copy_one(obj: dict[str, Any]) -> tuple[int, bool]:
            async with sem:
                key = str(obj["key"])
                size = int(obj.get("size") or 0)
                started_at = datetime.utcnow()
                elapsed_start = asyncio.get_running_loop().time()
                last_exc: Exception | None = None
                for attempt in range(1, _MIGRATION_OBJECT_MAX_ATTEMPTS + 1):
                    try:
                        presigned_url = await asyncio.to_thread(
                            _presign_get_object,
                            client,
                            source.bucket,
                            key,
                            job.presign_expires_seconds,
                        )
                        await self._copy_object_to_sink(presigned_url, key, size, sink_config, owner_user, job.job_id)
                        await self._append_object_log(
                            {
                                "completed_at": datetime.utcnow().isoformat() + "Z",
                                "started_at": started_at.isoformat() + "Z",
                                "elapsed_seconds": round(asyncio.get_running_loop().time() - elapsed_start, 3),
                                "attempts": attempt,
                                "job_id": job.job_id,
                                "owner_email": job.owner_email,
                                "source_bucket": source.bucket,
                                "target_bucket": target.bucket,
                                "source_key": key,
                                "target_key": self._target_log_key(key, sink_config, owner_user),
                                "size": size,
                                "page_marker": job.marker,
                                "page_next_marker": job.next_marker,
                                "pages_completed_before_page": job.pages_completed,
                            }
                        )
                        return size, True
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        last_exc = exc
                        logger.warning(
                            "Migration object copy failed: job=%s key=%s attempt=%d/%d error=%s",
                            job.job_id[:8],
                            key,
                            attempt,
                            _MIGRATION_OBJECT_MAX_ATTEMPTS,
                            repr(exc),
                        )
                        if attempt < _MIGRATION_OBJECT_MAX_ATTEMPTS:
                            await asyncio.sleep(min(2 ** (attempt - 1), 5))

                await self._append_failed_object_log(
                    {
                        "failed_at": datetime.utcnow().isoformat() + "Z",
                        "started_at": started_at.isoformat() + "Z",
                        "elapsed_seconds": round(asyncio.get_running_loop().time() - elapsed_start, 3),
                        "attempts": _MIGRATION_OBJECT_MAX_ATTEMPTS,
                        "job_id": job.job_id,
                        "owner_email": job.owner_email,
                        "source_bucket": source.bucket,
                        "target_bucket": target.bucket,
                        "source_key": key,
                        "target_key": self._target_log_key(key, sink_config, owner_user),
                        "size": size,
                        "page_marker": job.marker,
                        "page_next_marker": job.next_marker,
                        "pages_completed_before_page": job.pages_completed,
                        "error_type": type(last_exc).__name__ if last_exc else "",
                        "error": repr(last_exc) if last_exc else "",
                    }
                )
                logger.error(
                    "Migration object skipped after retries: job=%s key=%s attempts=%d failed_log=%s",
                    job.job_id[:8],
                    key,
                    _MIGRATION_OBJECT_MAX_ATTEMPTS,
                    self._failed_object_log_path,
                )
                return 0, False

        pending: set[asyncio.Task[tuple[int, bool]]] = {asyncio.create_task(copy_one(obj)) for obj in objects}
        page_done_objects = 0
        page_failed_objects = 0
        next_progress_log_at = asyncio.get_running_loop().time() + _MIGRATION_PROGRESS_LOG_INTERVAL_SECONDS
        next_status_check_at = asyncio.get_running_loop().time() + _MIGRATION_PROGRESS_LOG_INTERVAL_SECONDS
        while pending:
            done, pending = await asyncio.wait(
                pending,
                timeout=1.0,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for copied in done:
                copied_bytes, success = copied.result()
                page_bytes += copied_bytes
                page_done_objects += 1
                if not success:
                    page_failed_objects += 1
            now = asyncio.get_running_loop().time()
            if now >= next_status_check_at:
                latest = await self._load(job.job_id)
                if latest is None or latest.status in (MigrationJobStatus.CANCELLED, MigrationJobStatus.FAILED):
                    for task in pending:
                        task.cancel()
                    return
                next_status_check_at = now + _MIGRATION_PROGRESS_LOG_INTERVAL_SECONDS
            if pending and now >= next_progress_log_at:
                logger.info(
                    "Migration page progress: job=%s page_done_objects=%d/%d page_failed_objects=%d page_done_bytes=%d/%d "
                    "pending_objects=%d pages_completed=%d processed_objects=%d processed_bytes=%d marker=%s",
                    job.job_id[:8],
                    page_done_objects,
                    len(objects),
                    page_failed_objects,
                    page_bytes,
                    job.current_page_bytes,
                    len(pending),
                    latest.pages_completed,
                    latest.processed_objects,
                    latest.processed_bytes,
                    latest.marker or "-",
                )
                next_progress_log_at = now + _MIGRATION_PROGRESS_LOG_INTERVAL_SECONDS

        latest = await self._load(job.job_id) or job
        latest.processed_objects += len(objects)
        latest.processed_bytes += page_bytes
        latest.pages_completed += 1
        latest.marker = latest.next_marker
        latest.active_task_ids = []
        latest.current_page_objects = 0
        latest.current_page_bytes = 0
        latest.current_page_started_at = None
        if not latest.marker:
            latest.status = MigrationJobStatus.COMPLETED
            latest.completed_at = datetime.utcnow()
        await self._save(latest)
        logger.info(
            "Migration page completed: job=%s page_objects=%d page_failed_objects=%d page_bytes=%d pages_completed=%d "
            "processed_objects=%d processed_bytes=%d next_marker=%s status=%s",
            latest.job_id[:8],
            len(objects),
            page_failed_objects,
            page_bytes,
            latest.pages_completed,
            latest.processed_objects,
            latest.processed_bytes,
            latest.marker or "-",
            latest.status.value,
        )

    async def _copy_object_to_sink(
        self,
        source_url: str,
        source_key: str,
        size: int,
        sink_config: dict[str, str],
        owner_user: Any,
        job_id: str,
    ) -> None:
        object_key = derive_sink_object_key(source_key, owner_user)
        if size == 0:
            await self._put_tos_object(sink_config, object_key, b"")
            return
        if size <= _COPY_CHUNK_SIZE:
            await self._copy_small_object_to_tos(source_url, sink_config, object_key, size)
            return

        sink = TosSink(sink_config)
        session_id = await sink.initialize_upload(object_key, {"migration_job_id": job_id})
        try:
            await self._copy_large_object_to_tos(source_url, source_key, size, sink, session_id)
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await asyncio.shield(sink.abort_upload(session_id))
            raise
        except Exception:
            with contextlib.suppress(Exception):
                await asyncio.shield(sink.abort_upload(session_id))
            raise

    async def _copy_large_object_to_tos(
        self,
        source_url: str,
        source_key: str,
        expected_size: int,
        sink: TosSink,
        session_id: str,
    ) -> None:
        parts = []
        total_parts = (expected_size + _COPY_CHUNK_SIZE - 1) // _COPY_CHUNK_SIZE
        next_to_schedule = 1
        next_to_upload = 1
        pending: dict[int, asyncio.Task[bytes]] = {}
        completed: dict[int, bytes] = {}

        async with httpx.AsyncClient(follow_redirects=True, timeout=_source_download_timeout()) as client:
            try:
                while next_to_upload <= total_parts:
                    for part_number, task in list(pending.items()):
                        if task.done():
                            completed[part_number] = task.result()
                            del pending[part_number]

                    while (
                        next_to_schedule <= total_parts
                        and len(pending) + len(completed) < _SOURCE_RANGE_CONCURRENCY
                    ):
                        start = (next_to_schedule - 1) * _COPY_CHUNK_SIZE
                        length = min(_COPY_CHUNK_SIZE, expected_size - start)
                        pending[next_to_schedule] = asyncio.create_task(
                            self._download_source_range(
                                client,
                                source_url,
                                source_key,
                                next_to_schedule,
                                start,
                                length,
                            )
                        )
                        next_to_schedule += 1

                    data = completed.pop(next_to_upload, None)
                    if data is None:
                        if next_to_upload in pending:
                            data = await pending.pop(next_to_upload)
                        else:
                            done, _ = await asyncio.wait(
                                pending.values(),
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            for done_task in done:
                                part_number = next(
                                    number for number, task in pending.items() if task is done_task
                                )
                                completed[part_number] = done_task.result()
                                del pending[part_number]
                            continue

                    parts.append(await sink.upload_part(session_id, next_to_upload, data))
                    del data
                    next_to_upload += 1
            except asyncio.CancelledError:
                for task in pending.values():
                    task.cancel()
                raise
            except Exception:
                for task in pending.values():
                    task.cancel()
                raise

        await sink.complete_upload(session_id, parts)

    async def _download_source_range(
        self,
        client: httpx.AsyncClient,
        source_url: str,
        source_key: str,
        part_number: int,
        start: int,
        length: int,
    ) -> bytes:
        end = start + length - 1
        headers = {"Range": f"bytes={start}-{end}"}
        last_exc: Exception | None = None
        for attempt in range(1, _SOURCE_RANGE_MAX_ATTEMPTS + 1):
            try:
                async with self._range_gate:
                    async with client.stream("GET", source_url, headers=headers) as resp:
                        if resp.status_code != 206:
                            raise RuntimeError(
                                f"Source did not honor Range for {source_key}: "
                                f"part={part_number} range={start}-{end} status={resp.status_code}"
                            )
                        resp.raise_for_status()
                        buf = bytearray()
                        async for chunk in resp.aiter_bytes(chunk_size=_SOURCE_STREAM_CHUNK_SIZE):
                            buf.extend(chunk)
                            if len(buf) > length:
                                raise ValueError(
                                    f"Source returned too many bytes for {source_key}: "
                                    f"part={part_number} expected={length} actual>{len(buf)}"
                                )
                    if len(buf) != length:
                        raise ValueError(
                            f"Source range size mismatch for {source_key}: "
                            f"part={part_number} expected={length} actual={len(buf)}"
                        )
                return bytes(buf)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last_exc = exc
                if attempt >= _SOURCE_RANGE_MAX_ATTEMPTS:
                    break
                logger.warning(
                    "Migration large object range retry: key=%s part=%d range=%d-%d attempt=%d/%d error=%s",
                    source_key,
                    part_number,
                    start,
                    end,
                    attempt + 1,
                    _SOURCE_RANGE_MAX_ATTEMPTS,
                    repr(exc),
                )
                await asyncio.sleep(min(2 ** (attempt - 1), 5))
        raise RuntimeError(
            f"Failed to download source range for {source_key}: part={part_number} range={start}-{end}"
        ) from last_exc

    async def _copy_small_object_to_tos(
        self,
        source_url: str,
        sink_config: dict[str, str],
        object_key: str,
        expected_size: int,
    ) -> None:
        async with httpx.AsyncClient(follow_redirects=True, timeout=_source_download_timeout()) as client:
            resp = await client.get(source_url)
            resp.raise_for_status()
            content = resp.content
        if expected_size >= 0 and len(content) != expected_size:
            raise ValueError(f"Downloaded object size mismatch: expected={expected_size} actual={len(content)}")
        await self._put_tos_object(sink_config, object_key, content)

    async def _append_object_log(self, record: dict[str, Any]) -> None:
        async with self._object_log_lock:
            await self._append_jsonl(self._log_path_for_record(self._object_log_path, record), record)

    async def _append_failed_object_log(self, record: dict[str, Any]) -> None:
        async with self._failed_object_log_lock:
            await self._append_jsonl(self._log_path_for_record(self._failed_object_log_path, record), record)

    @staticmethod
    def _safe_log_component(value: Any) -> str:
        text = str(value or "").strip() or "unknown"
        return re.sub(r"[^A-Za-z0-9_.-]+", "-", text).strip("-") or "unknown"

    @classmethod
    def _log_path_for_record(cls, base_path: Path, record: dict[str, Any]) -> Path:
        if base_path.name not in {_MIGRATION_OBJECT_LOG_PATH.name, _MIGRATION_FAILED_OBJECT_LOG_PATH.name}:
            return base_path
        owner = cls._safe_log_component(record.get("owner_email", "").split("@", 1)[0])
        source = cls._safe_log_component(record.get("source_bucket"))
        target = cls._safe_log_component(record.get("target_bucket"))
        return base_path.with_name(f"{base_path.stem}__{owner}__{source}__{target}.jsonl")

    @staticmethod
    async def _append_jsonl(path: Path, record: dict[str, Any]) -> None:
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"

        def _write() -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(line)

        await asyncio.to_thread(_write)

    @staticmethod
    def _target_log_key(source_key: str, sink_config: dict[str, str], owner_user: Any) -> str:
        object_key = derive_sink_object_key(source_key, owner_user)
        prefix = sink_config.get("prefix", "").strip("/")
        return f"{prefix}/{object_key}" if prefix else object_key

    async def _put_tos_object(self, sink_config: dict[str, str], object_key: str, content: bytes) -> None:
        def _put() -> None:
            import tos  # type: ignore[import-untyped]

            endpoint = sink_config["endpoint"]
            client = tos.TosClientV2(
                ak=sink_config["ak"],
                sk=sink_config["sk"],
                endpoint=endpoint,
                region=sink_config["region"],
            )
            prefix = sink_config.get("prefix", "").strip("/")
            full_key = f"{prefix}/{object_key}" if prefix else object_key
            client.put_object(sink_config["bucket"], full_key, content=content)

        await asyncio.to_thread(_put)

    def _owner_user(self, job: MigrationJob) -> Any:
        if not job.owner_email:
            return None
        local = job.owner_email.split("@", 1)[0]
        return SimpleNamespace(id=job.owner_id, email=job.owner_email, username=local, role="user")


def create_migrations_router() -> APIRouter:
    router = APIRouter(prefix="/api/migrations", tags=["Migrations"])

    def _get_manager(request: Request) -> Any:
        mgr = getattr(request.app.state, "task_manager", None)
        if mgr is None:
            raise HTTPException(503, "Task manager not initialized")
        return mgr

    def _get_controller(request: Request) -> BucketMigrationController:
        controller = getattr(request.app.state, "migration_controller", None)
        if controller is None:
            raise HTTPException(503, "Migration controller not initialized")
        return controller

    @router.post("/bucket", status_code=201, response_model=CreateBucketMigrationResponse)
    async def create_bucket_migration(body: CreateBucketMigrationRequest, request: Request) -> CreateBucketMigrationResponse:
        """Expand a source bucket into existing stream-to-sink transfer tasks."""

        mgr = _get_manager(request)
        user = getattr(request.state, "user", None)
        owner_id = getattr(user, "id", None) if user else None

        try:
            client = _build_s3_client(body.source)
            sink_config = _target_sink_config(body.target) if body.target else {}
        except (RuntimeError, ValueError) as exc:
            raise HTTPException(400, str(exc))
        if not body.dry_run and body.target is None:
            raise HTTPException(400, "target is required unless dry_run is true")

        try:
            objects, truncated, next_marker = await asyncio.to_thread(
                _list_source_objects,
                client,
                body.source,
                body.max_objects,
                body.marker,
            )
        except Exception as exc:
            logger.exception("Failed to list source bucket %s", body.source.bucket)
            raise HTTPException(400, f"Failed to list source bucket: {exc}")

        tasks: list[MigrationTaskItem] = []
        errors: list[MigrationObjectError] = []
        total_size = sum(int(obj.get("size") or 0) for obj in objects)

        if body.dry_run:
            return CreateBucketMigrationResponse(
                dry_run=True,
                source_bucket=body.source.bucket,
                target_bucket=body.target.bucket if body.target else "",
                source_prefix=body.source.prefix or "",
                total_objects=len(objects),
                created_tasks=0,
                total_size=total_size,
                truncated=truncated,
                next_marker=next_marker,
                tasks=[
                    MigrationTaskItem(task_id="", key=str(obj["key"]), size=int(obj.get("size") or 0), filename=str(obj["key"]))
                    for obj in objects
                ],
                errors=[],
            )

        for obj in objects:
            key = str(obj["key"])
            size = int(obj.get("size") or 0)
            try:
                presigned_url = await asyncio.to_thread(
                    _presign_get_object,
                    client,
                    body.source.bucket,
                    key,
                    body.presign_expires_seconds,
                )
                task = await mgr.create_task(
                    source_url=presigned_url,
                    sink_plugin="tos",
                    sink_config=sink_config,
                    sink_preset="",
                    retention="download_once",
                    retention_ttl=0,
                    owner_id=owner_id,
                    user=user,
                    filename=key,
                )
                tasks.append(
                    MigrationTaskItem(
                        task_id=task.task_id,
                        key=key,
                        size=size,
                        filename=task.filename,
                    )
                )
            except Exception as exc:
                logger.warning("Failed to create migration task for %s: %s", key, exc)
                errors.append(MigrationObjectError(key=key, error=str(exc)))

        return CreateBucketMigrationResponse(
            dry_run=False,
            source_bucket=body.source.bucket,
            target_bucket=body.target.bucket if body.target else "",
            source_prefix=body.source.prefix or "",
            total_objects=len(objects),
            created_tasks=len(tasks),
            total_size=total_size,
            truncated=truncated,
            next_marker=next_marker,
            tasks=tasks,
            errors=errors,
        )

    @router.post("/jobs", status_code=201, response_model=MigrationJobResponse)
    async def create_migration_job(body: CreateMigrationJobRequest, request: Request) -> MigrationJobResponse:
        controller = _get_controller(request)
        user = getattr(request.state, "user", None)
        try:
            job = await controller.create_job(body, user=user)
        except ValueError as exc:
            raise HTTPException(400, str(exc))
        return _job_response(job)

    @router.get("/jobs", response_model=list[MigrationJobResponse])
    async def list_migration_jobs(request: Request) -> list[MigrationJobResponse]:
        controller = _get_controller(request)
        return [_job_response(job) for job in await controller.list_jobs()]

    @router.get("/jobs/{job_id}", response_model=MigrationJobResponse)
    async def get_migration_job(job_id: str, request: Request) -> MigrationJobResponse:
        controller = _get_controller(request)
        job = await controller.get_job(job_id)
        if job is None:
            raise HTTPException(404, "Migration job not found")
        return _job_response(job)

    @router.post("/jobs/{job_id}/pause", response_model=MigrationJobResponse)
    async def pause_migration_job(job_id: str, request: Request) -> MigrationJobResponse:
        controller = _get_controller(request)
        job = await controller.pause_job(job_id)
        if job is None:
            raise HTTPException(404, "Migration job not found")
        return _job_response(job)

    @router.post("/jobs/{job_id}/resume", response_model=MigrationJobResponse)
    async def resume_migration_job(job_id: str, request: Request) -> MigrationJobResponse:
        controller = _get_controller(request)
        job = await controller.resume_job(job_id)
        if job is None:
            raise HTTPException(404, "Migration job not found")
        return _job_response(job)

    @router.post("/jobs/{job_id}/cancel", response_model=MigrationJobResponse)
    async def cancel_migration_job(job_id: str, request: Request) -> MigrationJobResponse:
        controller = _get_controller(request)
        job = await controller.cancel_job(job_id)
        if job is None:
            raise HTTPException(404, "Migration job not found")
        return _job_response(job)

    return router
