"""Transfer task data models."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class TaskStatus(str, Enum):
    PENDING = "pending"
    DOWNLOADING = "downloading"
    PUSHING = "pushing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TransferTask(BaseModel):
    """Represents a download-and-optionally-push job."""

    task_id: str = Field(..., description="Unique task identifier")
    source_url: str = Field(..., description="Remote URL to download")
    source_plugin: str = Field("", description="Matched source plugin name")

    sink_plugin: str = Field("", description="Sink plugin name (empty = local-only)")
    sink_config: dict = Field(default_factory=dict, description="Resolved sink configuration")
    sink_preset: str = Field("", description="Named server-side preset; recorded for retry/audit")

    status: TaskStatus = Field(TaskStatus.PENDING, description="Current task status")
    progress: float = Field(0.0, description="Overall progress 0.0 – 1.0")
    downloaded_bytes: int = Field(0, description="Bytes downloaded so far")
    pushed_parts: int = Field(0, description="Parts pushed to sink so far")
    error: str = Field("", description="Error message (empty = no error)")

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = Field(None)

    filename: str = Field("", description="Resolved filename")
    file_size: int = Field(0, description="Total file size (0 = unknown)")
    file_id: str = Field("", description="Registered file_id in TusStorage")

    retention: str = Field("download_once", description="download_once / permanent / ttl")
    retention_ttl: int = Field(0, description="TTL seconds (0 = not applicable)")

    owner_id: Optional[int] = Field(None, description="Owning user ID")

    speed: float = Field(0.0, description="Current overall speed in bytes/sec")
    download_speed: float = Field(0.0, description="Download speed in bytes/sec")
    push_speed: float = Field(0.0, description="Push speed in bytes/sec")
    download_progress: float = Field(0.0, description="Download phase progress 0.0-1.0")
    push_progress: float = Field(0.0, description="Push phase progress 0.0-1.0")

    sink_session_id: str = Field("", description="Sink multipart session ID")
    sink_result_url: str = Field("", description="Final URL after sink push")
    superseded_by: str = Field("", description="Task ID that replaced this one via retry")
    # Parts already uploaded to the sink (persisted so we can resume across restart).
    # Each item is {"part_number": int, "etag": str, "extra": dict}.
    uploaded_parts: list[dict] = Field(default_factory=list, description="Persisted PartResult list for resume")


class CreateTaskRequest(BaseModel):
    """REST request body for creating a transfer task."""

    source_url: str = Field(..., description="URL to download")
    filename: str = Field("", description="Override filename (empty = auto-detect)")
    sink_plugin: str = Field("", description="Sink plugin name (empty = no push)")
    sink_config: dict = Field(default_factory=dict, description="Explicit sink config (overrides presets)")
    sink_preset: str = Field("", description="Named server-side preset under sinks.presets.<sink>.<name>")
    retention: str = Field("download_once", description="download_once / permanent / ttl")
    retention_ttl: int = Field(0, description="TTL in seconds (0 = not applicable)")


class TaskResponse(BaseModel):
    """REST response for a single task."""

    task_id: str
    source_url: str
    source_plugin: str
    sink_plugin: str
    status: TaskStatus
    progress: float
    downloaded_bytes: int
    pushed_parts: int
    error: str
    filename: str
    file_size: int
    file_id: str
    retention: str
    retention_ttl: int
    speed: float
    download_speed: float
    push_speed: float
    download_progress: float
    push_progress: float
    sink_result_url: str
    superseded_by: str
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime]
