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

    sink_plugin: Optional[str] = Field(None, description="Sink plugin name (None = local-only)")
    sink_config: dict = Field(default_factory=dict, description="Resolved sink configuration")

    status: TaskStatus = Field(TaskStatus.PENDING, description="Current task status")
    progress: float = Field(0.0, description="Overall progress 0.0 – 1.0")
    downloaded_bytes: int = Field(0, description="Bytes downloaded so far")
    pushed_parts: int = Field(0, description="Parts pushed to sink so far")
    error: Optional[str] = Field(None, description="Error message on failure")

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = Field(None)

    filename: Optional[str] = Field(None, description="Resolved filename")
    file_size: Optional[int] = Field(None, description="Total file size")
    file_id: Optional[str] = Field(None, description="Registered file_id in TusStorage")

    retention: str = Field("permanent", description="permanent / download_once / ttl")
    retention_ttl: Optional[int] = Field(None, description="TTL seconds (retention=ttl)")

    owner_id: Optional[int] = Field(None, description="Owning user ID")

    sink_session_id: Optional[str] = Field(None, description="Sink multipart session ID")
    sink_result_url: Optional[str] = Field(None, description="Final URL after sink push")


class CreateTaskRequest(BaseModel):
    """REST request body for creating a transfer task."""

    source_url: str = Field(..., description="URL to download")
    sink_plugin: Optional[str] = Field(None, description="Sink plugin name")
    sink_config: Optional[dict] = Field(None, description="Explicit sink config (overrides presets)")
    retention: str = Field("permanent", description="permanent / download_once / ttl")
    retention_ttl: Optional[int] = Field(None, description="TTL in seconds")


class TaskResponse(BaseModel):
    """REST response for a single task."""

    task_id: str
    source_url: str
    source_plugin: str
    sink_plugin: Optional[str]
    status: TaskStatus
    progress: float
    downloaded_bytes: int
    pushed_parts: int
    error: Optional[str]
    filename: Optional[str]
    file_size: Optional[int]
    file_id: Optional[str]
    retention: str
    retention_ttl: Optional[int]
    sink_result_url: Optional[str]
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime]
