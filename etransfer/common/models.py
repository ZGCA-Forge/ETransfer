"""Pydantic models for EasyTransfer."""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class UploadStatus(str, Enum):
    """Upload status enum."""

    CREATED = "created"
    UPLOADING = "uploading"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class FileStatus(str, Enum):
    """File availability status."""

    PARTIAL = "partial"
    COMPLETE = "complete"


class FileInfo(BaseModel):
    """Information about a file."""

    file_id: str = Field(..., description="Unique file identifier")
    filename: str = Field(..., description="Original filename")
    size: int = Field(..., description="Total file size in bytes")
    mime_type: str = Field("application/octet-stream", description="MIME type")
    checksum: str = Field("", description="File checksum (SHA256)")
    status: FileStatus = Field(FileStatus.PARTIAL, description="File status")
    uploaded_size: int = Field(0, description="Bytes uploaded so far")
    chunk_size: int = Field(0, description="Chunk size used (0 = non-chunked)")
    total_chunks: int = Field(0, description="Total number of chunks (0 = non-chunked)")
    uploaded_chunks: int = Field(0, description="Number of chunks uploaded")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = Field(None, description="Upload expiration time")
    metadata: dict = Field(default_factory=dict, description="Additional metadata")

    @property
    def progress(self) -> float:
        """Calculate upload progress percentage."""
        if self.size == 0:
            return 100.0
        return (self.uploaded_size / self.size) * 100

    @property
    def is_complete(self) -> bool:
        """Check if upload is complete."""
        return self.uploaded_size >= self.size


class EndpointInfo(BaseModel):
    """Traffic information for a server endpoint (host:port)."""

    endpoint: str = Field(..., description="Endpoint address (host:port)")
    url: str = Field("", description="Full HTTP URL for client access")
    upload_rate: float = Field(0.0, description="Current upload rate (bytes/sec)")
    download_rate: float = Field(0.0, description="Current download rate (bytes/sec)")
    bytes_sent: int = Field(0, description="Total bytes sent")
    bytes_recv: int = Field(0, description="Total bytes received")


class ServerInfo(BaseModel):
    """Server information response."""

    version: str = Field(..., description="Server version")
    tus_version: str = Field(..., description="TUS protocol version")
    tus_extensions: list[str] = Field(..., description="Supported TUS extensions")
    max_upload_size: Optional[int] = Field(None, description="Max upload size (None = unlimited)")
    chunk_size: int = Field(..., description="Default chunk size")
    endpoints: list[EndpointInfo] = Field(default_factory=list, description="Server endpoints")
    total_files: int = Field(0, description="Total files on server")
    total_size: int = Field(0, description="Total storage used")
    retention_policies: list[str] = Field(
        default_factory=lambda: ["download_once", "ttl", "permanent"],
        description="Retention policies available to ordinary clients. "
        "'permanent' is omitted when the server disables it.",
    )
    default_retention: str = Field(
        "download_once",
        description="Default retention applied when the client does not specify one.",
    )


class FileListResponse(BaseModel):
    """Response for file listing."""

    files: list[FileInfo] = Field(default_factory=list)
    total: int = Field(0)
    page: int = Field(1)
    page_size: int = Field(20)


class DownloadInfo(BaseModel):
    """Information for downloading a file."""

    file_id: str
    filename: str
    size: int
    available_size: int = Field(..., description="Bytes available for download")
    is_upload_complete: bool = Field(False, description="Whether the upload has finished")
    mime_type: str = Field("application/octet-stream")
    checksum: str = Field("")
    supports_range: bool = True
    chunked_storage: bool = Field(False, description="True = chunk-based download mode")
    chunk_size: int = Field(0, description="Chunk size (0 = non-chunked)")
    total_chunks: int = Field(0, description="Total chunks (0 = non-chunked)")
    available_chunks: list[int] = Field(default_factory=list, description="Available chunk indices")
    chunks_consumed: int = Field(0, description="Chunks already downloaded+deleted (download_once)")
    upload_active: bool = Field(True, description="Whether the upload is actively receiving data")


class ErrorResponse(BaseModel):
    """Error response model."""

    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Error message")
    details: dict = Field(default_factory=dict, description="Additional error details")


class AuthVerifyRequest(BaseModel):
    """Request to verify authentication token."""

    token: str = Field(..., description="API token to verify")


class AuthVerifyResponse(BaseModel):
    """Response for token verification."""

    valid: bool = Field(..., description="Whether token is valid")
    expires_at: Optional[datetime] = Field(None, description="Token expiration")
