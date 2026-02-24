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
    mime_type: Optional[str] = Field(None, description="MIME type")
    checksum: Optional[str] = Field(None, description="File checksum (SHA256)")
    status: FileStatus = Field(FileStatus.PARTIAL, description="File status")
    uploaded_size: int = Field(0, description="Bytes uploaded so far")
    chunk_size: int = Field(..., description="Chunk size used")
    total_chunks: int = Field(..., description="Total number of chunks")
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


class NetworkInterface(BaseModel):
    """Information about a network interface."""

    name: str = Field(..., description="Interface name (e.g., eth0)")
    ip_address: str = Field(..., description="IP address")
    is_up: bool = Field(True, description="Whether interface is up")
    speed_mbps: Optional[int] = Field(None, description="Link speed in Mbps")
    bytes_sent: int = Field(0, description="Total bytes sent")
    bytes_recv: int = Field(0, description="Total bytes received")
    upload_rate: float = Field(0.0, description="Current upload rate (bytes/sec)")
    download_rate: float = Field(0.0, description="Current download rate (bytes/sec)")
    upload_load_percent: float = Field(0.0, description="Upload load as % of speed")
    download_load_percent: float = Field(0.0, description="Download load as % of speed")
    total_load_percent: float = Field(0.0, description="Total load as % of speed")


class ServerInfo(BaseModel):
    """Server information response."""

    version: str = Field(..., description="Server version")
    tus_version: str = Field(..., description="TUS protocol version")
    tus_extensions: list[str] = Field(..., description="Supported TUS extensions")
    max_upload_size: Optional[int] = Field(None, description="Max upload size")
    chunk_size: int = Field(..., description="Default chunk size")
    interfaces: list[NetworkInterface] = Field(default_factory=list, description="Network interfaces")
    total_files: int = Field(0, description="Total files on server")
    total_size: int = Field(0, description="Total storage used")


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
    mime_type: Optional[str] = None
    checksum: Optional[str] = None
    supports_range: bool = True
    # Chunk-based streaming fields
    chunked_storage: bool = Field(False, description="True = chunk-based download mode")
    chunk_size: Optional[int] = Field(None, description="Chunk size (when chunked_storage=True)")
    total_chunks: Optional[int] = Field(None, description="Total chunks (when chunked_storage=True)")
    available_chunks: Optional[list[int]] = Field(None, description="Available chunk indices")


class ErrorResponse(BaseModel):
    """Error response model."""

    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Error message")
    details: Optional[dict] = Field(None, description="Additional error details")


class AuthVerifyRequest(BaseModel):
    """Request to verify authentication token."""

    token: str = Field(..., description="API token to verify")


class AuthVerifyResponse(BaseModel):
    """Response for token verification."""

    valid: bool = Field(..., description="Whether token is valid")
    expires_at: Optional[datetime] = Field(None, description="Token expiration")
