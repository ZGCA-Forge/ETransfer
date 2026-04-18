"""TUS protocol specific models."""

import base64
from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class RetentionPolicy(str, Enum):
    """File retention policy after upload completes."""

    PERMANENT = "permanent"  # Keep forever (until manual delete)
    DOWNLOAD_ONCE = "download_once"  # Delete after first complete download (阅后即焚)
    TTL = "ttl"  # Delete after TTL expires (定时过期)


class TusMetadata(BaseModel):
    """TUS Upload Metadata parsed from Upload-Metadata header."""

    filename: str = Field(..., description="Original filename")
    filetype: str = Field("application/octet-stream", description="MIME type")
    checksum: str = Field("", description="File checksum")
    retention: str = Field(
        "", description="Retention policy (empty = defer to server default): permanent/download_once/ttl"
    )
    retention_ttl: int = Field(0, description="TTL in seconds (0 = not applicable)")
    relative_path: str = Field("", description="Relative path for folder uploads")
    folder_id: str = Field("", description="Folder group ID for folder uploads")
    folder_name: str = Field("", description="Original folder name")
    sink: str = Field("", description="Sink plugin name for upload forwarding")
    sink_config: str = Field("", description="Base64-encoded JSON sink config")
    sink_preset: str = Field("", description="Named server-side preset under sinks.presets.<sink>.<name>")

    @classmethod
    def from_header(cls, header_value: str) -> "TusMetadata":
        """Parse Upload-Metadata header value.

        Format: key1 base64value1,key2 base64value2,...
        """
        metadata = {}
        if not header_value:
            raise ValueError("Empty metadata header")

        for item in header_value.split(","):
            item = item.strip()
            if not item:
                continue

            parts = item.split(" ", 1)
            key = parts[0].strip()

            if len(parts) == 2:
                try:
                    value = base64.b64decode(parts[1].strip()).decode("utf-8")
                except Exception:
                    value = parts[1].strip()
            else:
                value = ""

            metadata[key] = value

        if "filename" not in metadata:
            raise ValueError("filename is required in metadata")

        return cls(
            filename=metadata.get("filename", ""),
            filetype=metadata.get("filetype", "application/octet-stream"),
            checksum=metadata.get("checksum", ""),
            retention=metadata.get("retention", ""),
            retention_ttl=(int(metadata["retention_ttl"]) if metadata.get("retention_ttl") else 0),
            relative_path=metadata.get("relativePath") or metadata.get("relative_path", ""),
            folder_id=metadata.get("folderId") or metadata.get("folder_id", ""),
            folder_name=metadata.get("folderName") or metadata.get("folder_name", ""),
            sink=metadata.get("sink", ""),
            sink_config=metadata.get("sink_config", ""),
            sink_preset=metadata.get("sink_preset", ""),
        )

    def to_header(self) -> str:
        """Convert to Upload-Metadata header value."""
        items = []
        items.append(f"filename {base64.b64encode(self.filename.encode()).decode()}")

        if self.filetype:
            items.append(f"filetype {base64.b64encode(self.filetype.encode()).decode()}")

        if self.checksum:
            items.append(f"checksum {base64.b64encode(self.checksum.encode()).decode()}")

        if self.retention:
            items.append(f"retention {base64.b64encode(self.retention.encode()).decode()}")

        if self.retention_ttl:
            items.append(f"retention_ttl {base64.b64encode(str(self.retention_ttl).encode()).decode()}")

        if self.relative_path:
            items.append(f"relativePath {base64.b64encode(self.relative_path.encode()).decode()}")

        if self.folder_id:
            items.append(f"folderId {base64.b64encode(self.folder_id.encode()).decode()}")

        if self.folder_name:
            items.append(f"folderName {base64.b64encode(self.folder_name.encode()).decode()}")

        if self.sink:
            items.append(f"sink {base64.b64encode(self.sink.encode()).decode()}")

        if self.sink_config:
            items.append(f"sink_config {base64.b64encode(self.sink_config.encode()).decode()}")

        if self.sink_preset:
            items.append(f"sink_preset {base64.b64encode(self.sink_preset.encode()).decode()}")

        return ",".join(items)


class TusUpload(BaseModel):
    """TUS Upload state stored in Redis."""

    file_id: str = Field(..., description="Unique upload identifier")
    filename: str = Field(..., description="Original filename")
    size: int = Field(..., description="Total file size")
    offset: int = Field(0, description="Current upload offset")
    metadata: dict = Field(default_factory=dict, description="Upload metadata")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = Field(None, description="Upload expiration time")
    is_final: bool = Field(False, description="Whether upload is complete")
    storage_path: str = Field(..., description="Path to stored file")
    checksum: str = Field("", description="File checksum")
    mime_type: str = Field("application/octet-stream", description="MIME type")

    # Retention policy
    retention: str = Field("download_once", description="Retention: permanent/download_once/ttl")
    retention_ttl: int = Field(0, description="TTL in seconds (0 = not applicable)")
    retention_expires_at: Optional[datetime] = Field(
        None, description="When the file should be deleted (set after upload completes)"
    )
    download_count: int = Field(0, description="Number of completed downloads")

    # Owner tracking (for user-level quota)
    owner_id: Optional[int] = Field(None, description="Owning user ID (None = anonymous)")

    # Parallel upload: track received byte ranges
    received_ranges: list[list[int]] = Field(
        default_factory=list,
        description="Sorted list of [start, end) byte ranges received",
    )

    # Chunk-based storage (for streaming relay / download_once)
    chunked_storage: bool = Field(False, description="True = store as individual chunk files")
    chunk_size: int = Field(
        32 * 1024 * 1024,  # DEFAULT_CHUNK_SIZE
        description="Chunk size in bytes (must match upload/download boundary)",
    )

    # Finalization fields (populated when is_final = True)
    available_size: int = Field(0, description="Bytes available for download")
    completed_at: str = Field("", description="ISO timestamp when upload completed")
    total_chunks: int = Field(0, description="Total number of chunks (0 = non-chunked)")

    # download_once consumption tracking
    chunks_consumed: int = Field(0, description="Number of chunks downloaded+deleted (download_once)")

    # Sink forwarding state
    sink_plugin: str = Field("", description="Sink plugin name for forwarding")
    sink_session_id: str = Field("", description="Sink multipart session ID")
    sink_parts: list[dict] = Field(default_factory=list, description="Completed sink part results")
    sink_part_size: int = Field(0, description="Coalesced sink part size in bytes (0 = 1:1 with TUS chunk)")
    sink_flush_cursor: int = Field(0, description="Bytes already flushed to sink")
    sink_parts_pushed: int = Field(0, description="Number of sink parts successfully pushed")

    # Folder upload
    relative_path: str = Field("", description="Relative path for folder uploads")
    folder_id: str = Field("", description="Folder group ID")
    folder_name: str = Field("", description="Original folder name")

    def _merge_range(self, start: int, end: int) -> None:
        """Merge a new [start, end) range into received_ranges (sorted, coalesced)."""
        new = [start, end]
        merged: list[list[int]] = []
        inserted = False
        for r in self.received_ranges:
            if r[1] < new[0]:
                merged.append(r)
            elif new[1] < r[0]:
                if not inserted:
                    merged.append(new)
                    inserted = True
                merged.append(r)
            else:
                new = [min(new[0], r[0]), max(new[1], r[1])]
        if not inserted:
            merged.append(new)
        self.received_ranges = merged
        if self.received_ranges and self.received_ranges[0][0] == 0:
            self.offset = self.received_ranges[0][1]

    @property
    def received_bytes(self) -> int:
        """Total bytes received across all ranges."""
        return sum(r[1] - r[0] for r in self.received_ranges)

    @property
    def is_complete(self) -> bool:
        """Check if upload is complete (all bytes received)."""
        if not self.received_ranges:
            return self.offset >= self.size
        return (
            len(self.received_ranges) == 1
            and self.received_ranges[0][0] == 0
            and self.received_ranges[0][1] >= self.size
        )

    @property
    def remaining(self) -> int:
        """Get remaining bytes to upload."""
        return max(0, self.size - self.offset)

    def to_redis_dict(self) -> dict:
        """Convert to dict for Redis storage."""
        data = self.model_dump()
        data["created_at"] = self.created_at.isoformat()
        data["updated_at"] = self.updated_at.isoformat()
        if self.expires_at:
            data["expires_at"] = self.expires_at.isoformat()
        if self.retention_expires_at:
            data["retention_expires_at"] = self.retention_expires_at.isoformat()
        data["received_bytes"] = self.received_bytes
        return data

    @classmethod
    def from_redis_dict(cls, data: dict) -> "TusUpload":
        """Create from Redis dict."""
        data.pop("received_bytes", None)
        for dt_field in ("created_at", "updated_at", "expires_at", "retention_expires_at"):
            if data.get(dt_field) and isinstance(data[dt_field], str):
                data[dt_field] = datetime.fromisoformat(data[dt_field])
        _str_fields = (
            "checksum",
            "mime_type",
            "completed_at",
            "sink_plugin",
            "sink_session_id",
            "relative_path",
            "folder_id",
            "folder_name",
        )
        for f in _str_fields:
            if f in data and data[f] is None:
                data[f] = ""
        if "retention_ttl" in data and data["retention_ttl"] is None:
            data["retention_ttl"] = 0
        return cls(**data)


class TusCapabilities(BaseModel):
    """TUS server capabilities."""

    version: str = Field("1.0.0", description="TUS protocol version")
    extensions: list[str] = Field(
        default_factory=lambda: [
            "creation",
            "creation-with-upload",
            "termination",
            "checksum",
            "expiration",
        ]
    )
    max_size: int = Field(0, description="Maximum upload size (0 = no limit)")
    checksum_algorithms: list[str] = Field(default_factory=lambda: ["sha1", "sha256", "md5"])


class TusError(Exception):
    """TUS protocol error."""

    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        self.message = message
        super().__init__(message)


class TusErrors:
    """Common TUS errors."""

    @staticmethod
    def invalid_version() -> TusError:
        return TusError(412, "Precondition Failed: Invalid Tus-Resumable header")

    @staticmethod
    def upload_not_found() -> TusError:
        return TusError(404, "Upload not found")

    @staticmethod
    def invalid_offset() -> TusError:
        return TusError(409, "Conflict: Upload offset mismatch")

    @staticmethod
    def invalid_content_type() -> TusError:
        return TusError(415, "Unsupported Media Type")

    @staticmethod
    def upload_too_large() -> TusError:
        return TusError(413, "Request Entity Too Large")

    @staticmethod
    def upload_expired() -> TusError:
        return TusError(410, "Gone: Upload has expired")

    @staticmethod
    def checksum_mismatch() -> TusError:
        return TusError(460, "Checksum Mismatch")

    @staticmethod
    def missing_header(header: str) -> TusError:
        return TusError(400, f"Bad Request: Missing required header {header}")
