"""Base class for Source plugins (download providers)."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import urlparse

from pydantic import BaseModel, Field


class RemoteFileInfo(BaseModel):
    """Metadata about a remote file before downloading."""

    filename: str = Field(..., description="Remote filename")
    size: Optional[int] = Field(None, description="File size in bytes (None if unknown)")
    mime_type: Optional[str] = Field(None, description="MIME type")
    etag: Optional[str] = Field(None, description="ETag or checksum")
    extra: dict = Field(default_factory=dict, description="Source-specific metadata")


class BaseSource(ABC):
    """Abstract base for download-source plugins.

    Subclasses declare which hosts they handle. The registry matches URLs
    by domain automatically — users never need custom scheme prefixes.

    Attributes:
        name: Machine-readable plugin identifier (e.g. ``"direct"``).
        display_name: Human-readable name shown in the UI.
        supported_hosts: Domain patterns this source handles
            (e.g. ``["huggingface.co", "hf.co"]``).
            ``DirectLinkSource`` uses an empty list and acts as the
            lowest-priority fallback.
        priority: Higher values are matched first. ``DirectLinkSource``
            uses ``-1`` so specialised plugins always win.
    """

    name: str = ""
    display_name: str = ""
    supported_hosts: list[str] = []
    priority: int = 0

    def matches_host(self, url: str) -> bool:
        """Quick check: does *url*'s host appear in ``supported_hosts``?"""
        try:
            host = urlparse(url).hostname or ""
        except Exception:
            return False
        return any(host == h or host.endswith(f".{h}") for h in self.supported_hosts)

    @abstractmethod
    async def can_handle(self, url: str) -> bool:
        """Return ``True`` if this source can download *url*.

        Called after ``matches_host`` passes. Subclasses may do deeper
        validation (path patterns, HEAD requests, etc.).
        """

    @abstractmethod
    async def get_file_info(self, url: str) -> RemoteFileInfo:
        """Fetch metadata for the remote resource without downloading it."""

    @abstractmethod
    async def download(
        self,
        url: str,
        dest: Path,
        on_progress: Optional[Callable[[int, Optional[int]], Any]] = None,
    ) -> Path:
        """Download *url* into *dest* (directory).

        Args:
            url: Remote resource URL.
            dest: Local directory to write the file into.
            on_progress: ``(downloaded_bytes, total_bytes_or_none)`` callback.

        Returns:
            Full path to the downloaded file.
        """
