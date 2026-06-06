"""Base class for Source plugins (download providers)."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import urlparse

from pydantic import BaseModel, Field


class RemoteFileInfo(BaseModel):
    """Metadata about a remote file before downloading."""

    filename: str = Field(..., description="Remote filename")
    size: int = Field(0, description="File size in bytes (0 = unknown)")
    mime_type: str = Field("application/octet-stream", description="MIME type")
    etag: str = Field("", description="ETag or checksum")
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

    def auth_headers(self, token: Optional[str] = None) -> dict[str, str]:
        """Build per-request HTTP auth headers from a source credential *token*.

        Default: no auth (public resources only). Sources that support
        token-based access (e.g. Hugging Face gated/private repos) override
        this to return e.g. ``{"Authorization": "Bearer <token>"}``.
        """
        return {}

    @abstractmethod
    async def can_handle(self, url: str) -> bool:
        """Return ``True`` if this source can download *url*.

        Called after ``matches_host`` passes. Subclasses may do deeper
        validation (path patterns, HEAD requests, etc.).
        """

    @abstractmethod
    async def get_file_info(self, url: str, *, token: Optional[str] = None) -> RemoteFileInfo:
        """Fetch metadata for the remote resource without downloading it.

        Args:
            url: Remote resource URL.
            token: Optional source credential for gated/private resources.
        """

    @abstractmethod
    async def download(
        self,
        url: str,
        dest: Path,
        on_progress: Optional[Callable[[int, Optional[int]], Any]] = None,
        *,
        token: Optional[str] = None,
    ) -> Path:
        """Download *url* into *dest* (directory).

        Args:
            url: Remote resource URL.
            dest: Local directory to write the file into.
            on_progress: ``(downloaded_bytes, total_bytes_or_none)`` callback.
            token: Optional source credential for gated/private resources.

        Returns:
            Full path to the downloaded file.
        """
