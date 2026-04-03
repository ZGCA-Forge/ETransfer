"""Base class for Sink plugins (push/forward targets)."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional

from pydantic import BaseModel, Field


class PartResult(BaseModel):
    """Result of uploading a single part to the remote sink."""

    part_number: int
    etag: str = ""
    extra: dict = Field(default_factory=dict)


@dataclass
class SinkContext:
    """Runtime context passed to ``BaseSink.resolve_config``.

    Carries all the information a sink needs to decide which bucket /
    credentials to use for the current request.
    """

    user: Any = None
    client_metadata: dict = field(default_factory=dict)
    retention: str = "permanent"
    filename: str = ""
    file_size: Optional[int] = None


class BaseSink(ABC):
    """Abstract base for push-target plugins.

    Each sink implements an S3-style multipart upload lifecycle:
    ``initialize_upload`` -> N * ``upload_part`` -> ``complete_upload``.

    Attributes:
        name: Machine-readable identifier (e.g. ``"tos"``).
        display_name: Human-readable label for the UI.
        supports_multipart: Whether this sink supports chunked upload.
    """

    name: str = ""
    display_name: str = ""
    supports_multipart: bool = True

    def __init__(self, config: Optional[dict] = None) -> None:
        self._config: dict = config or {}

    # ── Multipart lifecycle ───────────────────────────────────

    @abstractmethod
    async def initialize_upload(self, object_key: str, metadata: dict) -> str:
        """Start a multipart upload session. Returns a *session_id*."""

    @abstractmethod
    async def upload_part(self, session_id: str, part_number: int, data: bytes) -> PartResult:
        """Upload one part. Returns ``PartResult`` with at least ``etag``."""

    @abstractmethod
    async def complete_upload(self, session_id: str, parts: list[PartResult]) -> str:
        """Finalize the multipart upload. Returns the final object URL / path."""

    @abstractmethod
    async def abort_upload(self, session_id: str) -> None:
        """Cancel an in-progress multipart upload and clean up."""

    # ── Config schema / resolution ────────────────────────────

    @classmethod
    def get_config_schema(cls) -> dict:
        """JSON Schema for the configuration this sink requires.

        The frontend renders a form from this schema so users can
        supply or override settings (bucket, endpoint, credentials ...).
        """
        return {}

    @classmethod
    def resolve_config(cls, context: SinkContext, server_presets: dict) -> dict:
        """Dynamically resolve sink configuration from context.

        Priority:
          1. Client explicitly provided ``sink_config`` in metadata.
          2. User's group-level preset in *server_presets*.
          3. User's role-level preset.
          4. Global ``"default"`` preset.

        Subclasses may override for custom logic.
        """
        explicit = context.client_metadata.get("sink_config")
        if explicit and isinstance(explicit, dict):
            return explicit

        if context.user is not None:
            role = getattr(context.user, "role", "user")
            group = getattr(context.user, "group", None)
            if group and group in server_presets:
                return dict(server_presets[group])
            if role in server_presets:
                return dict(server_presets[role])

        return dict(server_presets.get("default", {}))
