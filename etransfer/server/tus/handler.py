"""TUS protocol handler for FastAPI."""

import asyncio
import hashlib
import uuid
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, Response

from etransfer.common.constants import (
    AUTH_HEADER,
    CONTENT_TYPE_OFFSET,
    DEFAULT_CHUNK_SIZE,
    TUS_VERSION,
    UPLOAD_EXPIRATION_SECONDS,
    RedisKeys,
    TusHeaders,
)
from etransfer.server.auth.models import RoleQuota
from etransfer.server.tus.models import TusCapabilities, TusErrors, TusMetadata, TusUpload
from etransfer.server.tus.quota import QuotaService
from etransfer.server.tus.storage import TusStorage


def create_tus_router(
    storage: TusStorage,
    max_size: Optional[int] = None,
) -> APIRouter:
    """Create a TUS protocol router.

    Retention config (default_retention, default_retention_ttl,
    token_retention_policies) is read from ``request.app.state.settings``
    at request time so that hot-reloaded config takes effect immediately.

    Args:
        storage: TUS storage backend
        max_size: Maximum upload size (None = unlimited)

    Returns:
        FastAPI router with TUS endpoints
    """
    router = APIRouter(tags=["TUS"])
    capabilities = TusCapabilities(max_size=max_size)  # type: ignore[call-arg]
    quota_svc = QuotaService(storage)

    def get_tus_headers() -> dict:
        """Get common TUS response headers."""
        return {
            TusHeaders.TUS_RESUMABLE: TUS_VERSION,
            TusHeaders.TUS_VERSION: TUS_VERSION,
        }

    def validate_tus_version(request: Request) -> None:
        """Validate Tus-Resumable header."""
        tus_version = request.headers.get(TusHeaders.TUS_RESUMABLE)
        if tus_version and tus_version != TUS_VERSION:
            raise TusErrors.invalid_version()

    @router.options("/tus")
    @router.options("/tus/{file_id}")
    async def tus_options(request: Request) -> Response:
        """Handle OPTIONS request - return server capabilities."""
        headers = get_tus_headers()
        headers[TusHeaders.TUS_EXTENSION] = ",".join(capabilities.extensions)
        if capabilities.max_size:
            headers[TusHeaders.TUS_MAX_SIZE] = str(capabilities.max_size)

        return Response(
            status_code=204,
            headers=headers,
        )

    @router.post("/tus")
    async def tus_create(request: Request) -> Response:
        """Handle POST request - create new upload."""
        validate_tus_version(request)

        # Parse Upload-Length header
        upload_length_str = request.headers.get(TusHeaders.UPLOAD_LENGTH)
        if not upload_length_str:
            raise HTTPException(400, "Missing Upload-Length header")

        try:
            upload_length = int(upload_length_str)
        except ValueError:
            raise HTTPException(400, "Invalid Upload-Length header")

        # Check max single file size
        if max_size and upload_length > max_size:
            raise HTTPException(413, "Upload exceeds maximum size")

        # Check user-level quota at CREATE time and reserve space in Redis.
        # This avoids per-chunk DB queries during PATCH.
        owner_id = getattr(getattr(request.state, "user", None), "id", None)
        user_db = getattr(request.app.state, "user_db", None)
        if user_db and owner_id:
            user = await user_db.get_user(owner_id)
            if user:
                role_quotas = getattr(request.app.state, "parsed_role_quotas", {})
                effective = await user_db.get_effective_quota(user, role_quotas)

                # Check max single upload size
                if effective.max_upload_size and upload_length > effective.max_upload_size:
                    raise HTTPException(
                        413,
                        f"File exceeds your upload limit ({effective.max_upload_size} bytes)",
                    )

                # Check total storage: DB committed + Redis reserved
                if effective.max_storage_size:
                    reserved = await quota_svc.get_reserved(owner_id)
                    total_used = user.storage_used + reserved
                    if total_used + upload_length > effective.max_storage_size:
                        raise HTTPException(
                            507,
                            f"Storage quota exceeded (used={user.storage_used}, "
                            f"reserved={reserved}, limit={effective.max_storage_size})",
                        )

                # Reserve quota in Redis (atomic)
                await quota_svc.reserve(owner_id, upload_length)

        # Parse metadata
        metadata_header = request.headers.get(TusHeaders.UPLOAD_METADATA, "")
        try:
            if metadata_header:
                tus_metadata = TusMetadata.from_header(metadata_header)
            else:
                tus_metadata = TusMetadata(filename=f"upload_{uuid.uuid4().hex[:8]}")  # type: ignore[call-arg]
        except ValueError as e:
            raise HTTPException(400, f"Invalid Upload-Metadata: {e}")

        # Generate file ID
        file_id = uuid.uuid4().hex

        # Calculate expiration
        expires_at = datetime.utcnow() + timedelta(seconds=UPLOAD_EXPIRATION_SECONDS)

        # Determine retention policy: client metadata > token policy > server default
        retention = tus_metadata.retention
        retention_ttl = tus_metadata.retention_ttl

        # If client didn't specify, check token-level policy.
        # Read retention config from app.state.settings (hot-reloadable).
        if not retention:
            _settings = getattr(request.app.state, "settings", None)
            _def_ret = getattr(_settings, "default_retention", "permanent")
            _def_ttl = getattr(_settings, "default_retention_ttl", None)
            _tok_pol = getattr(_settings, "token_retention_policies", None) or {}

            token = request.headers.get(AUTH_HEADER, "")
            if token and token in _tok_pol:
                tp = _tok_pol[token]
                retention = tp.get("default_retention", _def_ret)
                if retention_ttl is None:
                    retention_ttl = tp.get("default_ttl", _def_ttl)
            else:
                retention = _def_ret
                if retention_ttl is None:
                    retention_ttl = _def_ttl

        if retention not in ("permanent", "download_once", "ttl"):
            retention = "permanent"

        # Chunk-based storage for download_once (streaming relay)
        use_chunked = retention == "download_once"
        chunk_size = DEFAULT_CHUNK_SIZE

        # Create upload record
        upload = TusUpload(  # type: ignore[call-arg]
            file_id=file_id,
            filename=tus_metadata.filename,
            size=upload_length,
            offset=0,
            metadata={
                "filetype": tus_metadata.filetype,
                "checksum": tus_metadata.checksum,
                "retention": retention,
                "retention_ttl": retention_ttl,
            },
            expires_at=expires_at,
            storage_path=(str(storage.get_chunk_dir(file_id)) if use_chunked else str(storage.get_file_path(file_id))),
            mime_type=tus_metadata.filetype,
            checksum=tus_metadata.checksum,
            retention=retention,
            retention_ttl=retention_ttl,
            owner_id=owner_id,
            chunked_storage=use_chunked,
            chunk_size=chunk_size,
        )

        # Save to storage
        await storage.create_upload(upload)

        # Build location URL
        location = str(request.url).rstrip("/") + f"/{file_id}"

        # Check for creation-with-upload extension
        body = await request.body()
        if body and "creation-with-upload" in capabilities.extensions:
            content_type = request.headers.get(TusHeaders.CONTENT_TYPE)
            if content_type == CONTENT_TYPE_OFFSET:
                if use_chunked:
                    # Write as chunk file
                    chunk_index = 0
                    await storage.write_chunk_file(file_id, chunk_index, body)
                    await storage.mark_chunk_available(file_id, chunk_index)
                else:
                    await storage.write_chunk(file_id, body, 0)
                upload.offset = len(body)
                upload.updated_at = datetime.utcnow()
                await storage.update_upload(upload)

        headers = get_tus_headers()
        headers[TusHeaders.LOCATION] = location
        headers[TusHeaders.UPLOAD_OFFSET] = str(upload.offset)
        if use_chunked:
            headers["X-Chunk-Size"] = str(chunk_size)
        if upload.expires_at:
            headers[TusHeaders.UPLOAD_EXPIRES] = upload.expires_at.isoformat()

        return Response(
            status_code=201,
            headers=headers,
        )

    @router.head("/tus/{file_id}")
    async def tus_head(file_id: str, request: Request) -> Response:
        """Handle HEAD request - get upload offset."""
        validate_tus_version(request)

        upload = await storage.get_upload(file_id)
        if not upload:
            raise HTTPException(404, "Upload not found")

        # Check expiration
        if upload.expires_at and upload.expires_at < datetime.utcnow():
            await storage.delete_upload(file_id)
            raise HTTPException(410, "Upload has expired")

        headers = get_tus_headers()
        headers[TusHeaders.UPLOAD_OFFSET] = str(upload.offset)
        headers[TusHeaders.UPLOAD_LENGTH] = str(upload.size)
        if upload.chunked_storage:
            headers["X-Chunk-Size"] = str(upload.chunk_size)
            available = await storage.get_available_chunks(file_id)
            headers["X-Available-Chunks"] = str(len(available))
        if upload.expires_at:
            headers[TusHeaders.UPLOAD_EXPIRES] = upload.expires_at.isoformat()

        # Add cache control to prevent caching
        headers["Cache-Control"] = "no-store"

        return Response(
            status_code=200,
            headers=headers,
        )

    @router.patch("/tus/{file_id}")
    async def tus_patch(file_id: str, request: Request) -> Response:
        """Handle PATCH request - upload chunk.

        Optimised hot path:
        1. Validate headers (no I/O)
        2. Stream body directly to disk via pwrite (overlaps network + disk)
        3. Merge received range under local asyncio lock (2 Redis calls)
        4. Return 204 immediately
        """
        validate_tus_version(request)

        # ── Header validation (pure CPU, no I/O) ─────────────
        content_type = request.headers.get(TusHeaders.CONTENT_TYPE)
        if content_type != CONTENT_TYPE_OFFSET:
            raise HTTPException(415, f"Unsupported Content-Type: {content_type}")

        offset_str = request.headers.get(TusHeaders.UPLOAD_OFFSET)
        if not offset_str:
            raise HTTPException(400, "Missing Upload-Offset header")
        try:
            offset = int(offset_str)
        except ValueError:
            raise HTTPException(400, "Invalid Upload-Offset header")

        content_length_str = request.headers.get("content-length", "")
        content_length = int(content_length_str) if content_length_str else 0
        if content_length <= 0:
            raise HTTPException(400, "Missing or zero Content-Length")

        # Fast existence + size check (1 Redis GET, no JSON)
        upload_size = await storage.get_upload_size(file_id)
        if upload_size is None:
            raise HTTPException(404, "Upload not found")

        if offset + content_length > upload_size:
            raise HTTPException(400, "Chunk exceeds upload size")

        # ── Global quota check (per-PATCH, mirrors Kotlin model) ──
        allowed, _usage = await storage.check_quota(content_length)
        if not allowed:
            return Response(
                status_code=507,
                content='{"error":"Quota exceeded","message":"Storage quota full. Wait for space to free up and retry.","retry_after":5}',
                media_type="application/json",
                headers={**get_tus_headers(), "Retry-After": "5"},
            )

        # ── Stream body → disk (no full-body buffering) ──────
        # For chunked storage: compute chunk index and write to chunk file
        upload_record = await storage.get_upload(file_id)
        if upload_record and upload_record.chunked_storage:
            chunk_index = offset // upload_record.chunk_size
            written = await storage.write_chunk_file_streaming(
                file_id,
                chunk_index,
                request._receive,
                content_length,
            )
            await storage.mark_chunk_available(file_id, chunk_index)
        else:
            written = await storage.write_chunk_streaming(
                file_id,
                request._receive,
                offset,
                content_length,
            )

        # ── Merge range under local asyncio lock (2 Redis calls) ──
        upload, completed = await storage.merge_range_atomic(
            file_id,
            offset,
            written,
        )
        if upload is None:
            raise HTTPException(404, "Upload not found")

        # ── Finalize if complete ─────────────────────────────
        if completed:
            user_db = getattr(request.app.state, "user_db", None)
            if user_db and upload.owner_id:
                await user_db.update_storage_used(upload.owner_id, upload.size)
                await quota_svc.release(upload.owner_id, upload.size)
            await storage.finalize_upload(file_id)

        # ── Response ─────────────────────────────────────────
        headers = get_tus_headers()
        headers[TusHeaders.UPLOAD_OFFSET] = str(upload.offset)
        headers["X-Received-Bytes"] = str(upload.received_bytes)
        return Response(status_code=204, headers=headers)

    @router.delete("/tus/{file_id}")
    async def tus_delete(file_id: str, request: Request) -> Response:
        """Handle DELETE request - terminate upload."""
        validate_tus_version(request)

        upload = await storage.get_upload(file_id)
        if not upload:
            raise HTTPException(404, "Upload not found")

        user_db = getattr(request.app.state, "user_db", None)
        if user_db and upload.owner_id:
            if upload.is_final:
                # Already committed to DB — revert storage_used
                await user_db.update_storage_used(upload.owner_id, -upload.size)
            else:
                # Still in-flight — release the reservation
                await quota_svc.release(upload.owner_id, upload.size)

        await storage.delete_upload(file_id)

        return Response(
            status_code=204,
            headers=get_tus_headers(),
        )

    return router


class TusHandler:
    """TUS protocol handler wrapper for easier integration."""

    def __init__(
        self,
        storage: TusStorage,
        max_size: Optional[int] = None,
    ) -> None:
        self.storage = storage
        self.max_size = max_size
        self.router = create_tus_router(storage, max_size)

    def get_router(self) -> APIRouter:
        """Get the TUS router."""
        return self.router
