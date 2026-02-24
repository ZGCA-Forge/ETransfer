"""File management API routes."""

import asyncio
import logging
import os
from typing import Any, AsyncIterator, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse

from etransfer.common.fileutil import pread
from etransfer.common.models import DownloadInfo, ErrorResponse, FileInfo, FileListResponse, FileStatus
from etransfer.server.tus.storage import TusStorage

logger = logging.getLogger("etransfer.server.files")


def create_files_router(storage: TusStorage) -> APIRouter:
    """Create file management router.

    Args:
        storage: TUS storage backend

    Returns:
        FastAPI router
    """
    # Expand default thread pool for concurrent pread I/O
    from etransfer.server.io_pool import io_pool as _io_pool

    router = APIRouter(prefix="/api/files", tags=["Files"])

    @router.get(
        "",
        response_model=FileListResponse,
        responses={500: {"model": ErrorResponse}},
    )
    async def list_files(
        page: int = Query(1, ge=1, description="Page number"),
        page_size: int = Query(20, ge=1, le=100, description="Page size"),
        include_partial: bool = Query(True, description="Include partial uploads"),
    ) -> FileListResponse:
        """List available files.

        Returns files that are either complete or in-progress (partial).
        Partial files can still be downloaded for their uploaded portion.
        """
        try:
            # Get completed files
            files_data = await storage.list_files()

            # Get partial uploads if requested
            uploads = []
            if include_partial:
                uploads = await storage.list_uploads(include_completed=False, include_partial=True)

            # Convert to FileInfo models
            all_files = []

            for f in files_data:
                all_files.append(
                    FileInfo(  # type: ignore[call-arg]
                        file_id=f["file_id"],
                        filename=f["filename"],
                        size=f["size"],
                        uploaded_size=f["size"],
                        mime_type=f.get("mime_type"),
                        checksum=f.get("checksum"),
                        created_at=f.get("created_at", ""),
                        updated_at=f.get("updated_at", ""),
                        expires_at=f.get("expires_at"),
                        metadata={
                            "retention": f.get("retention", "permanent"),
                            "retention_ttl": f.get("retention_ttl"),
                            "retention_expires_at": f.get("retention_expires_at"),
                            "download_count": f.get("download_count", 0),
                        },
                    )
                )

            for u in uploads:
                all_files.append(
                    FileInfo(  # type: ignore[call-arg]
                        file_id=u.file_id,
                        filename=u.filename,
                        size=u.size,
                        uploaded_size=u.offset,
                        mime_type=u.mime_type,
                        checksum=u.checksum,
                        created_at=u.created_at,
                        updated_at=u.updated_at,
                        expires_at=u.expires_at,
                        metadata={
                            "retention": u.retention,
                            "retention_ttl": u.retention_ttl,
                            "retention_expires_at": (
                                u.retention_expires_at.isoformat() if u.retention_expires_at else None
                            ),
                            "download_count": u.download_count,
                        },
                    )
                )

            # Sort by created_at descending (handle None gracefully)
            all_files.sort(key=lambda f: f.created_at or "", reverse=True)

            # Paginate
            total = len(all_files)
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            page_files = all_files[start_idx:end_idx]

            return FileListResponse(
                files=page_files,
                total=total,
                page=page,
                page_size=page_size,
            )
        except Exception as e:
            logger.exception("Failed to list files")
            raise HTTPException(500, f"Failed to list files: {e}")

    @router.get(
        "/{file_id}",
        response_model=FileInfo,
        responses={404: {"model": ErrorResponse}},
    )
    async def get_file(file_id: str) -> FileInfo:
        """Get file information."""
        info = await storage.get_file_info(file_id)
        if not info:
            raise HTTPException(404, f"File not found: {file_id}")

        uploaded_size = info.get("available_size", 0)
        total_size = info["size"]
        is_complete = info.get("is_complete", uploaded_size >= total_size)

        file_metadata = {
            "retention": info.get("retention", "permanent"),
            "retention_ttl": info.get("retention_ttl"),
            "retention_expires_at": info.get("retention_expires_at"),
            "download_count": info.get("download_count", 0),
        }

        return FileInfo(  # type: ignore[call-arg]
            file_id=file_id,
            filename=info["filename"],
            size=total_size,
            uploaded_size=uploaded_size,
            mime_type=info.get("mime_type"),
            checksum=info.get("checksum"),
            status=FileStatus.COMPLETE if is_complete else FileStatus.PARTIAL,
            chunk_size=storage.chunk_size,
            total_chunks=(total_size + storage.chunk_size - 1) // storage.chunk_size,
            uploaded_chunks=(uploaded_size + storage.chunk_size - 1) // storage.chunk_size,
            metadata=file_metadata,
        )

    @router.get(
        "/{file_id}/download",
        responses={
            200: {"description": "File data"},
            206: {"description": "Partial content"},
            404: {"model": ErrorResponse},
            416: {"model": ErrorResponse},
        },
    )
    async def download_file(
        file_id: str,
        request: Request,
        background_tasks: BackgroundTasks,
        chunk: Optional[int] = Query(None, description="Chunk index for chunked downloads"),
    ) -> Response:
        """Download a file.

        For chunked files (download_once): use ?chunk=N to download by chunk index.
        For non-chunked files: standard HTTP Range support.
        """
        # Get file info
        info = await storage.get_file_info(file_id)
        if not info:
            raise HTTPException(404, f"File not found: {file_id}")

        filename = info["filename"]
        mime_type = info.get("mime_type", "application/octet-stream")
        retention = info.get("retention", "permanent")
        is_chunked = info.get("chunked_storage", False)

        # ── Chunk-based download path ──
        if is_chunked:
            if chunk is None:
                raise HTTPException(
                    400,
                    "This file uses chunked storage. Use ?chunk=N to download by chunk index.",
                )
            chunk_size = info.get("chunk_size", 32 * 1024 * 1024)
            total_chunks = info.get("total_chunks", 0)
            if chunk < 0 or (total_chunks and chunk >= total_chunks):
                raise HTTPException(416, f"Chunk index out of range: 0-{total_chunks - 1}")

            if not await storage.is_chunk_available(file_id, chunk):
                raise HTTPException(404, f"Chunk {chunk} not available yet")

            data = await storage.read_chunk_file(file_id, chunk)

            headers = {
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Content-Length": str(len(data)),
                "X-Chunk-Index": str(chunk),
                "X-Chunk-Size": str(chunk_size),
                "X-Total-Chunks": str(total_chunks),
                "X-Retention-Policy": retention,
            }

            # For download_once: delete chunk after sending
            if retention == "download_once":
                headers["X-Retention-Warning"] = "Chunk will be deleted after download"

                async def _delete_chunk() -> None:
                    await storage.delete_chunk_file(file_id, chunk)  # type: ignore[arg-type]
                    # Release per-chunk quota
                    owner_id = info.get("owner_id")
                    user_db = getattr(request.app.state, "user_db", None)
                    if user_db and owner_id:
                        await user_db.update_storage_used(owner_id, -len(data))
                    # Check if all chunks downloaded — if so, clean up metadata
                    remaining = await storage.get_available_chunks(file_id)
                    if not remaining:
                        await storage.record_download(file_id)
                        await storage.delete_upload(file_id)

                background_tasks.add_task(_delete_chunk)

            return Response(
                content=data,
                status_code=200,
                media_type=mime_type,
                headers=headers,
            )

        # ── Standard (non-chunked) download path with Range support ──
        available_size = info["available_size"]
        total_size = info["size"]

        # Resolve file path on disk
        file_path = storage.get_file_path(file_id)
        if not file_path.exists():
            upload = await storage.get_upload(file_id)
            if upload and upload.is_final:
                file_path = storage.get_final_path(file_id, upload.filename)
        if not file_path.exists():
            raise HTTPException(404, f"File data not found: {file_id}")

        # Parse Range header
        range_header = request.headers.get("Range")
        start = 0
        end = available_size - 1

        if range_header:
            try:
                range_spec = range_header.replace("bytes=", "")
                if "-" in range_spec:
                    parts = range_spec.split("-")
                    if parts[0]:
                        start = int(parts[0])
                    if parts[1]:
                        end = min(int(parts[1]), available_size - 1)
            except ValueError:
                raise HTTPException(416, "Invalid Range header")

            if start >= available_size or start > end:
                raise HTTPException(
                    416,
                    f"Range not satisfiable. Available: 0-{available_size - 1}",
                )

        content_length = end - start + 1
        is_full_download = start == 0 and end == available_size - 1

        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Accept-Ranges": "bytes",
            "Content-Length": str(content_length),
            "X-Retention-Policy": retention,
        }

        retention_expires = info.get("retention_expires_at")
        if retention_expires:
            headers["X-Retention-Expires"] = str(retention_expires)

        if retention == "download_once" and is_full_download:
            headers["X-Retention-Warning"] = "File will be deleted after this download"

        download_count = info.get("download_count", 0) + 1
        headers["X-Download-Count"] = str(download_count)

        # Record download for non-chunked files
        if is_full_download:

            async def _after_download() -> None:
                result = await storage.record_download(file_id)
                if result["should_delete"]:
                    owner_id = info.get("owner_id")
                    file_size = info.get("size", 0)
                    user_db = getattr(request.app.state, "user_db", None)
                    if user_db and owner_id:
                        await user_db.update_storage_used(owner_id, -file_size)
                    await storage.delete_upload(file_id)

            background_tasks.add_task(_after_download)

        file_path_str = str(file_path)

        # ---- Fast path: pread for Range requests ----
        if range_header:
            loop = asyncio.get_running_loop()

            def _sync_pread() -> bytes:
                fd = os.open(file_path_str, os.O_RDONLY)
                try:
                    return pread(fd, content_length, start)
                finally:
                    os.close(fd)

            data = await loop.run_in_executor(_io_pool, _sync_pread)

            headers["Content-Range"] = f"bytes {start}-{end}/{total_size}"
            return Response(
                content=data,
                status_code=206,
                media_type=mime_type,
                headers=headers,
            )

        # ---- Full download: streaming with large buffer ----
        _BUF_SIZE = 4 * 1024 * 1024  # 4 MB

        async def generate_fast() -> AsyncIterator[bytes]:
            loop = asyncio.get_running_loop()
            fd = os.open(file_path_str, os.O_RDONLY)
            try:
                offset = start
                remaining = content_length
                while remaining > 0:
                    to_read = min(_BUF_SIZE, remaining)
                    buf = await loop.run_in_executor(_io_pool, pread, fd, to_read, offset)
                    if not buf:
                        break
                    yield buf
                    offset += len(buf)
                    remaining -= len(buf)
            finally:
                os.close(fd)

        if available_size < total_size:
            headers["Content-Range"] = f"bytes {start}-{end}/{total_size}"
            return StreamingResponse(
                generate_fast(),
                status_code=206,
                media_type=mime_type,
                headers=headers,
            )

        return StreamingResponse(
            generate_fast(),
            status_code=200,
            media_type=mime_type,
            headers=headers,
        )

    @router.get(
        "/{file_id}/info/download",
        response_model=DownloadInfo,
        responses={404: {"model": ErrorResponse}},
    )
    async def get_download_info(file_id: str) -> DownloadInfo:
        """Get download information for a file.

        Returns file metadata needed to start a download,
        including available size for partial downloads.
        For chunked files, includes chunk_size, total_chunks, and available_chunks.
        """
        info = await storage.get_file_info(file_id)
        if not info:
            raise HTTPException(404, f"File not found: {file_id}")

        is_chunked = info.get("chunked_storage", False)
        chunk_size = info.get("chunk_size") if is_chunked else None
        total_chunks = info.get("total_chunks") if is_chunked else None
        available_chunks = await storage.get_available_chunks(file_id) if is_chunked else None

        return DownloadInfo(
            file_id=file_id,
            filename=info["filename"],
            size=info["size"],
            available_size=info["available_size"],
            mime_type=info.get("mime_type"),
            checksum=info.get("checksum"),
            chunked_storage=is_chunked,
            chunk_size=chunk_size,
            total_chunks=total_chunks,
            available_chunks=available_chunks,
        )

    @router.delete(
        "/{file_id}",
        responses={
            200: {"description": "File deleted"},
            404: {"model": ErrorResponse},
        },
    )
    async def delete_file(file_id: str, request: Request) -> dict[str, str]:
        """Delete a file."""
        info = await storage.get_file_info(file_id)
        if not info:
            raise HTTPException(404, f"File not found: {file_id}")

        # Decrement user storage_used
        owner_id = info.get("owner_id")
        file_size = info.get("size", 0)
        user_db = getattr(request.app.state, "user_db", None)
        if user_db and owner_id:
            await user_db.update_storage_used(owner_id, -file_size)

        await storage.delete_upload(file_id)
        return {"status": "deleted", "file_id": file_id}

    @router.post(
        "/cleanup",
        responses={200: {"description": "Cleanup result"}},
    )
    async def trigger_cleanup() -> dict[str, Any]:
        """Manually trigger cleanup of expired uploads and TTL-expired files.

        This is useful for testing - normally cleanup runs periodically.
        """
        cleaned = await storage.cleanup_expired()
        return {
            "status": "ok",
            "cleaned": cleaned,
        }

    return router
