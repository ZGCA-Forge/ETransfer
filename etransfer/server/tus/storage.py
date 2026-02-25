"""TUS storage backend with pluggable state management."""

import asyncio
import json
import os
import shutil
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

import aiofiles  # type: ignore[import-untyped]
import aiofiles.os  # type: ignore[import-untyped]

from etransfer.common.constants import RedisKeys
from etransfer.common.fileutil import ftruncate, pwrite
from etransfer.server.io_pool import io_pool
from etransfer.server.tus.models import TusUpload

if TYPE_CHECKING:
    from etransfer.server.services.state import StateManager


class TusStorage:
    """TUS file storage backend with pluggable state management.

    This storage backend handles:
    - File data storage on disk
    - Upload state management via StateManager interface
    - Distributed locking for multi-worker support

    The state backend can be:
    - Memory: For development and single-process deployments
    - File: For persistence without external dependencies
    - Redis: For production multi-worker deployments
    """

    def __init__(
        self,
        storage_path: Path,
        state_manager: "StateManager",
        chunk_size: int = 4 * 1024 * 1024,
        max_storage_size: Optional[int] = None,
    ) -> None:
        """Initialize TUS storage.

        Args:
            storage_path: Base path for file storage
            state_manager: StateManager instance for state storage
            chunk_size: Default chunk size for operations
            max_storage_size: Maximum total storage in bytes (None = unlimited)
        """
        self.storage_path = Path(storage_path)
        self.state = state_manager
        self.chunk_size = chunk_size
        self.max_storage_size = max_storage_size

        # Per-file asyncio locks (replaces Redis distributed lock for single-worker)
        self._local_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

        # Ensure directories exist
        self.uploads_path = self.storage_path / "uploads"
        self.files_path = self.storage_path / "files"
        self.temp_path = self.storage_path / "temp"

    async def initialize(self) -> None:
        """Initialize storage directories."""
        for path in [self.uploads_path, self.files_path, self.temp_path]:
            path.mkdir(parents=True, exist_ok=True)

    def get_file_path(self, file_id: str) -> Path:
        """Get the storage path for a file."""
        return self.uploads_path / file_id

    def get_final_path(self, file_id: str, filename: str) -> Path:
        """Get the final storage path for a completed file."""
        return self.files_path / f"{file_id}_{filename}"

    def get_chunk_dir(self, file_id: str) -> Path:
        """Get the chunk directory for a chunked upload."""
        return self.uploads_path / file_id

    def get_chunk_path(self, file_id: str, chunk_index: int) -> Path:
        """Get the path for a specific chunk file."""
        return self.get_chunk_dir(file_id) / f"chunk_{chunk_index:06d}"

    def _chunk_state_key(self, file_id: str, chunk_index: int) -> str:
        """State key for chunk availability."""
        return f"et:chunk:{file_id}:{chunk_index}"

    def _upload_key(self, file_id: str) -> str:
        """Get key for upload state."""
        return f"{RedisKeys.UPLOAD_PREFIX}{file_id}"

    def _lock_key(self, file_id: str) -> str:
        """Get key for upload lock."""
        return f"{RedisKeys.LOCK_PREFIX}{file_id}"

    def _file_key(self, file_id: str) -> str:
        """Get key for file info."""
        return f"{RedisKeys.FILE_PREFIX}{file_id}"

    async def acquire_lock(self, file_id: str, timeout: int = 30) -> bool:
        """Acquire distributed lock for an upload.

        Args:
            file_id: Upload identifier
            timeout: Lock timeout in seconds

        Returns:
            True if lock acquired, False otherwise
        """
        lock_key = self._lock_key(file_id)
        # Use SET NX EX for atomic lock acquisition
        return await self.state.set(lock_key, "1", nx=True, ex=timeout)

    async def release_lock(self, file_id: str) -> None:
        """Release distributed lock for an upload."""
        lock_key = self._lock_key(file_id)
        await self.state.delete(lock_key)

    async def merge_range_atomic(self, file_id: str, offset: int, length: int) -> tuple[Optional["TusUpload"], bool]:
        """Merge a received range into upload state under local asyncio lock.

        Replaces the Redis distributed lock + get + merge + set + release
        pattern with a single local lock and 2 Redis calls (get + set).

        Returns:
            (upload, is_complete) — the updated upload and whether it's done.
            upload is None if the upload record was not found.
        """
        async with self._local_locks[file_id]:
            upload = await self.get_upload(file_id)
            if not upload:
                return None, False

            new_end = offset + length

            # Idempotent: already received?
            for r in upload.received_ranges:
                if r[0] <= offset and new_end <= r[1]:
                    return upload, False

            upload._merge_range(offset, new_end)
            upload.updated_at = datetime.utcnow()

            completed = False
            if upload.is_complete:
                upload.is_final = True
                completed = True

            await self.update_upload(upload)
            return upload, completed

    async def acquire_lock_with_retry(
        self,
        file_id: str,
        timeout: int = 30,
        retry_interval: float = 0.05,
        max_wait: float = 15.0,
    ) -> bool:
        """Acquire lock with spin-retry.

        Args:
            file_id: Upload identifier.
            timeout: Lock auto-expiry in seconds (prevents deadlocks).
            retry_interval: Seconds between retry attempts.
            max_wait: Maximum total seconds to wait before giving up.

        Returns:
            True if the lock was acquired, False on timeout.
        """
        import asyncio

        waited = 0.0
        while waited < max_wait:
            if await self.acquire_lock(file_id, timeout):
                return True
            await asyncio.sleep(retry_interval)
            waited += retry_interval
        return False

    async def create_upload(self, upload: TusUpload) -> None:
        """Create a new upload record.

        Args:
            upload: TUS upload object
        """
        # Save state
        key = self._upload_key(upload.file_id)
        await self.state.set(
            key,
            json.dumps(upload.to_redis_dict()),
            ex=86400 * 7,  # 7 days TTL
        )

        # Lightweight size key for fast PATCH validation (no JSON parse)
        await self.state.set(
            f"et:upload:{upload.file_id}:size",
            str(upload.size),
            ex=86400 * 7,
        )

        if upload.chunked_storage:
            # Chunk-based: create directory for chunk files
            chunk_dir = self.get_chunk_dir(upload.file_id)
            chunk_dir.mkdir(parents=True, exist_ok=True)
        else:
            # Single-file: create empty file (no pre-allocation to respect storage quota)
            file_path = self.get_file_path(upload.file_id)
            async with aiofiles.open(file_path, "wb") as _:
                pass  # Empty file; grows as chunks arrive

    async def get_upload(self, file_id: str) -> Optional[TusUpload]:
        """Get upload record by ID.

        Args:
            file_id: Upload identifier

        Returns:
            TusUpload object or None if not found
        """
        key = self._upload_key(file_id)
        data = await self.state.get(key)
        if not data:
            return None

        try:
            upload_dict = json.loads(data)
            return TusUpload.from_redis_dict(upload_dict)
        except (json.JSONDecodeError, ValueError):
            return None

    async def get_upload_size(self, file_id: str) -> Optional[int]:
        """Get just the upload size (fast, no JSON parse).

        Returns None if the upload doesn't exist.
        """
        val = await self.state.get(f"et:upload:{file_id}:size")
        if val is None:
            return None
        return int(val)

    async def update_upload(self, upload: TusUpload) -> None:
        """Update upload record.

        Args:
            upload: TUS upload object with updated state
        """
        key = self._upload_key(upload.file_id)
        upload.updated_at = datetime.utcnow()
        await self.state.set(
            key,
            json.dumps(upload.to_redis_dict()),
            ex=86400 * 7,  # 7 days TTL
        )

    async def delete_upload(self, file_id: str) -> None:
        """Delete upload record and associated file.

        Args:
            file_id: Upload identifier
        """
        # Get upload info to find final path
        upload = await self.get_upload(file_id)

        # Delete from state (both upload and file keys, plus size key)
        upload_key = self._upload_key(file_id)
        file_key = self._file_key(file_id)
        await self.state.delete(upload_key)
        await self.state.delete(file_key)
        await self.state.delete(f"et:upload:{file_id}:size")

        # Clean up chunk availability keys
        chunk_keys = await self.state.scan_keys(f"et:chunk:{file_id}:*")
        for k in chunk_keys:
            await self.state.delete(k)

        # Delete chunk directory if it exists
        chunk_dir = self.get_chunk_dir(file_id)
        if chunk_dir.is_dir():
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(io_pool, shutil.rmtree, str(chunk_dir), True)
        else:
            # Delete single file from uploads directory
            file_path = self.get_file_path(file_id)
            try:
                await aiofiles.os.remove(file_path)
            except FileNotFoundError:
                pass

        # Delete file from files directory (completed files)
        if upload and upload.filename:
            final_path = self.get_final_path(file_id, upload.filename)
            try:
                await aiofiles.os.remove(final_path)
            except FileNotFoundError:
                pass

        # Also scan files directory for any file starting with this file_id
        if self.files_path.exists():
            for f in self.files_path.iterdir():
                if f.is_file() and f.name.startswith(file_id):
                    try:
                        await aiofiles.os.remove(f)
                    except FileNotFoundError:
                        pass

        # Release any locks
        await self.release_lock(file_id)

    # Shared thread pool for synchronous file I/O (pwrite / pread)
    # Use shared I/O pool from io_pool module

    async def write_chunk(
        self,
        file_id: str,
        data: bytes,
        offset: int,
    ) -> int:
        """Write chunk data to file at arbitrary offset (supports parallel writes).

        Uses os.pwrite for zero-seek, zero-lock writes directly to the target
        offset. Pre-allocates the file as a sparse file on first write.

        Args:
            file_id: Upload identifier
            data: Chunk data bytes
            offset: Byte offset to write at

        Returns:
            Number of bytes written
        """
        file_path = self.get_file_path(file_id)
        await self._ensure_sparse_file(file_id, file_path)

        # os.pwrite: atomic positional write, no seek, no file-level lock needed
        loop = asyncio.get_running_loop()
        written = await loop.run_in_executor(io_pool, self._sync_pwrite, str(file_path), data, offset)
        return written

    async def write_chunk_streaming(
        self,
        file_id: str,
        receive: Any,
        offset: int,
        content_length: int,
    ) -> int:
        """Stream request body directly to disk via pwrite.

        Reads the body in sub-chunks and writes each to disk immediately,
        avoiding buffering the entire chunk in memory.

        Args:
            file_id: Upload identifier
            receive: ASGI receive callable (from request)
            offset: Byte offset to start writing at
            content_length: Expected total bytes

        Returns:
            Total bytes written
        """
        file_path = self.get_file_path(file_id)
        await self._ensure_sparse_file(file_id, file_path)

        fd = os.open(str(file_path), os.O_WRONLY | getattr(os, "O_BINARY", 0))
        total_written = 0
        try:
            while total_written < content_length:
                message = await receive()
                body = message.get("body", b"")
                if body:
                    loop = asyncio.get_running_loop()
                    n = await loop.run_in_executor(io_pool, pwrite, fd, body, offset + total_written)
                    total_written += n
                if not message.get("more_body", False):
                    break
        finally:
            os.close(fd)
        return total_written

    async def _ensure_sparse_file(self, file_id: str, file_path: Any) -> None:
        """Create sparse file at full size if it doesn't exist yet."""
        if not file_path.exists():
            upload = await self.get_upload(file_id)
            if upload:
                fd = os.open(str(file_path), os.O_CREAT | os.O_WRONLY | getattr(os, "O_BINARY", 0), 0o644)
                try:
                    ftruncate(fd, upload.size)
                finally:
                    os.close(fd)

    @staticmethod
    def _sync_pwrite(path: str, data: bytes, offset: int) -> int:
        """Synchronous pwrite — runs in thread pool."""
        fd = os.open(path, os.O_WRONLY | getattr(os, "O_BINARY", 0))
        try:
            return pwrite(fd, data, offset)
        finally:
            os.close(fd)

    # ------------------------------------------------------------------
    # Chunk-based file I/O (for streaming relay / download_once)
    # ------------------------------------------------------------------

    async def write_chunk_file(
        self,
        file_id: str,
        chunk_index: int,
        data: bytes,
    ) -> int:
        """Write data to an individual chunk file.

        Args:
            file_id: Upload identifier
            chunk_index: Chunk index
            data: Chunk data bytes

        Returns:
            Number of bytes written
        """
        chunk_path = self.get_chunk_path(file_id, chunk_index)
        chunk_path.parent.mkdir(parents=True, exist_ok=True)
        loop = asyncio.get_running_loop()

        def _write() -> int:
            with open(chunk_path, "wb") as f:
                return f.write(data)

        return await loop.run_in_executor(io_pool, _write)

    async def write_chunk_file_streaming(
        self,
        file_id: str,
        chunk_index: int,
        receive: Any,
        content_length: int,
    ) -> int:
        """Stream request body directly to a chunk file.

        Args:
            file_id: Upload identifier
            chunk_index: Chunk index
            receive: ASGI receive callable
            content_length: Expected total bytes

        Returns:
            Total bytes written
        """
        chunk_path = self.get_chunk_path(file_id, chunk_index)
        chunk_path.parent.mkdir(parents=True, exist_ok=True)

        fd = os.open(str(chunk_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0), 0o644)
        total_written = 0
        try:
            while total_written < content_length:
                message = await receive()
                body = message.get("body", b"")
                if body:
                    loop = asyncio.get_running_loop()
                    n = await loop.run_in_executor(io_pool, os.write, fd, body)
                    total_written += n
                if not message.get("more_body", False):
                    break
        finally:
            os.close(fd)
        return total_written

    async def read_chunk_file(self, file_id: str, chunk_index: int) -> bytes:
        """Read an individual chunk file.

        Args:
            file_id: Upload/file identifier
            chunk_index: Chunk index

        Returns:
            Chunk data bytes
        """
        chunk_path = self.get_chunk_path(file_id, chunk_index)
        if not chunk_path.exists():
            raise FileNotFoundError(f"Chunk not found: {file_id} chunk {chunk_index}")
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(io_pool, chunk_path.read_bytes)

    async def delete_chunk_file(self, file_id: str, chunk_index: int) -> None:
        """Delete a single chunk file from disk and state.

        Args:
            file_id: Upload/file identifier
            chunk_index: Chunk index
        """
        chunk_path = self.get_chunk_path(file_id, chunk_index)
        try:
            await aiofiles.os.remove(chunk_path)
        except FileNotFoundError:
            pass
        await self.state.delete(self._chunk_state_key(file_id, chunk_index))

    async def mark_chunk_available(self, file_id: str, chunk_index: int) -> None:
        """Mark a chunk as available for download in state backend."""
        await self.state.set(self._chunk_state_key(file_id, chunk_index), "1")

    async def is_chunk_available(self, file_id: str, chunk_index: int) -> bool:
        """Check if a chunk is available for download."""
        return await self.state.exists(self._chunk_state_key(file_id, chunk_index))

    async def get_available_chunks(self, file_id: str) -> list[int]:
        """Get list of available chunk indices for a file."""
        pattern = f"et:chunk:{file_id}:*"
        keys = await self.state.scan_keys(pattern)
        indices = []
        prefix = f"et:chunk:{file_id}:"
        for k in keys:
            try:
                indices.append(int(k[len(prefix) :]))
            except (ValueError, IndexError):
                continue
        indices.sort()
        return indices

    async def read_chunk(
        self,
        file_id: str,
        offset: int,
        length: int,
    ) -> bytes:
        """Read chunk data from file.

        Args:
            file_id: Upload/file identifier
            offset: Byte offset to read from
            length: Number of bytes to read

        Returns:
            Chunk data bytes
        """
        # Try uploads path first, then files path
        file_path = self.get_file_path(file_id)
        if not file_path.exists():
            # Check if it's a finalized file
            upload = await self.get_upload(file_id)
            if upload and upload.is_final:
                file_path = self.get_final_path(file_id, upload.filename)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_id}")

        async with aiofiles.open(file_path, "rb") as f:
            await f.seek(offset)
            return await f.read(length)  # type: ignore[no-any-return]

    async def get_available_size(self, file_id: str) -> int:
        """Get the size of data available for download.

        This returns the current upload offset, allowing
        partial downloads of in-progress uploads.

        Args:
            file_id: Upload/file identifier

        Returns:
            Available bytes for download
        """
        upload = await self.get_upload(file_id)
        if not upload:
            # Check if it's a finalized file
            file_key = self._file_key(file_id)
            file_data = await self.state.get(file_key)
            if file_data:
                info = json.loads(file_data)
                return info.get("size", 0)  # type: ignore[no-any-return]
            return 0

        return upload.offset

    async def finalize_upload(self, file_id: str) -> None:
        """Finalize a completed upload.

        For single-file uploads: moves file from uploads to files directory.
        For chunked uploads: keeps chunks in place (no merge).

        Args:
            file_id: Upload identifier
        """
        upload = await self.get_upload(file_id)
        if not upload:
            raise ValueError(f"Upload not found: {file_id}")

        if not upload.is_complete:
            raise ValueError(f"Upload not complete: {file_id}")

        # Calculate retention_expires_at for TTL-based retention
        retention_expires_at = None
        if upload.retention == "ttl" and upload.retention_ttl:
            from datetime import timedelta

            retention_expires_at = datetime.utcnow() + timedelta(seconds=upload.retention_ttl)
            upload.retention_expires_at = retention_expires_at

        if upload.chunked_storage:
            # Chunked: keep chunks in uploads/{file_id}/ — no move
            storage_path = str(self.get_chunk_dir(file_id))
        else:
            # Single-file: move to final location
            src_path = self.get_file_path(file_id)
            dst_path = self.get_final_path(file_id, upload.filename)
            if src_path.exists():
                await aiofiles.os.rename(str(src_path), str(dst_path))
            storage_path = str(dst_path)

        # Store file info
        total_chunks = (upload.size + upload.chunk_size - 1) // upload.chunk_size if upload.chunked_storage else 0

        # Populate finalization fields on the TusUpload object
        upload.is_final = True
        upload.storage_path = storage_path
        upload.available_size = upload.size
        upload.completed_at = datetime.utcnow().isoformat()
        upload.total_chunks = total_chunks
        upload.download_count = 0

        file_key = self._file_key(file_id)
        await self.state.set(file_key, json.dumps(upload.to_redis_dict()))

        # Update upload record
        await self.update_upload(upload)

    async def list_uploads(
        self,
        include_completed: bool = True,
        include_partial: bool = True,
    ) -> list[TusUpload]:
        """List all uploads.

        Args:
            include_completed: Include completed uploads
            include_partial: Include partial uploads

        Returns:
            List of TusUpload objects
        """
        pattern = f"{RedisKeys.UPLOAD_PREFIX}*"
        uploads = []

        # Scan all upload keys
        keys = await self.state.scan_keys(pattern)
        for key in keys:
            data = await self.state.get(key)
            if data:
                try:
                    upload = TusUpload.from_redis_dict(json.loads(data))
                    if upload.is_final and not include_completed:
                        continue
                    if not upload.is_final and not include_partial:
                        continue
                    uploads.append(upload)
                except (json.JSONDecodeError, ValueError, AttributeError, TypeError):
                    continue

        return uploads

    async def list_files(self) -> list[dict]:
        """List all completed files.

        Returns:
            List of file info dicts
        """
        pattern = f"{RedisKeys.FILE_PREFIX}*"
        files = []

        keys = await self.state.scan_keys(pattern)
        for key in keys:
            data = await self.state.get(key)
            if data:
                try:
                    files.append(json.loads(data))
                except json.JSONDecodeError:
                    continue

        return files

    async def get_file_info(self, file_id: str) -> Optional[dict]:
        """Get file info by ID.

        Args:
            file_id: File identifier

        Returns:
            File info dict or None
        """
        file_key = self._file_key(file_id)
        data = await self.state.get(file_key)
        if data:
            return json.loads(data)  # type: ignore[no-any-return]

        # Try upload record (partial — not yet finalized)
        upload = await self.get_upload(file_id)
        if upload:
            d = upload.to_redis_dict()
            # For partial uploads, available_size = contiguous offset
            d["available_size"] = upload.offset
            return d

        return None

    async def record_download(self, file_id: str) -> dict:
        """Record a download and return retention info.

        For download_once files, marks for deletion.
        Returns dict with retention status.

        Args:
            file_id: File identifier

        Returns:
            Dict with 'should_delete', 'retention', 'download_count'
        """
        # Check file_key first (completed files)
        file_key = self._file_key(file_id)
        data = await self.state.get(file_key)
        if data:
            upload = TusUpload.from_redis_dict(json.loads(data))
            upload.download_count += 1
            await self.state.set(file_key, json.dumps(upload.to_redis_dict()))

            return {
                "should_delete": upload.retention == "download_once",
                "retention": upload.retention,
                "download_count": upload.download_count,
            }

        # Check upload record (partial files)
        partial = await self.get_upload(file_id)
        if partial:
            partial.download_count += 1
            await self.update_upload(partial)
            return {
                "should_delete": partial.retention == "download_once",
                "retention": partial.retention,
                "download_count": partial.download_count,
            }

        return {"should_delete": False, "retention": "permanent", "download_count": 0}

    async def cleanup_expired(self, user_db: Any = None) -> int:
        """Clean up expired uploads and retention-expired files.

        Handles:
        - Incomplete uploads past their upload expiration
        - Completed files past their TTL retention_expires_at

        Args:
            user_db: Optional UserDB instance for updating user storage_used

        Returns:
            Number of items cleaned up
        """
        now = datetime.utcnow()
        cleaned = 0

        # Clean expired incomplete uploads
        uploads = await self.list_uploads(include_completed=False)
        for upload in uploads:
            if upload.expires_at and upload.expires_at < now:
                if user_db and upload.owner_id:
                    await user_db.update_storage_used(upload.owner_id, -upload.offset)
                await self.delete_upload(upload.file_id)
                cleaned += 1

        # Clean TTL-expired completed files
        files = await self.list_files()
        for f in files:
            expires = f.get("retention_expires_at")
            if expires:
                if isinstance(expires, str):
                    expires = datetime.fromisoformat(expires)
                if expires < now:
                    file_id = f["file_id"]
                    owner_id = f.get("owner_id")
                    file_size = f.get("size", 0)
                    if user_db and owner_id:
                        await user_db.update_storage_used(owner_id, -file_size)
                    await self.delete_upload(file_id)
                    cleaned += 1

        return cleaned

    async def get_storage_usage(self) -> dict:
        """Get current storage usage statistics.

        Returns:
            Dict with:
            - used: total bytes used on disk
            - max: max storage size (None if unlimited)
            - available: bytes available (None if unlimited)
            - usage_percent: percentage used (0 if unlimited)
            - files_count: number of completed files
            - uploads_count: number of in-progress uploads
            - is_full: True if storage is at capacity
        """
        # Calculate actual disk usage (recursive to include chunked upload subdirs)
        used = 0
        for path_dir in [self.uploads_path, self.files_path]:
            if path_dir.exists():
                for f in path_dir.rglob("*"):
                    if f.is_file():
                        used += f.stat().st_size

        files = await self.list_files()
        uploads = await self.list_uploads(include_completed=False)

        result = {
            "used": used,
            "max": self.max_storage_size,
            "files_count": len(files),
            "uploads_count": len(uploads),
        }

        if self.max_storage_size:
            available = max(0, self.max_storage_size - used)  # type: ignore[assignment]
            result["available"] = available
            result["usage_percent"] = (
                round((used / self.max_storage_size) * 100, 2)  # type: ignore[assignment]
                if self.max_storage_size > 0
                else 0
            )
            result["is_full"] = used >= self.max_storage_size
        else:
            result["available"] = None
            result["usage_percent"] = 0
            result["is_full"] = False

        return result

    async def check_quota(self, additional_bytes: int = 0) -> tuple[bool, dict]:
        """Check if storage quota allows the operation.

        Args:
            additional_bytes: Bytes about to be written

        Returns:
            (allowed, storage_info) tuple
        """
        # Fast path: no global storage limit configured — skip all I/O
        if not self.max_storage_size:
            return True, {"used": 0, "max": None}

        usage = await self.get_storage_usage()
        would_use = usage["used"] + additional_bytes
        allowed = would_use <= self.max_storage_size
        return allowed, usage  # type: ignore[no-any-return]
