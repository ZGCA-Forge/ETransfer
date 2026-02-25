"""Chunked file downloader with HTTP Range support."""

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable, Optional

import httpx

from etransfer.client.cache import LocalCache
from etransfer.common.constants import AUTH_HEADER, DEFAULT_CHUNK_SIZE
from etransfer.common.fileutil import pwrite
from etransfer.common.models import DownloadInfo

logger = logging.getLogger("etransfer.client.downloader")


class ChunkDownloader:
    """Download files in chunks with resume support.

    Uses HTTP Range requests to download files in parallel chunks.
    Supports resume through local chunk cache.

    When *endpoints* is provided, download requests are distributed
    evenly across all endpoints (round-robin per worker).
    """

    def __init__(
        self,
        server_url: str,
        token: Optional[str] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_concurrent: int = 5,
        cache: Optional[LocalCache] = None,
        timeout: float = 120.0,
        endpoints: Optional[list[str]] = None,
        use_pwrite: bool = False,
    ) -> None:
        self.server_url = server_url.rstrip("/")
        self.token = token
        self.chunk_size = chunk_size
        self.max_concurrent = max_concurrent
        self._global_cache = cache  # only used as fallback
        self.timeout = timeout
        self.use_pwrite = use_pwrite
        self.endpoints = [u.rstrip("/") for u in endpoints] if endpoints else []

        self._headers: dict[str, str] = {}
        if token:
            self._headers[AUTH_HEADER] = token
            self._headers["Authorization"] = f"Bearer {token}"

        # Build per-endpoint HTTP clients for parallel downloads
        self._clients = self._build_clients()

        # Primary client for metadata queries (always first endpoint)
        self._client = self._clients[0]

    @staticmethod
    def _local_cache_for(output_path: Path, size_hint: int = 0) -> LocalCache:
        """Create a ``LocalCache`` rooted next to *output_path*.

        For ``/data/movie.mp4`` the cache dir is ``/data/.movie.mp4.part/``.
        This keeps partial chunks visible alongside the target file and
        avoids polluting a global ``~/.etransfer/cache`` directory.

        *size_hint* (bytes) ensures the cache limit is large enough to
        hold the entire file so that LRU eviction does not delete chunks
        mid-download.
        """
        parent = output_path.parent
        name = output_path.name
        cache_dir = parent / f".{name}.part"
        # Ensure cache can hold the whole file (+ 10 % headroom)
        min_mb = max(1024, (size_hint * 110 // 100) // (1024 * 1024) + 1)
        return LocalCache(cache_dir=cache_dir, max_size_mb=min_mb)

    @staticmethod
    def _part_dir_for(output_path: Path) -> Path:
        """Return the ``.{name}.part/`` directory path for an output file."""
        return output_path.parent / f".{output_path.name}.part"

    @staticmethod
    def _write_part_meta(part_dir: Path, file_id: str, filename: str, size: int, chunk_size: int) -> None:
        """Write ``meta.json`` into the ``.part/`` folder for resume detection."""
        import json

        part_dir.mkdir(parents=True, exist_ok=True)
        meta = {
            "file_id": file_id,
            "filename": filename,
            "size": size,
            "chunk_size": chunk_size,
        }
        (part_dir / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    @staticmethod
    def read_part_meta(part_dir: Path) -> Optional[dict]:
        """Read ``meta.json`` from a ``.part/`` folder. Returns None if missing/invalid."""
        import json

        meta_path = part_dir / "meta.json"
        if not meta_path.exists():
            return None
        try:
            return json.loads(meta_path.read_text(encoding="utf-8"))  # type: ignore[no-any-return]
        except Exception:
            return None

    def _write_at(self, fd_or_path: "int | str", data: bytes, offset: int) -> None:
        """Write *data* at *offset* using pwrite (fd) or seek+write (path).

        When ``self.use_pwrite`` is True, *fd_or_path* must be an open fd.
        Otherwise it is a file path string opened briefly for each write.
        """
        if self.use_pwrite:
            pwrite(fd_or_path, data, offset)  # type: ignore[arg-type]
        else:
            with open(fd_or_path, "r+b") as f:  # type: ignore[arg-type]
                f.seek(offset)
                f.write(data)

    def _build_clients(self) -> list[httpx.Client]:
        urls = self.endpoints or [self.server_url]
        conns_per = max(2, self.max_concurrent // len(urls) + 1)
        clients: list[httpx.Client] = []
        for url in urls:
            clients.append(
                httpx.Client(
                    base_url=url,
                    timeout=self.timeout,
                    limits=httpx.Limits(
                        max_connections=conns_per + 1,
                        max_keepalive_connections=conns_per + 1,
                    ),
                    headers=self._headers,
                )
            )
        return clients

    def close(self) -> None:
        """Close all HTTP clients."""
        for c in self._clients:
            c.close()

    def _get_download_url(self, file_id: str) -> str:
        """Get download URL for a file."""
        return f"{self.server_url}/api/files/{file_id}/download"

    def get_file_info(self, file_id: str) -> DownloadInfo:
        """Get file information for download.

        Args:
            file_id: File identifier

        Returns:
            DownloadInfo object
        """
        with httpx.Client(timeout=self.timeout) as client:
            response = client.get(
                f"{self.server_url}/api/files/{file_id}/info/download",
                headers=self._headers,
            )
            response.raise_for_status()
            return DownloadInfo(**response.json())

    def download_chunk(
        self,
        file_id: str,
        chunk_index: int,
        chunk_size: Optional[int] = None,
        total_size: Optional[int] = None,
        client: Optional[httpx.Client] = None,
        *,
        chunked_mode: bool = False,
    ) -> bytes:
        """Download a single chunk.

        Args:
            file_id: File identifier
            chunk_index: Chunk index
            chunk_size: Chunk size (uses default if not specified)
            total_size: Total file size (for last chunk calculation)
            client: HTTP client to use (defaults to primary)
            chunked_mode: If True, use ?chunk=N instead of Range header

        Returns:
            Chunk data
        """
        http = client or self._client

        if chunked_mode:
            # Chunk-index mode: server stores individual chunk files
            response = http.get(
                f"/api/files/{file_id}/download",
                params={"chunk": chunk_index},
            )
            response.raise_for_status()
            return response.content

        # Range mode: server has a single file
        chunk_size = chunk_size or self.chunk_size
        start = chunk_index * chunk_size
        end = start + chunk_size - 1
        if total_size and end >= total_size:
            end = total_size - 1

        headers = {"Range": f"bytes={start}-{end}"}
        response = http.get(f"/api/files/{file_id}/download", headers=headers)
        response.raise_for_status()
        return response.content

    def download_file(
        self,
        file_id: str,
        output_path: Path,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        use_cache: bool = True,
    ) -> bool:
        """Download a file with chunked transfer and automatic resume.

        On start, writes ``meta.json`` into a ``.{name}.part/`` folder next
        to the output file.  If the ``.part/`` folder already exists with
        cached chunks, those chunks are skipped (resume).  On completion the
        ``.part/`` folder is removed.

        Automatically detects chunked_storage mode from server and uses
        chunk-index downloads (?chunk=N) instead of Range headers.

        Args:
            file_id: File identifier
            output_path: Output file path
            progress_callback: Progress callback (downloaded_bytes, total_bytes)
            use_cache: Use local chunk cache

        Returns:
            True if download successful
        """
        # Get file info
        info = self.get_file_info(file_id)
        total_size = info.size
        chunked_mode = info.chunked_storage

        # Write part meta for resume detection
        part_dir = self._part_dir_for(Path(output_path))
        self._write_part_meta(
            part_dir,
            file_id,
            info.filename,
            total_size,
            info.chunk_size or self.chunk_size,
        )

        if chunked_mode:
            # Chunk-index mode: server stores individual chunk files
            chunk_size = info.chunk_size or self.chunk_size
            total_chunks = info.total_chunks or ((total_size + chunk_size - 1) // chunk_size)
            success = self._download_chunked(
                file_id,
                output_path,
                total_size,
                chunk_size,
                total_chunks,
                progress_callback,
                use_cache,
            )
            if success:
                self._cleanup_part_dir(part_dir)
            return success

        # Range mode: standard download — always use cache for resume
        available_size = info.available_size
        total_chunks = (available_size + self.chunk_size - 1) // self.chunk_size

        cache = self._local_cache_for(Path(output_path), size_hint=available_size)
        cache.set_file_meta(file_id, info.filename, available_size, self.chunk_size)
        cached_chunks = set(cache.get_cached_chunks(file_id))

        downloaded_bytes = len(cached_chunks) * self.chunk_size
        if progress_callback and cached_chunks:
            progress_callback(downloaded_bytes, available_size)

        with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            futures = {}
            for i in range(total_chunks):
                if i in cached_chunks:
                    continue
                future = executor.submit(
                    self._download_and_cache_chunk,
                    file_id,
                    i,
                    available_size,
                    cache,
                )
                futures[future] = i

            for future in futures:
                try:
                    chunk_data = future.result()
                    downloaded_bytes += len(chunk_data)
                    if progress_callback:
                        progress_callback(downloaded_bytes, available_size)
                except Exception as e:
                    print(f"Error downloading chunk {futures[future]}: {e}")
                    return False

        success = cache.assemble_file(file_id, output_path)
        if success:
            cache.clear_file(file_id)
            self._cleanup_part_dir(part_dir)
        return success

    @staticmethod
    def _cleanup_part_dir(part_dir: Path) -> None:
        """Remove the ``.part/`` folder after successful download."""
        import shutil

        try:
            shutil.rmtree(part_dir)
        except Exception:
            pass

    def _download_chunked(
        self,
        file_id: str,
        output_path: Path,
        total_size: int,
        chunk_size: int,
        total_chunks: int,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        use_cache: bool = True,
    ) -> bool:
        """Download a file using chunk-index mode (?chunk=N).

        Each chunk is requested by index; server deletes download_once
        chunks after serving them.
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        path_str = str(output_path)

        # Pre-allocate sparse file
        with open(output_path, "wb") as f:
            if total_size > 0:
                f.seek(total_size - 1)
                f.write(b"\0")

        downloaded_bytes = 0
        _lock = threading.Lock()
        _chunk_iter = iter(range(total_chunks))
        _iter_lock = threading.Lock()

        fd = os.open(path_str, os.O_WRONLY | getattr(os, "O_BINARY", 0)) if self.use_pwrite else -1
        # For pwrite: fd is shared (pwrite is thread-safe)
        # For seek+write: each worker opens its own handle via _write_at

        def _download_worker(http: httpx.Client) -> bool:
            nonlocal downloaded_bytes
            while True:
                with _iter_lock:
                    idx = next(_chunk_iter, None)
                if idx is None:
                    return True
                try:
                    data = self.download_chunk(
                        file_id,
                        idx,
                        client=http,
                        chunked_mode=True,
                    )
                    self._write_at(fd if self.use_pwrite else path_str, data, idx * chunk_size)
                    with _lock:
                        downloaded_bytes += len(data)
                        if progress_callback:
                            progress_callback(downloaded_bytes, total_size)
                except Exception as e:
                    print(f"Error downloading chunk {idx}: {e}")
                    return False
            return True

        try:
            with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
                futures = [
                    executor.submit(_download_worker, self._clients[i % len(self._clients)])
                    for i in range(self.max_concurrent)
                ]
                for fut in futures:
                    if not fut.result():
                        return False
        finally:
            if self.use_pwrite and fd != -1:
                os.close(fd)

        return True

    def _download_range_direct(
        self,
        file_id: str,
        output_path: Path,
        total_size: int,
        total_chunks: int,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> bool:
        """Direct-write Range-based download (non-chunked files)."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        path_str = str(output_path)

        # Pre-allocate sparse file
        with open(output_path, "wb") as f:
            f.seek(total_size - 1)
            f.write(b"\0")

        downloaded_bytes = 0
        _lock = threading.Lock()
        _chunk_iter = iter(range(total_chunks))
        _iter_lock = threading.Lock()

        fd = os.open(path_str, os.O_WRONLY | getattr(os, "O_BINARY", 0)) if self.use_pwrite else -1

        def _download_worker(http: httpx.Client) -> bool:
            nonlocal downloaded_bytes
            while True:
                with _iter_lock:
                    idx = next(_chunk_iter, None)
                if idx is None:
                    return True
                try:
                    data = self.download_chunk(
                        file_id,
                        idx,
                        total_size=total_size,
                        client=http,
                    )
                    self._write_at(fd if self.use_pwrite else path_str, data, idx * self.chunk_size)
                    with _lock:
                        downloaded_bytes += len(data)
                        if progress_callback:
                            progress_callback(downloaded_bytes, total_size)
                except Exception as e:
                    print(f"Error downloading chunk {idx}: {e}")
                    return False
            return True

        try:
            # Assign workers round-robin across endpoint clients
            with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
                futures = [
                    executor.submit(
                        _download_worker,
                        self._clients[i % len(self._clients)],
                    )
                    for i in range(self.max_concurrent)
                ]
                for future in futures:
                    if not future.result():
                        return False
        finally:
            if self.use_pwrite and fd != -1:
                os.close(fd)

        return True

    def _download_and_cache_chunk(
        self,
        file_id: str,
        chunk_index: int,
        total_size: int,
        cache: Optional[LocalCache] = None,
    ) -> bytes:
        """Download a chunk and optionally cache it.

        Args:
            file_id: File identifier
            chunk_index: Chunk index
            total_size: Total file size
            cache: LocalCache instance to store the chunk (None = no caching)

        Returns:
            Chunk data
        """
        data = self.download_chunk(file_id, chunk_index, total_size=total_size)

        if cache is not None:
            cache.put_chunk(file_id, chunk_index, data)

        return data

    def download_file_follow(
        self,
        file_id: str,
        output_path: Path,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        poll_interval: float = 2.0,
    ) -> bool:
        """Download a file that may still be uploading.

        Polls for new data and downloads chunks as they become available.
        For chunked_storage files, polls available_chunks from server.
        Finishes when the upload is complete and all data is downloaded.
        Uses cache-based storage for resume support.

        Args:
            file_id: File identifier
            output_path: Output file path
            progress_callback: Progress callback (downloaded_bytes, total_bytes)
            poll_interval: Seconds between polling for new data

        Returns:
            True if download successful
        """
        import time

        info = self.get_file_info(file_id)
        chunked_mode = info.chunked_storage

        logger.debug(
            "follow mode: chunked=%s available_size=%s available_chunks=%s is_upload_complete=%s",
            chunked_mode,
            info.available_size,
            len(info.available_chunks) if info.available_chunks else None,
            getattr(info, "is_upload_complete", None),
        )

        # Write part meta for resume detection
        part_dir = self._part_dir_for(Path(output_path))
        self._write_part_meta(
            part_dir,
            file_id,
            info.filename,
            info.size,
            info.chunk_size or self.chunk_size,
        )

        if chunked_mode:
            success = self._download_file_follow_chunked(
                file_id,
                output_path,
                progress_callback,
                poll_interval,
            )
            if success:
                self._cleanup_part_dir(part_dir)
            return success

        # Range-based follow download (non-chunked) — always use cache for resume
        downloaded_chunks: set[int] = set()
        downloaded_bytes = 0
        last_available = 0
        total_size = 0
        cache: Optional[LocalCache] = None

        while True:
            info = self.get_file_info(file_id)
            total_size = info.size
            available = info.available_size
            is_complete = available >= total_size

            logger.debug(
                "range poll: available=%d total=%d last_available=%d is_complete=%s",
                available,
                total_size,
                last_available,
                is_complete,
            )

            if available > last_available:
                last_available = available
                total_chunks = (available + self.chunk_size - 1) // self.chunk_size

                if cache is None:
                    cache = self._local_cache_for(Path(output_path), size_hint=total_size)
                    cache.set_file_meta(file_id, info.filename, total_size, self.chunk_size)
                    # Pick up any previously cached chunks (resume)
                    downloaded_chunks = set(cache.get_cached_chunks(file_id))
                    downloaded_bytes = len(downloaded_chunks) * self.chunk_size
                    if progress_callback and downloaded_chunks:
                        progress_callback(downloaded_bytes, total_size)

                # Download newly available chunks
                new_indices = [i for i in range(total_chunks) if i not in downloaded_chunks]
                if new_indices:
                    with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
                        futures = {
                            executor.submit(
                                self._download_and_cache_chunk,
                                file_id,
                                i,
                                available,
                                cache,
                            ): i
                            for i in new_indices
                        }

                        for future in futures:
                            try:
                                chunk_data = future.result()
                                downloaded_chunks.add(futures[future])
                                downloaded_bytes += len(chunk_data)
                                if progress_callback:
                                    progress_callback(downloaded_bytes, total_size)
                            except Exception as e:
                                print(f"Error downloading chunk {futures[future]}: {e}")
                                return False

            if is_complete and downloaded_bytes >= total_size:
                break

            if not is_complete:
                time.sleep(poll_interval)

        # Assemble file from cache
        if cache is not None:
            cache.set_file_meta(file_id, info.filename, total_size, self.chunk_size)
            success = cache.assemble_file(file_id, output_path)
            if success:
                cache.clear_file(file_id)
                self._cleanup_part_dir(part_dir)
            return success

        return True

    def _download_file_follow_chunked(
        self,
        file_id: str,
        output_path: Path,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        poll_interval: float = 2.0,
    ) -> bool:
        """Follow-mode download for chunked_storage files.

        Polls available_chunks from server, downloads newly available
        chunks by index, writes to output file at correct offset.
        Handles download_once files where metadata is deleted after
        all chunks are consumed.
        """
        import time

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        downloaded_chunks: set[int] = set()
        downloaded_bytes = 0
        total_size = 0
        chunk_size = self.chunk_size
        total_chunks = 0
        fd: int = -1
        path_str = str(output_path)

        try:
            while True:
                try:
                    info = self.get_file_info(file_id)
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404 and downloaded_chunks:
                        # Fallback: server cleaned up after upload complete + all chunks consumed
                        if len(downloaded_chunks) >= total_chunks and total_chunks > 0:
                            break
                    raise

                total_size = info.size
                chunk_size = info.chunk_size or self.chunk_size
                total_chunks = info.total_chunks or ((total_size + chunk_size - 1) // chunk_size)
                available = info.available_chunks or []
                is_upload_complete = getattr(info, "is_upload_complete", False)

                logger.debug(
                    "chunked poll: avail_chunks=%d total_chunks=%d downloaded=%d is_complete=%s",
                    len(available),
                    total_chunks,
                    len(downloaded_chunks),
                    is_upload_complete,
                )

                # Pre-allocate file on first iteration
                if fd == -1 and total_size > 0:
                    with open(output_path, "wb") as f:
                        f.seek(total_size - 1)
                        f.write(b"\0")
                    if self.use_pwrite:
                        fd = os.open(path_str, os.O_WRONLY | getattr(os, "O_BINARY", 0))
                    else:
                        fd = 0  # sentinel: file created but no fd needed

                # Find new chunks to download
                new_chunks = [c for c in available if c not in downloaded_chunks]

                if new_chunks:
                    _lock = threading.Lock()

                    def _write_chunk(idx: int) -> int:
                        http = self._clients[idx % len(self._clients)]
                        data = self.download_chunk(
                            file_id,
                            idx,
                            client=http,
                            chunked_mode=True,
                        )
                        self._write_at(fd if self.use_pwrite else path_str, data, idx * chunk_size)
                        return len(data)

                    with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
                        futures = {executor.submit(_write_chunk, idx): idx for idx in new_chunks}

                        for future in futures:
                            try:
                                size = future.result()
                                idx = futures[future]
                                downloaded_chunks.add(idx)
                                with _lock:
                                    downloaded_bytes += size
                                    if progress_callback:
                                        progress_callback(downloaded_bytes, total_size)
                            except Exception as e:
                                print(f"Error downloading chunk {futures[future]}: {e}")
                                return False

                # Done when all chunks downloaded
                if len(downloaded_chunks) >= total_chunks and is_upload_complete:
                    break

                time.sleep(poll_interval)
        finally:
            if self.use_pwrite and fd > 0:
                os.close(fd)

        return True
