"""Multi-threaded parallel TUS uploader with prefetch pipeline."""

import base64
import os
import queue
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Any, Callable, Optional
from urllib.parse import urlparse

import httpx

from etransfer.common.constants import DEFAULT_CHUNK_SIZE, TUS_VERSION
from etransfer.common.fileutil import pread

_SENTINEL = None  # marks end of prefetch queue


class ParallelUploader:
    """Multi-threaded uploader inspired by aria2.

    Splits a file into ranges and uploads them concurrently via TUS PATCH
    requests.  The server must support out-of-order PATCH (non-standard TUS
    extension implemented by ETransfer).

    A background prefetch thread reads chunks from disk using positional read
    into a bounded queue so that upload workers are never blocked on I/O.

    When *endpoints* is provided, workers are distributed round-robin
    across endpoints.  CREATE always goes to the first endpoint.
    """

    def __init__(
        self,
        client: Any,
        file_path: str,
        file_size: int,
        metadata: dict[str, str],
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_concurrent: int = 4,
        retries: int = 3,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        endpoints: Optional[list[str]] = None,
        wait_on_quota: bool = True,
        quota_poll_interval: float = 5.0,
        quota_max_wait: float = 3600.0,
        resume_url: Optional[str] = None,
    ) -> None:
        self.client = client
        self.file_path = file_path
        self.file_size = file_size
        self.metadata = metadata
        self.chunk_size = chunk_size
        self.max_concurrent = max_concurrent
        self.retries = retries
        self.progress_callback = progress_callback
        self.endpoints = endpoints or []
        self.url: Optional[str] = resume_url
        self._uploaded_bytes = 0
        self._lock = threading.Lock()
        self.wait_on_quota = wait_on_quota
        self.quota_poll_interval = quota_poll_interval
        self.quota_max_wait = quota_max_wait
        self._cancelled = threading.Event()  # Ctrl+C / cancel signal

    # ── helpers ────────────────────────────────────────────────

    def _patch_url_for_endpoint(self, endpoint: str) -> str:
        """Rewrite ``self.url`` so the host:port matches *endpoint*.

        Handles both absolute (``http://host/tus/id``) and relative
        (``/tus/id``) Location headers returned by the server.
        """
        if not self.url:
            raise RuntimeError("upload URL not set")
        parsed_url = urlparse(self.url)
        parsed_ep = urlparse(endpoint.rstrip("/"))
        # If self.url is relative (no scheme), build absolute from endpoint
        if not parsed_url.scheme:
            return f"{endpoint.rstrip('/')}{self.url}"
        return parsed_url._replace(
            scheme=parsed_ep.scheme,
            netloc=parsed_ep.netloc,
        ).geturl()

    @property
    def file_id(self) -> Optional[str]:
        """Extract file ID from the TUS upload URL."""
        if not self.url:
            return None
        return self.url.rstrip("/").split("/")[-1]

    def ensure_created(self) -> str:
        """Ensure the TUS upload resource exists, creating it if needed.

        Returns:
            The file ID.
        """
        if not self.url:
            self.url = self._create_upload()
        fid = self.file_id
        if not fid:
            raise RuntimeError("Failed to obtain file ID")
        return fid

    # ── TUS CREATE ───────────────────────────────────────────

    def _create_upload(self) -> str:
        """Create the TUS upload resource and return its URL."""
        tus_url = self.client.url
        headers = dict(self.client.headers or {})
        headers["Tus-Resumable"] = TUS_VERSION
        headers["Upload-Length"] = str(self.file_size)

        parts = []
        for k, v in self.metadata.items():
            parts.append(f"{k} {base64.b64encode(v.encode()).decode()}")
        if parts:
            headers["Upload-Metadata"] = ",".join(parts)

        with httpx.Client(timeout=30.0) as c:
            resp = c.post(tus_url, headers=headers)
            resp.raise_for_status()
            location = resp.headers.get("Location", "")
            if not location:
                raise RuntimeError("Server did not return Location header")
            return location

    # ── Chunk plan ───────────────────────────────────────────

    def _build_chunk_plan(self) -> list[tuple[int, int]]:
        """Return a list of ``(offset, length)`` tuples covering the file."""
        chunks: list[tuple[int, int]] = []
        offset = 0
        while offset < self.file_size:
            length = min(self.chunk_size, self.file_size - offset)
            chunks.append((offset, length))
            offset += length
        return chunks

    # ── Upload with prefetch pipeline ────────────────────────

    def _query_server_ranges(self) -> Optional[list[list[int]]]:
        """Query the server via TUS HEAD for already-received byte ranges.

        Parses ``X-Received-Ranges`` (precise, for parallel uploads) first,
        falling back to ``Upload-Offset`` (contiguous from 0).

        Returns:
            A list of [start, end) ranges already uploaded, or an empty
            list when the upload exists but has no progress yet.
            ``None`` when the upload is not found on the server.
        """
        if not self.url:
            return None
        base_headers = dict(self.client.headers or {})
        base_headers["Tus-Resumable"] = TUS_VERSION
        try:
            with httpx.Client(timeout=30.0) as c:
                resp = c.head(self.url, headers=base_headers)
                if resp.status_code in (404, 410):
                    # Upload expired or not found — cannot resume
                    return None
                resp.raise_for_status()

                # Prefer X-Received-Ranges for precise parallel resume
                ranges_header = resp.headers.get("X-Received-Ranges", "")
                if ranges_header:
                    ranges: list[list[int]] = []
                    for part in ranges_header.split(","):
                        part = part.strip()
                        if "-" in part:
                            s, e = part.split("-", 1)
                            ranges.append([int(s), int(e)])
                    if ranges:
                        return ranges

                # Fallback: contiguous offset
                offset = int(resp.headers.get("Upload-Offset", "0"))
                if offset > 0:
                    return [[0, offset]]
                # Upload exists on server but has 0 bytes — still valid
                return []
        except Exception:
            return None

    def upload(self) -> Optional[str]:
        """Upload the file using a prefetch pipeline + thread pool.

        If ``resume_url`` was provided at construction, the uploader queries
        the server for already-uploaded ranges and skips those chunks.

        Returns:
            The upload URL on success.
        """
        # Determine which chunks are already uploaded (for resume)
        already_uploaded_ranges: list[list[int]] = []
        if self.url:
            # Resume mode: query server for progress
            server_ranges = self._query_server_ranges()
            if server_ranges is None:
                # Server doesn't know about this upload — start fresh
                self.url = None
            else:
                already_uploaded_ranges = server_ranges

        if not self.url:
            self.url = self._create_upload()

        chunks = self._build_chunk_plan()
        if not chunks:
            return self.url

        # Filter out already-uploaded chunks
        if already_uploaded_ranges:
            remaining_chunks = []
            skipped_bytes = 0
            for offset, length in chunks:
                chunk_end = offset + length
                already_done = False
                for r in already_uploaded_ranges:
                    if r[0] <= offset and chunk_end <= r[1]:
                        already_done = True
                        break
                if already_done:
                    skipped_bytes += length
                else:
                    remaining_chunks.append((offset, length))
            chunks = remaining_chunks
            self._uploaded_bytes = skipped_bytes
            if self.progress_callback:
                self.progress_callback(self._uploaded_bytes, self.file_size)

        if not chunks:
            return self.url

        # Derive base server URL from the client, not from self.url
        # (self.url may be a relative path like /tus/xxx)
        client_url = getattr(self.client, "server_url", None) or ""
        if not client_url and self.client and hasattr(self.client, "url"):
            # tusclient.TusClient.url is the TUS endpoint
            tus_url = self.client.url or ""
            client_url = tus_url.rsplit("/tus", 1)[0] if "/tus" in tus_url else tus_url
        endpoints = self.endpoints or [client_url] if client_url else [self.url.rsplit("/tus/", 1)[0]]
        base_headers = dict(self.client.headers or {})

        # Bounded queue: prefetch enough to keep all workers fed
        prefetch_depth = max(4, self.max_concurrent * 2)
        chunk_queue: queue.Queue[Optional[tuple[int, int, bytes]]] = queue.Queue(maxsize=prefetch_depth)

        fd = os.open(self.file_path, os.O_RDONLY | getattr(os, "O_BINARY", 0))

        # One HTTP client per endpoint
        clients: list[tuple[httpx.Client, str]] = []
        for ep in endpoints:
            conns = max(2, self.max_concurrent // len(endpoints) + 1)
            c = httpx.Client(
                timeout=120.0,
                headers=base_headers,
                limits=httpx.Limits(
                    max_connections=conns + 1,
                    max_keepalive_connections=conns + 1,
                ),
            )
            clients.append((c, self._patch_url_for_endpoint(ep)))

        # ── Install SIGINT handler to set cancel flag ─────────
        _prev_handler = signal.getsignal(signal.SIGINT)

        def _sigint_handler(signum: int, frame: Any) -> None:
            self._cancelled.set()
            print("\n\x1b[33m⚠ Cancelling upload...\x1b[0m", flush=True)
            # Close all HTTP clients to abort in-flight PATCH requests
            for _c, _ in clients:
                try:
                    _c.close()
                except Exception:
                    pass

        signal.signal(signal.SIGINT, _sigint_handler)

        # ── Prefetch thread ──────────────────────────────────
        def _prefetch() -> None:
            try:
                for offset, length in chunks:
                    if self._cancelled.is_set():
                        break
                    data = pread(fd, length, offset)
                    while not self._cancelled.is_set():
                        try:
                            chunk_queue.put((offset, length, data), timeout=0.5)
                            break
                        except queue.Full:
                            continue
            finally:
                for _ in range(self.max_concurrent):
                    try:
                        chunk_queue.put(_SENTINEL, timeout=1)
                    except queue.Full:
                        pass

        prefetch_thread = threading.Thread(target=_prefetch, daemon=True)
        prefetch_thread.start()

        # ── Upload worker (each bound to one endpoint) ───────
        def _upload_worker(http: httpx.Client, patch_url: str) -> None:
            while not self._cancelled.is_set():
                try:
                    item = chunk_queue.get(timeout=0.5)
                except queue.Empty:
                    continue
                if item is _SENTINEL:
                    return
                offset, length, data = item
                if self._cancelled.is_set():
                    return
                self._upload_chunk_with_retry(http, offset, data, patch_url)

        try:
            with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
                futures = [
                    executor.submit(
                        _upload_worker,
                        clients[i % len(clients)][0],
                        clients[i % len(clients)][1],
                    )
                    for i in range(self.max_concurrent)
                ]
                pending = set(futures)
                while pending:
                    if self._cancelled.is_set():
                        break
                    done, pending = wait(pending, timeout=0.5)
                    for f in done:
                        f.result()
            prefetch_thread.join(timeout=2)
        finally:
            signal.signal(signal.SIGINT, _prev_handler)
            os.close(fd)
            for c, _ in clients:
                c.close()

        if self._cancelled.is_set():
            raise KeyboardInterrupt()

        return self.url

    # ── Single chunk upload with retry ───────────────────────

    def _upload_chunk_with_retry(
        self,
        http: httpx.Client,
        offset: int,
        data: bytes,
        patch_url: Optional[str] = None,
    ) -> None:
        """Upload a single chunk, retrying on transient failures."""
        url = patch_url or self.url
        headers = {
            "Tus-Resumable": TUS_VERSION,
            "Upload-Offset": str(offset),
            "Content-Type": "application/offset+octet-stream",
            "Content-Length": str(len(data)),
        }

        last_exc: Optional[Exception] = None
        quota_waited = 0.0
        for attempt in range(self.retries):
            if self._cancelled.is_set():
                return
            try:
                resp = http.patch(url, content=data, headers=headers)  # type: ignore[arg-type]
                if resp.status_code in (204, 200, 409):
                    with self._lock:
                        self._uploaded_bytes += len(data)
                        if self.progress_callback:
                            self.progress_callback(self._uploaded_bytes, self.file_size)
                    return
                if resp.status_code == 507 and self.wait_on_quota:
                    # Quota exceeded — wait and retry (don't count as a retry attempt)
                    while quota_waited < self.quota_max_wait:
                        if self._cancelled.is_set():
                            return
                        time.sleep(self.quota_poll_interval)
                        quota_waited += self.quota_poll_interval
                        retry_resp = http.patch(url, content=data, headers=headers)  # type: ignore[arg-type]
                        if retry_resp.status_code in (204, 200, 409):
                            with self._lock:
                                self._uploaded_bytes += len(data)
                                if self.progress_callback:
                                    self.progress_callback(self._uploaded_bytes, self.file_size)
                            return
                        if retry_resp.status_code != 507:
                            break  # Different error, fall through to normal retry
                    last_exc = RuntimeError(
                        f"PATCH {offset} failed: HTTP 507 quota exceeded after waiting {quota_waited:.0f}s"
                    )
                else:
                    last_exc = RuntimeError(f"PATCH {offset} failed: HTTP {resp.status_code} {resp.text[:200]}")
            except Exception as e:
                if self._cancelled.is_set():
                    return
                last_exc = e

        if self._cancelled.is_set():
            return
        raise RuntimeError(f"Chunk at offset {offset} failed after {self.retries} retries") from last_exc
