"""Multi-threaded parallel TUS uploader with prefetch pipeline."""

import base64
import os
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    extension implemented by EasyTransfer).

    A background prefetch thread reads chunks from disk using ``os.pread``
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
        self.url: Optional[str] = None
        self._uploaded_bytes = 0
        self._lock = threading.Lock()
        self.wait_on_quota = wait_on_quota
        self.quota_poll_interval = quota_poll_interval
        self.quota_max_wait = quota_max_wait

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

    def upload(self) -> Optional[str]:
        """Upload the file using a prefetch pipeline + thread pool.

        Returns:
            The upload URL on success.
        """
        self.url = self._create_upload()
        chunks = self._build_chunk_plan()
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

        fd = os.open(self.file_path, os.O_RDONLY)

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

        # ── Prefetch thread ──────────────────────────────────
        def _prefetch() -> None:
            try:
                for offset, length in chunks:
                    data = pread(fd, length, offset)
                    chunk_queue.put((offset, length, data))
            finally:
                for _ in range(self.max_concurrent):
                    chunk_queue.put(_SENTINEL)

        prefetch_thread = threading.Thread(target=_prefetch, daemon=True)
        prefetch_thread.start()

        # ── Upload worker (each bound to one endpoint) ───────
        def _upload_worker(http: httpx.Client, patch_url: str) -> None:
            while True:
                item = chunk_queue.get()
                if item is _SENTINEL:
                    return
                offset, length, data = item
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
                for future in as_completed(futures):
                    future.result()
            prefetch_thread.join()
        finally:
            os.close(fd)
            for c, _ in clients:
                c.close()

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
                last_exc = e

        raise RuntimeError(f"Chunk at offset {offset} failed after {self.retries} retries") from last_exc
