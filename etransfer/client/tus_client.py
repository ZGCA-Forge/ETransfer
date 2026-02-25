"""Extended TUS client for EasyTransfer."""

import mimetypes
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import urljoin

import httpx
from tusclient.client import TusClient

# Re-export for backward compatibility
from etransfer.client.parallel_uploader import ParallelUploader  # noqa: F401
from etransfer.client.uploader import EasyTransferUploader  # noqa: F401
from etransfer.common.constants import AUTH_HEADER, DEFAULT_CHUNK_SIZE, TUS_VERSION
from etransfer.common.models import FileInfo, ServerInfo


class EasyTransferClient(TusClient):
    """Extended TUS client with API token auth, server info, and file listing."""

    def __init__(
        self,
        server_url: str,
        token: Optional[str] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        **kwargs: Any,
    ) -> None:
        tus_url = urljoin(server_url.rstrip("/") + "/", "tus")

        headers = kwargs.pop("headers", {})
        if token:
            headers[AUTH_HEADER] = token
            headers["Authorization"] = f"Bearer {token}"
        headers["Tus-Resumable"] = TUS_VERSION

        super().__init__(tus_url, headers=headers, **kwargs)

        self.server_url = server_url.rstrip("/")
        self.token = token
        self.chunk_size = chunk_size
        self._http = httpx.Client(
            base_url=self.server_url,
            headers=headers,
            timeout=120.0,
        )

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._http.close()

    def __enter__(self) -> "EasyTransferClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    # ── Server queries ───────────────────────────────────────

    def get_server_info(self) -> ServerInfo:
        """Get server information."""
        resp = self._http.get("/api/info")
        resp.raise_for_status()
        return ServerInfo(**resp.json())

    def list_files(
        self,
        page: int = 1,
        per_page: int = 20,
        page_size: Optional[int] = None,
        status: Optional[str] = None,
        include_partial: bool = False,
    ) -> list[dict[str, Any]]:
        """List files on the server."""
        effective_per_page = page_size if page_size is not None else per_page
        params: dict[str, Any] = {"page": page, "per_page": effective_per_page}
        if status:
            params["status"] = status
        if include_partial:
            params["include_partial"] = "true"
        resp = self._http.get("/api/files", params=params)
        resp.raise_for_status()
        data = resp.json()
        # Normalise: server may return {"files": [...]} or a bare list
        if isinstance(data, dict):
            return data.get("files", [])  # type: ignore[no-any-return]
        return data  # type: ignore[no-any-return]

    def get_file_info(self, file_id: str) -> FileInfo:
        """Get file information."""
        resp = self._http.get(f"/api/files/{file_id}")
        resp.raise_for_status()
        return FileInfo(**resp.json())

    def delete_file(self, file_id: str) -> bool:
        """Delete a file."""
        resp = self._http.delete(f"/api/files/{file_id}")
        resp.raise_for_status()
        return resp.status_code in (200, 204)

    def get_download_url(self, file_id: str) -> str:
        """Get the download URL for a file."""
        return f"{self.server_url}/api/files/{file_id}/download"

    def get_best_endpoint(self) -> Optional[str]:
        """Get the best server endpoint based on traffic."""
        try:
            info = self.get_server_info()
            endpoints = getattr(info, "advertised_endpoints", None) or []
            if endpoints:
                return endpoints[0]  # type: ignore[no-any-return]
        except Exception:
            pass
        return None

    def select_best_endpoint(self, for_upload: bool = True) -> str:
        """Select the best server endpoint based on traffic load."""
        try:
            resp = self._http.get("/api/endpoints")
            resp.raise_for_status()
            data = resp.json()

            if for_upload and data.get("best_for_upload"):
                return data["best_for_upload"]  # type: ignore[no-any-return]
            if not for_upload and data.get("best_for_download"):
                return data["best_for_download"]  # type: ignore[no-any-return]

            endpoints = data.get("endpoints", [])
            if not endpoints:
                return self.server_url

            key = "upload_load_percent" if for_upload else "download_load_percent"
            available = [e for e in endpoints if e.get("is_available", True)]
            if not available:
                return self.server_url

            best = min(available, key=lambda x: x.get(key, 100))
            return best.get("url", self.server_url)  # type: ignore[no-any-return]
        except Exception:
            return self.server_url

    def get_endpoints(self) -> dict:
        """Get all available endpoints with their load status."""
        resp = self._http.get("/api/endpoints")
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    def get_traffic(self) -> dict:
        """Get real-time traffic information."""
        resp = self._http.get("/api/traffic")
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    def get_storage_status(self) -> dict:
        """Get storage status."""
        resp = self._http.get("/api/storage")
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    def test_endpoint_connectivity(
        self,
        endpoint_url: str,
        timeout: float = 5.0,
    ) -> dict:
        """Test connectivity to a specific endpoint.

        Returns:
            Dict with ``reachable``, ``latency_ms``, and optional ``error``.
        """
        import time

        try:
            start = time.monotonic()
            with httpx.Client(timeout=timeout) as c:
                headers = {}
                if self.token:
                    headers[AUTH_HEADER] = self.token
                resp = c.get(f"{endpoint_url.rstrip('/')}/api/info", headers=headers)
                latency = (time.monotonic() - start) * 1000
                return {
                    "url": endpoint_url,
                    "reachable": resp.status_code == 200,
                    "latency_ms": round(latency, 1),
                    "status_code": resp.status_code,
                }
        except Exception as e:
            return {
                "url": endpoint_url,
                "reachable": False,
                "latency_ms": None,
                "error": str(e),
            }

    def test_all_endpoints(self, timeout: float = 5.0) -> dict:
        """Test connectivity to all advertised endpoints."""
        try:
            data = self.get_endpoints()
            endpoints = data.get("endpoints", [])
        except Exception:
            endpoints = []

        results = []
        for ep in endpoints:
            url = ep.get("url", "")
            if url:
                result = self.test_endpoint_connectivity(url, timeout)
                result["load"] = {
                    "upload": ep.get("upload_load_percent"),
                    "download": ep.get("download_load_percent"),
                }
                results.append(result)

        return {
            "endpoints": results,
            "reachable_count": sum(1 for r in results if r.get("reachable")),
            "total_count": len(results),
        }

    def select_best_reachable_endpoint(
        self,
        for_upload: bool = True,
        timeout: float = 5.0,
        prefer_low_latency: bool = True,
    ) -> str:
        """Select the best reachable endpoint by testing connectivity."""
        test_results = self.test_all_endpoints(timeout)
        reachable = [r for r in test_results["endpoints"] if r.get("reachable")]
        if not reachable:
            return self.server_url

        load_key = "upload_load_percent" if for_upload else "download_load_percent"

        if prefer_low_latency:
            reachable.sort(key=lambda x: (x.get("latency_ms", 999), x.get(load_key, 100)))
        else:
            reachable.sort(key=lambda x: (x.get(load_key, 100), x.get("latency_ms", 999)))

        return reachable[0]["url"]  # type: ignore[no-any-return]

    # ── Upload helpers ───────────────────────────────────────

    def create_uploader(
        self,
        file_path: str,
        metadata: Optional[dict[str, str]] = None,
        chunk_size: Optional[int] = None,
        retries: int = 3,
        retry_delay: float = 1.0,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        retention: Optional[str] = None,
        retention_ttl: Optional[int] = None,
        **kwargs: Any,
    ) -> EasyTransferUploader:
        """Create an uploader for a file.

        Args:
            file_path: Path to file to upload
            metadata: Additional metadata
            chunk_size: Override chunk size
            retries: Number of retry attempts
            retry_delay: Delay between retries
            progress_callback: Callback for progress updates
            retention: Retention policy (permanent/download_once/ttl)
            retention_ttl: TTL in seconds (only for retention='ttl')
            **kwargs: Additional uploader arguments

        Returns:
            EasyTransferUploader instance
        """
        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        mime_type = mimetypes.guess_type(file_path)[0] or "application/octet-stream"

        upload_metadata: dict[str, str] = {
            "filename": file_path_obj.name,
            "filetype": mime_type,
        }
        if retention:
            upload_metadata["retention"] = retention
        if retention_ttl is not None:
            upload_metadata["retention_ttl"] = str(retention_ttl)
        if metadata:
            upload_metadata.update(metadata)

        return EasyTransferUploader(
            client=self,
            file_path=str(file_path),
            file_size=file_path_obj.stat().st_size,
            metadata=upload_metadata,
            chunk_size=chunk_size or self.chunk_size,
            retries=retries,
            retry_delay=retry_delay,
            progress_callback=progress_callback,
            **kwargs,
        )

    def create_parallel_uploader(
        self,
        file_path: str,
        metadata: Optional[dict[str, str]] = None,
        chunk_size: Optional[int] = None,
        max_concurrent: int = 4,
        retries: int = 3,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        endpoints: Optional[list[str]] = None,
        retention: Optional[str] = None,
        retention_ttl: Optional[int] = None,
        wait_on_quota: bool = True,
        resume_url: Optional[str] = None,
    ) -> ParallelUploader:
        """Create a parallel uploader for a file.

        If ``resume_url`` is provided, the uploader skips TUS CREATE and
        queries the server for already-uploaded ranges to resume.
        """
        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        mime_type = mimetypes.guess_type(file_path)[0] or "application/octet-stream"

        upload_metadata = {
            "filename": file_path_obj.name,
            "filetype": mime_type,
        }
        if retention:
            upload_metadata["retention"] = retention
        if retention_ttl is not None:
            upload_metadata["retention_ttl"] = str(retention_ttl)
        if metadata:
            upload_metadata.update(metadata)

        return ParallelUploader(
            client=self,
            file_path=str(file_path),
            file_size=file_path_obj.stat().st_size,
            metadata=upload_metadata,
            chunk_size=chunk_size or self.chunk_size,
            max_concurrent=max_concurrent,
            retries=retries,
            progress_callback=progress_callback,
            endpoints=endpoints,
            wait_on_quota=wait_on_quota,
            resume_url=resume_url,
        )
