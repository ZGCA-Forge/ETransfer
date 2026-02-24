"""Extended TUS uploader with progress tracking and retry handling."""

import time
from typing import Any, Callable, Optional

import httpx
from tusclient.exceptions import TusCommunicationError, TusUploadFailed
from tusclient.uploader import Uploader

from etransfer.common.constants import DEFAULT_CHUNK_SIZE


class EasyTransferUploader(Uploader):
    """Extended TUS uploader with progress callbacks and quota-aware retry."""

    def __init__(
        self,
        client: Any,
        file_path: str,
        file_size: int,
        metadata: dict[str, str],
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        retries: int = 3,
        retry_delay: float = 1.0,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            file_path=file_path,
            client=client,
            chunk_size=chunk_size,
            metadata=metadata,
            retries=retries,
            retry_delay=int(retry_delay),
            **kwargs,
        )
        self.file_size = file_size
        self.progress_callback = progress_callback
        self._uploaded_bytes = 0

    @property
    def uploaded_bytes(self) -> int:
        return self._uploaded_bytes

    @property
    def progress(self) -> float:
        if self.file_size == 0:
            return 100.0
        return (self._uploaded_bytes / self.file_size) * 100

    def upload_chunk(self) -> Any:
        result = super().upload_chunk()
        self._uploaded_bytes = self.offset
        if self.progress_callback:
            self.progress_callback(self._uploaded_bytes, self.file_size)
        return result

    def upload(
        self,
        stop_at: Optional[int] = None,
        wait_on_quota: bool = True,
        poll_interval: float = 5.0,
        max_wait: float = 3600.0,
        quota_callback: Optional[Callable[[dict], None]] = None,
    ) -> Optional[str]:
        """Upload the file with progress tracking and quota-aware retry.

        When the server returns HTTP 507 (storage quota exceeded), the
        uploader polls and waits for space to be freed, then resumes.
        """
        total_waited = 0.0

        # Create upload if not already created
        if not self.url:
            while True:
                try:
                    self.set_url(self.create_url())
                    self.offset = 0
                    break
                except TusCommunicationError as e:
                    if "507" in str(e) and wait_on_quota:
                        storage_info = self._get_storage_info()
                        if quota_callback:
                            quota_callback(storage_info)
                        time.sleep(poll_interval)
                        total_waited += poll_interval
                        if total_waited >= max_wait:
                            raise RuntimeError(
                                f"Storage quota exceeded during create. Waited {total_waited:.0f}s."
                            ) from e
                    else:
                        raise
        else:
            self.get_offset()

        self._uploaded_bytes = self.offset

        # Upload chunks
        while self.offset < self.file_size:
            if stop_at and self.offset >= stop_at:
                break

            try:
                self.upload_chunk()
                total_waited = 0.0
            except (TusCommunicationError, TusUploadFailed) as e:
                if "507" in str(e) and wait_on_quota:
                    if total_waited >= max_wait:
                        raise RuntimeError(f"Storage quota exceeded. Waited {total_waited:.0f}s, giving up.") from e

                    storage_info = self._get_storage_info()
                    if quota_callback:
                        quota_callback(storage_info)

                    time.sleep(poll_interval)
                    total_waited += poll_interval

                    try:
                        self.get_offset()
                        self._uploaded_bytes = self.offset
                    except Exception:
                        pass
                else:
                    raise

        return self.url  # type: ignore[no-any-return]

    def _get_storage_info(self) -> dict:
        """Query server storage status."""
        try:
            headers = {}
            if hasattr(self, "client") and self.client and hasattr(self.client, "headers"):
                headers = dict(self.client.headers or {})
            base_url = self.url.rsplit("/tus/", 1)[0] if self.url else ""
            if not base_url and hasattr(self, "client") and self.client:
                base_url = getattr(self.client, "server_url", "")
            if base_url:
                with httpx.Client(timeout=10.0) as c:
                    r = c.get(f"{base_url}/api/storage", headers=headers)
                    if r.status_code == 200:
                        return r.json()  # type: ignore[no-any-return]
        except Exception:
            pass
        return {}
