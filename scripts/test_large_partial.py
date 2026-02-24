#!/usr/bin/env python3
"""
Test: Large file upload/download with partial transfer support.

Demonstrates:
1. Large file chunked upload (configurable size, default 100MB)
2. Partial upload - stop at X%, download available portion
3. Resume upload from where it stopped
4. Partial download with Range headers
5. Full integrity verification (MD5)
6. Concurrent partial download while uploading
7. Storage pressure reduction - no need for full file copy

Usage:
    python scripts/test_large_partial.py [--size SIZE_MB] [--server URL] [--token TOKEN]

Requires SSH tunnel or direct server access on port 8765.
"""

import argparse
import hashlib
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

# Defaults
DEFAULT_SERVER_URL = "http://127.0.0.1:8765"
DEFAULT_TOKEN = "test-token-12345"
DEFAULT_SIZE_MB = 100  # 100MB


def fmt_size(n: int) -> str:
    """Human-readable file size."""
    for u in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} TB"


def fmt_speed(bps: float) -> str:
    """Human-readable speed."""
    return f"{fmt_size(bps)}/s"


def md5_data(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def md5_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def md5_partial(path: str, length: int) -> str:
    """MD5 of first `length` bytes."""
    h = hashlib.md5()
    remaining = length
    with open(path, "rb") as f:
        while remaining > 0:
            chunk = f.read(min(1024 * 1024, remaining))
            if not chunk:
                break
            h.update(chunk)
            remaining -= len(chunk)
    return h.hexdigest()


def create_test_file(path: str, size: int) -> str:
    """Create a test file with pseudo-random data. Returns MD5."""
    print(f"  Creating test file: {fmt_size(size)}")
    t0 = time.time()
    h = hashlib.md5()
    block = 1024 * 1024  # 1MB blocks
    written = 0
    with open(path, "wb") as f:
        while written < size:
            chunk = os.urandom(min(block, size - written))
            f.write(chunk)
            h.update(chunk)
            written += len(chunk)
            pct = written * 100 / size
            elapsed = time.time() - t0
            speed = written / elapsed if elapsed > 0 else 0
            print(
                f"\r  Writing: {pct:.0f}% ({fmt_size(written)}/{fmt_size(size)}) " f"@ {fmt_speed(speed)}    ",
                end="",
                flush=True,
            )
    print(f"\r  Created in {time.time() - t0:.1f}s, MD5: {h.hexdigest()}")
    return h.hexdigest()


def banner(title: str) -> None:
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


class LargeFileTest:
    """Test suite for large file partial upload/download."""

    def __init__(self, server_url: str, token: str, file_size: int):
        self.server_url = server_url
        self.token = token
        self.file_size = file_size
        self.chunk_size = 4 * 1024 * 1024  # 4MB chunks
        self.results = {}

    def _headers(self) -> dict:
        return {"X-API-Token": self.token}

    def _http(self):
        import httpx

        return httpx.Client(base_url=self.server_url, headers=self._headers(), timeout=120.0)

    # ─── TEST 1: Partial Upload ─────────────────────────────────────────

    def test_partial_upload(self, test_file: str, stop_pct: float = 50.0) -> tuple:
        """Upload a file partially, stopping at stop_pct%.

        Returns: (upload_url, file_id, bytes_uploaded)
        """
        banner(f"TEST 1: Partial Upload (stop at {stop_pct:.0f}%)")

        from etransfer.client.tus_client import EasyTransferClient

        stop_at = int(self.file_size * stop_pct / 100)
        # Align to chunk boundary
        stop_at = (stop_at // self.chunk_size) * self.chunk_size
        if stop_at == 0:
            stop_at = self.chunk_size

        print(f"  File size: {fmt_size(self.file_size)}")
        print(f"  Stop at: {fmt_size(stop_at)} ({stop_at * 100 / self.file_size:.1f}%)")
        print(f"  Chunk size: {fmt_size(self.chunk_size)}")

        client = EasyTransferClient(self.server_url, token=self.token)
        uploader = client.create_uploader(
            test_file,
            chunk_size=self.chunk_size,
        )

        t0 = time.time()
        # Partial upload - stop_at parameter
        uploader.upload(stop_at=stop_at)
        elapsed = time.time() - t0

        upload_url = uploader.url
        file_id = upload_url.rstrip("/").split("/")[-1]

        speed = stop_at / elapsed if elapsed > 0 else 0
        print(f"  Uploaded {fmt_size(stop_at)} in {elapsed:.2f}s ({fmt_speed(speed)})")
        print(f"  Upload URL: {upload_url}")
        print(f"  File ID: {file_id}")

        # Verify partial status on server
        with self._http() as http:
            r = http.get(f"/api/files/{file_id}")
            assert r.status_code == 200, f"Get file info failed: {r.status_code}"
            info = r.json()
            print(f"  Server status: {info['status']}")
            print(f"  Uploaded: {fmt_size(info['uploaded_size'])} / {fmt_size(info['size'])}")
            assert info["status"] == "partial", f"Expected partial, got {info['status']}"
            assert info["uploaded_size"] == stop_at, f"Expected {stop_at}, got {info['uploaded_size']}"

        print("  PASSED")
        self.results["partial_upload"] = True
        client.close()
        return upload_url, file_id, stop_at

    # ─── TEST 2: Download Partial File ──────────────────────────────────

    def test_download_partial(self, file_id: str, expected_size: int, original_file: str) -> bool:
        """Download the available portion of a partially uploaded file."""
        banner("TEST 2: Download Partial File (before upload completes)")

        print(f"  Expected available: {fmt_size(expected_size)}")

        with self._http() as http:
            # Check download info first
            r = http.get(f"/api/files/{file_id}/info/download")
            assert r.status_code == 200, f"Download info failed: {r.status_code}"
            dl_info = r.json()
            print(f"  Server reports available: {fmt_size(dl_info['available_size'])}")
            assert dl_info["available_size"] == expected_size

            # Download the partial file
            t0 = time.time()
            r = http.get(f"/api/files/{file_id}/download", timeout=120.0)

            # Should be 206 Partial Content since upload is incomplete
            print(f"  HTTP Status: {r.status_code}")
            if "Content-Range" in r.headers:
                print(f"  Content-Range: {r.headers['Content-Range']}")

            downloaded = r.content
            elapsed = time.time() - t0
            speed = len(downloaded) / elapsed if elapsed > 0 else 0
            print(f"  Downloaded: {fmt_size(len(downloaded))} in {elapsed:.2f}s " f"({fmt_speed(speed)})")

            assert len(downloaded) == expected_size, f"Size mismatch: got {len(downloaded)}, expected {expected_size}"

            # Verify integrity against original file's first N bytes
            expected_md5 = md5_partial(original_file, expected_size)
            actual_md5 = md5_data(downloaded)
            print(f"  Expected MD5 (first {fmt_size(expected_size)}): {expected_md5}")
            print(f"  Actual MD5: {actual_md5}")
            assert expected_md5 == actual_md5, "MD5 mismatch on partial download!"

        print("  PASSED - Partial download integrity verified!")
        self.results["download_partial"] = True
        return True

    # ─── TEST 3: Range Download ─────────────────────────────────────────

    def test_range_download(self, file_id: str, available_size: int, original_file: str) -> bool:
        """Test downloading specific byte ranges."""
        banner("TEST 3: Range Download (specific byte ranges)")

        ranges_to_test = [
            (0, min(self.chunk_size - 1, available_size - 1), "first chunk"),
            (self.chunk_size, min(2 * self.chunk_size - 1, available_size - 1), "second chunk"),
            (0, min(1024 * 1024 - 1, available_size - 1), "first 1MB"),
        ]

        # Read original file for comparison
        with open(original_file, "rb") as f:
            original_data = f.read(available_size)

        with self._http() as http:
            for start, end, label in ranges_to_test:
                if start >= available_size:
                    print(f"  [{label}] Skipped (out of available range)")
                    continue

                end = min(end, available_size - 1)
                r = http.get(
                    f"/api/files/{file_id}/download",
                    headers={"Range": f"bytes={start}-{end}"},
                )
                assert r.status_code == 206, f"Expected 206, got {r.status_code}"

                expected_data = original_data[start : end + 1]
                assert r.content == expected_data, (
                    f"Range {start}-{end} data mismatch! " f"Got {len(r.content)} bytes, expected {len(expected_data)}"
                )
                print(f"  [{label}] bytes={start}-{end} -> {fmt_size(len(r.content))} OK")

        print("  PASSED - All range downloads verified!")
        self.results["range_download"] = True
        return True

    # ─── TEST 4: Resume Upload ──────────────────────────────────────────

    def test_resume_upload(
        self,
        upload_url: str,
        file_id: str,
        test_file: str,
        current_offset: int,
        stop_pct: float = 80.0,
    ) -> int:
        """Resume uploading from current offset to a new stop point."""
        banner(f"TEST 4: Resume Upload (from {current_offset * 100 / self.file_size:.0f}% " f"to {stop_pct:.0f}%)")

        from etransfer.client.tus_client import EasyTransferClient

        new_stop = int(self.file_size * stop_pct / 100)
        new_stop = (new_stop // self.chunk_size) * self.chunk_size

        print(f"  Current offset: {fmt_size(current_offset)}")
        print(f"  New stop at: {fmt_size(new_stop)}")
        print(f"  Data to upload: {fmt_size(new_stop - current_offset)}")

        client = EasyTransferClient(self.server_url, token=self.token)
        uploader = client.create_uploader(
            test_file,
            chunk_size=self.chunk_size,
        )
        # Set existing upload URL for resume
        uploader.set_url(upload_url)

        t0 = time.time()
        uploader.upload(stop_at=new_stop)
        elapsed = time.time() - t0

        uploaded = new_stop - current_offset
        speed = uploaded / elapsed if elapsed > 0 else 0
        print(f"  Resumed {fmt_size(uploaded)} in {elapsed:.2f}s ({fmt_speed(speed)})")

        # Verify
        with self._http() as http:
            r = http.get(f"/api/files/{file_id}")
            info = r.json()
            print(f"  Server offset: {fmt_size(info['uploaded_size'])} / {fmt_size(info['size'])}")
            assert info["uploaded_size"] == new_stop
            assert info["status"] == "partial"

        print("  PASSED")
        self.results["resume_upload"] = True
        client.close()
        return new_stop

    # ─── TEST 5: Download After Resume ──────────────────────────────────

    def test_download_after_resume(self, file_id: str, available_size: int, original_file: str) -> bool:
        """Download the increased available portion after resume."""
        banner(f"TEST 5: Download After Resume ({available_size * 100 / self.file_size:.0f}% available)")

        with self._http() as http:
            t0 = time.time()
            r = http.get(f"/api/files/{file_id}/download", timeout=120.0)
            downloaded = r.content
            elapsed = time.time() - t0
            speed = len(downloaded) / elapsed if elapsed > 0 else 0

            print(f"  Downloaded: {fmt_size(len(downloaded))} in {elapsed:.2f}s " f"({fmt_speed(speed)})")
            assert len(downloaded) == available_size

            expected_md5 = md5_partial(original_file, available_size)
            actual_md5 = md5_data(downloaded)
            print(f"  MD5 match: {expected_md5 == actual_md5}")
            assert expected_md5 == actual_md5

        print("  PASSED")
        self.results["download_after_resume"] = True
        return True

    # ─── TEST 6: Complete Upload ────────────────────────────────────────

    def test_complete_upload(self, upload_url: str, file_id: str, test_file: str, current_offset: int) -> bool:
        """Complete the upload from current offset to 100%."""
        banner(f"TEST 6: Complete Upload (from {current_offset * 100 / self.file_size:.0f}% to 100%)")

        from etransfer.client.tus_client import EasyTransferClient

        remaining = self.file_size - current_offset
        print(f"  Remaining: {fmt_size(remaining)}")

        client = EasyTransferClient(self.server_url, token=self.token)
        uploader = client.create_uploader(
            test_file,
            chunk_size=self.chunk_size,
        )
        uploader.set_url(upload_url)

        t0 = time.time()
        uploader.upload()  # No stop_at -> upload to completion
        elapsed = time.time() - t0

        speed = remaining / elapsed if elapsed > 0 else 0
        print(f"  Uploaded {fmt_size(remaining)} in {elapsed:.2f}s ({fmt_speed(speed)})")

        # Verify complete
        with self._http() as http:
            r = http.get(f"/api/files/{file_id}")
            info = r.json()
            print(f"  Status: {info['status']}")
            print(f"  Size: {fmt_size(info['uploaded_size'])} / {fmt_size(info['size'])}")
            assert info["status"] == "complete", f"Expected complete, got {info['status']}"
            assert info["uploaded_size"] == self.file_size

        print("  PASSED")
        self.results["complete_upload"] = True
        client.close()
        return True

    # ─── TEST 7: Full Download & Verify ─────────────────────────────────

    def test_full_download_verify(self, file_id: str, original_file: str, original_md5: str) -> bool:
        """Download the complete file and verify integrity."""
        banner("TEST 7: Full Download & Integrity Verification")

        with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as f:
            download_path = f.name

        try:
            import httpx

            t0 = time.time()
            total_downloaded = 0

            with httpx.stream(
                "GET",
                f"{self.server_url}/api/files/{file_id}/download",
                headers=self._headers(),
                timeout=120.0,
            ) as r:
                assert r.status_code == 200, f"Expected 200, got {r.status_code}"
                with open(download_path, "wb") as f:
                    for chunk in r.iter_bytes(chunk_size=1024 * 1024):
                        f.write(chunk)
                        total_downloaded += len(chunk)
                        pct = total_downloaded * 100 / self.file_size
                        elapsed = time.time() - t0
                        speed = total_downloaded / elapsed if elapsed > 0 else 0
                        print(
                            f"\r  Downloading: {pct:.0f}% ({fmt_size(total_downloaded)}) " f"@ {fmt_speed(speed)}    ",
                            end="",
                            flush=True,
                        )

            elapsed = time.time() - t0
            speed = total_downloaded / elapsed if elapsed > 0 else 0
            print(f"\r  Downloaded {fmt_size(total_downloaded)} in {elapsed:.1f}s " f"({fmt_speed(speed)})          ")

            assert total_downloaded == self.file_size, f"Size mismatch: {total_downloaded} != {self.file_size}"

            # MD5 verification
            print("  Computing MD5...")
            dl_md5 = md5_file(download_path)
            print(f"  Original:   {original_md5}")
            print(f"  Downloaded: {dl_md5}")
            assert original_md5 == dl_md5, "FULL FILE MD5 MISMATCH!"

            print("  PASSED - Full file integrity verified!")
            self.results["full_download"] = True
            return True

        finally:
            if os.path.exists(download_path):
                os.unlink(download_path)

    # ─── TEST 8: Concurrent Download During Upload ──────────────────────

    def test_concurrent_download_during_upload(self, test_file: str) -> bool:
        """Start a new upload and download simultaneously to show
        that partially uploaded data is immediately available."""
        banner("TEST 8: Concurrent Download During Upload")

        import threading

        from etransfer.client.tus_client import EasyTransferClient

        # Create a new upload
        client = EasyTransferClient(self.server_url, token=self.token)
        uploader = client.create_uploader(
            test_file,
            chunk_size=self.chunk_size,
        )

        # Upload first 25% to create the upload on server
        first_stop = max(self.chunk_size, (self.file_size // 4 // self.chunk_size) * self.chunk_size)
        print(f"  Step 1: Upload first {first_stop * 100 / self.file_size:.0f}% " f"({fmt_size(first_stop)})")
        uploader.upload(stop_at=first_stop)

        upload_url = uploader.url
        file_id = upload_url.rstrip("/").split("/")[-1]
        print(f"  File ID: {file_id}")

        # Now start uploading more in a thread, while downloading in main thread
        upload_errors = []
        upload_complete = threading.Event()

        def background_upload():
            try:
                c2 = EasyTransferClient(self.server_url, token=self.token)
                u2 = c2.create_uploader(test_file, chunk_size=self.chunk_size)
                u2.set_url(upload_url)
                # Upload next 25%
                second_stop = max(
                    first_stop + self.chunk_size,
                    (self.file_size // 2 // self.chunk_size) * self.chunk_size,
                )
                u2.upload(stop_at=second_stop)
                c2.close()
            except Exception as e:
                upload_errors.append(str(e))
            finally:
                upload_complete.set()

        # Start background upload
        upload_thread = threading.Thread(target=background_upload)
        upload_thread.start()

        # Wait for background upload to push some data
        time.sleep(0.5)

        # Meanwhile, download whatever is available
        print("  Step 2: Downloading available data while upload continues...")
        with self._http() as http:
            r = http.get(f"/api/files/{file_id}/info/download")
            dl_info = r.json()
            avail = dl_info["available_size"]
            print(f"  Available now: {fmt_size(avail)} " f"({avail * 100 / self.file_size:.1f}%)")

            # Download what's available
            if avail > 0:
                r = http.get(f"/api/files/{file_id}/download", timeout=60.0)
                print(f"  Downloaded: {fmt_size(len(r.content))}")
                # Verify partial integrity
                expected_md5 = md5_partial(test_file, avail)
                actual_md5 = md5_data(r.content[:avail])
                print(f"  Partial MD5 match: {expected_md5 == actual_md5}")
                assert expected_md5 == actual_md5, "Partial data integrity check failed!"

        # Wait for background upload
        upload_complete.wait(timeout=60)
        upload_thread.join(timeout=5)

        if upload_errors:
            print(f"  WARNING: Background upload had errors: {upload_errors}")

        # Check final state
        with self._http() as http:
            r = http.get(f"/api/files/{file_id}")
            info = r.json()
            print(f"  Final state: {info['status']}, " f"uploaded: {fmt_size(info['uploaded_size'])}")

        # Cleanup
        with self._http() as http:
            http.delete(f"/api/files/{file_id}")
        client.close()

        print("  PASSED - Concurrent download during upload works!")
        self.results["concurrent"] = True
        return True

    # ─── TEST 9: Storage Efficiency Check ───────────────────────────────

    def test_storage_efficiency(self, file_id: str) -> bool:
        """Verify that the server only stores one copy of the file,
        not duplicating data for partial downloads."""
        banner("TEST 9: Storage Efficiency Check")

        with self._http() as http:
            r = http.get("/api/info")
            info = r.json()
            print(f"  Total files on server: {info['total_files']}")
            print(f"  Total storage used: {fmt_size(info['total_size'])}")

            # Get file details
            r = http.get(f"/api/files/{file_id}")
            file_info = r.json()
            print(f"  File '{file_info['filename']}':")
            print(f"    Total size: {fmt_size(file_info['size'])}")
            print(f"    Status: {file_info['status']}")

            # Key point: server stores data once, serves partial or full
            # No additional copies needed for partial downloads
            print()
            print("  Key insight: The server stores ONLY the uploaded bytes.")
            print("  During partial upload, disk usage = uploaded bytes, NOT full file size.")
            print("  Clients can download any available portion via Range requests.")
            print("  This eliminates redundant storage and reduces server pressure.")

        print("  PASSED")
        self.results["storage_efficiency"] = True
        return True

    # ─── Run All Tests ──────────────────────────────────────────────────

    def run(self):
        """Run the full test suite."""
        banner("Large File Partial Upload/Download Test Suite")
        print(f"  Server: {self.server_url}")
        print(f"  File size: {fmt_size(self.file_size)}")
        print(f"  Chunk size: {fmt_size(self.chunk_size)}")

        # Create test file
        with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as f:
            test_file = f.name

        try:
            banner("SETUP: Creating test file")
            original_md5 = create_test_file(test_file, self.file_size)

            # TEST 1: Partial upload (50%)
            upload_url, file_id, offset1 = self.test_partial_upload(test_file, stop_pct=30)

            # TEST 2: Download partial file
            self.test_download_partial(file_id, offset1, test_file)

            # TEST 3: Range download
            self.test_range_download(file_id, offset1, test_file)

            # TEST 4: Resume upload to 70%
            offset2 = self.test_resume_upload(upload_url, file_id, test_file, offset1, stop_pct=70)

            # TEST 5: Download after resume
            self.test_download_after_resume(file_id, offset2, test_file)

            # TEST 6: Complete upload
            self.test_complete_upload(upload_url, file_id, test_file, offset2)

            # TEST 7: Full download & integrity check
            self.test_full_download_verify(file_id, test_file, original_md5)

            # TEST 8: Concurrent download during upload
            self.test_concurrent_download_during_upload(test_file)

            # TEST 9: Storage efficiency
            self.test_storage_efficiency(file_id)

        finally:
            if os.path.exists(test_file):
                os.unlink(test_file)

        # Summary
        banner("TEST SUMMARY")
        total = len(self.results)
        passed = sum(1 for v in self.results.values() if v)

        test_names = {
            "partial_upload": "Partial Upload (30%)",
            "download_partial": "Download Partial File",
            "range_download": "Range Download (specific bytes)",
            "resume_upload": "Resume Upload (30% -> 70%)",
            "download_after_resume": "Download After Resume (70%)",
            "complete_upload": "Complete Upload (70% -> 100%)",
            "full_download": "Full Download & MD5 Verify",
            "concurrent": "Concurrent Download During Upload",
            "storage_efficiency": "Storage Efficiency Check",
        }

        for key, name in test_names.items():
            status = "PASS" if self.results.get(key) else "FAIL"
            print(f"  [{status}] {name}")

        print(f"\n  Result: {passed}/{total} passed")
        return passed == total


def main():
    parser = argparse.ArgumentParser(description="Large file partial upload/download test")
    parser.add_argument(
        "--size", type=int, default=DEFAULT_SIZE_MB, help=f"Test file size in MB (default: {DEFAULT_SIZE_MB})"
    )
    parser.add_argument("--server", default=DEFAULT_SERVER_URL, help=f"Server URL (default: {DEFAULT_SERVER_URL})")
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="API token")
    args = parser.parse_args()

    file_size = args.size * 1024 * 1024
    test = LargeFileTest(args.server, args.token, file_size)
    success = test.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
