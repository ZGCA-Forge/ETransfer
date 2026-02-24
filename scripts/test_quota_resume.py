#!/usr/bin/env python3
"""
Test: Storage quota limit + automatic polling + resume upload.

Scenario:
1. Start server with a small storage quota (e.g., 15MB)
2. Upload a 10MB file -> succeeds, fills 10MB of 15MB quota
3. Start uploading a 10MB file -> uploads 5MB, then hits quota (507)
4. Client enters polling mode, waiting for space
5. Meanwhile, delete the first file from another "client" (simulating download + cleanup)
6. Client detects space freed, automatically resumes upload from where it stopped
7. Upload completes successfully with full integrity verification

This tests:
- Storage quota enforcement (HTTP 507)
- Client-side polling with Retry-After
- Seamless resume after quota freed (断点续传)
- No data corruption through the entire flow
"""

import argparse
import hashlib
import json
import os
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

DEFAULT_SERVER_URL = "http://127.0.0.1:8765"
DEFAULT_TOKEN = "test-token-12345"


def fmt_size(n):
    if n is None:
        return "unlimited"
    for u in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} TB"


def md5_file(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def create_test_file(path, size):
    h = hashlib.md5()
    with open(path, "wb") as f:
        written = 0
        while written < size:
            chunk = os.urandom(min(1024 * 1024, size - written))
            f.write(chunk)
            h.update(chunk)
            written += len(chunk)
    return h.hexdigest()


def banner(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def get_http(server_url, token):
    import httpx

    return httpx.Client(
        base_url=server_url,
        headers={"X-API-Token": token},
        timeout=30.0,
    )


class QuotaResumeTest:
    """Test storage quota + auto-resume workflow."""

    def __init__(self, server_url, token):
        self.server_url = server_url
        self.token = token
        self.server_process = None
        self.results = {}

        # Test parameters
        self.quota_size = 15 * 1024 * 1024  # 15MB quota
        self.file1_size = 10 * 1024 * 1024  # 10MB file (fills 10/15)
        self.file2_size = 10 * 1024 * 1024  # 10MB file (needs 10 more, only 5 avail)
        self.chunk_size = 2 * 1024 * 1024  # 2MB chunks

    def _start_local_server(self):
        """Start a local server with quota configured."""
        import subprocess

        env = os.environ.copy()
        env["ETRANSFER_AUTH_TOKENS"] = json.dumps([self.token])
        env["ETRANSFER_STATE_BACKEND"] = "file"
        env["ETRANSFER_MAX_STORAGE_SIZE"] = str(self.quota_size)
        # Use a fresh temp storage to avoid quota from old files
        self._storage_dir = tempfile.mkdtemp(prefix="et_quota_test_")
        env["ETRANSFER_STORAGE_PATH"] = self._storage_dir

        self.server_process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "etransfer.server.main:app",
                "--host",
                "0.0.0.0",
                "--port",
                "8766",  # Use different port to avoid conflict
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=str(Path(__file__).parent.parent),
        )

        # Wait for startup
        self.server_url = "http://127.0.0.1:8766"
        import httpx

        for i in range(30):
            try:
                r = httpx.get(f"{self.server_url}/api/health", timeout=2.0)
                if r.status_code == 200:
                    print(f"  Server started (PID {self.server_process.pid})")
                    return True
            except Exception:
                pass
            time.sleep(0.5)
        print("  ERROR: Server failed to start")
        return False

    def _stop_local_server(self):
        if self.server_process:
            self.server_process.terminate()
            self.server_process.wait(timeout=10)
            print("  Server stopped")

        # Cleanup storage
        if hasattr(self, "_storage_dir") and os.path.exists(self._storage_dir):
            import shutil

            shutil.rmtree(self._storage_dir, ignore_errors=True)

    # ─── TESTS ──────────────────────────────────────────────────────────

    def test_0_check_quota(self):
        """Verify quota is configured."""
        banner("TEST 0: Verify Storage Quota Configuration")
        with get_http(self.server_url, self.token) as http:
            r = http.get("/api/storage")
            assert r.status_code == 200, f"Storage endpoint failed: {r.status_code}"
            info = r.json()
            print(f"  Max storage: {info['max_formatted']}")
            print(f"  Used: {info['used_formatted']}")
            print(f"  Available: {info['available_formatted']}")
            print(f"  Can accept uploads: {info['can_accept_uploads']}")
            assert info["max"] == self.quota_size, f"Expected quota {self.quota_size}, got {info['max']}"
            assert info["can_accept_uploads"], "Server should accept uploads initially"
        print("  PASSED")
        self.results["check_quota"] = True

    def test_1_upload_first_file(self, test_file1):
        """Upload first file that fits within quota."""
        banner("TEST 1: Upload First File (within quota)")
        from etransfer.client.tus_client import EasyTransferClient

        client = EasyTransferClient(self.server_url, token=self.token)
        uploader = client.create_uploader(test_file1, chunk_size=self.chunk_size)

        t0 = time.time()
        url = uploader.upload()
        elapsed = time.time() - t0
        file_id = url.rstrip("/").split("/")[-1]

        print(f"  Uploaded {fmt_size(self.file1_size)} in {elapsed:.2f}s")
        print(f"  File ID: {file_id}")

        # Check storage
        with get_http(self.server_url, self.token) as http:
            r = http.get("/api/storage")
            info = r.json()
            print(f"  Storage: {info['used_formatted']} / {info['max_formatted']} " f"({info['usage_percent']}%)")
            print(f"  Available: {info['available_formatted']}")

        print("  PASSED")
        self.results["upload_first"] = True
        client.close()
        return file_id

    def test_2_quota_exceeded_and_resume(self, test_file2, file1_id):
        """Upload second file: hits quota, waits, resumes after cleanup."""
        banner("TEST 2: Upload Hits Quota -> Poll -> Cleanup -> Resume")

        from etransfer.client.tus_client import EasyTransferClient

        quota_events = []
        resume_event = threading.Event()

        def on_quota_wait(storage_info):
            """Called when client is waiting for quota."""
            quota_events.append(storage_info)
            used = storage_info.get("used_formatted", "?")
            avail = storage_info.get("available_formatted", "?")
            print(
                f"    [Quota Wait] Storage: {used} used, {avail} available, " f"waiting... (poll #{len(quota_events)})"
            )

        def cleanup_after_delay():
            """Simulate a downloader taking the file after some time."""
            time.sleep(8)  # Wait 8 seconds then free space
            print("\n    [Cleanup] Deleting first file to free space...")
            with get_http(self.server_url, self.token) as http:
                r = http.delete(f"/api/files/{file1_id}")
                print(f"    [Cleanup] Delete response: {r.status_code}")
                r = http.get("/api/storage")
                info = r.json()
                print(f"    [Cleanup] Storage after delete: " f"{info['used_formatted']} / {info['max_formatted']}")
            resume_event.set()

        # Start cleanup thread (simulates "取出" after a delay)
        cleanup_thread = threading.Thread(target=cleanup_after_delay, daemon=True)
        cleanup_thread.start()

        # Start upload - will hit quota partway through, then resume
        print(f"  Starting upload of {fmt_size(self.file2_size)}...")
        print("  Expected: will fill quota ~5MB in, then wait for cleanup...")

        client = EasyTransferClient(self.server_url, token=self.token)
        uploader = client.create_uploader(test_file2, chunk_size=self.chunk_size)

        t0 = time.time()
        url = uploader.upload(
            wait_on_quota=True,
            poll_interval=3.0,
            max_wait=60.0,
            quota_callback=on_quota_wait,
        )
        elapsed = time.time() - t0

        file_id = url.rstrip("/").split("/")[-1]

        print(f"\n  Upload completed in {elapsed:.1f}s (including {len(quota_events)} " f"quota waits)")
        print(f"  File ID: {file_id}")

        # Verify the file is complete
        with get_http(self.server_url, self.token) as http:
            r = http.get(f"/api/files/{file_id}")
            info = r.json()
            print(f"  Status: {info['status']}")
            print(f"  Size: {fmt_size(info['uploaded_size'])} / {fmt_size(info['size'])}")
            assert info["status"] == "complete", f"Expected complete, got {info['status']}"

        assert len(quota_events) > 0, "Expected at least one quota wait event"
        print(f"  Quota was exceeded {len(quota_events)} times before space freed")

        print("  PASSED")
        self.results["quota_resume"] = True
        client.close()
        cleanup_thread.join(timeout=5)
        return file_id

    def test_3_verify_integrity(self, file_id, original_md5, original_size):
        """Download the resumed file and verify integrity."""
        banner("TEST 3: Download & Verify Integrity After Resume")

        import httpx

        with httpx.stream(
            "GET",
            f"{self.server_url}/api/files/{file_id}/download",
            headers={"X-API-Token": self.token},
            timeout=60.0,
        ) as r:
            assert r.status_code == 200, f"Download failed: {r.status_code}"
            h = hashlib.md5()
            total = 0
            for chunk in r.iter_bytes(chunk_size=1024 * 1024):
                h.update(chunk)
                total += len(chunk)

        dl_md5 = h.hexdigest()
        print(f"  Downloaded: {fmt_size(total)}")
        print(f"  Original MD5:   {original_md5}")
        print(f"  Downloaded MD5: {dl_md5}")
        assert total == original_size, f"Size mismatch: {total} != {original_size}"
        assert dl_md5 == original_md5, "MD5 MISMATCH after quota resume!"

        print("  PASSED - File integrity verified after quota-interrupted upload!")
        self.results["integrity"] = True

    def test_4_resume_after_client_restart(self, test_file2):
        """Simulate client crash/restart, then resume."""
        banner("TEST 4: Resume After Client 'Crash' & Restart")

        from etransfer.client.tus_client import EasyTransferClient

        # Upload 40% then "crash"
        client1 = EasyTransferClient(self.server_url, token=self.token)
        uploader1 = client1.create_uploader(test_file2, chunk_size=self.chunk_size)
        stop_at = (self.file2_size * 4 // 10 // self.chunk_size) * self.chunk_size
        url = uploader1.upload(stop_at=stop_at)
        file_id = url.rstrip("/").split("/")[-1]
        print(f"  Uploaded 40% ({fmt_size(stop_at)}), then 'crashed'")
        client1.close()

        # "Restart" - new client instance, resume from URL
        time.sleep(1)
        client2 = EasyTransferClient(self.server_url, token=self.token)
        uploader2 = client2.create_uploader(test_file2, chunk_size=self.chunk_size)
        uploader2.set_url(url)

        t0 = time.time()
        uploader2.upload()  # Resume from server's offset
        elapsed = time.time() - t0

        print(f"  Resumed and completed in {elapsed:.2f}s")

        # Verify
        with get_http(self.server_url, self.token) as http:
            r = http.get(f"/api/files/{file_id}")
            info = r.json()
            print(f"  Status: {info['status']}")
            assert info["status"] == "complete"

        # Verify integrity
        import httpx

        with httpx.stream(
            "GET",
            f"{self.server_url}/api/files/{file_id}/download",
            headers={"X-API-Token": self.token},
            timeout=60.0,
        ) as r:
            h = hashlib.md5()
            for chunk in r.iter_bytes(chunk_size=1024 * 1024):
                h.update(chunk)

        expected_md5 = md5_file(test_file2)
        assert h.hexdigest() == expected_md5, "MD5 mismatch after restart resume!"
        print(f"  MD5 verified: {h.hexdigest()}")

        # Cleanup
        with get_http(self.server_url, self.token) as http:
            http.delete(f"/api/files/{file_id}")
        client2.close()

        print("  PASSED - Resume after client restart works!")
        self.results["restart_resume"] = True

    # ─── Run ────────────────────────────────────────────────────────────

    def run(self):
        banner("Storage Quota + Auto-Resume Test Suite")
        print(f"  Quota: {fmt_size(self.quota_size)}")
        print(f"  File 1: {fmt_size(self.file1_size)}")
        print(f"  File 2: {fmt_size(self.file2_size)}")
        print(f"  Chunk: {fmt_size(self.chunk_size)}")

        # Start server
        banner("SETUP: Starting Server with Quota")
        if not self._start_local_server():
            return False

        # Create test files
        f1 = tempfile.NamedTemporaryFile(suffix="_file1.bin", delete=False)
        f2 = tempfile.NamedTemporaryFile(suffix="_file2.bin", delete=False)
        f1.close()
        f2.close()

        try:
            banner("SETUP: Creating Test Files")
            print(f"  File 1: {fmt_size(self.file1_size)}")
            md5_1 = create_test_file(f1.name, self.file1_size)
            print(f"    MD5: {md5_1}")
            print(f"  File 2: {fmt_size(self.file2_size)}")
            md5_2 = create_test_file(f2.name, self.file2_size)
            print(f"    MD5: {md5_2}")

            # Test 0: Check quota
            self.test_0_check_quota()

            # Test 1: Upload first file
            file1_id = self.test_1_upload_first_file(f1.name)

            # Test 2: Second upload hits quota, polls, resumes after cleanup
            file2_id = self.test_2_quota_exceeded_and_resume(f2.name, file1_id)

            # Test 3: Verify integrity after resume
            self.test_3_verify_integrity(file2_id, md5_2, self.file2_size)

            # Cleanup file2 for next test
            with get_http(self.server_url, self.token) as http:
                http.delete(f"/api/files/{file2_id}")

            # Test 4: Resume after client crash/restart
            self.test_4_resume_after_client_restart(f2.name)

        finally:
            os.unlink(f1.name)
            os.unlink(f2.name)
            self._stop_local_server()

        # Summary
        banner("TEST SUMMARY")
        test_names = {
            "check_quota": "Storage Quota Configuration",
            "upload_first": "Upload First File (within quota)",
            "quota_resume": "Quota Exceeded -> Poll -> Resume",
            "integrity": "Integrity After Quota Resume",
            "restart_resume": "Resume After Client Restart",
        }
        total = len(test_names)
        passed = sum(1 for k in test_names if self.results.get(k))
        for key, name in test_names.items():
            status = "PASS" if self.results.get(key) else "FAIL"
            print(f"  [{status}] {name}")
        print(f"\n  Result: {passed}/{total} passed")
        return passed == total


def main():
    parser = argparse.ArgumentParser(description="Storage quota + resume test")
    parser.add_argument("--server", default=DEFAULT_SERVER_URL)
    parser.add_argument("--token", default=DEFAULT_TOKEN)
    args = parser.parse_args()

    test = QuotaResumeTest(args.server, args.token)
    success = test.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
