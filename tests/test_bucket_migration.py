from __future__ import annotations

import asyncio
import fnmatch
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from etransfer.server.routes import migrations
from etransfer.server.tasks.models import TaskStatus


class FakeS3Client:
    def __init__(self) -> None:
        self.list_calls: list[dict[str, Any]] = []
        self.presign_calls: list[dict[str, Any]] = []

    def list_objects(self, **kwargs: Any) -> dict[str, Any]:
        self.list_calls.append(kwargs)
        if "Marker" not in kwargs:
            return {
                "IsTruncated": True,
                "NextMarker": "datasets/a.txt",
                "Contents": [
                    {"Key": "datasets/", "Size": 0},
                    {"Key": "datasets/a.txt", "Size": 10},
                ],
            }
        return {
            "IsTruncated": False,
            "Contents": [
                {"Key": "datasets/b.bin", "Size": 20},
            ],
        }

    def generate_presigned_url(self, operation: str, Params: dict[str, str], ExpiresIn: int) -> str:
        self.presign_calls.append({"operation": operation, "params": Params, "expires": ExpiresIn})
        return f"https://source.example/{Params['Bucket']}/{Params['Key']}?signed=1"


class DummyTaskManager:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def create_task(self, **kwargs: Any) -> SimpleNamespace:
        self.calls.append(kwargs)
        return SimpleNamespace(task_id=f"task-{len(self.calls)}", filename=kwargs["filename"])


class ControllerTaskManager:
    def __init__(self) -> None:
        self.tasks: dict[str, SimpleNamespace] = {}
        self.deleted: list[str] = []
        self.cancelled: list[str] = []

    async def create_task(self, **kwargs: Any) -> SimpleNamespace:
        task_id = f"task-{len(self.tasks) + 1}"
        task = SimpleNamespace(task_id=task_id, filename=kwargs["filename"], status=TaskStatus.PENDING)
        self.tasks[task_id] = task
        return task

    async def get_task(self, task_id: str) -> SimpleNamespace | None:
        return self.tasks.get(task_id)

    async def delete_task_state(self, task_id: str) -> bool:
        if task_id in self.tasks:
            self.deleted.append(task_id)
            del self.tasks[task_id]
            return True
        return False

    async def cancel_task(self, task_id: str) -> bool:
        task = self.tasks.get(task_id)
        if task is None:
            return False
        task.status = TaskStatus.CANCELLED
        self.cancelled.append(task_id)
        return True


class DictState:
    def __init__(self) -> None:
        self.data: dict[str, str] = {}

    async def get(self, key: str) -> str | None:
        return self.data.get(key)

    async def set(self, key: str, value: str, ex: int | None = None, nx: bool = False) -> bool:
        if nx and key in self.data:
            return False
        self.data[key] = value
        return True

    async def delete(self, key: str) -> int:
        existed = key in self.data
        self.data.pop(key, None)
        return int(existed)

    async def keys(self, pattern: str) -> list[str]:
        return [key for key in self.data if fnmatch.fnmatch(key, pattern)]


class PagedS3Client:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def list_objects(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        marker = kwargs.get("Marker", "")
        if not marker:
            return {
                "IsTruncated": True,
                "NextMarker": "k2",
                "Contents": [
                    {"Key": "k1", "Size": 1},
                    {"Key": "k2", "Size": 2},
                ],
            }
        return {
            "IsTruncated": False,
            "Contents": [
                {"Key": "k3", "Size": 3},
            ],
        }

    def generate_presigned_url(self, operation: str, Params: dict[str, str], ExpiresIn: int) -> str:
        return f"https://source.example/{Params['Key']}?signed=1"


class EmptyUserDB:
    async def list_users(self) -> list[Any]:
        return []


def _client_with_manager(manager: DummyTaskManager) -> TestClient:
    app = FastAPI()
    app.state.task_manager = manager
    app.include_router(migrations.create_migrations_router())
    return TestClient(app)


def _job_request() -> migrations.CreateMigrationJobRequest:
    return migrations.CreateMigrationJobRequest(
        source={
            "endpoint": "https://bucket.storage.example.com",
            "bucket": "bucket",
            "ak": "src-ak",
            "sk": "src-sk",
        },
        target={
            "endpoint": "object-storage.example.com",
            "region": "example-region",
            "bucket": "target",
            "ak": "dst-ak",
            "sk": "dst-sk",
        },
        owner_email="user-a@example.com",
        page_size=2,
        cleanup_completed_tasks=True,
    )


async def _wait_for(predicate, timeout: float = 1.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        value = await predicate()
        if value:
            return value
        await asyncio.sleep(0.01)
    raise TimeoutError("condition not met")


@pytest.mark.asyncio
async def test_migration_controller_pages_and_cleans_child_tasks(monkeypatch, tmp_path: Path) -> None:
    fake_s3 = PagedS3Client()
    state = DictState()
    manager = ControllerTaskManager()
    controller = migrations.BucketMigrationController(
        state,
        manager,
        poll_interval=0.01,
        shutdown_drain_timeout=0.05,
        shutdown_log_interval=0.01,
        object_log_path=tmp_path / "objects.jsonl",
    )
    monkeypatch.setattr(migrations, "_build_s3_client", lambda source: fake_s3)
    copied: list[str] = []

    async def fake_copy(source_url: str, source_key: str, size: int, sink_config: dict[str, str], owner_user: Any, job_id: str) -> None:
        copied.append(source_key)

    monkeypatch.setattr(controller, "_copy_object_to_sink", fake_copy)

    try:
        job = await controller.create_job(_job_request())

        async def completed():
            current = await controller.get_job(job.job_id)
            return current if current and current.status == migrations.MigrationJobStatus.COMPLETED else None

        done = await _wait_for(completed, timeout=8.0)
        assert done.processed_objects == 3
        assert done.processed_bytes == 6
        assert done.pages_completed == 2
        assert done.active_task_ids == []
        assert set(copied) == {"k1", "k2", "k3"}
        assert fake_s3.calls[1]["Marker"] == "k2"
        records = [
            json.loads(line)
            for line in (tmp_path / "objects.jsonl").read_text(encoding="utf-8").splitlines()
        ]
        assert {record["source_key"] for record in records} == {"k1", "k2", "k3"}
        assert {record["target_key"] for record in records} == {
            "user-a/k1",
            "user-a/k2",
            "user-a/k3",
        }
        assert sum(record["size"] for record in records) == 6
    finally:
        await controller.shutdown()


@pytest.mark.asyncio
async def test_migration_controller_pause_and_resume(monkeypatch, tmp_path: Path) -> None:
    fake_s3 = PagedS3Client()
    state = DictState()
    manager = ControllerTaskManager()
    controller = migrations.BucketMigrationController(
        state,
        manager,
        poll_interval=0.01,
        shutdown_drain_timeout=0.05,
        shutdown_log_interval=0.01,
        object_log_path=tmp_path / "objects.jsonl",
    )
    monkeypatch.setattr(migrations, "_build_s3_client", lambda source: fake_s3)
    copy_started = asyncio.Event()
    allow_copy = asyncio.Event()

    async def blocking_copy(source_url: str, source_key: str, size: int, sink_config: dict[str, str], owner_user: Any, job_id: str) -> None:
        copy_started.set()
        await allow_copy.wait()

    monkeypatch.setattr(controller, "_copy_object_to_sink", blocking_copy)

    try:
        job = await controller.create_job(_job_request())
        await asyncio.wait_for(copy_started.wait(), timeout=1)
        await controller.pause_job(job.job_id)
        allow_copy.set()

        async def paused_after_page():
            current = await controller.get_job(job.job_id)
            if current and current.status == migrations.MigrationJobStatus.PAUSED and current.processed_objects == 2:
                return current
            return None

        paused = await _wait_for(paused_after_page)
        assert paused.marker == "k2"
        await controller.resume_job(job.job_id)

        async def completed_or_advanced():
            current = await controller.get_job(job.job_id)
            return current if current and current.status == migrations.MigrationJobStatus.COMPLETED else None

        resumed = await _wait_for(completed_or_advanced)
        assert resumed.processed_objects == 3
    finally:
        await controller.shutdown()


@pytest.mark.asyncio
async def test_migration_controller_shutdown_pauses_and_startup_resumes(monkeypatch, tmp_path: Path) -> None:
    fake_s3 = PagedS3Client()
    state = DictState()
    manager = ControllerTaskManager()
    controller = migrations.BucketMigrationController(
        state,
        manager,
        poll_interval=0.01,
        shutdown_drain_timeout=0.05,
        shutdown_log_interval=0.01,
        object_log_path=tmp_path / "objects.jsonl",
    )
    monkeypatch.setattr(migrations, "_build_s3_client", lambda source: fake_s3)
    copy_started = asyncio.Event()
    allow_copy = asyncio.Event()
    copied: list[str] = []

    async def blocking_copy(source_url: str, source_key: str, size: int, sink_config: dict[str, str], owner_user: Any, job_id: str) -> None:
        copy_started.set()
        await allow_copy.wait()
        copied.append(source_key)

    monkeypatch.setattr(controller, "_copy_object_to_sink", blocking_copy)

    job = await controller.create_job(_job_request())
    await asyncio.wait_for(copy_started.wait(), timeout=1)
    await controller.shutdown()

    paused = await controller.get_job(job.job_id)
    assert paused is not None
    assert paused.status == migrations.MigrationJobStatus.PAUSED
    assert paused.error == "interrupted by shutdown"
    assert paused.marker == ""
    assert paused.active_task_ids == []
    assert paused.current_page_objects == 0

    resumed_controller = migrations.BucketMigrationController(
        state,
        manager,
        poll_interval=0.01,
        object_log_path=tmp_path / "objects.jsonl",
    )

    async def resumed_copy(source_url: str, source_key: str, size: int, sink_config: dict[str, str], owner_user: Any, job_id: str) -> None:
        copied.append(source_key)

    monkeypatch.setattr(resumed_controller, "_copy_object_to_sink", resumed_copy)

    try:
        assert await resumed_controller.resume_interrupted() == 1

        async def completed():
            current = await resumed_controller.get_job(job.job_id)
            return current if current and current.status == migrations.MigrationJobStatus.COMPLETED else None

        done = await _wait_for(completed, timeout=8.0)
        assert done.processed_objects == 3
        assert done.error == ""
        assert set(copied) == {"k1", "k2", "k3"}
    finally:
        await resumed_controller.shutdown()


@pytest.mark.asyncio
async def test_migration_controller_skips_object_after_retries(monkeypatch, tmp_path: Path) -> None:
    fake_s3 = PagedS3Client()
    state = DictState()
    manager = ControllerTaskManager()
    controller = migrations.BucketMigrationController(
        state,
        manager,
        poll_interval=0.01,
        object_log_path=tmp_path / "objects.jsonl",
        failed_object_log_path=tmp_path / "failed_objects.jsonl",
    )
    monkeypatch.setattr(migrations, "_build_s3_client", lambda source: fake_s3)

    async def failing_copy(source_url: str, source_key: str, size: int, sink_config: dict[str, str], owner_user: Any, job_id: str) -> None:
        raise RuntimeError("copy boom")

    monkeypatch.setattr(controller, "_copy_object_to_sink", failing_copy)

    try:
        job = await controller.create_job(_job_request())

        async def completed():
            current = await controller.get_job(job.job_id)
            return current if current and current.status == migrations.MigrationJobStatus.COMPLETED else None

        done = await _wait_for(completed, timeout=8.0)
        assert done.processed_objects == 3
        assert done.processed_bytes == 0
        assert done.pages_completed == 2
        assert done.error == ""
        records = [
            json.loads(line)
            for line in (tmp_path / "failed_objects.jsonl").read_text(encoding="utf-8").splitlines()
        ]
        assert {record["source_key"] for record in records} == {"k1", "k2", "k3"}
        assert all(record["attempts"] == 3 for record in records)
        assert all(record["error_type"] == "RuntimeError" for record in records)
    finally:
        await controller.shutdown()


@pytest.mark.asyncio
async def test_copy_object_uses_direct_put_for_small_objects(monkeypatch) -> None:
    controller = migrations.BucketMigrationController(DictState(), ControllerTaskManager())
    direct_puts: list[dict[str, Any]] = []

    async def fake_direct_put(
        source_url: str,
        sink_config: dict[str, str],
        object_key: str,
        expected_size: int,
    ) -> None:
        direct_puts.append(
            {
                "source_url": source_url,
                "sink_config": sink_config,
                "object_key": object_key,
                "expected_size": expected_size,
            }
        )

    class FailMultipartSink:
        def __init__(self, config: dict[str, str]) -> None:
            raise AssertionError("small objects should not use multipart upload")

    monkeypatch.setattr(controller, "_copy_small_object_to_tos", fake_direct_put)
    monkeypatch.setattr(migrations, "TosSink", FailMultipartSink)

    sink_config = {"endpoint": "object-storage.example.com", "region": "example-region", "bucket": "target"}
    await controller._copy_object_to_sink(
        "https://source.example/small.txt",
        "small.txt",
        migrations._COPY_CHUNK_SIZE,
        sink_config,
        SimpleNamespace(email="user-a@example.com", username="user-a"),
        "job-1",
    )

    assert direct_puts == [
        {
            "source_url": "https://source.example/small.txt",
            "sink_config": sink_config,
            "object_key": "user-a/small.txt",
            "expected_size": migrations._COPY_CHUNK_SIZE,
        }
    ]


@pytest.mark.asyncio
async def test_copy_large_object_downloads_ranges_concurrently_and_uploads_in_order(monkeypatch) -> None:
    controller = migrations.BucketMigrationController(DictState(), ControllerTaskManager())
    first_part = b"a" * migrations._COPY_CHUNK_SIZE
    final_part = b"b" * 5
    stream_calls: list[dict[str, Any]] = []
    sink_instances: list[Any] = []
    first_part_attempts = 0

    class FakeStreamResponse:
        def __init__(self, status_code: int, chunks: list[bytes], error: Exception | None = None) -> None:
            self.status_code = status_code
            self._chunks = chunks
            self._error = error

        async def __aenter__(self) -> "FakeStreamResponse":
            return self

        async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            return None

        def raise_for_status(self) -> None:
            if self.status_code >= 400:
                raise RuntimeError(f"bad status {self.status_code}")

        async def aiter_bytes(self, chunk_size: int) -> Any:
            for chunk in self._chunks:
                yield chunk
            if self._error:
                raise self._error

    class FakeAsyncClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        async def __aenter__(self) -> "FakeAsyncClient":
            return self

        async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            return None

        def stream(self, method: str, url: str, headers: dict[str, str] | None = None) -> FakeStreamResponse:
            nonlocal first_part_attempts
            stream_calls.append({"method": method, "url": url, "headers": headers or {}})
            range_header = (headers or {}).get("Range", "")
            if range_header == f"bytes=0-{migrations._COPY_CHUNK_SIZE - 1}":
                first_part_attempts += 1
                if first_part_attempts == 1:
                    return FakeStreamResponse(206, [first_part[:1024]], RuntimeError("source dropped"))
                return FakeStreamResponse(206, [first_part])
            if range_header == f"bytes={migrations._COPY_CHUNK_SIZE}-{migrations._COPY_CHUNK_SIZE + 4}":
                return FakeStreamResponse(206, [final_part])
            raise AssertionError(f"unexpected range {range_header}")

    class FakeMultipartSink:
        def __init__(self, config: dict[str, str]) -> None:
            self.config = config
            self.uploads: list[tuple[int, bytes]] = []
            self.completed = False
            self.aborted = False
            sink_instances.append(self)

        async def initialize_upload(self, object_key: str, metadata: dict[str, str]) -> str:
            return "upload-1"

        async def upload_part(self, session_id: str, part_number: int, data: bytes) -> Any:
            self.uploads.append((part_number, data))
            return SimpleNamespace(part_number=part_number, etag=f"etag-{part_number}")

        async def complete_upload(self, session_id: str, parts: list[Any]) -> str:
            self.completed = True
            return "tos://target/key"

        async def abort_upload(self, session_id: str) -> None:
            self.aborted = True

    monkeypatch.setattr(migrations.httpx, "AsyncClient", FakeAsyncClient)
    monkeypatch.setattr(migrations, "TosSink", FakeMultipartSink)

    real_sleep = asyncio.sleep

    async def fake_sleep(delay: float) -> None:
        await real_sleep(0)

    monkeypatch.setattr(migrations.asyncio, "sleep", fake_sleep)

    await controller._copy_object_to_sink(
        "https://source.example/large.bin",
        "large.bin",
        len(first_part) + len(final_part),
        {"endpoint": "object-storage.example.com", "region": "example-region", "bucket": "target"},
        SimpleNamespace(email="user-a@example.com", username="user-a"),
        "job-1",
    )

    assert [call["headers"] for call in stream_calls] == [
        {"Range": f"bytes=0-{migrations._COPY_CHUNK_SIZE - 1}"},
        {"Range": f"bytes={migrations._COPY_CHUNK_SIZE}-{migrations._COPY_CHUNK_SIZE + 4}"},
        {"Range": f"bytes=0-{migrations._COPY_CHUNK_SIZE - 1}"},
    ]
    assert len(sink_instances) == 1
    assert sink_instances[0].uploads == [(1, first_part), (2, final_part)]
    assert sink_instances[0].completed is True
    assert sink_instances[0].aborted is False


@pytest.mark.asyncio
async def test_migration_controller_allows_missing_owner_email(monkeypatch, tmp_path: Path) -> None:
    fake_s3 = PagedS3Client()
    state = DictState()
    manager = ControllerTaskManager()
    controller = migrations.BucketMigrationController(
        state,
        manager,
        user_db=EmptyUserDB(),
        poll_interval=0.01,
        object_log_path=tmp_path / "objects.jsonl",
    )
    monkeypatch.setattr(migrations, "_build_s3_client", lambda source: fake_s3)

    try:
        job = await controller.create_job(_job_request())
        assert job.owner_id is None
        assert job.owner_email == "user-a@example.com"
    finally:
        await controller.shutdown()


def test_bucket_endpoint_url_is_normalized_to_region_endpoint() -> None:
    source = migrations.BucketSourceConfig(
        endpoint="https://source-bucket-a.storage.example.com",
        bucket="source-bucket-a",
        ak="ak",
        sk="sk",
    )

    assert migrations._normalize_source_endpoint(source) == "https://storage.example.com"


def test_migration_object_log_stats_counts_completed_records(tmp_path: Path) -> None:
    log_path = tmp_path / "bucket_migration_objects.jsonl"
    log_path.write_text(
        "\n".join(
            [
                json.dumps({"source_key": "a", "target_key": "user-a/a", "size": 10}),
                json.dumps({"source_key": "b", "target_key": "user-a/b", "size": 20}),
                "{bad-json",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / "bucket_migration_objects__user-b__src__dst.jsonl").write_text(
        json.dumps({"source_key": "c", "target_key": "user-b/c", "size": 30}),
        encoding="utf-8",
    )
    (tmp_path / "bucket_migration_failed_objects__user-b__src__dst.jsonl").write_text(
        json.dumps({"source_key": "d", "target_key": "user-b/d", "size": 40}),
        encoding="utf-8",
    )

    assert migrations.read_migration_object_log_stats(log_path) == {
        "migration_object_count": 3,
        "migration_download_bytes": 60,
        "migration_push_bytes": 60,
    }


def test_default_migration_object_log_stats_do_not_scan_jsonl(monkeypatch) -> None:
    monkeypatch.setattr(
        migrations,
        "_scan_migration_object_log_stats",
        lambda log_path: (_ for _ in ()).throw(AssertionError("should not scan production logs")),
    )

    assert migrations.read_migration_object_log_stats() == {
        "migration_object_count": 0,
        "migration_download_bytes": 0,
        "migration_push_bytes": 0,
    }


def test_migration_default_logs_are_split_by_owner_and_bucket() -> None:
    record = {
        "owner_email": "user-b@example.com",
        "source_bucket": "source-bucket-b",
        "target_bucket": "target-bucket-b",
    }

    assert migrations.BucketMigrationController._log_path_for_record(
        Path("local_deploy/bucket_migration_objects.jsonl"),
        record,
    ) == Path("local_deploy/bucket_migration_objects__user-b__source-bucket-b__target-bucket-b.jsonl")
    assert migrations.BucketMigrationController._log_path_for_record(
        Path("local_deploy/bucket_migration_failed_objects.jsonl"),
        record,
    ) == Path("local_deploy/bucket_migration_failed_objects__user-b__source-bucket-b__target-bucket-b.jsonl")


def test_bucket_migration_dry_run_only_lists_source(monkeypatch) -> None:
    fake_s3 = FakeS3Client()
    manager = DummyTaskManager()
    monkeypatch.setattr(migrations, "_build_s3_client", lambda source: fake_s3)

    client = _client_with_manager(manager)
    response = client.post(
        "/api/migrations/bucket",
        json={
            "dry_run": True,
            "source": {
                "endpoint": "https://source-bucket-a.storage.example.com",
                "bucket": "source-bucket-a",
                "ak": "src-ak",
                "sk": "src-sk",
                "prefix": "datasets/",
            },
            "max_objects": 10,
        },
    )

    assert response.status_code == 201, response.text
    body = response.json()
    assert body["dry_run"] is True
    assert body["created_tasks"] == 0
    assert body["total_objects"] == 2
    assert body["next_marker"] == "datasets/a.txt"
    assert body["tasks"][0]["task_id"] == ""
    assert manager.calls == []
    assert fake_s3.presign_calls == []
    assert "src-ak" not in response.text
    assert "src-sk" not in response.text


def test_bucket_migration_expands_source_bucket(monkeypatch) -> None:
    fake_s3 = FakeS3Client()
    manager = DummyTaskManager()
    monkeypatch.setattr(migrations, "_build_s3_client", lambda source: fake_s3)

    client = _client_with_manager(manager)
    response = client.post(
        "/api/migrations/bucket",
        json={
            "source": {
                "endpoint": "storage.example.com",
                "bucket": "source-bucket",
                "ak": "src-ak",
                "sk": "src-sk",
                "region": "us-east-1",
                "prefix": "datasets/",
                "secure": True,
            },
            "target": {
                "endpoint": "https://object-storage.example.com",
                "region": "example-region",
                "bucket": "target-bucket",
                "ak": "dst-ak",
                "sk": "dst-sk",
                "prefix": "backup",
            },
            "max_objects": 10,
            "presign_expires_seconds": 3600,
        },
    )

    assert response.status_code == 201, response.text
    body = response.json()
    assert body["batch"] is True
    assert body["total_objects"] == 2
    assert body["created_tasks"] == 2
    assert body["total_size"] == 30
    assert body["tasks"][0]["key"] == "datasets/a.txt"
    assert body["tasks"][1]["key"] == "datasets/b.bin"
    assert body["errors"] == []
    assert "src-ak" not in response.text
    assert "src-sk" not in response.text
    assert "dst-ak" not in response.text
    assert "dst-sk" not in response.text

    assert [call["filename"] for call in manager.calls] == ["datasets/a.txt", "datasets/b.bin"]
    assert all(call["sink_plugin"] == "tos" for call in manager.calls)
    assert manager.calls[0]["sink_config"] == {
        "endpoint": "object-storage.example.com",
        "region": "example-region",
        "bucket": "target-bucket",
        "ak": "dst-ak",
        "sk": "dst-sk",
        "prefix": "backup",
    }
    assert fake_s3.list_calls[0]["Prefix"] == "datasets/"
    assert fake_s3.list_calls[1]["Marker"] == "datasets/a.txt"
    assert fake_s3.presign_calls[0]["expires"] == 3600


def test_bucket_migration_reports_per_object_create_errors(monkeypatch) -> None:
    fake_s3 = FakeS3Client()
    manager = DummyTaskManager()

    async def failing_create_task(**kwargs: Any) -> SimpleNamespace:
        if kwargs["filename"].endswith("b.bin"):
            raise RuntimeError("boom")
        return await DummyTaskManager.create_task(manager, **kwargs)

    manager.create_task = failing_create_task  # type: ignore[method-assign]
    monkeypatch.setattr(migrations, "_build_s3_client", lambda source: fake_s3)

    client = _client_with_manager(manager)
    response = client.post(
        "/api/migrations/bucket",
        json={
            "source": {
                "endpoint": "http://internal-storage.example.com",
                "bucket": "source-bucket",
                "ak": "src-ak",
                "sk": "src-sk",
            },
            "target": {
                "endpoint": "object-storage.example.com",
                "region": "example-region",
                "bucket": "target-bucket",
                "ak": "dst-ak",
                "sk": "dst-sk",
            },
        },
    )

    assert response.status_code == 201, response.text
    body = response.json()
    assert body["created_tasks"] == 1
    assert body["errors"] == [{"key": "datasets/b.bin", "error": "boom"}]
