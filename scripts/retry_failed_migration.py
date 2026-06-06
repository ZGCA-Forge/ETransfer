from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import redis.asyncio as redis

from etransfer.server.routes import migrations

LOGGER = logging.getLogger("retry_failed_migration")
DEFAULT_REDIS_URL = "redis://localhost:6379/0"


class NoopState:
    async def set(self, *args: Any, **kwargs: Any) -> bool:
        return True


class NoopTaskManager:
    pass


def utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")


def existing_success_keys(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {str(record["source_key"]) for record in load_jsonl(path) if record.get("source_key")}


async def load_job(redis_client: redis.Redis, job_id: str) -> dict[str, Any]:
    raw = await redis_client.get(f"et:migration:{job_id}")
    if not raw:
        raise RuntimeError(f"Redis migration job not found: {job_id}")
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return json.loads(raw)


async def save_state(redis_client: redis.Redis, state_key: str, state: dict[str, Any]) -> None:
    state["updated_at"] = utc_now()
    await redis_client.set(state_key, json.dumps(state, ensure_ascii=False, separators=(",", ":")))


async def retry_one(
    controller: migrations.BucketMigrationController,
    s3_clients: dict[str, Any],
    jobs: dict[str, dict[str, Any]],
    primary_job: dict[str, Any],
    record: dict[str, Any],
    retry_run_id: str,
    success_log: Path,
    retry_failed_log: Path,
    max_attempts: int,
    default_owner_email: str,
) -> tuple[bool, int]:
    source_key = str(record["source_key"])
    size = int(record.get("size") or 0)
    original_job_id = str(record.get("job_id") or primary_job["job_id"])
    job = jobs.get(original_job_id) or primary_job
    source_config = job["source_config"]
    target_config = dict(job["target_config"])
    owner_email = str(record.get("owner_email") or job.get("owner_email") or default_owner_email)
    owner = SimpleNamespace(
        id=job.get("owner_id"),
        email=owner_email,
        username=owner_email.split("@", 1)[0],
        role="user",
    )
    expected_target_key = str(record.get("target_key") or "")
    actual_target_key = migrations.BucketMigrationController._target_log_key(source_key, target_config, owner)
    if expected_target_key and expected_target_key != actual_target_key:
        LOGGER.warning(
            "Target key mismatch for %s: failed_log=%s computed=%s",
            source_key,
            expected_target_key,
            actual_target_key,
        )

    last_error: Exception | None = None
    started_at = time.monotonic()
    for attempt in range(1, max_attempts + 1):
        try:
            source_url = await asyncio.to_thread(
                migrations._presign_get_object,
                s3_clients[original_job_id],
                source_config["bucket"],
                source_key,
                migrations._MAX_PRESIGN_EXPIRES,
            )
            await controller._copy_object_to_sink(
                source_url,
                source_key,
                size,
                target_config,
                owner,
                retry_run_id,
            )
            elapsed = round(time.monotonic() - started_at, 3)
            append_jsonl(
                success_log,
                {
                    "completed_at": utc_now(),
                    "retry_run_id": retry_run_id,
                    "original_job_id": original_job_id,
                    "owner_email": owner_email,
                    "source_bucket": source_config["bucket"],
                    "target_bucket": target_config["bucket"],
                    "source_key": source_key,
                    "target_key": actual_target_key,
                    "size": size,
                    "attempts": attempt,
                    "elapsed_seconds": elapsed,
                    "original_failed_at": record.get("failed_at"),
                    "original_error_type": record.get("error_type"),
                },
            )
            return True, size
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_error = exc
            LOGGER.warning(
                "Retry object failed: key=%s attempt=%d/%d error=%r",
                source_key,
                attempt,
                max_attempts,
                exc,
            )
            if attempt < max_attempts:
                await asyncio.sleep(min(2 ** (attempt - 1), 10))

    elapsed = round(time.monotonic() - started_at, 3)
    append_jsonl(
        retry_failed_log,
        {
            "failed_at": utc_now(),
            "retry_run_id": retry_run_id,
            "original_job_id": original_job_id,
            "owner_email": owner_email,
            "source_bucket": source_config["bucket"],
            "target_bucket": target_config["bucket"],
            "source_key": source_key,
            "target_key": actual_target_key,
            "size": size,
            "attempts": max_attempts,
            "elapsed_seconds": elapsed,
            "original_failed_at": record.get("failed_at"),
            "original_error_type": record.get("error_type"),
            "error_type": type(last_error).__name__ if last_error else "Unknown",
            "error": repr(last_error),
        },
    )
    return False, 0


async def run(args: argparse.Namespace) -> None:
    failed_log = Path(args.failed_log)
    if not failed_log.exists():
        raise FileNotFoundError(failed_log)

    run_id = args.run_id or datetime.now(UTC).strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8]
    label = args.label.strip() or "retry"
    success_log = Path(args.success_log or f"local_deploy/bucket_migration_retry_success__{label}__{run_id}.jsonl")
    retry_failed_log = Path(
        args.retry_failed_log or f"local_deploy/bucket_migration_retry_failed__{label}__{run_id}.jsonl"
    )
    progress_log = Path(args.progress_log or f"local_deploy/bucket_migration_retry_progress__{label}__{run_id}.jsonl")
    state_key = f"et:migration_retry:{label}_failed:{run_id}"

    all_records = load_jsonl(failed_log)
    deduped_by_key = {str(record["source_key"]): record for record in all_records if record.get("source_key")}
    deduped = list(deduped_by_key.values())
    already_done = existing_success_keys(success_log) if args.resume else set()
    records = [record for record in deduped if str(record["source_key"]) not in already_done]
    if args.limit > 0:
        records = records[: args.limit]

    redis_client = redis.from_url(args.redis_url, decode_responses=True)
    primary_job = await load_job(redis_client, args.primary_job_id)
    job_ids = {str(record.get("job_id") or args.primary_job_id) for record in records}
    for job_id in (args.primary_job_id, args.legacy_job_id):
        if job_id:
            job_ids.add(job_id)
    jobs = {job_id: await load_job(redis_client, job_id) for job_id in job_ids}
    s3_clients = {
        job_id: migrations._build_s3_client(migrations.BucketSourceConfig.model_validate(job["source_config"]))
        for job_id, job in jobs.items()
    }

    migrations._SOURCE_RANGE_CONCURRENCY = args.range_concurrency
    migrations._SOURCE_RANGE_GLOBAL_CONCURRENCY = args.global_range_concurrency
    migrations._SOURCE_RANGE_MAX_ATTEMPTS = args.range_max_attempts
    migrations._SOURCE_CONNECT_TIMEOUT_SECONDS = args.source_connect_timeout
    migrations._SOURCE_READ_TIMEOUT_SECONDS = args.source_read_timeout
    migrations._SOURCE_POOL_TIMEOUT_SECONDS = args.source_pool_timeout
    controller = migrations.BucketMigrationController(
        NoopState(),
        NoopTaskManager(),
        object_log_path=success_log,
        failed_object_log_path=retry_failed_log,
    )

    state = {
        "retry_run_id": run_id,
        "status": "running",
        "source_failed_log": str(failed_log),
        "success_log": str(success_log),
        "failed_log": str(retry_failed_log),
        "progress_log": str(progress_log),
        "total_failed_lines": len(all_records),
        "total_unique_objects": len(deduped),
        "skipped_already_success": len(already_done),
        "limit": args.limit,
        "pending_objects": len(records),
        "completed_objects": 0,
        "failed_objects": 0,
        "completed_bytes": 0,
        "started_at": utc_now(),
    }
    await save_state(redis_client, state_key, state)
    LOGGER.info(
        "Retry run %s: records=%d unique=%d pending=%d success_log=%s failed_log=%s redis_key=%s",
        run_id,
        len(all_records),
        len(deduped),
        len(records),
        success_log,
        retry_failed_log,
        state_key,
    )
    if args.dry_run:
        state["status"] = "dry_run"
        state["finished_at"] = utc_now()
        await save_state(redis_client, state_key, state)
        await redis_client.aclose()
        return

    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    for record in records:
        queue.put_nowait(record)

    lock = asyncio.Lock()

    async def worker(worker_id: int) -> None:
        while True:
            try:
                record = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                ok, copied_bytes = await retry_one(
                    controller,
                    s3_clients,
                    jobs,
                    primary_job,
                    record,
                    run_id,
                    success_log,
                    retry_failed_log,
                    args.max_attempts,
                    args.owner_email,
                )
                async with lock:
                    if ok:
                        state["completed_objects"] += 1
                        state["completed_bytes"] += copied_bytes
                    else:
                        state["failed_objects"] += 1
                    state["pending_objects"] = queue.qsize()
                    append_jsonl(
                        progress_log,
                        {
                            "at": utc_now(),
                            "retry_run_id": run_id,
                            "worker_id": worker_id,
                            "completed_objects": state["completed_objects"],
                            "failed_objects": state["failed_objects"],
                            "pending_objects": state["pending_objects"],
                            "completed_bytes": state["completed_bytes"],
                        },
                    )
                    if (state["completed_objects"] + state["failed_objects"]) % args.state_every == 0:
                        await save_state(redis_client, state_key, state)
            finally:
                queue.task_done()

    try:
        await asyncio.gather(*(worker(i + 1) for i in range(args.concurrency)))
        state["status"] = "completed" if state["failed_objects"] == 0 else "completed_with_failures"
    except asyncio.CancelledError:
        state["status"] = "cancelled"
        raise
    except Exception as exc:
        state["status"] = "failed"
        state["error"] = repr(exc)
        raise
    finally:
        state["finished_at"] = utc_now()
        state["pending_objects"] = queue.qsize()
        await save_state(redis_client, state_key, state)
        await redis_client.aclose()
        LOGGER.info("Retry run %s finished with status=%s", run_id, state["status"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Retry failed bucket migration objects.")
    parser.add_argument("--failed-log", required=True)
    parser.add_argument("--redis-url", default=DEFAULT_REDIS_URL)
    parser.add_argument("--primary-job-id", required=True)
    parser.add_argument("--legacy-job-id", default="")
    parser.add_argument("--owner-email", required=True)
    parser.add_argument("--label", default="retry")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--success-log", default="")
    parser.add_argument("--retry-failed-log", default="")
    parser.add_argument("--progress-log", default="")
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--range-concurrency", type=int, default=1)
    parser.add_argument("--global-range-concurrency", type=int, default=4)
    parser.add_argument("--range-max-attempts", type=int, default=3)
    parser.add_argument("--source-connect-timeout", type=float, default=5)
    parser.add_argument("--source-read-timeout", type=float, default=10)
    parser.add_argument("--source-pool-timeout", type=float, default=5)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--state-every", type=int, default=20)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(run(parse_args()))


if __name__ == "__main__":
    main()
