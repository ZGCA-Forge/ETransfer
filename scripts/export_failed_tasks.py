"""Export failed transfer tasks from Redis to JSON/CSV/Markdown.

Usage:
    python scripts/export_failed_tasks.py [--out-dir cursor_docs] [--redis-url redis://localhost:6379/0]

Produces three artifacts named with the current timestamp prefix:
    <ts> failed_tasks.json   full task records
    <ts> failed_tasks.csv    flat table for spreadsheets
    <ts> failed_tasks.md     human-readable summary
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

try:
    import redis
except ImportError:
    sys.exit("redis-py not installed. Run: pip install redis")


_TASK_PREFIX = "et:task:"
_CSV_FIELDS = [
    "task_id",
    "status",
    "source_plugin",
    "sink_plugin",
    "filename",
    "file_size",
    "owner_id",
    "retention",
    "progress",
    "downloaded_bytes",
    "pushed_parts",
    "created_at",
    "updated_at",
    "completed_at",
    "source_url",
    "error",
    "superseded_by",
]


def _lookup_owner_map_redis(client: redis.Redis) -> dict[int, str]:
    """Best-effort map owner_id -> display name from Redis user records."""
    mapping: dict[int, str] = {}
    for key in client.scan_iter(match="et:user:*", count=500):
        raw = client.get(key)
        if not raw:
            continue
        try:
            u = json.loads(raw)
        except json.JSONDecodeError:
            continue
        uid = u.get("id")
        if uid is None:
            continue
        label = u.get("email") or u.get("username") or u.get("display_name") or f"#{uid}"
        mapping[int(uid)] = str(label)
    return mapping


def _lookup_owner_map_mysql(dsn: str) -> dict[int, str]:
    """Map owner_id -> display name from MySQL `users` table.

    DSN format: mysql://user:pass@host:port/db
    """
    try:
        from urllib.parse import urlparse

        import pymysql  # type: ignore
    except ImportError:
        return {}

    u = urlparse(dsn)
    try:
        conn = pymysql.connect(
            host=u.hostname or "127.0.0.1",
            port=u.port or 3306,
            user=u.username or "",
            password=u.password or "",
            database=(u.path or "/").lstrip("/"),
            charset="utf8mb4",
            connect_timeout=5,
        )
    except Exception:
        return {}
    mapping: dict[int, str] = {}
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, username, display_name, email FROM users")
            for uid, username, display, email in cur.fetchall():
                mapping[int(uid)] = display or username or email or f"#{uid}"
    finally:
        conn.close()
    return mapping


def _collect_failed(client: redis.Redis) -> list[dict]:
    failed: list[dict] = []
    for key in client.scan_iter(match=f"{_TASK_PREFIX}*", count=500):
        raw = client.get(key)
        if not raw:
            continue
        try:
            task = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if task.get("status") == "failed":
            failed.append(task)
    failed.sort(key=lambda t: t.get("updated_at") or "", reverse=True)
    return failed


def _write_json(path: Path, tasks: list[dict]) -> None:
    path.write_text(json.dumps(tasks, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path: Path, tasks: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for t in tasks:
            row = {k: t.get(k, "") for k in _CSV_FIELDS}
            if isinstance(row.get("error"), str):
                row["error"] = row["error"].replace("\n", " ").strip()
            writer.writerow(row)


def _fmt_size(n: int | None) -> str:
    if not n:
        return "-"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _write_markdown(path: Path, tasks: list[dict], owners: dict[int, str]) -> None:
    total = len(tasks)
    plugin_ct = Counter(t.get("source_plugin") or "-" for t in tasks)
    owner_ct = Counter(t.get("owner_id") for t in tasks)
    error_ct = Counter((t.get("error") or "").split(":", 1)[0][:80] for t in tasks)

    lines: list[str] = []
    lines.append(f"# 失败任务导出 ({total} 条)")
    lines.append("")
    lines.append(f"- 导出时间: {datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"- 数据来源: Redis `{_TASK_PREFIX}*`")
    lines.append("")

    lines.append("## 分布统计")
    lines.append("")
    lines.append("### 按 source_plugin")
    lines.append("")
    lines.append("| 插件 | 数量 |")
    lines.append("| --- | ---: |")
    for name, count in plugin_ct.most_common():
        lines.append(f"| {name} | {count} |")
    lines.append("")

    lines.append("### 按 owner")
    lines.append("")
    lines.append("| owner_id | 用户 | 数量 |")
    lines.append("| ---: | --- | ---: |")
    for oid, count in owner_ct.most_common():
        name = owners.get(oid, "-") if oid is not None else "API/匿名"
        lines.append(f"| {oid if oid is not None else '-'} | {name} | {count} |")
    lines.append("")

    lines.append("### 按错误前缀")
    lines.append("")
    lines.append("| 错误摘要 | 数量 |")
    lines.append("| --- | ---: |")
    for err, count in error_ct.most_common():
        lines.append(f"| {err or '(空)'} | {count} |")
    lines.append("")

    lines.append("## 明细")
    lines.append("")
    lines.append("| updated_at | task_id | owner | 插件 | 文件名 | 大小 | 错误 |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- |")
    for t in tasks:
        tid = (t.get("task_id") or "")[:8]
        owner = owners.get(t.get("owner_id"), "-") if t.get("owner_id") is not None else "-"
        fname = (t.get("filename") or "-").replace("|", "\\|")
        size = _fmt_size(t.get("file_size") or 0)
        err = (t.get("error") or "").replace("\n", " ").replace("|", "\\|")[:140]
        lines.append(
            f"| {t.get('updated_at', '')[:19]} | `{tid}` | {owner} | "
            f"{t.get('source_plugin', '')}→{t.get('sink_plugin', '') or '-'} | {fname} | {size} | {err} |"
        )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--redis-url", default="redis://localhost:6379/0")
    parser.add_argument(
        "--mysql-dsn",
        default="",
        help="Optional MySQL DSN for owner name lookup, e.g. mysql://user:pass@host:3306/et",
    )
    parser.add_argument(
        "--out-dir",
        default="cursor_docs",
        help="Output directory (relative to project root).",
    )
    args = parser.parse_args()

    client = redis.Redis.from_url(args.redis_url, decode_responses=True)
    client.ping()

    tasks = _collect_failed(client)
    owners = _lookup_owner_map_mysql(args.mysql_dsn) if args.mysql_dsn else {}
    if not owners:
        owners = _lookup_owner_map_redis(client)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d %H-%M-%S")

    json_path = out_dir / f"{ts} failed_tasks.json"
    csv_path = out_dir / f"{ts} failed_tasks.csv"
    md_path = out_dir / f"{ts} failed_tasks.md"

    _write_json(json_path, tasks)
    _write_csv(csv_path, tasks)
    _write_markdown(md_path, tasks, owners)

    print(f"Exported {len(tasks)} failed task(s):")
    for p in (json_path, csv_path, md_path):
        print(f"  - {p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
