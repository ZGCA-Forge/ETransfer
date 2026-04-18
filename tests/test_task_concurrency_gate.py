"""Tests for the dynamic task concurrency gate.

Covers:

- The gate caps concurrent acquires at the configured limit.
- ``set_limit`` immediately wakes parked waiters when raised.
- ``set_limit`` lowers the cap without disturbing in-flight tasks.
- ``ServerSettings.max_concurrent_tasks`` is hot-reloadable and the YAML
  ``tasks.max_concurrent`` key is parsed correctly.
- ``GET /api/info`` exposes the live cap.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import pytest
import yaml

from etransfer.server.config import (
    HOT_RELOADABLE_FIELDS,
    ServerSettings,
    _parse_yaml_to_settings_dict,
    reload_hot_settings,
)
from etransfer.server.tasks.manager import _DynamicTaskGate


# ── Gate behaviour ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_gate_caps_concurrent_acquires() -> None:
    gate = _DynamicTaskGate(2)
    inside = 0
    peak = 0
    started = asyncio.Event()
    release = asyncio.Event()

    async def worker() -> None:
        nonlocal inside, peak
        async with gate:
            inside += 1
            peak = max(peak, inside)
            started.set()
            await release.wait()
            inside -= 1

    workers = [asyncio.create_task(worker()) for _ in range(5)]
    await started.wait()
    await asyncio.sleep(0.05)
    assert peak == 2

    release.set()
    await asyncio.gather(*workers)
    assert peak == 2


@pytest.mark.asyncio
async def test_gate_raise_limit_wakes_waiters() -> None:
    gate = _DynamicTaskGate(1)
    seen: list[int] = []
    release = asyncio.Event()

    async def worker(i: int) -> None:
        async with gate:
            seen.append(i)
            await release.wait()

    tasks = [asyncio.create_task(worker(i)) for i in range(3)]
    await asyncio.sleep(0.05)
    assert seen == [0]

    gate.set_limit(3)
    await asyncio.sleep(0.05)
    assert sorted(seen) == [0, 1, 2]

    release.set()
    await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_gate_lower_limit_does_not_evict_in_flight() -> None:
    gate = _DynamicTaskGate(3)
    release = asyncio.Event()
    active = 0
    peak = 0

    async def worker() -> None:
        nonlocal active, peak
        async with gate:
            active += 1
            peak = max(peak, active)
            await release.wait()
            active -= 1

    tasks = [asyncio.create_task(worker()) for _ in range(3)]
    await asyncio.sleep(0.05)
    assert active == 3

    gate.set_limit(1)
    await asyncio.sleep(0.05)
    assert active == 3

    release.set()
    await asyncio.gather(*tasks)
    assert peak == 3
    assert gate.limit == 1


# ── Config plumbing ─────────────────────────────────────────────


def test_max_concurrent_tasks_default_and_yaml_parsing() -> None:
    settings = ServerSettings()
    assert settings.max_concurrent_tasks == 50

    parsed = _parse_yaml_to_settings_dict({"tasks": {"max_concurrent": 73}})
    assert parsed["max_concurrent_tasks"] == 73


def test_max_concurrent_tasks_is_hot_reloadable(tmp_path: Path) -> None:
    assert "max_concurrent_tasks" in HOT_RELOADABLE_FIELDS

    cfg = tmp_path / "server.yaml"
    cfg.write_text(yaml.safe_dump({"tasks": {"max_concurrent": 12}}), encoding="utf-8")

    settings = ServerSettings()
    settings._config_path = cfg  # type: ignore[attr-defined]

    changes = reload_hot_settings(settings)
    assert changes.get("max_concurrent_tasks") == (50, 12)
    assert settings.max_concurrent_tasks == 12


# ── End-to-end: /api/info exposes the live cap ──────────────────


def test_info_endpoint_exposes_max_concurrent(memory_server: dict) -> None:
    body = httpx.get(f"{memory_server['url']}/api/info", timeout=5).json()
    assert body["max_concurrent_tasks"] == memory_server["settings"].max_concurrent_tasks
