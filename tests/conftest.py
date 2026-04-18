"""Shared pytest fixtures for the CLI / integration tests.

Provides:

- ``free_port``               — grab an unused TCP port.
- ``fake_bucket_server``      — run the in-memory S3 mock on its own port.
- ``memory_server``           — run the real EasyTransfer server in-process,
                                  using ``state_backend="memory"``, with the
                                  ``FakeBucketSink`` plugin registered and
                                  preset wired up to ``fake_bucket_server``.
- ``cli_env``                 — isolate ``~/.etransfer`` into a tmp dir and
                                  pre-populate it with the test server URL +
                                  auth token.
- ``cli_runner``              — a Typer ``CliRunner`` bound to the CLI app.

Any ``ETRANSFER_*`` environment variables present on the developer's machine
are stripped for the test session so they cannot leak into the server
settings or CLI.
"""

from __future__ import annotations

import contextlib
import os
import socket
import threading
import time
from pathlib import Path
from typing import Iterator

import httpx
import pytest
import uvicorn
from typer.testing import CliRunner

# Strip user-level ETRANSFER_* env vars *before* any etransfer import, so they
# don't pollute ServerSettings during tests.
for _k in list(os.environ):
    if _k.startswith("ETRANSFER_"):
        os.environ.pop(_k, None)

from etransfer.client import cli as cli_module
from etransfer.plugins.registry import plugin_registry
from etransfer.server.config import ServerSettings
from etransfer.server.main import create_app

from tests._fake_bucket import create_fake_bucket_app
from tests._fake_bucket_sink import FakeBucketSink


TEST_TOKEN = "test-token-12345"
TEST_BUCKET = "test-bucket"


# ── low-level helpers ──────────────────────────────────────────


def _free_port() -> int:
    """Return an unused TCP port on localhost."""
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _wait_for_port(host: str, port: int, timeout: float = 10.0) -> None:
    """Block until ``host:port`` accepts TCP connections, or raise."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"Server at {host}:{port} did not become ready")


class _ThreadedServer:
    """uvicorn.Server wrapper that runs on a daemon thread."""

    def __init__(self, app, host: str, port: int) -> None:
        cfg = uvicorn.Config(app, host=host, port=port, log_level="warning", access_log=False)
        self.server = uvicorn.Server(cfg)
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.host = host
        self.port = port

    def start(self) -> None:
        self.thread.start()
        _wait_for_port(self.host, self.port)

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=5)


# ── fixtures ───────────────────────────────────────────────────


@pytest.fixture(scope="session")
def free_port() -> int:
    return _free_port()


@pytest.fixture(scope="session")
def fake_bucket_server() -> Iterator[dict]:
    """Run the fake-bucket S3 mock on its own port for the whole test session."""
    port = _free_port()
    app = create_fake_bucket_app()
    srv = _ThreadedServer(app, "127.0.0.1", port)
    srv.start()
    base_url = f"http://127.0.0.1:{port}"
    try:
        yield {"url": base_url, "app": app}
    finally:
        srv.stop()


@pytest.fixture()
def memory_server(tmp_path: Path, fake_bucket_server: dict) -> Iterator[dict]:
    """Run the real EasyTransfer server backed by the memory state backend.

    One fresh instance per test, on its own port.  Returns a dict with
    ``url``, ``token``, ``storage_path`` and ``settings``.
    """
    # Register the fake-bucket sink programmatically.  ``discover()`` is
    # idempotent and only scans entry_points, so our manual registration
    # survives.
    plugin_registry.register_sink(FakeBucketSink)

    storage = tmp_path / "server-storage"
    storage.mkdir()

    port = _free_port()
    settings = ServerSettings(
        host="127.0.0.1",
        port=port,
        state_backend="memory",
        storage_path=storage,
        auth_enabled=True,
        auth_tokens=[TEST_TOKEN],
        user_system_enabled=False,
        allow_permanent_retention=True,
        cleanup_interval=999_999,
        config_watch=False,
        sink_presets={
            "fake_bucket": {
                "default": {
                    "endpoint": fake_bucket_server["url"],
                    "bucket": TEST_BUCKET,
                }
            },
        },
    )

    app = create_app(settings)
    srv = _ThreadedServer(app, "127.0.0.1", port)
    srv.start()

    base_url = f"http://127.0.0.1:{port}"

    # Sanity check
    r = httpx.get(f"{base_url}/api/health", timeout=5)
    r.raise_for_status()

    try:
        yield {
            "url": base_url,
            "token": TEST_TOKEN,
            "storage_path": storage,
            "settings": settings,
            "fake_bucket_url": fake_bucket_server["url"],
        }
    finally:
        srv.stop()


@pytest.fixture()
def cli_env(tmp_path: Path, memory_server: dict, monkeypatch: pytest.MonkeyPatch) -> Iterator[dict]:
    """Isolate ``~/.etransfer`` and pre-load the test server + token."""
    cfg_dir = tmp_path / "dot-etransfer"
    cfg_dir.mkdir()
    cfg_file = cfg_dir / "client.json"

    monkeypatch.setattr(cli_module, "CLIENT_CONFIG_DIR", cfg_dir, raising=True)
    monkeypatch.setattr(cli_module, "CLIENT_CONFIG_FILE", cfg_file, raising=True)

    cli_module._save_client_config(
        {
            "server": memory_server["url"],
            "token": memory_server["token"],
        }
    )

    # Do not let a user's env leak into the CLI either.
    for k in list(os.environ):
        if k.startswith("ETRANSFER_"):
            monkeypatch.delenv(k, raising=False)

    yield {
        "config_dir": cfg_dir,
        "config_file": cfg_file,
        "server": memory_server,
    }


@pytest.fixture()
def cli_runner() -> CliRunner:
    """Typer CliRunner bound to our ``app``."""
    return CliRunner()


@pytest.fixture()
def host_file(fake_bucket_server: dict):
    """Helper: register a file on the fake bucket's HTTP server.

    Returns a callable ``(name, data) -> url``.
    """

    def _host(name: str, data: bytes) -> str:
        url = f"{fake_bucket_server['url']}/hosted/{name}"
        r = httpx.put(url, content=data, timeout=15)
        r.raise_for_status()
        return url

    return _host


@pytest.fixture()
def run_cli(cli_runner: CliRunner):
    """Convenience: ``run_cli("upload", "file.bin")`` returns a Result."""

    def _run(*args: str, **kwargs):
        return cli_runner.invoke(cli_module.app, list(args), **kwargs)

    return _run
