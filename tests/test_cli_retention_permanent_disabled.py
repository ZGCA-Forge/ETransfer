"""Coverage for the ``retention.allow_permanent=False`` flag.

Two layers are exercised:

1. ``/api/info`` filters out the ``permanent`` policy and the CLI sees this
   immediately because :class:`useServerInfo` (frontend) and the CLI both
   read the live endpoint.
2. The privilege layer (``etransfer.server.auth.privilege``) only enforces
   the rejection for *non-privileged* callers — static API tokens (which is
   what the CLI uses) bypass the check.  We assert both branches via direct
   calls to :func:`enforce_retention_policy`.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Iterator

import httpx
import pytest
from fastapi import HTTPException

from etransfer.common.constants import AUTH_HEADER
from etransfer.server.auth.privilege import enforce_retention_policy, is_privileged_caller


@pytest.fixture()
def disabled_permanent(memory_server) -> Iterator[dict]:
    memory_server["settings"].allow_permanent_retention = False
    try:
        yield memory_server
    finally:
        memory_server["settings"].allow_permanent_retention = True


def _fake_request(token: str | None, settings) -> SimpleNamespace:
    """Build a tiny duck-type that satisfies ``enforce_retention_policy``."""
    headers = {AUTH_HEADER: token} if token else {}
    return SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(settings=settings)),
        headers=headers,
        state=SimpleNamespace(),
    )


def test_api_info_excludes_permanent_when_disabled(disabled_permanent):
    body = httpx.get(f"{disabled_permanent['url']}/api/info", timeout=5).json()
    assert "permanent" not in body["retention_policies"]
    assert body["default_retention"] == "download_once"


def test_api_info_default_includes_permanent(memory_server):
    body = httpx.get(f"{memory_server['url']}/api/info", timeout=5).json()
    assert "permanent" in body["retention_policies"]


def test_privileged_token_can_still_use_permanent_when_disabled(disabled_permanent):
    """API tokens are admin-equivalent and intentionally bypass the policy."""
    settings = disabled_permanent["settings"]
    req = _fake_request(disabled_permanent["token"], settings)
    assert is_privileged_caller(req) is True
    enforce_retention_policy(req, "permanent")  # must NOT raise


def test_unprivileged_caller_is_rejected_when_disabled(disabled_permanent):
    settings = disabled_permanent["settings"]
    # Token absent → not privileged (auth middleware would 401 first in real
    # traffic, but the policy layer should still reject defensively).
    req = _fake_request(None, settings)
    # auth_tokens is non-empty → middleware would block, so policy must also
    # treat the caller as non-privileged here.
    assert is_privileged_caller(req) is False
    with pytest.raises(HTTPException) as excinfo:
        enforce_retention_policy(req, "permanent")
    assert excinfo.value.status_code == 403


def test_non_permanent_retention_never_blocked(disabled_permanent):
    settings = disabled_permanent["settings"]
    for r in ("download_once", "ttl"):
        enforce_retention_policy(_fake_request(None, settings), r)


def test_cli_upload_with_default_retention_still_works(
    tmp_path, cli_env, run_cli, disabled_permanent
):
    """The default flow (``download_once``) must continue to work."""
    src = tmp_path / "ok.bin"
    src.write_bytes(b"x" * 4096)

    result = run_cli("upload", str(src))
    assert result.exit_code == 0, result.output

    files = httpx.get(
        f"{disabled_permanent['url']}/api/files",
        headers={"X-API-Token": disabled_permanent["token"]},
        timeout=10,
    ).json()["files"]
    assert any(f["filename"] == "ok.bin" for f in files), files
