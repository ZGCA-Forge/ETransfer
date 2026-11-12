"""Live smoke tests against the real ModelScope hub.

Skipped by default. Enable with::

    ETRANSFER_LIVE_MODELSCOPE=1 pytest tests/test_modelscope_live.py -v

These verify the assumptions the offline tests encode in mocks — most
importantly that the model file endpoint is ``/repo/files`` while the dataset
endpoint is ``/repo/tree``, that HEAD alone does not report a size, and that
``/resolve/`` URLs serve real bytes.

Network access to modelscope.cn is required; nothing here writes to a bucket.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from etransfer.common import modelscope_hub as ms
from etransfer.plugins.sources.modelscope import ModelScopeSource

pytestmark = pytest.mark.skipif(
    os.environ.get("ETRANSFER_LIVE_MODELSCOPE") != "1",
    reason="set ETRANSFER_LIVE_MODELSCOPE=1 to run live ModelScope tests",
)

MODEL = "Qwen/Qwen3.5-0.8B"
MODEL_URL = f"https://modelscope.cn/models/{MODEL}"
DATASET = "AI-ModelScope/gsm8k"
DATASET_URL = f"https://modelscope.cn/datasets/{DATASET}"


@pytest.mark.asyncio
async def test_discovery_finds_the_model() -> None:
    result = await ms.list_models(search="Qwen3.5-0.8B", page_size=5)
    assert result["total_count"] > 0
    ids = [item["id"] for item in result["items"]]
    assert MODEL in ids, f"{MODEL} not in {ids}"

    record = next(item for item in result["items"] if item["id"] == MODEL)
    assert record["file_size"] > 1_000_000_000
    assert record["gated"] is False


@pytest.mark.asyncio
async def test_model_repo_listing_uses_files_endpoint() -> None:
    ref = ms.RepoRef(kind="model", owner="Qwen", name="Qwen3.5-0.8B")
    files = await ms.list_repo_files(ref)

    assert files, "expected files in the repo"
    paths = {f["path"] for f in files}
    assert "config.json" in paths
    assert any(p.endswith(".safetensors") for p in paths)
    # No directory nodes leaked into the blob list.
    assert not any(p in ("main", "master") for p in paths)

    total = sum(f["size"] for f in files)
    assert total > 1_000_000_000
    # The repo metadata agrees with the file sum (within rounding).
    info = await ms.get_repo_info(ref)
    assert abs(info["file_size"] - total) < 50_000_000


@pytest.mark.asyncio
async def test_dataset_listing_uses_tree_endpoint() -> None:
    ref = ms.RepoRef(kind="dataset", owner="AI-ModelScope", name="gsm8k")
    files = await ms.list_repo_files(ref)

    assert files
    paths = {f["path"] for f in files}
    assert "README.md" in paths
    assert any(p.endswith(".parquet") for p in paths)


@pytest.mark.asyncio
async def test_file_info_reports_real_size() -> None:
    """ModelScope's HEAD omits Content-Length; the ranged GET must recover it."""
    src = ModelScopeSource()
    info = await src.get_file_info(f"{MODEL_URL}/resolve/master/config.json")

    assert info.filename == "config.json"
    assert info.size > 0, "size must come from Content-Range, not a bare HEAD"
    assert info.extra["repo"] == MODEL


@pytest.mark.asyncio
async def test_real_download_bytes_match_reported_size() -> None:
    src = ModelScopeSource()
    url = f"{MODEL_URL}/resolve/master/configuration.json"
    info = await src.get_file_info(url)

    with tempfile.TemporaryDirectory() as tmp:
        out = await src.download(url, Path(tmp))
        data = out.read_bytes()

    assert len(data) == info.size
    assert data.strip().startswith(b"{")


@pytest.mark.asyncio
async def test_large_lfs_file_supports_range_requests() -> None:
    """Chunked download depends on Range support on the resolve URL."""
    import httpx

    ref = ms.RepoRef(kind="model", owner="Qwen", name="Qwen3.5-0.8B")
    files = await ms.list_repo_files(ref)
    big = max(files, key=lambda f: f["size"])
    url = ms.build_resolve_url(ref, big["path"])

    async with httpx.AsyncClient(follow_redirects=True, timeout=60) as client:
        resp = await client.get(url, headers={"Range": "bytes=0-1023"})

    assert resp.status_code in (200, 206)
    assert len(resp.content) == 1024
    content_range = resp.headers.get("content-range", "")
    assert content_range.startswith("bytes 0-1023/")
    assert int(content_range.rsplit("/", 1)[-1]) == big["size"]


@pytest.mark.asyncio
async def test_object_keys_are_namespaced_and_stable() -> None:
    ref = ms.RepoRef(kind="model", owner="Qwen", name="Qwen3.5-0.8B")
    key = ms.derive_repo_object_key(ref, "config.json")
    assert key == f"modelscope/models/{MODEL}/master/config.json"
    assert key == ms.derive_repo_object_key(ref, "config.json")


@pytest.mark.asyncio
async def test_collections_endpoint_is_reachable() -> None:
    result = await ms.list_collections(page_size=3)
    assert result["total_count"] > 0
    for item in result["items"]:
        assert "slug" in item
