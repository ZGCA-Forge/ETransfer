"""CLI tests for folder upload and `et folders *` subcommands."""

from __future__ import annotations

import hashlib
import zipfile
from pathlib import Path

import httpx


def _make_folder(root: Path, name: str) -> Path:
    """Create a small on-disk folder with three files in two subdirs."""
    folder = root / name
    (folder / "sub").mkdir(parents=True)
    (folder / "a.txt").write_bytes(b"alpha-" * 100)
    (folder / "b.bin").write_bytes(b"\x00\x01\x02\x03" * 200)
    (folder / "sub" / "c.md").write_bytes(b"# hello\n" * 50)
    return folder


def _api_folders(server: dict) -> list[dict]:
    r = httpx.get(
        f"{server['url']}/api/folders",
        headers={"X-API-Token": server["token"]},
        timeout=10,
    )
    r.raise_for_status()
    return list(r.json()["folders"])


def test_upload_folder_then_list_get_download_delete(tmp_path, cli_env, run_cli, memory_server):
    folder = _make_folder(tmp_path, "myfolder")

    # --- upload (directory) ---
    result = run_cli("upload", str(folder), "--retention", "permanent", "--threads", "2")
    assert result.exit_code == 0, result.output

    # --- list via API ---
    folders = _api_folders(memory_server)
    assert len(folders) >= 1
    my = next(f for f in folders if f["folder_name"] == "myfolder")
    folder_id = my["folder_id"]
    assert my["file_count"] == 3

    # --- `et folders list` (smoke) ---
    result = run_cli("folders", "list")
    assert result.exit_code == 0, result.output

    # --- `et folders get <short>` ---
    result = run_cli("folders", "get", folder_id[:8])
    assert result.exit_code == 0, result.output

    # --- `et folders download` ---
    out_zip = tmp_path / "out" / "myfolder.zip"
    result = run_cli("folders", "download", folder_id[:8], "-o", str(out_zip))
    assert result.exit_code == 0, result.output
    assert out_zip.exists() and out_zip.stat().st_size > 0

    # verify archive content
    with zipfile.ZipFile(out_zip) as zf:
        names = {n for n in zf.namelist() if not n.endswith("/")}
        # names are prefixed with folder_name
        assert any(n.endswith("a.txt") for n in names)
        assert any(n.endswith("b.bin") for n in names)
        assert any(n.endswith("c.md") for n in names)
        # sanity: content matches
        with zf.open(next(n for n in names if n.endswith("a.txt"))) as fh:
            assert fh.read() == b"alpha-" * 100

    # --- `et folders delete -f` ---
    result = run_cli("folders", "delete", folder_id[:8], "-f")
    assert result.exit_code == 0, result.output

    remaining = [f for f in _api_folders(memory_server) if f["folder_id"] == folder_id]
    assert remaining == []
