"""aria2c download engine — multi-connection download with resume support."""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
from pathlib import Path
from typing import Any, Callable, Optional

logger = logging.getLogger("etransfer.plugins.aria2")


def is_available() -> bool:
    """Check if aria2c is installed."""
    return shutil.which("aria2c") is not None


async def download(
    url: str,
    dest: Path,
    filename: Optional[str] = None,
    connections: int = 8,
    headers: Optional[dict[str, str]] = None,
    on_progress: Optional[Callable[[int, Optional[int]], Any]] = None,
) -> Path:
    """Download a file using aria2c with multi-connection support.

    Args:
        url: Download URL.
        dest: Target directory.
        filename: Override filename (None = auto-detect from server).
        connections: Number of parallel connections per file.
        headers: Extra HTTP headers (e.g. Authorization).
        on_progress: ``(downloaded_bytes, total_bytes_or_none)`` callback.

    Returns:
        Path to the downloaded file.
    """
    dest.mkdir(parents=True, exist_ok=True)

    cmd = [
        "aria2c",
        "--console-log-level=error",
        "--summary-interval=1",
        "--file-allocation=none",
        f"-x{connections}",
        f"-s{connections}",
        "-k", "1M",
        "--continue=true",
        "--auto-file-renaming=false",
        "--allow-overwrite=true",
        f"-d", str(dest),
    ]

    if filename:
        cmd.extend(["-o", filename])

    if headers:
        for k, v in headers.items():
            cmd.extend(["--header", f"{k}: {v}"])

    cmd.append(url)

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )

    total: Optional[int] = None
    downloaded = 0

    async def _read_output() -> None:
        nonlocal total, downloaded
        assert proc.stdout is not None
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace").strip()
            # Parse aria2c progress: [#abc 100MiB/500MiB(20%) ...]
            if text.startswith("[#") and "/" in text:
                try:
                    part = text.split("]")[0]
                    sizes = part.split(" ")[1]
                    if "/" in sizes:
                        dl_str, total_str = sizes.split("/")
                        downloaded = _parse_size(dl_str.split("(")[0])
                        total_part = total_str.split("(")[0].rstrip(")").rstrip("%")
                        total = _parse_size(total_part)
                        if on_progress:
                            on_progress(downloaded, total)
                except (IndexError, ValueError):
                    pass

    await _read_output()
    retcode = await proc.wait()

    if retcode != 0:
        raise RuntimeError(f"aria2c exited with code {retcode}")

    # Find the downloaded file
    if filename:
        result = dest / filename
        if result.exists():
            return result

    # Fallback: find the newest file in dest
    files = sorted(dest.iterdir(), key=lambda f: f.stat().st_mtime if f.is_file() else 0, reverse=True)
    for f in files:
        if f.is_file() and not f.name.startswith("."):
            final_size = f.stat().st_size
            if on_progress:
                on_progress(final_size, final_size)
            logger.info("aria2c downloaded: %s (%d bytes)", f, final_size)
            return f

    raise FileNotFoundError(f"No file found in {dest} after aria2c download")


def _parse_size(s: str) -> int:
    """Parse aria2c size strings like '100MiB', '1.5GiB', '500KiB'."""
    s = s.strip()
    multipliers = {
        "GiB": 1024**3, "MiB": 1024**2, "KiB": 1024,
        "GB": 1000**3, "MB": 1000**2, "KB": 1000,
        "B": 1,
    }
    for suffix, mult in multipliers.items():
        if s.endswith(suffix):
            num = s[:-len(suffix)]
            return int(float(num) * mult)
    return int(float(s))
