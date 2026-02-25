"""Command-line interface for EasyTransfer client."""

import json
import threading
import time
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from rich.table import Table
from rich.text import Text

from etransfer.common.constants import AUTH_HEADER, DEFAULT_CHUNK_SIZE, DEFAULT_SERVER_PORT

app = typer.Typer(
    name="etransfer",
    help="EasyTransfer - Fast TUS-based file transfer tool",
    no_args_is_help=True,
    add_completion=False,
)

console = Console()

# Server subcommand
server_app = typer.Typer(help="Server management commands", add_completion=False)
app.add_typer(server_app, name="server")

# ---------------------------------------------------------------------------
# Client config helpers  (~/.etransfer/client.json)
# Single-server model: one server address + cached session token.
# ---------------------------------------------------------------------------

CLIENT_CONFIG_DIR = Path.home() / ".etransfer"
CLIENT_CONFIG_FILE = CLIENT_CONFIG_DIR / "client.json"


def _load_client_config() -> dict:
    """Load the client configuration from disk."""
    if CLIENT_CONFIG_FILE.exists():
        try:
            return json.loads(CLIENT_CONFIG_FILE.read_text(encoding="utf-8"))  # type: ignore[no-any-return]
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def _save_client_config(config: dict) -> None:
    """Save the client configuration to disk."""
    CLIENT_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    CLIENT_CONFIG_FILE.write_text(
        json.dumps(config, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _normalise_server_url(address: str) -> str:
    """Normalise a user-provided address into a full URL.

    Accepts: ``host``, ``host:port``, ``http://host:port``, ``https://host:port``.
    When no scheme is given and no port is specified, defaults to :8765.
    If the user provides a full URL (http/https), it is kept as-is.
    """
    from etransfer.common.constants import DEFAULT_SERVER_PORT

    address = address.strip().rstrip("/")
    if address.startswith(("http://", "https://")):
        return address
    # No scheme — treat as host or host:port
    if ":" not in address:
        address = f"{address}:{DEFAULT_SERVER_PORT}"
    return f"http://{address}"


def _get_server_url() -> str:
    """Return the configured server URL or exit with an error."""
    cfg = _load_client_config()
    url = cfg.get("server")
    if not url:
        print_error("Server not configured. Run [bold]etransfer setup <address>[/bold] first.")
        raise typer.Exit(1)
    return url  # type: ignore[no-any-return]


def _get_token() -> Optional[str]:
    """Return the cached session/api token (if any)."""
    cfg = _load_client_config()
    return cfg.get("token")


def format_size(size: int) -> str:
    """Format size in human-readable format."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024  # type: ignore[assignment]
    return f"{size:.1f} PB"


def format_rate(rate: float) -> str:
    """Format transfer rate."""
    return f"{format_size(int(rate))}/s"


def create_transfer_progress() -> Progress:
    """Create a beautiful progress bar for transfers."""
    return Progress(
        SpinnerColumn(spinner_name="dots"),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=40, complete_style="green", finished_style="bright_green"),
        TaskProgressColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        console=console,
        expand=True,
    )


def print_success(message: str) -> None:
    """Print success message."""
    console.print(f"[bold green]✓[/bold green] {message}")


def print_error(message: str) -> None:
    """Print error message."""
    console.print(f"[bold red]✗[/bold red] {message}")


def print_info(message: str) -> None:
    """Print info message."""
    console.print(f"[bold blue]ℹ[/bold blue] {message}")


def print_warning(message: str) -> None:
    """Print warning message."""
    console.print(f"[bold yellow]⚠[/bold yellow] {message}")


def _select_endpoint(server: str, token: Optional[str], for_upload: bool = True) -> str:
    """Query the server for the best available endpoint.

    Calls ``/api/endpoints`` to discover all instances (via Redis in
    multi-instance setups), picks the one with the lowest traffic rate,
    and verifies it is reachable.  Falls back to *server* silently.
    """
    try:
        from etransfer.client.tus_client import EasyTransferClient

        with EasyTransferClient(server, token=token) as client:
            best = client.select_best_reachable_endpoint(for_upload=for_upload, timeout=3.0)
            return best
    except Exception:
        return server


@app.command()
def setup(
    address: str = typer.Argument(
        ...,
        help="Server address (host:port or http://host:port)",
    ),
) -> None:
    """Configure the EasyTransfer server address.

    This is the first step before using any other command.
    Only one server is supported at a time.

    Examples:
        etransfer setup 192.168.1.100:8765
        etransfer setup myserver.example.com:8765
        etransfer setup http://myserver.example.com:8765
    """
    import httpx

    server_url = _normalise_server_url(address)
    console.print()

    # Probe connectivity
    with console.status("[bold cyan]Connecting to server...", spinner="dots"):
        try:
            r = httpx.get(f"{server_url}/api/health", timeout=10)
            r.raise_for_status()
        except httpx.ConnectError:
            print_error(f"Cannot connect to server: {server_url}")
            raise typer.Exit(1)
        except Exception as e:
            print_error(f"Server health-check failed: {e}")
            raise typer.Exit(1)

    print_success(f"Server reachable: {server_url}")

    # Check if server requires authentication
    auth_required = False
    auth_provider = None
    try:
        r = httpx.get(f"{server_url}/api/users/login-info", timeout=10)
        if r.status_code == 200:
            login_info = r.json()
            auth_required = True
            auth_provider = login_info.get("provider", "oidc")
            issuer = login_info.get("issuer", "")
            print_info(f"Server requires login ({auth_provider}: {issuer})")
        elif r.status_code == 501:
            print_info("Server does not require login (user system disabled)")
    except Exception:
        print_info("Could not detect login requirements")

    # Save config (preserve existing token if server unchanged)
    cfg = _load_client_config()
    old_server = cfg.get("server")
    cfg["server"] = server_url

    # If server changed, clear old session
    if old_server and old_server != server_url:
        cfg.pop("token", None)
        cfg.pop("session_token", None)  # clean up legacy key
        cfg.pop("username", None)
        cfg.pop("role", None)
        cfg.pop("expires_at", None)
        print_warning("Server changed — previous session cleared")

    _save_client_config(cfg)
    print_success(f"Config saved to {CLIENT_CONFIG_FILE}")

    if auth_required and not cfg.get("token"):
        console.print()
        print_info("Run [bold]etransfer login[/bold] to authenticate.")


@app.command()
def upload(
    file_path: Optional[Path] = typer.Argument(None, help="Path to file to upload"),
    token: Optional[str] = typer.Option(
        None,
        "--token",
        "-t",
        help="API token (overrides saved session)",
        envvar="ETRANSFER_TOKEN",
    ),
    chunk_size: int = typer.Option(
        DEFAULT_CHUNK_SIZE,
        "--chunk-size",
        "-c",
        help="Chunk size in bytes",
    ),
    retention: Optional[str] = typer.Option(
        "download_once",
        "--retention",
        "-r",
        help="Retention: download_once (default), permanent, ttl",
    ),
    retention_ttl: Optional[int] = typer.Option(
        None,
        "--retention-ttl",
        help="TTL in seconds (only for --retention ttl)",
    ),
    threads: int = typer.Option(
        4,
        "--threads",
        "-j",
        help="Number of concurrent upload threads",
        min=1,
        max=32,
    ),
    wait_on_quota: bool = typer.Option(
        True,
        "--wait-on-quota/--no-wait-on-quota",
        help="Auto-wait and resume when storage quota is full",
    ),
) -> None:
    """Upload a file to the server."""
    if file_path is None:
        console.print()
        console.print("[bold cyan]et upload[/bold cyan] — Upload a file to the server\n")
        console.print("[bold]Usage:[/bold]  et upload <FILE> [OPTIONS]\n")
        console.print("[bold]Options:[/bold]")
        console.print("  -r, --retention TEXT     Retention policy [default: download_once]")
        console.print("                           [dim]download_once — auto-delete after first download[/dim]")
        console.print("                           [dim]permanent    — keep forever[/dim]")
        console.print("                           [dim]ttl          — auto-expire after N seconds[/dim]")
        console.print("      --retention-ttl INT  TTL in seconds (only for --retention ttl)")
        console.print("  -c, --chunk-size INT     Chunk size in bytes [default: 10MB]")
        console.print("  -j, --threads INT        Parallel upload threads [default: 4]")
        console.print("  -t, --token TEXT         API token (overrides saved session)")
        console.print()
        console.print("[bold]Examples:[/bold]")
        console.print("  [dim]et upload myfile.zip[/dim]                              # download once (default)")
        console.print("  [dim]et upload myfile.zip -r permanent[/dim]                 # keep forever")
        console.print("  [dim]et upload myfile.zip -r ttl --retention-ttl 3600[/dim]  # expire in 1h")
        console.print("  [dim]et upload largefile.iso -j 8[/dim]                      # 8 threads")
        return

    server = _get_server_url()
    token = token or _get_token()

    if not file_path.exists():
        print_error(f"File not found: {file_path}")
        raise typer.Exit(1)

    if retention and retention not in ("permanent", "download_once", "ttl"):
        print_error(f"Invalid retention: {retention} (use permanent/download_once/ttl)")
        raise typer.Exit(1)

    if retention == "ttl" and not retention_ttl:
        print_error("--retention-ttl is required when using --retention ttl")
        raise typer.Exit(1)

    # Endpoint selection — pick least-loaded reachable instance
    target = _select_endpoint(server, token, for_upload=True)
    if target != server:
        console.print(f"  [dim]-> Redirecting to best endpoint: [bold]{target}[/bold][/dim]")
        server = target

    file_size = file_path.stat().st_size

    # Build info text
    info_lines = f"[bold]{file_path.name}[/bold]\n"
    info_lines += f"[dim]Size: {format_size(file_size)} | Chunk: {format_size(chunk_size)} | Threads: {threads}[/dim]"
    if retention:
        label = {"permanent": "Permanent", "download_once": "Download Once", "ttl": f"TTL {retention_ttl}s"}
        info_lines += f"\n[dim]Retention: {label.get(retention, retention)}[/dim]"

    # Print header
    console.print()
    panel = Panel(
        info_lines,
        title="[bold cyan]Upload[/bold cyan]",
        border_style="cyan",
    )
    console.print(panel)

    console.print("[dim]  Press [bold]q[/bold]+Enter to cancel | [bold]s[/bold]+Enter for status[/dim]")

    try:
        import sys

        from etransfer.client.tus_client import EasyTransferClient

        with create_transfer_progress() as progress:
            task = progress.add_task("[cyan]Uploading", total=file_size)
            start_time = time.time()

            def update_progress(uploaded: int, total: int) -> None:
                progress.update(task, completed=uploaded)

            with EasyTransferClient(server, token=token, chunk_size=chunk_size) as client:
                uploader = client.create_parallel_uploader(
                    str(file_path),
                    chunk_size=chunk_size,
                    max_concurrent=threads,
                    progress_callback=update_progress,
                    retention=retention,
                    retention_ttl=retention_ttl,
                    wait_on_quota=wait_on_quota,
                )

                # Interactive input listener thread
                def _input_listener() -> None:
                    while not uploader._cancelled.is_set():
                        try:
                            line = sys.stdin.readline()
                            if not line:
                                continue
                            cmd = line.strip().lower()
                            if cmd in ("q", "quit", "cancel"):
                                uploader._cancelled.set()
                                return
                            elif cmd in ("s", "status"):
                                elapsed_now = time.time() - start_time
                                uploaded = uploader._uploaded_bytes
                                pct = (uploaded / file_size * 100) if file_size else 100
                                spd = uploaded / elapsed_now if elapsed_now > 0 else 0
                                console.print(
                                    f"\n[bold cyan]Status:[/bold cyan] "
                                    f"{format_size(uploaded)}/{format_size(file_size)} "
                                    f"({pct:.1f}%) | "
                                    f"{format_size(int(spd))}/s | "
                                    f"{elapsed_now:.0f}s elapsed"
                                )
                        except Exception:
                            return

                input_thread = threading.Thread(target=_input_listener, daemon=True)
                input_thread.start()

                location = uploader.upload()

        elapsed = time.time() - start_time
        avg_speed = file_size / elapsed if elapsed > 0 else 0

        console.print()
        print_success("Upload complete!")
        console.print(f"   [dim]Time: {elapsed:.1f}s | Avg Speed: {format_size(int(avg_speed))}/s[/dim]")

        if location:
            file_id = location.split("/")[-1]
            console.print(f"   [dim]File ID: [bold]{file_id}[/bold][/dim]")
            console.print(f"   [dim]Download: [bold]et download {file_id[:8]}[/bold][/dim]")
        if retention == "download_once":
            print_warning("File will be deleted after first download")
            console.print("   [dim]Keep file: use [bold]--retention permanent[/bold][/dim]")
            console.print("   [dim]Auto-expire: use [bold]--retention ttl --retention-ttl 3600[/bold][/dim]")
        elif retention == "ttl":
            print_info(f"File will expire in {retention_ttl}s after upload completes")
        elif retention == "permanent":
            console.print("   [dim]Retention: permanent (file won't auto-delete)[/dim]")

    except KeyboardInterrupt:
        console.print()
        print_warning("Upload cancelled by user.")
        raise typer.Exit(130)
    except Exception as e:
        console.print()
        print_error(f"Upload failed: {e}")
        raise typer.Exit(1)


@app.command()
def reupload(
    file_id: str = typer.Argument(..., help="File ID (or short prefix) of the partial upload to resume"),
    file_path: Path = typer.Argument(..., help="Path to the local file (must match server record)"),
    token: Optional[str] = typer.Option(
        None,
        "--token",
        "-t",
        help="API token (overrides saved session)",
        envvar="ETRANSFER_TOKEN",
    ),
    threads: int = typer.Option(
        4,
        "--threads",
        "-j",
        help="Number of concurrent upload threads",
        min=1,
        max=32,
    ),
    wait_on_quota: bool = typer.Option(
        True,
        "--wait-on-quota/--no-wait-on-quota",
        help="Auto-wait and resume when storage quota is full",
    ),
) -> None:
    """Resume a previously interrupted upload.

    Requires the file ID (or short prefix) from the original upload and the
    same local file.  The server record is validated against the local file:

      - Filename must match
      - File size must match

    Retention policy and chunk size are inherited from the original upload
    and cannot be changed.

    Examples:
        et reupload 580374 ./largefile.iso
        et reupload 580374 ./largefile.iso -j 8
    """
    import httpx

    server = _get_server_url()
    token = token or _get_token()

    if not file_path.exists():
        print_error(f"File not found: {file_path}")
        raise typer.Exit(1)

    # Resolve short prefix to full file ID
    resolved_id = _resolve_file_id(file_id, server, token)

    # Query server for the upload record via /api/files/{id}
    headers: dict[str, str] = {}
    if token:
        headers[AUTH_HEADER] = token
        headers["Authorization"] = f"Bearer {token}"

    try:
        with httpx.Client(timeout=30.0) as http:
            resp = http.get(f"{server}/api/files/{resolved_id}", headers=headers)
            resp.raise_for_status()
            record = resp.json()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            print_error(f"Upload [bold]{resolved_id[:8]}[/bold].. not found on server")
        else:
            print_error(f"Server error: {e.response.status_code}")
        raise typer.Exit(1)
    except Exception as e:
        print_error(f"Failed to query server: {e}")
        raise typer.Exit(1)

    # Parse server record
    server_status = record.get("status", "complete")
    uploaded_size = record.get("uploaded_size", 0)
    server_size = record.get("size", 0)
    server_filename = record.get("filename", "")
    server_chunk_size = record.get("chunk_size") or DEFAULT_CHUNK_SIZE

    # Validate: status must be partial
    if server_status == "complete" or uploaded_size >= server_size:
        print_error(
            f"Upload [bold]{resolved_id[:8]}[/bold].. is already complete "
            f"({format_size(server_size)}). Nothing to reupload."
        )
        raise typer.Exit(1)

    # Validate: filename must match
    local_filename = file_path.name
    if local_filename != server_filename:
        print_error(
            f"Filename mismatch: local [bold]{local_filename}[/bold] " f"!= server [bold]{server_filename}[/bold]"
        )
        raise typer.Exit(1)

    # Validate: file size must match
    local_size = file_path.stat().st_size
    if local_size != server_size:
        print_error(f"File size mismatch: local {format_size(local_size)} " f"!= server {format_size(server_size)}")
        raise typer.Exit(1)

    # Build resume URL: server_url + /tus/ + file_id
    resume_url = f"{server}/tus/{resolved_id}"

    progress_pct = (uploaded_size / server_size * 100) if server_size else 0
    metadata = record.get("metadata") or {}
    retention = metadata.get("retention", record.get("retention", "?"))

    # Print header
    console.print()
    info_lines = (
        f"[bold]{server_filename}[/bold]\n"
        f"[dim]Size: {format_size(server_size)} | Chunk: {format_size(server_chunk_size)} "
        f"| Threads: {threads}[/dim]\n"
        f"[dim]Retention: {retention} (from original upload)[/dim]\n"
        f"[bold yellow]Resuming[/bold yellow] [dim]{resolved_id[:8]}.. \u2014 "
        f"already uploaded {format_size(uploaded_size)} ({progress_pct:.0f}%)[/dim]"
    )
    console.print(Panel(info_lines, title="[bold cyan]Re-upload[/bold cyan]", border_style="yellow"))
    console.print("[dim]  Press [bold]q[/bold]+Enter to cancel | [bold]s[/bold]+Enter for status[/dim]")

    try:
        import sys

        from etransfer.client.tus_client import EasyTransferClient

        with create_transfer_progress() as progress:
            task = progress.add_task("[cyan]Uploading", total=local_size)
            start_time = time.time()

            def update_progress(uploaded: int, total: int) -> None:
                progress.update(task, completed=uploaded)

            with EasyTransferClient(server, token=token, chunk_size=server_chunk_size) as client:
                uploader = client.create_parallel_uploader(
                    str(file_path),
                    chunk_size=server_chunk_size,
                    max_concurrent=threads,
                    progress_callback=update_progress,
                    wait_on_quota=wait_on_quota,
                    resume_url=resume_url,
                )

                # Interactive input listener thread
                def _input_listener() -> None:
                    while not uploader._cancelled.is_set():
                        try:
                            line = sys.stdin.readline()
                            if not line:
                                continue
                            cmd = line.strip().lower()
                            if cmd in ("q", "quit", "cancel"):
                                uploader._cancelled.set()
                                return
                            elif cmd in ("s", "status"):
                                elapsed_now = time.time() - start_time
                                uploaded = uploader._uploaded_bytes
                                pct = (uploaded / local_size * 100) if local_size else 100
                                spd = uploaded / elapsed_now if elapsed_now > 0 else 0
                                console.print(
                                    f"\n[bold cyan]Status:[/bold cyan] "
                                    f"{format_size(uploaded)}/{format_size(local_size)} "
                                    f"({pct:.1f}%) | "
                                    f"{format_size(int(spd))}/s | "
                                    f"{elapsed_now:.0f}s elapsed"
                                )
                        except Exception:
                            return

                input_thread = threading.Thread(target=_input_listener, daemon=True)
                input_thread.start()

                location = uploader.upload()

        elapsed = time.time() - start_time
        avg_speed = local_size / elapsed if elapsed > 0 else 0

        console.print()
        print_success("Re-upload complete!")
        console.print(f"   [dim]Time: {elapsed:.1f}s | Avg Speed: {format_size(int(avg_speed))}/s[/dim]")

        if location:
            fid = location.split("/")[-1]
            console.print(f"   [dim]File ID: [bold]{fid}[/bold][/dim]")
            console.print(f"   [dim]Download: [bold]et download {fid[:8]}[/bold][/dim]")

    except KeyboardInterrupt:
        console.print()
        print_warning("Upload cancelled by user.")
        console.print(f"   [dim]Resume again: [bold]et reupload {resolved_id[:8]} {file_path.name}[/bold][/dim]")
        raise typer.Exit(130)
    except Exception as e:
        console.print()
        print_error(f"Re-upload failed: {e}")
        console.print(f"   [dim]Retry: [bold]et reupload {resolved_id[:8]} {file_path.name}[/bold][/dim]")
        raise typer.Exit(1)


def _resolve_file_id(prefix: str, server: str, token: Optional[str]) -> str:
    """Resolve a short file ID prefix to the full ID by querying the server.

    If the prefix is already 32 chars (full hex UUID without dashes), return as-is.
    Otherwise, list files and find a unique match.
    """
    if len(prefix) >= 32:
        return prefix

    from etransfer.client.tus_client import EasyTransferClient

    with EasyTransferClient(server, token=token) as client:
        # Fetch enough files to find a match (all pages would be ideal,
        # but a reasonable page size covers most cases)
        files = client.list_files(page=1, page_size=200, include_partial=True)

    matches = [f["file_id"] for f in files if f.get("file_id", "").startswith(prefix)]

    if len(matches) == 1:
        return matches[0]
    elif len(matches) == 0:
        print_error(f"No file found matching prefix [bold]{prefix}[/bold]")
        raise typer.Exit(1)
    else:
        print_error(f"Ambiguous prefix [bold]{prefix}[/bold] — matches {len(matches)} files:")
        for m in matches[:10]:
            console.print(f"  [dim]{m[:6]}.. {m}[/dim]")
        raise typer.Exit(1)


@app.command()
def download(
    file_id: Optional[str] = typer.Argument(None, help="File ID (or short prefix) to download"),
    token: Optional[str] = typer.Option(
        None,
        "--token",
        "-t",
        help="API token (overrides saved session)",
        envvar="ETRANSFER_TOKEN",
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Output directory or file path",
    ),
) -> None:
    """Download a file from the server.

    If a previous download was interrupted, the ``.{name}.part/`` folder
    is detected automatically and the download resumes from where it left off.
    """
    if file_id is None:
        console.print()
        console.print("[bold cyan]et download[/bold cyan] — Download a file from the server\n")
        console.print("[bold]Usage:[/bold]  et download <FILE_ID> [OPTIONS]\n")
        console.print("[bold]Arguments:[/bold]")
        console.print("  FILE_ID                  File ID or short prefix (e.g. 6a9111db)\n")
        console.print("[bold]Options:[/bold]")
        console.print("  -o, --output PATH        Output directory or file path [default: current dir]")
        console.print("  -t, --token TEXT         API token (overrides saved session)")
        console.print()
        console.print("[bold]Examples:[/bold]")
        console.print("  [dim]et download 6a9111db[/dim]                  # download to current dir")
        console.print("  [dim]et download 6a9111db -o ~/Downloads[/dim]   # download to specific dir")
        console.print("  [dim]et download 6a91 -o myfile.zip[/dim]        # short prefix + rename")
        console.print()
        console.print("[bold]Resume:[/bold]")
        console.print("  [dim]Interrupted downloads are resumed automatically.[/dim]")
        console.print("  [dim]Just re-run the same command — the .part/ folder is detected.[/dim]")
        return

    server = _get_server_url()
    token = token or _get_token()

    # Resolve short ID prefix to full ID
    file_id = _resolve_file_id(file_id, server, token)

    # Endpoint selection — pick least-loaded reachable instance
    target = _select_endpoint(server, token, for_upload=False)
    if target != server:
        console.print(f"  [dim]-> Redirecting to best endpoint: [bold]{target}[/bold][/dim]")
        server = target

    try:
        from etransfer.client.downloader import ChunkDownloader

        downloader = ChunkDownloader(server, token=token, max_concurrent=5)

        # Get file info
        info = downloader.get_file_info(file_id)

        # Determine output path
        if output is None:
            output_path = Path.cwd() / info.filename
        elif output.is_dir():
            output_path = output / info.filename
        else:
            output_path = output

        # Auto-detect resume from .part/ folder
        part_dir = downloader._part_dir_for(output_path)
        resuming = False
        cached_count = 0
        if part_dir.exists():
            part_meta = downloader.read_part_meta(part_dir)
            if part_meta and part_meta.get("file_id") == file_id:
                cache = downloader._local_cache_for(output_path)
                cached_count = len(cache.get_cached_chunks(file_id))
                if cached_count > 0:
                    resuming = True

        # Print header
        console.print()
        info_lines = (
            f"[bold]{info.filename}[/bold]\n"
            f"[dim]Size: {format_size(info.size)} | Available: {format_size(info.available_size)}[/dim]"
        )
        if resuming:
            chunk_sz = info.chunk_size or downloader.chunk_size
            cached_bytes = cached_count * chunk_sz
            pct = (cached_bytes / info.size * 100) if info.size else 0
            info_lines += (
                f"\n[bold yellow]Resuming[/bold yellow] [dim]— "
                f"already cached {cached_count} chunks ({format_size(cached_bytes)}, {pct:.0f}%)[/dim]"
            )
        console.print(Panel(info_lines, title="[bold cyan]Download[/bold cyan]", border_style="cyan"))

        is_partial = not getattr(info, "is_upload_complete", info.available_size >= info.size)

        if is_partial:
            print_warning(
                f"Only {format_size(info.available_size)} of {format_size(info.size)} "
                f"available (upload in progress) — will follow upload"
            )

        with create_transfer_progress() as progress:
            task = progress.add_task("[cyan]Downloading", total=info.size if is_partial else info.available_size)
            start_time = time.time()

            def update_progress(downloaded: int, total: int) -> None:
                progress.update(task, completed=downloaded, total=total)

            if is_partial:
                success = downloader.download_file_follow(
                    file_id,
                    output_path,
                    progress_callback=update_progress,
                )
            else:
                success = downloader.download_file(
                    file_id,
                    output_path,
                    progress_callback=update_progress,
                )

        elapsed = time.time() - start_time
        final_size = info.size if is_partial else info.available_size
        avg_speed = final_size / elapsed if elapsed > 0 else 0

        console.print()
        if success:
            print_success("Download complete!")
            console.print(f"   [dim]Time: {elapsed:.1f}s | Avg Speed: {format_size(int(avg_speed))}/s[/dim]")
            console.print(f"   [dim]Saved to: [bold]{output_path}[/bold][/dim]")
        else:
            print_error("Download failed")
            raise typer.Exit(1)

    except KeyboardInterrupt:
        console.print()
        print_warning("Download cancelled.")
        console.print(f"   [dim]Resume: re-run [bold]et download {file_id[:8]}[/bold][/dim]")
        raise typer.Exit(130)
    except Exception as e:
        console.print()
        print_error(f"Download failed: {e}")
        raise typer.Exit(1)


@app.command("list")
def list_files(
    token: Optional[str] = typer.Option(
        None,
        "--token",
        "-t",
        help="API token (overrides saved session)",
        envvar="ETRANSFER_TOKEN",
    ),
    page: int = typer.Option(1, "--page", "-p", help="Page number"),
    page_size: int = typer.Option(20, "--size", help="Page size"),
    all_files: bool = typer.Option(
        True,
        "--all/--complete",
        help="Show all files or only complete",
    ),
) -> None:
    """List files on the server."""
    server = _get_server_url()
    token = token or _get_token()

    try:
        from etransfer.client.tus_client import EasyTransferClient

        console.print()
        with console.status("[bold cyan]Loading files...", spinner="dots"):
            with EasyTransferClient(server, token=token) as client:
                files = client.list_files(
                    page=page,
                    page_size=page_size,
                    include_partial=all_files,
                )

        if not files:
            print_warning("No files found")
            return

        table = Table(
            title="[bold cyan]Files on Server[/bold cyan]",
            show_header=True,
            header_style="bold magenta",
            border_style="cyan",
        )
        table.add_column("ID", style="dim", width=8)
        table.add_column("Filename", style="white")
        table.add_column("Size", justify="right", style="green")
        table.add_column("Time", style="dim", width=17)
        table.add_column("Progress", justify="left")
        table.add_column("Status", justify="center")
        table.add_column("Retention", justify="center", style="dim")

        total_size = 0
        total_uploaded = 0
        for f in files:
            file_size = f.get("size", 0)
            uploaded_size = f.get("uploaded_size", file_size)
            total_size += file_size
            total_uploaded += uploaded_size

            # Compute progress from uploaded_size / size
            if file_size > 0:
                progress_pct = (uploaded_size / file_size) * 100
            else:
                progress_pct = 100.0
            bar_width = 10
            filled = int(bar_width * progress_pct / 100)
            bar = "█" * filled + "░" * (bar_width - filled)

            file_status = f.get("status", "complete")
            if file_status == "complete" or progress_pct >= 100:
                status = "[green]● Complete[/green]"
                progress_str = f"[green]{bar}[/green] 100%"
            else:
                status = "[yellow]◐ Partial[/yellow]"
                progress_str = f"[yellow]{bar}[/yellow] {progress_pct:.0f}%"

            # Retention info from metadata
            metadata = f.get("metadata") or {}
            retention = metadata.get("retention", f.get("retention", "permanent"))
            retention_labels = {
                "permanent": "[dim]permanent[/dim]",
                "download_once": "[red]1x download[/red]",
                "ttl": "[yellow]TTL[/yellow]",
            }
            retention_str = retention_labels.get(retention, retention)

            # Format created_at
            raw_time = f.get("created_at", "")
            if raw_time:
                try:
                    from datetime import datetime as _dt

                    if isinstance(raw_time, str):
                        # Handle ISO format with or without timezone
                        ts = raw_time.replace("Z", "+00:00")
                        dt = _dt.fromisoformat(ts)
                    else:
                        dt = raw_time
                    time_str = dt.strftime("%y-%m-%d %H:%M:%S")
                except Exception:
                    time_str = str(raw_time)[:17]
            else:
                time_str = ""

            table.add_row(
                f.get("file_id", "")[:6] + "..",
                f.get("filename", "unknown"),
                format_size(f.get("size", 0)),
                time_str,
                progress_str,
                status,
                retention_str,
            )

        console.print(table)
        size_info = f"Actual: {format_size(total_uploaded)}"
        if total_uploaded != total_size:
            size_info += f" / Expected: {format_size(total_size)}"
        console.print(f"[dim]Page {page} | {len(files)} files | {size_info}[/dim]")

    except Exception as e:
        print_error(f"Failed to list files: {e}")
        raise typer.Exit(1)


@app.command()
def delete(
    file_id: str = typer.Argument(..., help="File ID (or short prefix) to delete"),
    token: Optional[str] = typer.Option(
        None,
        "--token",
        "-t",
        help="API token (overrides saved session)",
        envvar="ETRANSFER_TOKEN",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Skip confirmation prompt",
    ),
) -> None:
    """Delete a file from the server."""
    server = _get_server_url()
    token = token or _get_token()

    try:
        from etransfer.client.tus_client import EasyTransferClient

        resolved_id = _resolve_file_id(file_id, server, token)

        if not force:
            confirm = typer.confirm(f"Delete file {resolved_id[:8]}..? This cannot be undone")
            if not confirm:
                print_warning("Cancelled.")
                raise typer.Exit(0)

        with console.status("[bold cyan]Deleting file...", spinner="dots"):
            with EasyTransferClient(server, token=token) as client:
                client.delete_file(resolved_id)

        print_success(f"File [bold]{resolved_id[:8]}[/bold].. deleted")

    except typer.Exit:
        raise
    except Exception as e:
        print_error(f"Failed to delete file: {e}")
        raise typer.Exit(1)


@app.command("delete-all")
def delete_all(
    token: Optional[str] = typer.Option(
        None,
        "--token",
        "-t",
        help="API token (overrides saved session)",
        envvar="ETRANSFER_TOKEN",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Skip confirmation prompt",
    ),
) -> None:
    """Delete ALL files from the server. Use with caution."""
    server = _get_server_url()
    token = token or _get_token()

    try:
        from etransfer.client.tus_client import EasyTransferClient

        with EasyTransferClient(server, token=token) as client:
            files = client.list_files(page=1, page_size=1000, include_partial=True)

        if not files:
            print_info("No files to delete")
            return

        total_size = sum(f.get("size", 0) for f in files)
        console.print(f"\n[bold red]⚠ About to delete {len(files)} file(s) " f"({format_size(total_size)})[/bold red]")

        if not force:
            confirm = typer.confirm("Delete ALL files? This cannot be undone")
            if not confirm:
                print_warning("Cancelled.")
                raise typer.Exit(0)

        deleted = 0
        failed = 0
        with console.status("[bold cyan]Deleting files...", spinner="dots") as status:
            with EasyTransferClient(server, token=token) as client:
                for f in files:
                    fid = f.get("file_id", "")
                    fname = f.get("filename", "unknown")
                    status.update(f"[bold cyan]Deleting {fname} ({fid[:8]})..")
                    try:
                        client.delete_file(fid)
                        deleted += 1
                    except Exception as e:
                        console.print(f"  [red]✗[/red] {fid[:8]}.. {fname}: {e}")
                        failed += 1

        print_success(f"Deleted {deleted} file(s)")
        if failed:
            print_warning(f"Failed to delete {failed} file(s)")

    except typer.Exit:
        raise
    except Exception as e:
        print_error(f"Failed: {e}")
        raise typer.Exit(1)


@app.command()
def info(
    token: Optional[str] = typer.Option(
        None,
        "--token",
        "-t",
        help="API token (overrides saved session)",
        envvar="ETRANSFER_TOKEN",
    ),
) -> None:
    """Show client status, server info, user profile and quota."""
    import httpx

    from etransfer import __version__

    cfg = _load_client_config()
    server = cfg.get("server")
    token = token or _get_token()

    console.print()

    # ── Client & Version ──────────────────────────────────────
    client_lines = f"[bold]EasyTransfer:[/bold] v{__version__}"
    client_lines += f"\n[bold]Config:[/bold] {CLIENT_CONFIG_FILE}"
    if server:
        client_lines += f"\n[bold]Server:[/bold] {server}"
    else:
        client_lines += "\n[bold]Server:[/bold] [dim]not configured — run [bold]et setup <address>[/bold][/dim]"
        console.print(Panel(client_lines, title="[bold cyan]📋 Client[/bold cyan]", border_style="cyan"))
        return

    if cfg.get("token"):
        # Fetch display name from server if possible
        display_name = cfg.get("username", "?")
        role = cfg.get("role", "?")
        try:
            headers = {"Authorization": f"Bearer {token}"}
            r = httpx.get(f"{server}/api/users/me", headers=headers, timeout=5)
            if r.status_code == 200:
                me = r.json()
                display_name = me.get("display_name") or me.get("username", display_name)
                role = me.get("role", role)
        except Exception:
            pass
        client_lines += f"\n[bold]User:[/bold] {display_name} ({role})"
        if cfg.get("expires_at"):
            client_lines += f"\n[bold]Session expires:[/bold] {cfg['expires_at']}"
    else:
        client_lines += "\n[bold]Session:[/bold] [dim]not logged in[/dim]"

    console.print(Panel(client_lines, title="[bold cyan]📋 Client[/bold cyan]", border_style="cyan"))

    # ── Server Info ───────────────────────────────────────────
    try:
        from etransfer.client.tus_client import EasyTransferClient

        with EasyTransferClient(server, token=token) as client:
            server_info = client.get_server_info()

        srv_text = (
            f"[bold]Version:[/bold] {server_info.version}\n"
            f"[bold]TUS Version:[/bold] {server_info.tus_version}\n"
            f"[bold]Chunk Size:[/bold] {format_size(server_info.chunk_size)}\n"
            f"[bold]Total Files:[/bold] {server_info.total_files}\n"
            f"[bold]Total Storage:[/bold] {format_size(server_info.total_size)}"
        )
        if server_info.max_upload_size:
            srv_text += f"\n[bold]Max Upload:[/bold] {format_size(server_info.max_upload_size)}"

        console.print(Panel(srv_text, title="[bold cyan]🖥️  Server[/bold cyan]", border_style="cyan"))

        if server_info.endpoints:
            table = Table(
                title="[bold cyan]🌐 Endpoints[/bold cyan]",
                show_header=True,
                header_style="bold magenta",
                border_style="cyan",
            )
            table.add_column("Endpoint", style="cyan")
            table.add_column("↑ Upload", justify="right", style="green")
            table.add_column("↓ Download", justify="right", style="blue")

            for ep in server_info.endpoints:
                table.add_row(
                    ep.endpoint,
                    format_rate(ep.upload_rate),
                    format_rate(ep.download_rate),
                )
            console.print(table)

    except Exception:
        print_warning("Could not reach server")

    # ── Quota ─────────────────────────────────────────────────
    if not token:
        return

    try:
        headers = {"Authorization": f"Bearer {token}"}

        rq = httpx.get(f"{server}/api/users/me/quota", headers=headers, timeout=10)
        if rq.status_code == 200:
            qdata = rq.json()
            used = qdata.get("storage_used", 0)
            q = qdata.get("quota", {})
            max_storage = q.get("max_storage_size")
            pct = qdata.get("usage_percent")

            quota_text = f"[bold]Storage:[/bold] {format_size(used)}"
            if max_storage:
                quota_text += f" / {format_size(max_storage)}"
            if pct is not None:
                color = "green" if pct < 80 else ("yellow" if pct < 95 else "red")
                bar_w = 20
                filled = int(bar_w * pct / 100)
                bar = "█" * filled + "░" * (bar_w - filled)
                quota_text += f"\n[{color}]{bar}[/{color}] {pct:.1f}%"
            if qdata.get("is_over_quota"):
                quota_text += "\n[bold red]⚠ Over quota![/bold red]"
            if q.get("max_upload_size"):
                quota_text += f"\n[bold]Max Upload:[/bold] {format_size(q['max_upload_size'])}"

            console.print(Panel(quota_text, title="[bold cyan]📊 Quota[/bold cyan]", border_style="cyan"))

    except Exception:
        pass  # User info is optional

    console.print("\n[dim]⭐ Like EasyTransfer? Star us → [bold]https://github.com/ZGCA-Forge/ETransfer[/bold][/dim]")


@app.command()
def login(
    timeout: int = typer.Option(
        300,
        "--timeout",
        help="Login timeout in seconds",
    ),
) -> None:
    """Login via OIDC (server-driven).

    Queries the configured server for its authentication requirements,
    prints a login URL for you to open in a browser, then waits for the
    callback to complete. The session token is cached locally.

    Run ``etransfer setup <address>`` first to configure the server.
    """
    import httpx

    server = _get_server_url()
    console.print()

    # Step 1: Get login info from server
    with console.status("[bold cyan]Fetching login config from server...", spinner="dots"):
        try:
            r = httpx.get(f"{server}/api/users/login-info", timeout=10)
            if r.status_code == 501:
                print_info("Server does not require login (user system disabled)")
                return
            r.raise_for_status()
            login_config = r.json()
        except httpx.HTTPStatusError as e:
            print_error(f"Server returned error: {e.response.status_code}")
            raise typer.Exit(1)
        except httpx.ConnectError:
            print_error(f"Cannot connect to server: {server}")
            raise typer.Exit(1)

    provider = login_config.get("provider", "oidc")
    issuer = login_config.get("issuer", "")
    console.print(f"  [dim]Provider: {provider} ({issuer})[/dim]")

    # Step 2: Start CLI login flow
    with console.status("[bold cyan]Starting login flow...", spinner="dots"):
        r = httpx.post(f"{server}/api/users/login/start", timeout=10)
        r.raise_for_status()
        start_data = r.json()

    state = start_data["state"]
    authorize_url = start_data["authorize_url"]
    short_url = start_data.get("short_url")
    display_url = short_url or authorize_url

    # CLI: only print the URL, do NOT open browser
    console.print()
    console.print(
        Panel(
            f"[bold]Open this URL in your browser to login:[/bold]\n\n" f"[link={display_url}]{display_url}[/link]",
            title="[bold cyan]Login[/bold cyan]",
            border_style="cyan",
        )
    )
    print_info("Open the URL above in your browser to authenticate.")
    print_info("If auto-detection fails, you can paste the session token from the browser page.")

    # Step 3: Poll + manual input concurrently — first one wins
    console.print()
    poll_url = f"{server}/api/users/login/poll/{state}"
    poll_interval = 2
    result: dict = {}  # shared result container
    done_event = threading.Event()

    def _poll_loop() -> None:
        """Background thread: poll server until login completes or cancelled."""
        import httpx as _httpx

        elapsed = 0
        while not done_event.is_set() and elapsed < timeout:
            time.sleep(poll_interval)
            if done_event.is_set():
                return
            elapsed += poll_interval
            try:
                r = _httpx.get(poll_url, timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    if data.get("completed"):
                        result["source"] = "poll"
                        result["token"] = data["token"]
                        result["user"] = data.get("user", {})
                        result["expires_at"] = data.get("expires_at")
                        done_event.set()
                        return
            except Exception:
                pass

    poll_thread = threading.Thread(target=_poll_loop, daemon=True)
    poll_thread.start()

    # Main thread: prompt for manual token while poll runs in background
    print_info("Waiting for browser login... (or paste token below to skip)")
    console.print()

    # Use a non-blocking input loop so we can check done_event
    import sys

    console.print("[bold cyan]Token (press Enter after paste): [/bold cyan]", end="")
    sys.stdout.flush()

    def _stdin_reader() -> None:
        """Read stdin in a separate thread (works on Windows too)."""
        try:
            line = sys.stdin.readline()
            if line:
                manual_token = line.strip()
                if manual_token:
                    result["source"] = "manual"
                    result["manual_token"] = manual_token
                    done_event.set()
        except Exception:
            pass

    stdin_thread = threading.Thread(target=_stdin_reader, daemon=True)
    stdin_thread.start()

    # Wait until either poll or manual input completes
    done_event.wait(timeout=timeout)

    poll_thread.join(timeout=2)

    if not result:
        console.print()
        print_error(f"Login timed out after {timeout}s and no token provided.")
        raise typer.Exit(1)

    # --- Process result ---
    if result.get("source") == "poll":
        new_token = result["token"]
        user = result.get("user", {})
        username = user.get("username", "unknown")
        role = user.get("role", "user")
        groups = user.get("groups", [])

        console.print()
        print_success(f"Logged in as [bold]{username}[/bold] (role: {role})")
        if groups:
            console.print(f"   [dim]Groups: {', '.join(groups)}[/dim]")

        cfg = _load_client_config()
        cfg["token"] = new_token
        cfg["username"] = username
        cfg["role"] = role
        cfg["expires_at"] = result.get("expires_at")
        _save_client_config(cfg)
        print_info(f"Session saved to {CLIENT_CONFIG_FILE}")
        return

    # Manual token path
    manual_token = result["manual_token"]
    try:
        r = httpx.get(
            f"{server}/api/users/me",
            headers={"X-Session-Token": manual_token},
            timeout=10,
        )
        r.raise_for_status()
        user_data = r.json()
        username = user_data.get("username", "unknown")
        role = user_data.get("role", "user")
        groups = user_data.get("groups", [])

        console.print()
        print_success(f"Logged in as [bold]{username}[/bold] (role: {role})")
        if groups:
            console.print(f"   [dim]Groups: {', '.join(groups)}[/dim]")

        cfg = _load_client_config()
        cfg["token"] = manual_token
        cfg["username"] = username
        cfg["role"] = role
        cfg["expires_at"] = None
        _save_client_config(cfg)

        print_info(f"Session saved to {CLIENT_CONFIG_FILE}")
    except httpx.HTTPStatusError:
        print_error("Invalid token — server rejected it.")
        raise typer.Exit(1)
    except Exception as e:
        print_error(f"Failed to validate token: {e}")
        raise typer.Exit(1)


@app.command()
def logout() -> None:
    """Logout and invalidate session."""
    import httpx

    cfg = _load_client_config()
    server = cfg.get("server")
    token = cfg.get("token")

    if token and server:
        try:
            headers = {"Authorization": f"Bearer {token}"}
            httpx.post(f"{server}/api/users/logout", headers=headers, timeout=10)
        except Exception:
            pass

    # Clear session fields from config (keep server address)
    cfg.pop("token", None)
    cfg.pop("session_token", None)  # clean up legacy key
    cfg.pop("username", None)
    cfg.pop("role", None)
    cfg.pop("expires_at", None)
    _save_client_config(cfg)

    # Remove legacy session file if present
    legacy_session = Path.home() / ".etransfer" / "session"
    if legacy_session.exists():
        legacy_session.unlink()

    print_success("Logged out")


@server_app.command("recalculate-quota")
def recalculate_quota(
    token: Optional[str] = typer.Option(
        None,
        "--token",
        "-t",
        help="API token (overrides saved session)",
        envvar="ETRANSFER_TOKEN",
    ),
) -> None:
    """Recalculate storage quota for all users (admin only).

    Scans actual files on the server and updates each user's
    storage_used in the database.
    """
    import httpx

    server = _get_server_url()
    token = token or _get_token()
    if not token:
        print_error("Not logged in. Run [bold]etransfer login[/bold] first.")
        raise typer.Exit(1)

    try:
        headers = {"Authorization": f"Bearer {token}"}
        with console.status("[bold cyan]Recalculating storage...", spinner="dots"):
            r = httpx.post(
                f"{server}/api/admin/recalculate-storage",
                headers=headers,
                timeout=30,
            )
            r.raise_for_status()

        data = r.json()
        updated = data.get("updated", [])
        total_files = data.get("total_files", 0)
        orphan = data.get("orphan_bytes", 0)

        if updated:
            from rich.table import Table

            table = Table(
                title="[bold cyan]Updated Users[/bold cyan]",
                show_header=True,
                header_style="bold magenta",
                border_style="cyan",
            )
            table.add_column("User", style="white")
            table.add_column("Before", justify="right", style="red")
            table.add_column("After", justify="right", style="green")

            for u in updated:
                table.add_row(
                    u.get("username", str(u.get("user_id"))),
                    format_size(u.get("old", 0)),
                    format_size(u.get("new", 0)),
                )
            console.print(table)
        else:
            print_info("All users already have correct storage_used values")

        print_success(f"Scanned {total_files} files, updated {len(updated)} users")
        if orphan:
            print_warning(f"Orphan files (no owner): {format_size(orphan)}")

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            print_error("Session expired. Run [bold]etransfer login[/bold].")
        elif e.response.status_code == 403:
            print_error("Admin access required.")
        else:
            print_error(f"Failed: {e}")
        raise typer.Exit(1)
    except Exception as e:
        print_error(f"Failed: {e}")
        raise typer.Exit(1)


@server_app.command("gui")
def gui() -> None:
    """Launch the graphical user interface."""
    console.print()
    print_info("Launching GUI...")
    try:
        from etransfer.client.gui import run_gui

        run_gui()
    except ImportError as e:
        print_error(f"GUI dependencies not installed: {e}")
        console.print("[dim]Install with: pip install etransfer[gui][/dim]")
        raise typer.Exit(1)
    except Exception as e:
        print_error(f"Error launching GUI: {e}")
        raise typer.Exit(1)


@server_app.command("start")
def server_start(
    host: Optional[str] = typer.Option(None, "--host", "-h", help="Bind host (default: from config or 0.0.0.0)"),
    port: Optional[int] = typer.Option(None, "--port", "-p", help="Bind port (default: from config or 8765)"),
    workers: Optional[int] = typer.Option(
        None, "--workers", "-w", help="Number of workers (default: from config or 1)"
    ),
    storage: Optional[Path] = typer.Option(
        None, "--storage", help="Storage directory (default: from config or ./storage)"
    ),
    backend: Optional[str] = typer.Option(
        None, "--backend", "-b", help="State backend: memory, file, redis (default: from config or file)"
    ),
    redis: Optional[str] = typer.Option(None, "--redis", help="Redis URL (if using redis backend)"),
    config: Optional[Path] = typer.Option(
        None,
        "--config",
        "-c",
        help="Config file path (auto-discovered if not set)",
    ),
) -> None:
    """Start the ETransfer server.

    If --config is not provided, the config file is auto-discovered from:
      1. $ETRANSFER_CONFIG env var
      2. ./config.yaml
      3. ./config/config.yaml
      4. ~/.etransfer/server.yaml

    All options default to the value in the config file. CLI flags override config.
    """
    try:
        from etransfer.server.config import discover_config_path
    except ImportError:
        print_error("Server dependencies not installed. Run: pip install 'etransfer[server]'")
        raise typer.Exit(1)

    # Resolve config path for display
    resolved_config = config
    if resolved_config is None:
        resolved_config = discover_config_path()

    # Read config for display (before run_server loads it again)
    display_vals: dict = {}
    if resolved_config and resolved_config.exists():
        try:
            import yaml

            with open(resolved_config, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
            server_cfg = cfg.get("server", {})
            display_vals = {
                "host": host or server_cfg.get("host", "0.0.0.0"),  # nosec B104
                "port": port or server_cfg.get("port", DEFAULT_SERVER_PORT),
                "storage": storage or cfg.get("storage", {}).get("path", "./storage"),
                "backend": backend or cfg.get("state", {}).get("backend", "file"),
                "workers": workers or server_cfg.get("workers", 1),
            }
        except Exception:
            pass

    if not display_vals:
        display_vals = {
            "host": host or "0.0.0.0",  # nosec B104
            "port": port or DEFAULT_SERVER_PORT,
            "storage": storage or "./storage",
            "backend": backend or "file",
            "workers": workers or 1,
        }

    console.print()
    info_lines = (
        f"[bold]Host:[/bold] {display_vals['host']}:{display_vals['port']}\n"
        f"[bold]Storage:[/bold] {display_vals['storage']}\n"
        f"[bold]Backend:[/bold] {display_vals['backend']}\n"
        f"[bold]Workers:[/bold] {display_vals['workers']}\n"
    )
    if resolved_config:
        info_lines += f"[bold]Config:[/bold] {resolved_config.resolve()}"
    else:
        info_lines += "[bold]Config:[/bold] [dim]none (defaults + env vars)[/dim]"

    console.print(
        Panel(
            info_lines,
            title="[bold cyan]Starting Server[/bold cyan]",
            border_style="cyan",
        )
    )

    try:
        from etransfer.server.main import run_server
    except ImportError:
        print_error("Server dependencies not installed. Run: pip install 'etransfer[server]'")
        raise typer.Exit(1)

    try:
        run_server(
            host=host,
            port=port,
            workers=workers,
            config_path=config,
            storage_path=storage,
            state_backend=backend,
            redis_url=redis,
        )
    except Exception as e:
        print_error(f"Server error: {e}")
        raise typer.Exit(1)


def _read_token_from_config_yaml() -> Optional[str]:
    """Read the first auth token from the server config YAML (auto-discovered).

    This is used by admin commands (like ``server reload``) that run on the
    same machine as the server, so they can authenticate without requiring
    the user to pass ``--token`` every time.
    """
    try:
        import yaml

        from etransfer.server.config import discover_config_path

        path = discover_config_path()
        if not path:
            return None
        with open(path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        tokens = cfg.get("auth", {}).get("tokens", [])
        if tokens and isinstance(tokens, list):
            return tokens[0]  # type: ignore[no-any-return]
    except Exception:
        pass
    return None


@server_app.command("reload")
def server_reload(
    address: Optional[str] = typer.Argument(
        None,
        help="Server address (host:port). Default: read from config YAML or client config.",
    ),
    token: Optional[str] = typer.Option(
        None,
        "--token",
        "-t",
        help="API token. Default: read from server config YAML.",
    ),
    config: Optional[Path] = typer.Option(
        None,
        "--config",
        "-c",
        help="Server config YAML to read token from (auto-discovered if omitted).",
    ),
) -> None:
    """Reload hot-reloadable server config without restarting.

    Sends a reload request to a running EasyTransfer server.
    Token and address are automatically read from the server config YAML
    (same auto-discovery as ``et server start``), so in most cases you
    can simply run:

      et server reload

    The server re-reads its config file and applies changes to:

      - role_quotas       (per-role quota defaults)
      - auth_tokens       (valid API tokens)
      - max_storage_size  (storage quota)
      - advertised_endpoints
      - retention policies (default_retention, token_retention_policies)

    Changes that require a full restart:

      - host / port / workers (uvicorn binding)
      - storage_path / state_backend / redis_url
      - OIDC configuration
      - Database backend

    Note: Group quotas are stored in the database and managed via the
    API (PUT /api/groups/{id}/quota); they take effect immediately
    without reload.
    """
    import httpx

    # ── Read server config YAML for token & address defaults ──
    yaml_token = None
    yaml_address = None
    try:
        import yaml

        from etransfer.server.config import discover_config_path

        cfg_path = config or discover_config_path()
        if cfg_path and cfg_path.exists():
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
            tokens = cfg.get("auth", {}).get("tokens", [])
            if tokens and isinstance(tokens, list):
                yaml_token = tokens[0]
            srv = cfg.get("server", {})
            h = srv.get("host", "127.0.0.1")
            p = srv.get("port", DEFAULT_SERVER_PORT)
            if h == "0.0.0.0":  # nosec B104
                h = "127.0.0.1"
            yaml_address = f"http://{h}:{p}"
            console.print(f"[dim]Config: {cfg_path.resolve()}[/dim]")
    except Exception:
        pass

    # Resolve address: --argument > yaml > client config
    if address:
        server_url = _normalise_server_url(address)
    elif yaml_address:
        server_url = yaml_address
    else:
        server_url = _get_server_url()

    # Resolve token: --token > yaml > client config
    if not token:
        token = yaml_token
    if not token:
        token = _get_token()
    if not token:
        print_error("No admin token found. Ensure auth.tokens is set in config YAML, " "or provide --token.")
        raise typer.Exit(1)

    url = f"{server_url}/api/admin/reload-config"
    headers: dict[str, str] = {}

    from etransfer.common.constants import AUTH_HEADER

    headers[AUTH_HEADER] = token

    console.print(f"[dim]POST {url}[/dim]")

    try:
        resp = httpx.post(url, headers=headers, timeout=15)
    except httpx.ConnectError:
        print_error(f"Cannot connect to {server_url}")
        raise typer.Exit(1)

    if resp.status_code == 401:
        print_error("Unauthorized. Check your token or login as admin.")
        raise typer.Exit(1)
    if resp.status_code == 403:
        print_error("Forbidden. Admin access required.")
        raise typer.Exit(1)
    if resp.status_code != 200:
        print_error(f"Server returned {resp.status_code}: {resp.text}")
        raise typer.Exit(1)

    data = resp.json()

    if data.get("reloaded"):
        changes = data.get("changes", {})
        table = Table(title="Hot-Reloaded Changes", border_style="green")
        table.add_column("Field", style="bold")
        table.add_column("Old", style="red")
        table.add_column("New", style="green")
        for field, diff in changes.items():
            old_val = str(diff.get("old", ""))
            new_val = str(diff.get("new", ""))
            # Truncate long values for display
            if len(old_val) > 60:
                old_val = old_val[:57] + "..."
            if len(new_val) > 60:
                new_val = new_val[:57] + "..."
            table.add_row(field, old_val, new_val)
        console.print()
        console.print(table)
        print_success(f"Reloaded {len(changes)} field(s).")
    else:
        console.print("\n[yellow]No changes detected.[/yellow] Config file unchanged.")

    # Show helpful note
    note = data.get("note", "")
    if note:
        console.print(f"\n[dim]{note}[/dim]")

    # Show which fields are hot-reloadable vs static
    hot_fields = data.get("hot_reloadable", [])
    static_fields = data.get("requires_restart", [])
    if hot_fields or static_fields:
        console.print()
        info = Table.grid(padding=(0, 2))
        info.add_column(style="bold cyan")
        info.add_column()
        if hot_fields:
            info.add_row("Hot-reloadable:", ", ".join(hot_fields))
        if static_fields:
            info.add_row("Requires restart:", ", ".join(static_fields))
        console.print(info)
    console.print()


@server_app.command("config")
def server_config(
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Output config file path",
    ),
) -> None:
    """Generate a sample server configuration file.

    The generated config is meant to be placed in the config/ folder:
      etransfer server config -o config/config.yaml
    The config/ folder is git-ignored so your secrets stay local.
    """
    config_content = """# EasyTransfer Server Configuration
# Save to config/config.yaml (git-ignored) and customize for your deployment.
# See config/README.md for the full setup guide.

server:
  host: 0.0.0.0
  port: 8765
  workers: 1

storage:
  path: ./storage
  max_storage_size: null  # Total quota (e.g., "10GB")

# State backend: memory (testing), file (default), redis (multi-worker)
state:
  backend: file
  redis_url: redis://localhost:6379/0

auth:
  enabled: true
  tokens:
    - "your-api-token-here"

# File retention policies: permanent / download_once / ttl
retention:
  default: permanent
  # default_ttl: 86400
  # token_policies:
  #   guest-token:
  #     default_retention: download_once

network:
  interfaces: []  # Empty = auto-detect
  prefer_ipv4: true
  advertised_endpoints: []

# User system with OIDC
user_system:
  enabled: false
  oidc:
    issuer_url: "https://ca.auth.example.com"
    client_id: ""
    client_secret: ""
    callback_url: "http://localhost:8765/api/users/callback"
    scope: "openid profile email"
  database:
    backend: sqlite       # sqlite (default) or mysql
    path: ""              # SQLite path (default: <storage>/users.db)
    # mysql:              # MySQL settings (when backend=mysql)
    #   host: 127.0.0.1
    #   port: 3306
    #   user: root
    #   password: ""
    #   database: etransfer
  role_quotas:
    admin:
      max_storage_size: null
      max_upload_size: null
    user:
      max_storage_size: 10737418240  # 10GB
      max_upload_size: 5368709120     # 5GB
    guest:
      max_storage_size: 1073741824   # 1GB
      max_upload_size: 536870912      # 512MB
      upload_speed_limit: 10485760    # 10MB/s
      download_speed_limit: 10485760

cors:
  origins:
    - "*"
"""

    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(config_content)
        print_success(f"Config written to {output}")
    else:
        console.print(Panel(config_content, title="[bold cyan]Sample Config[/bold cyan]", border_style="cyan"))


if __name__ == "__main__":
    app()
