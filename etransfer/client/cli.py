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

from etransfer.common.constants import DEFAULT_CHUNK_SIZE, DEFAULT_SERVER_PORT

app = typer.Typer(
    name="etransfer",
    help="EasyTransfer - Fast TUS-based file transfer tool",
    no_args_is_help=True,
)

console = Console()

# Server subcommand
server_app = typer.Typer(help="Server management commands")
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
    file_path: Path = typer.Argument(..., help="Path to file to upload"),
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
        None,
        "--retention",
        "-r",
        help="Retention policy: permanent, download_once, ttl (default: server decides)",
    ),
    retention_ttl: Optional[int] = typer.Option(
        None,
        "--retention-ttl",
        help="TTL in seconds (only for --retention ttl)",
    ),
    wait_on_quota: bool = typer.Option(
        True,
        "--wait-on-quota/--no-wait-on-quota",
        help="Auto-wait and resume when storage quota is full",
    ),
) -> None:
    """Upload a file to the server."""
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

    file_size = file_path.stat().st_size

    # Build info text
    info_lines = f"[bold]{file_path.name}[/bold]\n"
    info_lines += f"[dim]Size: {format_size(file_size)} | Chunk: {format_size(chunk_size)}[/dim]"
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
        if retention == "download_once":
            print_warning("File will be deleted after first download")
        elif retention == "ttl":
            print_info(f"File will expire in {retention_ttl}s after upload completes")

    except KeyboardInterrupt:
        console.print()
        print_warning("Upload cancelled by user.")
        raise typer.Exit(130)
    except Exception as e:
        console.print()
        print_error(f"Upload failed: {e}")
        raise typer.Exit(1)


@app.command()
def download(
    file_id: str = typer.Argument(..., help="File ID to download"),
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
    no_cache: bool = typer.Option(
        True,
        "--no-cache/--cache",
        help="Direct-write mode (faster) vs chunk cache mode",
    ),
) -> None:
    """Download a file from the server."""
    server = _get_server_url()
    token = token or _get_token()

    try:
        from etransfer.client.downloader import ChunkDownloader

        downloader = ChunkDownloader(server, token=token, max_concurrent=5)

        # Get file info
        info = downloader.get_file_info(file_id)

        # Print header
        console.print()
        panel = Panel(
            f"[bold]{info.filename}[/bold]\n"
            f"[dim]Size: {format_size(info.size)} | Available: {format_size(info.available_size)}[/dim]",
            title="[bold cyan]📥 Download[/bold cyan]",
            border_style="cyan",
        )
        console.print(panel)

        if info.available_size < info.size:
            print_warning(
                f"Only {format_size(info.available_size)} of {format_size(info.size)} "
                f"available (upload in progress) — will follow upload"
            )

        # Determine output path
        if output is None:
            output_path = Path.cwd() / info.filename
        elif output.is_dir():
            output_path = output / info.filename
        else:
            output_path = output

        is_partial = info.available_size < info.size

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
                    use_cache=not no_cache,
                )
            else:
                success = downloader.download_file(
                    file_id,
                    output_path,
                    progress_callback=update_progress,
                    use_cache=not no_cache,
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
        print_warning("Download cancelled by user.")
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
        table.add_column("ID", style="dim", width=12)
        table.add_column("Filename", style="white")
        table.add_column("Size", justify="right", style="green")
        table.add_column("Progress", justify="right")
        table.add_column("Status", justify="center")
        table.add_column("Retention", justify="center", style="dim")

        for f in files:
            # Progress bar in text
            progress_pct = f.get("progress", 0)
            bar_width = 10
            filled = int(bar_width * progress_pct / 100)
            bar = "█" * filled + "░" * (bar_width - filled)

            file_status = f.get("status", "complete")
            if file_status == "complete":
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

            table.add_row(
                f.get("file_id", "")[:10] + "..",
                f.get("filename", "unknown"),
                format_size(f.get("size", 0)),
                progress_str,
                status,
                retention_str,
            )

        console.print(table)
        console.print(f"[dim]Page {page} | Showing {len(files)} files[/dim]")

    except Exception as e:
        print_error(f"Failed to list files: {e}")
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
    """Get server information."""
    server = _get_server_url()
    token = token or _get_token()

    try:
        from etransfer.client.tus_client import EasyTransferClient

        console.print()
        with console.status("[bold cyan]Connecting to server...", spinner="dots"):
            with EasyTransferClient(server, token=token) as client:
                server_info = client.get_server_info()

        # Server info panel
        info_text = (
            f"[bold]Version:[/bold] {server_info.version}\n"
            f"[bold]TUS Version:[/bold] {server_info.tus_version}\n"
            f"[bold]Chunk Size:[/bold] {format_size(server_info.chunk_size)}\n"
            f"[bold]Total Files:[/bold] {server_info.total_files}\n"
            f"[bold]Total Storage:[/bold] {format_size(server_info.total_size)}"
        )
        if server_info.max_upload_size:
            info_text += f"\n[bold]Max Upload:[/bold] {format_size(server_info.max_upload_size)}"

        panel = Panel(
            info_text,
            title="[bold cyan]🖥️  Server Information[/bold cyan]",
            border_style="cyan",
        )
        console.print(panel)

        # Network interfaces table
        if server_info.interfaces:
            table = Table(
                title="[bold cyan]🌐 Network Interfaces[/bold cyan]",
                show_header=True,
                header_style="bold magenta",
                border_style="cyan",
            )
            table.add_column("Interface", style="white")
            table.add_column("IP Address", style="cyan")
            table.add_column("Speed", justify="right")
            table.add_column("↑ Upload", justify="right", style="green")
            table.add_column("↓ Download", justify="right", style="blue")

            for iface in server_info.interfaces:
                table.add_row(
                    iface.name,
                    iface.ip_address,
                    f"{iface.speed_mbps} Mbps" if iface.speed_mbps else "N/A",
                    format_rate(iface.upload_rate),
                    format_rate(iface.download_rate),
                )

            console.print(table)

    except Exception as e:
        print_error(f"Failed to get server info: {e}")
        raise typer.Exit(1)


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
def whoami(
    token: Optional[str] = typer.Option(
        None,
        "--token",
        "-t",
        help="Session token (overrides saved session)",
        envvar="ETRANSFER_TOKEN",
    ),
) -> None:
    """Show current user info and quota."""
    import httpx

    server = _get_server_url()
    token = token or _get_token()

    if not token:
        print_error("Not logged in. Run [bold]etransfer login[/bold] first.")
        raise typer.Exit(1)

    console.print()
    try:
        headers = {"Authorization": f"Bearer {token}"}
        r = httpx.get(f"{server}/api/users/me", headers=headers, timeout=10)
        if r.status_code == 401:
            print_error("Session expired. Run [bold]etransfer login[/bold] again.")
            raise typer.Exit(1)
        r.raise_for_status()
        user = r.json()

        r2 = httpx.get(f"{server}/api/users/me/quota", headers=headers, timeout=10)
        quota = r2.json() if r2.status_code == 200 else {}

        info_text = (
            f"[bold]Username:[/bold] {user.get('username', '?')}\n"
            f"[bold]Display Name:[/bold] {user.get('display_name', 'N/A')}\n"
            f"[bold]Email:[/bold] {user.get('email', 'N/A')}\n"
            f"[bold]Role:[/bold] {user.get('role', '?')}\n"
            f"[bold]Admin:[/bold] {'Yes' if user.get('is_admin') else 'No'}\n"
            f"[bold]Groups:[/bold] {', '.join(user.get('groups', [])) or 'None'}"
        )

        if quota:
            q = quota.get("quota", {})
            max_storage = q.get("max_storage_size")
            storage_used = quota.get("storage_used", 0)
            usage_pct = quota.get("usage_percent")

            info_text += "\n\n[bold underline]Quota:[/bold underline]\n"
            info_text += f"  Storage Used: {format_size(storage_used)}"
            if max_storage:
                info_text += f" / {format_size(max_storage)}"
                if usage_pct is not None:
                    info_text += f" ({usage_pct}%)"
            else:
                info_text += " (unlimited)"
            if q.get("max_upload_size"):
                info_text += f"\n  Max Upload: {format_size(q['max_upload_size'])}"
            if q.get("upload_speed_limit"):
                info_text += f"\n  Upload Speed: {format_size(q['upload_speed_limit'])}/s"
            if q.get("download_speed_limit"):
                info_text += f"\n  Download Speed: {format_size(q['download_speed_limit'])}/s"

        console.print(
            Panel(
                info_text,
                title="[bold cyan]User Profile[/bold cyan]",
                border_style="cyan",
            )
        )

    except httpx.ConnectError:
        print_error(f"Cannot connect to server: {server}")
        raise typer.Exit(1)
    except Exception as e:
        print_error(f"Failed: {e}")
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


@app.command("status")
def client_status() -> None:
    """Show current client configuration and session status."""
    cfg = _load_client_config()

    if not cfg.get("server"):
        print_warning("Not configured. Run [bold]etransfer setup <address>[/bold] first.")
        return

    console.print()
    lines = f"[bold]Server:[/bold] {cfg['server']}"
    if cfg.get("token"):
        username = cfg.get("username", "?")
        role = cfg.get("role", "?")
        lines += f"\n[bold]Logged in as:[/bold] {username} (role: {role})"
        if cfg.get("expires_at"):
            lines += f"\n[bold]Session expires:[/bold] {cfg['expires_at']}"
    else:
        lines += "\n[bold]Session:[/bold] [dim]not logged in[/dim]"
    lines += f"\n[bold]Config file:[/bold] {CLIENT_CONFIG_FILE}"

    console.print(
        Panel(
            lines,
            title="[bold cyan]Client Status[/bold cyan]",
            border_style="cyan",
        )
    )


@app.command()
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


@app.command()
def version() -> None:
    """Show version information."""
    from etransfer import __version__

    console.print()
    text = Text()
    text.append("╭─────────────────────────────────────────╮\n", style="cyan")
    text.append("│", style="cyan")
    text.append("  EasyTransfer                           ", style="bold white")
    text.append("│\n", style="cyan")
    text.append("│", style="cyan")
    text.append(f"  Version: {__version__:<29}", style="dim white")
    text.append("│\n", style="cyan")
    text.append("│", style="cyan")
    text.append("  TUS Protocol Based File Transfer       ", style="dim white")
    text.append("│\n", style="cyan")
    text.append("╰─────────────────────────────────────────╯", style="cyan")
    console.print(text)


if __name__ == "__main__":
    app()
