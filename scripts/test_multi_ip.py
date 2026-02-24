#!/usr/bin/env python3
"""
Test script for multi-IP load balancing scenario.

Server is configured with multiple advertised IPs (127.0.0.1, 127.0.0.2, 127.0.0.3).
Client connects to one IP and discovers all available endpoints.

Usage:
    python scripts/test_multi_ip.py
"""

import asyncio
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Default port for EasyTransfer (like MC's 25565)
DEFAULT_PORT = 8765

# Test configuration
SERVER_IPS = ["127.0.0.1", "127.0.0.2", "127.0.0.3"]
CLIENT_CONNECT_IP = "127.0.0.1"  # Client initially connects here
TEST_FILE_SIZE = 10 * 1024 * 1024  # 10MB test file


def create_test_config(storage_path: Path) -> Path:
    """Create server config with multiple advertised endpoints."""
    import yaml

    config = {
        "server": {
            "host": "0.0.0.0",  # Listen on all interfaces
            "port": DEFAULT_PORT,
            "workers": 1,
        },
        "state": {
            "backend": "memory",
        },
        "auth": {
            "enabled": False,
        },
        "network": {
            "advertised_endpoints": SERVER_IPS,
        },
    }

    config_path = storage_path / "config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config, f)

    return config_path


def start_server(storage_path: Path, config_path: Path) -> subprocess.Popen:
    """Start the EasyTransfer server."""
    import json

    env = os.environ.copy()
    env["ETRANSFER_STORAGE_PATH"] = str(storage_path)
    env["ETRANSFER_STATE_BACKEND"] = "memory"
    env["ETRANSFER_AUTH_ENABLED"] = "false"
    env["ETRANSFER_PORT"] = str(DEFAULT_PORT)
    # JSON array format for pydantic-settings
    env["ETRANSFER_ADVERTISED_ENDPOINTS"] = json.dumps(SERVER_IPS)

    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "etransfer.server.main:app",
            "--host",
            "0.0.0.0",
            "--port",
            str(DEFAULT_PORT),
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=PROJECT_ROOT,
    )

    # Wait for server to be ready
    import httpx

    for i in range(30):
        try:
            resp = httpx.get(f"http://{CLIENT_CONNECT_IP}:{DEFAULT_PORT}/api/health", timeout=2)
            if resp.status_code == 200:
                print(f"[Server] Started and healthy on port {DEFAULT_PORT}")
                return proc
        except Exception:
            pass
        time.sleep(0.5)

    # Print server output if failed
    proc.terminate()
    output = proc.stdout.read().decode() if proc.stdout else ""
    print(f"[Server] Failed to start. Output:\n{output}")
    raise RuntimeError("Server failed to start")


def create_test_file(path: Path, size: int) -> None:
    """Create a test file with random content."""
    with open(path, "wb") as f:
        # Write in chunks to avoid memory issues
        chunk_size = 1024 * 1024  # 1MB
        remaining = size
        while remaining > 0:
            write_size = min(chunk_size, remaining)
            f.write(os.urandom(write_size))
            remaining -= write_size


def test_endpoint_discovery():
    """Test that client can discover all endpoints."""
    from rich.console import Console
    from rich.table import Table

    from etransfer.client.tus_client import EasyTransferClient

    console = Console()
    console.print("\n[bold cyan]═══ Test 1: Endpoint Discovery ═══[/bold cyan]\n")

    with EasyTransferClient(f"http://{CLIENT_CONNECT_IP}:{DEFAULT_PORT}") as client:
        # Get endpoints
        endpoints = client.get_endpoints()

        console.print(f"[green]✓[/green] Connected to {CLIENT_CONNECT_IP}:{DEFAULT_PORT}")
        console.print(f"[green]✓[/green] Discovered {endpoints['total_endpoints']} endpoints\n")

        # Display endpoints table
        table = Table(title="Available Endpoints")
        table.add_column("IP Address", style="cyan")
        table.add_column("URL", style="blue")
        table.add_column("Interface")
        table.add_column("Upload Load %", justify="right")
        table.add_column("Download Load %", justify="right")

        for ep in endpoints["endpoints"]:
            table.add_row(
                ep["ip_address"],
                ep["url"],
                ep.get("interface", "N/A"),
                f"{ep.get('upload_load_percent', 0):.1f}%",
                f"{ep.get('download_load_percent', 0):.1f}%",
            )

        console.print(table)
        console.print(f"\n[yellow]Best for Upload:[/yellow] {endpoints['best_for_upload']}")
        console.print(f"[yellow]Best for Download:[/yellow] {endpoints['best_for_download']}")

        # Verify all configured IPs are present
        discovered_ips = {ep["ip_address"] for ep in endpoints["endpoints"]}
        for ip in SERVER_IPS:
            if ip in discovered_ips:
                console.print(f"[green]✓[/green] Found configured IP: {ip}")
            else:
                console.print(f"[red]✗[/red] Missing configured IP: {ip}")

        return endpoints


def test_endpoint_connectivity():
    """Test connectivity to all endpoints."""
    from rich.console import Console
    from rich.table import Table

    from etransfer.client.tus_client import EasyTransferClient

    console = Console()
    console.print("\n[bold cyan]═══ Test 2: Endpoint Connectivity ═══[/bold cyan]\n")

    with EasyTransferClient(f"http://{CLIENT_CONNECT_IP}:{DEFAULT_PORT}") as client:
        results = client.test_all_endpoints(timeout=5.0)

        # Display results table
        table = Table(title="Connectivity Test Results")
        table.add_column("IP Address", style="cyan")
        table.add_column("Reachable", justify="center")
        table.add_column("Latency (ms)", justify="right")
        table.add_column("Status/Error")

        for ep in results["endpoints"]:
            reachable = "✓" if ep.get("reachable") else "✗"
            reachable_style = "green" if ep.get("reachable") else "red"
            latency = f"{ep.get('latency_ms', 'N/A')}" if ep.get("reachable") else "-"
            status = ep.get("status", ep.get("error", "unknown"))

            table.add_row(
                ep["ip_address"],
                f"[{reachable_style}]{reachable}[/{reachable_style}]",
                latency,
                status,
            )

        console.print(table)
        console.print(f"\n[green]Reachable:[/green] {results['reachable_count']}/{results['total_count']}")

        if results["best_reachable"]:
            console.print(
                f"[yellow]Best Endpoint:[/yellow] {results['best_reachable']} ({results['best_latency_ms']:.2f}ms)"
            )

        return results


def test_upload_with_endpoint_selection(storage_path: Path):
    """Test uploading a file using selected endpoint."""
    from rich.console import Console
    from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn

    from etransfer.client.tus_client import EasyTransferClient

    console = Console()
    console.print("\n[bold cyan]═══ Test 3: Upload with Endpoint Selection ═══[/bold cyan]\n")

    # Create test file
    test_file = storage_path / "test_upload.bin"
    console.print(f"Creating {TEST_FILE_SIZE // (1024*1024)}MB test file...")
    create_test_file(test_file, TEST_FILE_SIZE)

    with EasyTransferClient(f"http://{CLIENT_CONNECT_IP}:{DEFAULT_PORT}") as client:
        # Select best endpoint
        best_url = client.select_best_reachable_endpoint(
            for_upload=True,
            prefer_low_latency=True,
        )
        console.print(f"[yellow]Selected endpoint:[/yellow] {best_url}")

        # Create a new client for the selected endpoint
        with EasyTransferClient(best_url) as upload_client:
            # Create uploader with progress
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                console=console,
            ) as progress:
                task = progress.add_task("Uploading...", total=TEST_FILE_SIZE)

                def progress_callback(uploaded: int, total: int):
                    progress.update(task, completed=uploaded)

                uploader = upload_client.create_uploader(
                    str(test_file),
                    progress_callback=progress_callback,
                )
                upload_url = uploader.upload()

            console.print(f"[green]✓[/green] Upload complete: {upload_url}")
            return upload_url


def test_download_endpoints():
    """Test downloading file info from multiple endpoints."""
    from rich.console import Console

    from etransfer.client.tus_client import EasyTransferClient

    console = Console()
    console.print("\n[bold cyan]═══ Test 4: File Listing from Multiple Endpoints ═══[/bold cyan]\n")

    for ip in SERVER_IPS:
        url = f"http://{ip}:{DEFAULT_PORT}"
        try:
            with EasyTransferClient(url) as client:
                files = client.list_files()
                console.print(f"[green]✓[/green] {ip}: Found {len(files)} files")
                for f in files[:3]:  # Show first 3
                    console.print(f"    - {f.filename} ({f.size} bytes, {f.status.value})")
        except Exception as e:
            console.print(f"[red]✗[/red] {ip}: {e}")


def test_traffic_monitoring():
    """Test real-time traffic monitoring."""
    from rich.console import Console
    from rich.table import Table

    from etransfer.client.tus_client import EasyTransferClient

    console = Console()
    console.print("\n[bold cyan]═══ Test 5: Traffic Monitoring ═══[/bold cyan]\n")

    with EasyTransferClient(f"http://{CLIENT_CONNECT_IP}:{DEFAULT_PORT}") as client:
        traffic = client.get_traffic()

        table = Table(title="Traffic Statistics")
        table.add_column("Interface", style="cyan")
        table.add_column("Upload Rate", justify="right")
        table.add_column("Download Rate", justify="right")
        table.add_column("Speed (Mbps)", justify="right")
        table.add_column("Load %", justify="right")

        for iface in traffic["interfaces"][:5]:  # Limit to 5 interfaces
            table.add_row(
                iface["name"],
                iface["upload_rate_formatted"],
                iface["download_rate_formatted"],
                str(iface.get("speed_mbps", "N/A")),
                f"{iface.get('total_load_percent', 0):.1f}%",
            )

        console.print(table)

        console.print("\n[bold]Total:[/bold]")
        console.print(f"  Upload: {traffic['total']['upload_rate_formatted']}")
        console.print(f"  Download: {traffic['total']['download_rate_formatted']}")


def main():
    """Run all tests."""
    from rich.console import Console
    from rich.panel import Panel

    console = Console()

    console.print(
        Panel.fit(
            "[bold green]EasyTransfer Multi-IP Load Balancing Test[/bold green]\n\n"
            f"Server IPs: {', '.join(SERVER_IPS)}\n"
            f"Client connects to: {CLIENT_CONNECT_IP}:{DEFAULT_PORT}",
            title="Test Configuration",
        )
    )

    # Create temp directory
    with tempfile.TemporaryDirectory() as tmpdir:
        storage_path = Path(tmpdir) / "storage"
        storage_path.mkdir(parents=True)

        # Create config
        config_path = create_test_config(Path(tmpdir))

        # Start server
        console.print("\n[bold]Starting server...[/bold]")
        server_proc = None

        try:
            server_proc = start_server(storage_path, config_path)

            # Run tests
            test_endpoint_discovery()
            test_endpoint_connectivity()
            test_upload_with_endpoint_selection(Path(tmpdir))
            test_download_endpoints()
            test_traffic_monitoring()

            console.print("\n[bold green]═══ All Tests Completed ═══[/bold green]\n")

        except KeyboardInterrupt:
            console.print("\n[yellow]Tests interrupted[/yellow]")
        except Exception as e:
            console.print(f"\n[red]Test failed: {e}[/red]")
            import traceback

            traceback.print_exc()
        finally:
            if server_proc:
                console.print("[bold]Stopping server...[/bold]")
                server_proc.terminate()
                try:
                    server_proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    server_proc.kill()


if __name__ == "__main__":
    main()
