"""Server information API routes."""

from typing import Any, Optional

from fastapi import APIRouter, Request

from etransfer import __version__
from etransfer.common.constants import DEFAULT_SERVER_PORT, TUS_EXTENSIONS, TUS_VERSION
from etransfer.common.models import NetworkInterface, ServerInfo
from etransfer.server.services.ip_mgr import IPManager
from etransfer.server.services.traffic import TrafficMonitor
from etransfer.server.tus.storage import TusStorage


def create_info_router(
    storage: TusStorage,
    traffic_monitor: "TrafficMonitor",
    ip_manager: "IPManager",
    max_upload_size: Optional[int] = None,
    server_port: int = DEFAULT_SERVER_PORT,
) -> APIRouter:
    """Create server info router.

    ``advertised_endpoints`` is read from ``request.app.state.settings``
    at request time so hot-reloaded config takes effect immediately.

    Args:
        storage: TUS storage backend
        traffic_monitor: Traffic monitoring service
        ip_manager: IP/NIC management service
        max_upload_size: Maximum upload size
        server_port: Server port number

    Returns:
        FastAPI router
    """
    router = APIRouter(prefix="/api", tags=["Server Info"])
    _port = server_port

    @router.get("/info", response_model=ServerInfo)
    async def get_server_info() -> ServerInfo:
        """Get server information.

        Returns server capabilities, network interfaces, and storage stats.
        Each interface includes traffic load percentages for load balancing.
        """
        # Get network interfaces with traffic info
        interfaces = []
        for iface in ip_manager.get_interfaces():
            traffic = traffic_monitor.get_interface_traffic(iface.name)
            interfaces.append(
                NetworkInterface(
                    name=iface.name,
                    ip_address=iface.ip_address,
                    is_up=iface.is_up,
                    speed_mbps=traffic.get("speed_mbps") or iface.speed_mbps,
                    bytes_sent=traffic.get("bytes_sent", 0),
                    bytes_recv=traffic.get("bytes_recv", 0),
                    upload_rate=traffic.get("upload_rate", 0),
                    download_rate=traffic.get("download_rate", 0),
                    upload_load_percent=traffic.get("upload_load_percent", 0),
                    download_load_percent=traffic.get("download_load_percent", 0),
                    total_load_percent=traffic.get("total_load_percent", 0),
                )
            )

        # Get storage stats
        files = await storage.list_files()
        uploads = await storage.list_uploads()

        total_files = len(files) + len([u for u in uploads if u.is_final])
        total_size = sum(f.get("size", 0) for f in files)
        total_size += sum(u.offset for u in uploads)

        return ServerInfo(
            version=__version__,
            tus_version=TUS_VERSION,
            tus_extensions=TUS_EXTENSIONS,
            max_upload_size=max_upload_size,
            chunk_size=storage.chunk_size,
            interfaces=interfaces,
            total_files=total_files,
            total_size=total_size,
        )

    @router.get("/health")
    async def health_check() -> dict[str, Any]:
        """Health check endpoint."""
        return {"status": "healthy"}

    @router.get("/stats")
    async def get_stats() -> dict[str, Any]:
        """Get detailed server statistics."""
        # Get traffic stats
        traffic_stats = traffic_monitor.get_all_traffic()

        # Get file stats
        files = await storage.list_files()
        uploads = await storage.list_uploads(include_completed=False)

        return {
            "traffic": traffic_stats,
            "files": {
                "completed": len(files),
                "in_progress": len(uploads),
                "total_size": sum(f.get("size", 0) for f in files),
                "uploading_size": sum(u.offset for u in uploads),
            },
        }

    @router.get("/endpoints")
    async def get_endpoints(request: Request) -> dict[str, Any]:
        """Get available endpoints with traffic load status.

        Returns all available IP addresses and their current traffic load,
        allowing clients to select the best endpoint for upload/download.

        Response includes:
        - endpoints: List of endpoint info with IP, port, and load status
        - best_for_upload: Recommended endpoint for uploading
        - best_for_download: Recommended endpoint for downloading
        """
        # Read advertised_endpoints from settings (hot-reloadable)
        _settings = getattr(request.app.state, "settings", None)
        _advertised = (getattr(_settings, "advertised_endpoints", None) or []) if _settings else []

        endpoints = []
        best_upload_load = float("inf")
        best_download_load = float("inf")
        best_for_upload = None
        best_for_download = None

        # Use advertised endpoints if configured
        if _advertised:
            # Advertised endpoints - use configured IPs
            for ip_addr in _advertised:
                # Try to get traffic from matching interface
                traffic_data = {}
                speed_mbps = 0
                for iface in ip_manager.get_interfaces():
                    if iface.ip_address == ip_addr:
                        traffic_data = traffic_monitor.get_interface_traffic(iface.name)
                        speed_mbps = traffic_data.get("speed_mbps") or iface.speed_mbps or 0
                        break

                # If no matching interface, use aggregate traffic
                if not traffic_data:
                    total = traffic_monitor.get_total_traffic()
                    traffic_data = {
                        "upload_rate": total.get("upload_rate", 0),
                        "download_rate": total.get("download_rate", 0),
                        "upload_load_percent": 0,
                        "download_load_percent": 0,
                        "total_load_percent": 0,
                    }

                endpoint_info = {
                    "interface": "advertised",
                    "ip_address": ip_addr,
                    "url": f"http://{ip_addr}:{_port}",
                    "speed_mbps": speed_mbps,
                    "upload_rate": traffic_data.get("upload_rate", 0),
                    "download_rate": traffic_data.get("download_rate", 0),
                    "upload_load_percent": traffic_data.get("upload_load_percent", 0),
                    "download_load_percent": traffic_data.get("download_load_percent", 0),
                    "total_load_percent": traffic_data.get("total_load_percent", 0),
                    "is_available": True,  # Assume advertised endpoints are available
                }
                endpoints.append(endpoint_info)

                # Track best endpoints
                upload_load = traffic_data.get("upload_load_percent", 0)
                download_load = traffic_data.get("download_load_percent", 0)

                if upload_load < best_upload_load:
                    best_upload_load = upload_load
                    best_for_upload = endpoint_info["url"]

                if download_load < best_download_load:
                    best_download_load = download_load
                    best_for_download = endpoint_info["url"]
        else:
            # Auto-detect from interfaces
            for iface in ip_manager.get_interfaces():
                traffic = traffic_monitor.get_interface_traffic(iface.name)

                endpoint_info = {
                    "interface": iface.name,
                    "ip_address": iface.ip_address,
                    "url": f"http://{iface.ip_address}:{_port}",
                    "speed_mbps": traffic.get("speed_mbps") or iface.speed_mbps or 0,
                    "upload_rate": traffic.get("upload_rate", 0),
                    "download_rate": traffic.get("download_rate", 0),
                    "upload_load_percent": traffic.get("upload_load_percent", 0),
                    "download_load_percent": traffic.get("download_load_percent", 0),
                    "total_load_percent": traffic.get("total_load_percent", 0),
                    "is_available": iface.is_up,
                }
                endpoints.append(endpoint_info)

                # Track best endpoints
                upload_load = traffic.get("upload_load_percent", 0)
                download_load = traffic.get("download_load_percent", 0)

                if iface.is_up and upload_load < best_upload_load:
                    best_upload_load = upload_load
                    best_for_upload = endpoint_info["url"]

                if iface.is_up and download_load < best_download_load:
                    best_download_load = download_load
                    best_for_download = endpoint_info["url"]

        return {
            "endpoints": endpoints,
            "best_for_upload": best_for_upload,
            "best_for_download": best_for_download,
            "total_endpoints": len(endpoints),
            "server_port": _port,
        }

    @router.get("/storage")
    async def get_storage_status() -> dict[str, Any]:
        """Get storage quota and usage information.

        Returns current disk usage, quota limits, and whether
        the server can accept new uploads.
        """
        usage = await storage.get_storage_usage()

        def fmt(n: Optional[int]) -> str:
            if n is None:
                return "unlimited"
            val: float = float(n)
            for u in ("B", "KB", "MB", "GB", "TB"):
                if abs(val) < 1024:
                    return f"{val:.1f} {u}"
                val /= 1024
            return f"{val:.1f} PB"

        return {
            "used": usage["used"],
            "used_formatted": fmt(usage["used"]),
            "max": usage["max"],
            "max_formatted": fmt(usage["max"]) if usage["max"] else "unlimited",
            "available": usage.get("available"),
            "available_formatted": fmt(usage.get("available")),
            "usage_percent": usage.get("usage_percent", 0),
            "is_full": usage.get("is_full", False),
            "can_accept_uploads": not usage.get("is_full", False),
            "files_count": usage["files_count"],
            "uploads_count": usage["uploads_count"],
        }

    @router.get("/traffic")
    async def get_traffic() -> dict[str, Any]:
        """Get real-time traffic information for all interfaces.

        Returns current upload/download rates and load percentages
        for each network interface.
        """
        traffic_data = traffic_monitor.get_all_traffic()
        total = traffic_monitor.get_total_traffic()

        # Format for response
        interfaces = []
        for name, data in traffic_data.items():
            interfaces.append(
                {
                    "name": name,
                    "upload_rate": data["upload_rate"],
                    "download_rate": data["download_rate"],
                    "upload_rate_formatted": traffic_monitor.format_rate(data["upload_rate"]),
                    "download_rate_formatted": traffic_monitor.format_rate(data["download_rate"]),
                    "speed_mbps": data.get("speed_mbps", 0),
                    "upload_load_percent": data.get("upload_load_percent", 0),
                    "download_load_percent": data.get("download_load_percent", 0),
                    "total_load_percent": data.get("total_load_percent", 0),
                    "bytes_sent": data["bytes_sent"],
                    "bytes_recv": data["bytes_recv"],
                }
            )

        return {
            "interfaces": interfaces,
            "total": {
                "upload_rate": total["upload_rate"],
                "download_rate": total["download_rate"],
                "upload_rate_formatted": traffic_monitor.format_rate(total["upload_rate"]),
                "download_rate_formatted": traffic_monitor.format_rate(total["download_rate"]),
            },
        }

    return router
