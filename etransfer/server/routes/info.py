"""Server information API routes."""

from typing import Any, Optional

from fastapi import APIRouter, Request

from etransfer import __version__
from etransfer.common.constants import TUS_EXTENSIONS, TUS_VERSION
from etransfer.common.models import EndpointInfo, ServerInfo
from etransfer.server.services.instance_traffic import InstanceTrafficTracker
from etransfer.server.tus.storage import TusStorage


def create_info_router(
    storage: TusStorage,
    tracker: "InstanceTrafficTracker",
    max_upload_size: Optional[int] = None,
) -> APIRouter:
    """Create server info router.

    Args:
        storage: TUS storage backend
        tracker: Application-level instance traffic tracker
        max_upload_size: Maximum upload size

    Returns:
        FastAPI router
    """
    router = APIRouter(prefix="/api", tags=["Server Info"])

    @router.get("/info", response_model=ServerInfo)
    async def get_server_info(request: Request) -> ServerInfo:
        """Get server information.

        Returns server capabilities, endpoint traffic stats, and storage stats.
        Each endpoint represents a running server instance with its throughput.
        """
        # Get per-endpoint traffic (local + Redis peers)
        all_eps = await tracker.get_all_endpoints()
        endpoints = [EndpointInfo(**ep) for ep in all_eps]

        # Get storage stats
        files = await storage.list_files()
        uploads = await storage.list_uploads()

        total_files = len(files) + len([u for u in uploads if u.is_final])
        total_size = sum(f.get("size", 0) for f in files)
        total_size += sum(u.offset for u in uploads)

        # Effective retention policy advertised to clients.
        settings = getattr(request.app.state, "settings", None)
        default_retention = getattr(settings, "default_retention", "download_once") if settings else "download_once"
        allow_permanent = bool(getattr(settings, "allow_permanent_retention", True)) if settings else True
        policies = ["download_once", "ttl"]
        if allow_permanent:
            policies.append("permanent")

        return ServerInfo(
            version=__version__,
            tus_version=TUS_VERSION,
            tus_extensions=TUS_EXTENSIONS,
            max_upload_size=max_upload_size,
            chunk_size=storage.chunk_size,
            endpoints=endpoints,
            total_files=total_files,
            total_size=total_size,
            retention_policies=policies,
            default_retention=default_retention,
        )

    @router.get("/health")
    async def health_check() -> dict[str, Any]:
        """Health check endpoint."""
        return {"status": "healthy"}

    @router.get("/stats")
    async def get_stats(request: Request) -> dict[str, Any]:
        """Get detailed server statistics."""
        # Get per-endpoint traffic
        all_eps = await tracker.get_all_endpoints()

        total_up = sum(ep.get("bytes_recv", 0) for ep in all_eps)
        total_down = sum(ep.get("bytes_sent", 0) for ep in all_eps)

        # Get file stats
        files = await storage.list_files()
        uploads = await storage.list_uploads(include_completed=False)

        # User count (if user system enabled)
        user_count = 0
        user_db = getattr(request.app.state, "user_db", None)
        if user_db:
            try:
                users = await user_db.list_users()
                user_count = len(users)
            except Exception:
                pass

        # Task-level traffic (offline downloads + sink pushes)
        task_mgr = getattr(request.app.state, "task_manager", None)
        task_download_bytes = 0
        task_push_bytes = 0
        task_total = 0
        task_completed = 0
        task_active = 0
        if task_mgr:
            try:
                tasks = await task_mgr.list_tasks()
                task_total = len(tasks)
                for t in tasks:
                    task_download_bytes += t.downloaded_bytes
                    if t.sink_plugin and t.file_size:
                        if t.status.value == "completed":
                            task_push_bytes += t.file_size
                        elif t.status.value == "pushing":
                            task_push_bytes += t.pushed_parts * 128 * 1024 * 1024
                    if t.status.value == "completed":
                        task_completed += 1
                    elif t.status.value in ("downloading", "pushing", "pending"):
                        task_active += 1
            except Exception:
                pass

        return {
            "traffic": {ep["endpoint"]: ep for ep in all_eps},
            "total_upload_bytes": total_up,
            "total_download_bytes": total_down,
            "task_download_bytes": task_download_bytes,
            "task_push_bytes": task_push_bytes,
            "task_total": task_total,
            "task_completed": task_completed,
            "task_active": task_active,
            "files": {
                "completed": len(files),
                "in_progress": len(uploads),
                "total_size": sum(f.get("size", 0) for f in files),
                "uploading_size": sum(u.offset for u in uploads),
            },
            "user_count": user_count,
        }

    @router.get("/endpoints")
    async def get_endpoints(request: Request) -> dict[str, Any]:
        """Get available endpoints with traffic status.

        Returns all known server instances and their current throughput,
        allowing clients to select the best endpoint for upload/download.

        Response includes:
        - endpoints: List of endpoint info with address and rates
        - best_for_upload: Recommended endpoint for uploading
        - best_for_download: Recommended endpoint for downloading
        """
        all_eps = await tracker.get_all_endpoints()

        best_for_upload = None
        best_for_download = None
        lowest_up = float("inf")
        lowest_down = float("inf")

        for ep in all_eps:
            ur = ep.get("upload_rate", 0)
            dr = ep.get("download_rate", 0)
            url = ep.get("url", f"http://{ep['endpoint']}")
            if ur < lowest_up:
                lowest_up = ur
                best_for_upload = url
            if dr < lowest_down:
                lowest_down = dr
                best_for_download = url

        return {
            "endpoints": all_eps,
            "best_for_upload": best_for_upload,
            "best_for_download": best_for_download,
            "total_endpoints": len(all_eps),
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
        """Get real-time traffic information for all endpoints.

        Returns current upload/download rates for each server instance.
        """
        all_eps = await tracker.get_all_endpoints()

        def _fmt(rate: float) -> str:
            if rate < 1024:
                return f"{rate:.0f} B/s"
            elif rate < 1024 * 1024:
                return f"{rate / 1024:.1f} KB/s"
            elif rate < 1024 * 1024 * 1024:
                return f"{rate / (1024 * 1024):.1f} MB/s"
            else:
                return f"{rate / (1024 * 1024 * 1024):.2f} GB/s"

        formatted = []
        total_up = 0.0
        total_down = 0.0
        for ep in all_eps:
            ur = ep.get("upload_rate", 0)
            dr = ep.get("download_rate", 0)
            total_up += ur
            total_down += dr
            formatted.append(
                {
                    **ep,
                    "upload_rate_formatted": _fmt(ur),
                    "download_rate_formatted": _fmt(dr),
                }
            )

        return {
            "endpoints": formatted,
            "total": {
                "upload_rate": total_up,
                "download_rate": total_down,
                "upload_rate_formatted": _fmt(total_up),
                "download_rate_formatted": _fmt(total_down),
            },
        }

    return router
