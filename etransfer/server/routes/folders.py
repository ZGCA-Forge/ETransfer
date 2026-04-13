"""Folder management API routes."""

from __future__ import annotations

import asyncio
import io
import logging
import zipfile
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from etransfer.common.constants import AUTH_HEADER
from etransfer.server.tus.storage import TusStorage

logger = logging.getLogger("etransfer.server.routes.folders")


def _get_owner_filter(request: Request) -> Optional[int]:
    """Return owner_id for filtering, or None if caller is privileged."""
    _settings = getattr(request.app.state, "settings", None)
    active_tokens = set(_settings.auth_tokens) if _settings else set()
    api_token = request.headers.get(AUTH_HEADER, "")
    if api_token and api_token in active_tokens:
        return None

    user = getattr(request.state, "user", None)
    if user:
        is_admin = getattr(user, "is_admin", False) or getattr(user, "role", "") == "admin"
        if is_admin:
            return None
        return getattr(user, "id", None)
    return None


def create_folders_router(storage: TusStorage) -> APIRouter:
    router = APIRouter(prefix="/api/folders", tags=["Folders"])

    @router.get("")
    async def list_folders(request: Request) -> dict[str, Any]:
        """List all folder groups."""
        folders = await storage.list_folders()
        owner_id = _get_owner_filter(request)
        if owner_id is not None:
            folders = [f for f in folders if f.get("owner_id") == owner_id]
        return {"folders": folders, "total": len(folders)}

    async def _check_folder_access(folder_id: str, request: Request) -> None:
        files = await storage.get_folder_files(folder_id)
        owner_id = _get_owner_filter(request)
        if owner_id is not None and files:
            if all(f.get("owner_id") != owner_id for f in files):
                raise HTTPException(404, "Folder not found")

    @router.get("/{folder_id}")
    async def get_folder(folder_id: str, request: Request) -> dict[str, Any]:
        """Get folder details with file list."""
        await _check_folder_access(folder_id, request)
        files = await storage.get_folder_files(folder_id)
        if not files:
            raise HTTPException(404, "Folder not found")

        raw = await storage.state.get(f"et:folder:{folder_id}:name")
        folder_name = raw or folder_id[:8]
        total_size = sum(f["size"] for f in files)

        return {
            "folder_id": folder_id,
            "folder_name": folder_name,
            "files": files,
            "file_count": len(files),
            "total_size": total_size,
        }

    @router.get("/{folder_id}/download")
    async def download_folder(folder_id: str, request: Request) -> StreamingResponse:
        await _check_folder_access(folder_id, request)
        """Download all files in a folder as a zip archive (streamed)."""
        files = await storage.get_folder_files(folder_id)
        if not files:
            raise HTTPException(404, "Folder not found")

        raw = await storage.state.get(f"et:folder:{folder_id}:name")
        folder_name = raw or folder_id[:8]

        async def _stream_zip():
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                for f in files:
                    if not f["is_final"]:
                        continue
                    file_id = f["file_id"]
                    rel_path = f["relative_path"]
                    arc_name = f"{folder_name}/{rel_path}"

                    upload = await storage.get_upload(file_id)
                    if not upload or not upload.storage_path:
                        continue

                    from pathlib import Path

                    file_path = Path(upload.storage_path)
                    if not file_path.exists():
                        final_path = storage.get_final_path(file_id, upload.filename)
                        if final_path.exists():
                            file_path = final_path
                        else:
                            continue

                    zf.write(str(file_path), arc_name)

            buf.seek(0)
            while True:
                chunk = buf.read(4 * 1024 * 1024)
                if not chunk:
                    break
                yield chunk

        return StreamingResponse(
            _stream_zip(),
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="{folder_name}.zip"'},
        )

    @router.delete("/{folder_id}")
    async def delete_folder(folder_id: str, request: Request) -> dict[str, Any]:
        await _check_folder_access(folder_id, request)
        """Delete all files in a folder."""
        deleted = await storage.delete_folder(folder_id)
        if deleted == 0:
            raise HTTPException(404, "Folder not found")
        return {"message": f"Deleted {deleted} files", "folder_id": folder_id}

    return router
