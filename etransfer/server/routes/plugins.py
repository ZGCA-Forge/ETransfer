"""REST API routes for plugin discovery."""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from etransfer.plugins.base_sink import SinkContext
from etransfer.plugins.registry import plugin_registry

logger = logging.getLogger("etransfer.server.plugins")


class ResolveConfigRequest(BaseModel):
    retention: str = "permanent"
    filename: str = ""
    file_size: Optional[int] = None
    sink_config: Optional[dict] = None


def create_plugins_router() -> APIRouter:
    router = APIRouter(prefix="/api/plugins", tags=["Plugins"])

    @router.get("/sources")
    async def list_sources() -> list[dict]:
        return [s.to_dict() for s in plugin_registry.list_sources()]

    @router.get("/sinks")
    async def list_sinks() -> list[dict]:
        return [s.to_dict() for s in plugin_registry.list_sinks()]

    @router.get("/resolve")
    async def resolve_source(url: str = Query(..., description="URL to resolve")) -> dict:
        try:
            source = await plugin_registry.resolve_source(url)
            return {"source_plugin": source.name, "display_name": source.display_name}
        except ValueError as e:
            raise HTTPException(400, str(e))

    @router.post("/sinks/{sink_name}/resolve-config")
    async def resolve_sink_config(sink_name: str, body: ResolveConfigRequest, request: Request) -> dict:
        user = getattr(request.state, "user", None)
        ctx = SinkContext(
            user=user,
            client_metadata={"sink_config": body.sink_config} if body.sink_config else {},
            retention=body.retention,
            filename=body.filename,
            file_size=body.file_size,
        )
        sink_presets = getattr(request.app.state, "sink_presets", {})
        presets_for_sink = sink_presets.get(sink_name, {})
        try:
            config = plugin_registry.resolve_sink_config(sink_name, ctx, presets_for_sink)
        except KeyError:
            raise HTTPException(404, f"Unknown sink: {sink_name}")
        # Mask secret fields
        schema = plugin_registry.get_sink_class(sink_name).get_config_schema()
        masked = {}
        for k, v in config.items():
            field_def = schema.get(k, {})
            if field_def.get("secret") and isinstance(v, str) and len(v) > 4:
                masked[k] = v[:2] + "***" + v[-2:]
            else:
                masked[k] = v
        return {"sink": sink_name, "resolved_config": masked}

    return router
