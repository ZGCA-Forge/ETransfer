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
    retention: str = "download_once"
    filename: str = ""
    file_size: int = 0
    sink_config: dict = {}
    sink_preset: str = ""


def create_plugins_router() -> APIRouter:
    router = APIRouter(prefix="/api/plugins", tags=["Plugins"])

    @router.get("/sources")
    async def list_sources() -> list[dict]:
        return [s.to_dict() for s in plugin_registry.list_sources()]

    @router.get("/sinks")
    async def list_sinks(request: Request) -> list[dict]:
        sink_presets = getattr(request.app.state, "sink_presets", {})
        results = []
        for s in plugin_registry.list_sinks():
            d = s.to_dict()
            presets = sink_presets.get(d["name"], {})
            d["has_preset"] = bool(presets)
            schema = plugin_registry.get_sink_class(d["name"]).get_config_schema()

            def _safe_preview(cfg: dict) -> dict:
                return {k: v for k, v in cfg.items() if not schema.get(k, {}).get("secret")}

            if presets:
                # Multi-preset metadata. The frontend uses this to render one
                # entry per preset and to populate the CLI's --sink-preset arg.
                d["presets"] = [
                    {"name": name, "preview": _safe_preview(cfg)}
                    for name, cfg in presets.items()
                ]
                # Back-compat: existing fields that older frontends rely on.
                default_preset = presets.get("default") or next(iter(presets.values()))
                d["preset_preview"] = _safe_preview(default_preset)
            results.append(d)
        return results

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
        cm: dict = {}
        if body.sink_config:
            cm["sink_config"] = body.sink_config
        if body.sink_preset:
            cm["sink_preset"] = body.sink_preset
        ctx = SinkContext(
            user=user,
            client_metadata=cm,
            retention=body.retention,
            filename=body.filename,
            file_size=body.file_size,
        )
        sink_presets = getattr(request.app.state, "sink_presets", {})
        presets_for_sink = sink_presets.get(sink_name, {})
        try:
            config = plugin_registry.resolve_sink_config(sink_name, ctx, presets_for_sink)
        except KeyError as ke:
            # Distinguish "unknown plugin" (404) from "unknown preset" (400)
            if "preset" in str(ke).lower():
                raise HTTPException(400, str(ke))
            raise HTTPException(404, f"Unknown sink: {sink_name}")
        schema = plugin_registry.get_sink_class(sink_name).get_config_schema()
        safe = {k: v for k, v in config.items() if not schema.get(k, {}).get("secret")}
        return {"sink": sink_name, "resolved_config": safe}

    return router
