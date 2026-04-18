"""Tiny in-process S3-like bucket, used by the CLI test suite.

Exposes the minimal multipart-upload lifecycle that the ``fake_bucket`` sink
plugin needs:

    POST   /objects/{key:path}/init          -> {"session_id": "..."}
    PUT    /sessions/{sid}/parts/{n}         -> {"etag": "...", "md5": "...", "size": ...}
    POST   /sessions/{sid}/complete          -> {"url": "fake://<key>",
                                                  "md5": "...", "size": ...,
                                                  "parts": [...]}
    DELETE /sessions/{sid}                    -> 204 (abort + cleanup)
    GET    /objects/{key:path}               -> {"exists": bool, "size": N,
                                                  "md5": "...", "parts": N}
    GET    /objects/{key:path}/content       -> raw object bytes

All state is kept in process memory; creating a new app yields a fresh store.
"""

from __future__ import annotations

import hashlib
import uuid
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response


def create_fake_bucket_app() -> FastAPI:
    """Build a fresh fake-bucket FastAPI sub-app."""
    app = FastAPI(title="FakeBucket")

    sessions: dict[str, dict[str, Any]] = {}
    objects: dict[str, dict[str, Any]] = {}
    hosted_files: dict[str, bytes] = {}
    app.state.sessions = sessions
    app.state.objects = objects
    app.state.hosted_files = hosted_files

    @app.put("/hosted/{name:path}")
    async def put_hosted(name: str, request: Request) -> dict:
        """Register a file so it can be served via GET /hosted/<name>."""
        body = await request.body()
        hosted_files[name] = body
        return {"size": len(body)}

    @app.get("/hosted/{name:path}")
    async def get_hosted(name: str) -> Response:
        data = hosted_files.get(name)
        if data is None:
            raise HTTPException(404, "Not hosted")
        return Response(
            content=data,
            media_type="application/octet-stream",
            headers={
                "Content-Length": str(len(data)),
                "Content-Disposition": f'attachment; filename="{name.rsplit("/", 1)[-1]}"',
            },
        )

    @app.head("/hosted/{name:path}")
    async def head_hosted(name: str) -> Response:
        data = hosted_files.get(name)
        if data is None:
            raise HTTPException(404, "Not hosted")
        return Response(
            status_code=200,
            headers={
                "Content-Length": str(len(data)),
                "Content-Disposition": f'attachment; filename="{name.rsplit("/", 1)[-1]}"',
            },
        )

    @app.post("/objects/{key:path}/init")
    async def init_object(key: str) -> dict:
        sid = uuid.uuid4().hex
        sessions[sid] = {"key": key, "parts": {}}
        return {"session_id": sid}

    @app.put("/sessions/{sid}/parts/{n}")
    async def upload_part(sid: str, n: int, request: Request) -> dict:
        sess = sessions.get(sid)
        if sess is None:
            raise HTTPException(404, "Unknown session")
        body = await request.body()
        md5_hex = hashlib.md5(body, usedforsecurity=False).hexdigest()
        sess["parts"][n] = {"data": body, "md5": md5_hex, "size": len(body)}
        return {"etag": f'"{md5_hex}"', "md5": md5_hex, "size": len(body)}

    @app.post("/sessions/{sid}/complete")
    async def complete_object(sid: str) -> dict:
        sess = sessions.pop(sid, None)
        if sess is None:
            raise HTTPException(404, "Unknown session")
        parts = sess["parts"]
        ordered = sorted(parts.items(), key=lambda kv: kv[0])
        data = b"".join(p["data"] for _, p in ordered)
        md5_hex = hashlib.md5(data, usedforsecurity=False).hexdigest()
        objects[sess["key"]] = {
            "data": data,
            "md5": md5_hex,
            "size": len(data),
            "parts": [{"part_number": n, "md5": p["md5"], "size": p["size"]} for n, p in ordered],
        }
        return {
            "url": f"fake://{sess['key']}",
            "md5": md5_hex,
            "size": len(data),
            "parts": objects[sess["key"]]["parts"],
        }

    @app.delete("/sessions/{sid}", status_code=204)
    async def abort_object(sid: str) -> Response:
        sessions.pop(sid, None)
        return Response(status_code=204)

    @app.get("/objects/{key:path}/content")
    async def get_content(key: str) -> Response:
        obj = objects.get(key)
        if obj is None:
            raise HTTPException(404, "Object not found")
        return Response(content=obj["data"], media_type="application/octet-stream")

    @app.get("/objects/{key:path}")
    async def head_object(key: str) -> dict:
        obj = objects.get(key)
        if obj is None:
            return {"exists": False}
        return {
            "exists": True,
            "size": obj["size"],
            "md5": obj["md5"],
            "parts": len(obj["parts"]),
        }

    @app.get("/_introspect")
    async def introspect() -> dict:
        """Expose the state so tests can assert multipart ordering / counts."""
        return {
            "sessions": {
                sid: {
                    "key": s["key"],
                    "part_numbers": sorted(s["parts"].keys()),
                }
                for sid, s in sessions.items()
            },
            "objects": {
                k: {"size": o["size"], "md5": o["md5"], "parts": len(o["parts"])}
                for k, o in objects.items()
            },
        }

    return app
