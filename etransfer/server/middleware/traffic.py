"""ASGI middleware that counts upload / download bytes.

Intercepts TUS PATCH requests (uploads) and file download responses,
forwarding byte counts to the ``InstanceTrafficTracker``.
"""

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse


class TrafficCounterMiddleware(BaseHTTPMiddleware):
    """Count bytes transferred through TUS uploads and file downloads."""

    def __init__(self, app, tracker=None):  # type: ignore[no-untyped-def]
        super().__init__(app)
        self._tracker = tracker

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        tracker = self._tracker
        if tracker is None:
            return await call_next(request)

        path = request.url.path
        method = request.method.upper()

        # ── Upload: TUS PATCH → record Content-Length as upload bytes ──
        if method == "PATCH" and "/tus/" in path:
            cl = request.headers.get("content-length")
            response = await call_next(request)
            if response.status_code in (200, 204) and cl:
                try:
                    tracker.record_upload(int(cl))
                except (ValueError, TypeError):
                    pass
            return response

        # ── Download: GET /api/files/*/download → count streamed bytes ──
        if method == "GET" and "/api/files/" in path and path.endswith("/download"):
            response = await call_next(request)
            if isinstance(response, StreamingResponse) and 200 <= response.status_code < 300:
                cl = response.headers.get("content-length")
                if cl:
                    try:
                        tracker.record_download(int(cl))
                    except (ValueError, TypeError):
                        pass
            return response

        return await call_next(request)
