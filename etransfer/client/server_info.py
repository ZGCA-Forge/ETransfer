"""Server information utilities."""

from typing import Optional

import httpx

from etransfer.common.constants import AUTH_HEADER
from etransfer.common.models import ServerInfo


def get_server_info(
    server_url: str,
    token: Optional[str] = None,
    timeout: float = 10.0,
) -> ServerInfo:
    """Get server information.

    Args:
        server_url: Server base URL
        token: API authentication token
        timeout: Request timeout

    Returns:
        ServerInfo object
    """
    headers = {}
    if token:
        headers[AUTH_HEADER] = token

    with httpx.Client(timeout=timeout) as client:
        response = client.get(
            f"{server_url.rstrip('/')}/api/info",
            headers=headers,
        )
        response.raise_for_status()
        return ServerInfo(**response.json())


def check_server_health(
    server_url: str,
    token: Optional[str] = None,
    timeout: float = 5.0,
) -> bool:
    """Check if server is healthy.

    Args:
        server_url: Server base URL
        token: API authentication token
        timeout: Request timeout

    Returns:
        True if server is healthy
    """
    headers = {}
    if token:
        headers[AUTH_HEADER] = token

    try:
        with httpx.Client(timeout=timeout) as client:
            response = client.get(
                f"{server_url.rstrip('/')}/api/health",
                headers=headers,
            )
            return response.status_code == 200
    except Exception:
        return False
