"""Generic OIDC provider client.

Works with any standard OpenID Connect provider (Casdoor, Keycloak, Auth0,
Authentik, etc.). Endpoints are auto-discovered from the well-known
configuration URL, or can be manually overridden.
"""

import logging
from typing import Optional
from urllib.parse import urlencode

logger = logging.getLogger("etransfer.server.auth")

import httpx


class OIDCProvider:
    """OpenID Connect authorization code flow client."""

    def __init__(
        self,
        issuer_url: str,
        client_id: str,
        client_secret: str,
        callback_url: str,
        scope: str = "openid profile email",
        # Manual overrides (auto-discovered from issuer if not set)
        authorize_endpoint: Optional[str] = None,
        token_endpoint: Optional[str] = None,
        userinfo_endpoint: Optional[str] = None,
        end_session_endpoint: Optional[str] = None,
    ) -> None:
        self.issuer_url = issuer_url.rstrip("/")
        self.client_id = client_id
        self.client_secret = client_secret
        self.callback_url = callback_url
        self.scope = scope

        self._authorize_endpoint = authorize_endpoint
        self._token_endpoint = token_endpoint
        self._userinfo_endpoint = userinfo_endpoint
        self._end_session_endpoint = end_session_endpoint
        self._discovered = False

    async def discover(self) -> None:
        """Fetch OIDC discovery document and populate endpoints."""
        if self._discovered:
            return

        discovery_url = f"{self.issuer_url}/.well-known/openid-configuration"
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(discovery_url)
            resp.raise_for_status()
            config = resp.json()

        if not self._authorize_endpoint:
            self._authorize_endpoint = config.get("authorization_endpoint")
        if not self._token_endpoint:
            self._token_endpoint = config.get("token_endpoint")
        if not self._userinfo_endpoint:
            self._userinfo_endpoint = config.get("userinfo_endpoint")
        if not self._end_session_endpoint:
            self._end_session_endpoint = config.get("end_session_endpoint")

        self._discovered = True
        logger.info("Discovered endpoints from %s", discovery_url)

    @property
    def authorize_endpoint(self) -> str:
        return self._authorize_endpoint or f"{self.issuer_url}/login/oauth/authorize"

    @property
    def token_endpoint(self) -> str:
        return self._token_endpoint or f"{self.issuer_url}/api/login/oauth/access_token"

    @property
    def userinfo_endpoint(self) -> str:
        return self._userinfo_endpoint or f"{self.issuer_url}/api/userinfo"

    @property
    def end_session_endpoint(self) -> Optional[str]:
        return self._end_session_endpoint

    def get_authorize_url(
        self,
        state: Optional[str] = None,
        redirect_uri: Optional[str] = None,
    ) -> str:
        """Build the OIDC authorization redirect URL.

        Args:
            state: CSRF state parameter.
            redirect_uri: Override callback URL (e.g. derived from request Host
                for SSH / remote scenarios). Falls back to self.callback_url.
        """
        params = {
            "client_id": self.client_id,
            "redirect_uri": redirect_uri or self.callback_url,
            "response_type": "code",
            "scope": self.scope,
        }
        if state:
            params["state"] = state
        return f"{self.authorize_endpoint}?{urlencode(params)}"

    async def exchange_code(
        self,
        code: str,
        redirect_uri: Optional[str] = None,
    ) -> dict:
        """Exchange authorization code for access token.

        Args:
            code: Authorization code from OIDC callback.
            redirect_uri: Must match the redirect_uri used in the authorize
                request. Falls back to self.callback_url.
        """
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                self.token_endpoint,
                data={
                    "grant_type": "authorization_code",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "code": code,
                    "redirect_uri": redirect_uri or self.callback_url,
                },
                headers={"Accept": "application/json"},
            )
            resp.raise_for_status()
            data = resp.json()

            if "error" in data:
                raise ValueError(f"OIDC token error: {data['error']} - " f"{data.get('error_description', '')}")

            return data  # type: ignore[no-any-return]

    async def get_user_info(self, access_token: str) -> dict:
        """Fetch user profile from OIDC userinfo endpoint.

        Standard OIDC claims: sub, name, preferred_username, email, picture.
        Provider-specific claims (e.g. groups, isAdmin) are also returned.
        """
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                self.userinfo_endpoint,
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Accept": "application/json",
                },
            )
            resp.raise_for_status()
            return resp.json()  # type: ignore[no-any-return]

    def get_login_info(self) -> dict:
        """Return login configuration for clients."""
        return {
            "provider": "oidc",
            "issuer": self.issuer_url,
            "authorize_url": self.authorize_endpoint,
            "client_id": self.client_id,
            "callback_url": self.callback_url,
            "scope": self.scope,
            "end_session_url": self.end_session_endpoint,
        }
