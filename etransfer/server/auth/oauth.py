"""Generic OIDC provider client with DingTalk support.

Works with any standard OpenID Connect provider (Casdoor, Keycloak, Auth0,
Authentik, etc.). Endpoints are auto-discovered from the well-known
configuration URL, or can be manually overridden.

When ``provider="dingtalk"``, uses DingTalk-specific endpoints and
request formats (JSON body, camelCase fields, custom auth header).
"""

import logging
from typing import Optional
from urllib.parse import urlencode

logger = logging.getLogger("etransfer.server.auth")

import httpx

_DINGTALK_AUTHORIZE = "https://login.dingtalk.com/oauth2/auth"
_DINGTALK_TOKEN = "https://api.dingtalk.com/v1.0/oauth2/userAccessToken"
_DINGTALK_USERINFO = "https://api.dingtalk.com/v1.0/contact/users/me"


class OIDCProvider:
    """OpenID Connect authorization code flow client.

    Supports standard OIDC providers and DingTalk (via ``provider`` param).
    """

    def __init__(
        self,
        issuer_url: str,
        client_id: str,
        client_secret: str,
        callback_url: str,
        scope: str = "openid profile email",
        provider: str = "oidc",
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
        self.provider = provider

        self._authorize_endpoint = authorize_endpoint
        self._token_endpoint = token_endpoint
        self._userinfo_endpoint = userinfo_endpoint
        self._end_session_endpoint = end_session_endpoint
        self._discovered = False

    @property
    def is_dingtalk(self) -> bool:
        return self.provider == "dingtalk"

    async def discover(self) -> None:
        """Fetch OIDC discovery document and populate endpoints."""
        if self._discovered or self.is_dingtalk:
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
        if self.is_dingtalk:
            return _DINGTALK_AUTHORIZE
        return self._authorize_endpoint or f"{self.issuer_url}/login/oauth/authorize"

    @property
    def token_endpoint(self) -> str:
        if self.is_dingtalk:
            return _DINGTALK_TOKEN
        return self._token_endpoint or f"{self.issuer_url}/api/login/oauth/access_token"

    @property
    def userinfo_endpoint(self) -> str:
        if self.is_dingtalk:
            return _DINGTALK_USERINFO
        return self._userinfo_endpoint or f"{self.issuer_url}/api/userinfo"

    @property
    def end_session_endpoint(self) -> Optional[str]:
        return self._end_session_endpoint

    def get_authorize_url(
        self,
        state: Optional[str] = None,
        redirect_uri: Optional[str] = None,
    ) -> str:
        """Build the authorization redirect URL."""
        params = {
            "client_id": self.client_id,
            "redirect_uri": redirect_uri or self.callback_url,
            "response_type": "code",
            "scope": self.scope,
        }
        if state:
            params["state"] = state
        if self.is_dingtalk:
            params["prompt"] = "consent"
        return f"{self.authorize_endpoint}?{urlencode(params)}"

    async def exchange_code(
        self,
        code: str,
        redirect_uri: Optional[str] = None,
    ) -> dict:
        """Exchange authorization code for access token."""
        if self.is_dingtalk:
            return await self._dingtalk_exchange_code(code)

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
                raise ValueError(f"OIDC token error: {data['error']} - {data.get('error_description', '')}")

            return data  # type: ignore[no-any-return]

    async def _dingtalk_exchange_code(self, code: str) -> dict:
        """DingTalk-specific code exchange (JSON body, camelCase fields)."""
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                _DINGTALK_TOKEN,
                json={
                    "clientId": self.client_id,
                    "clientSecret": self.client_secret,
                    "code": code,
                    "grantType": "authorization_code",
                },
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            data = resp.json()

            if "accessToken" not in data:
                raise ValueError(f"DingTalk token error: {data}")

            # Decode JWT payload to extract openId/unionId/nick
            user_claims: dict = {}
            try:
                import base64
                import json as _json

                parts = data["accessToken"].split(".")
                if len(parts) >= 2:
                    payload = parts[1] + "=" * (4 - len(parts[1]) % 4)
                    user_claims = _json.loads(base64.urlsafe_b64decode(payload))
                    logger.info("DingTalk JWT claims keys: %s", list(user_claims.keys()))
            except Exception:
                pass

            return {
                "access_token": data["accessToken"],
                "refresh_token": data.get("refreshToken", ""),
                "corp_id": data.get("corpId", ""),
                "_jwt_claims": user_claims,
            }


    async def get_user_info(self, access_token: str) -> dict:
        """Fetch user profile from userinfo endpoint."""
        if self.is_dingtalk:
            return await self._dingtalk_get_user_info(access_token)

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

    async def _dingtalk_get_user_info(self, access_token: str) -> dict:
        """DingTalk user info.

        Tries /v1.0/contact/users/me first. If 403 (missing permission),
        falls back to JWT claims extracted during token exchange.
        """
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                _DINGTALK_USERINFO,
                headers={"x-acs-dingtalk-access-token": access_token},
            )
            if resp.status_code == 200:
                return resp.json()  # type: ignore[no-any-return]

            logger.info(
                "DingTalk /contact/users/me returned %d (add Contact.User.Read for full profile), using JWT fallback",
                resp.status_code,
            )

        # Fallback: use JWT claims from the access token
        if hasattr(self, "_jwt_claims") and self._jwt_claims:
            claims = self._jwt_claims
            return {
                "openId": claims.get("openId", claims.get("sub", "")),
                "unionId": claims.get("unionId", ""),
                "nick": claims.get("nick", claims.get("sub", access_token[:12])),
            }

        return {"openId": access_token[:32], "nick": access_token[:12]}

    def get_login_info(self) -> dict:
        """Return login configuration for clients."""
        return {
            "provider": self.provider,
            "issuer": self.issuer_url,
            "authorize_url": self.authorize_endpoint,
            "client_id": self.client_id,
            "callback_url": self.callback_url,
            "scope": self.scope,
            "end_session_url": self.end_session_endpoint,
        }
