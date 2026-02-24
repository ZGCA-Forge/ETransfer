"""Authentication API routes."""

from fastapi import APIRouter

from etransfer.common.models import AuthVerifyRequest, AuthVerifyResponse


def create_auth_router(valid_tokens: list[str]) -> APIRouter:
    """Create authentication router.

    Args:
        valid_tokens: List of valid API tokens

    Returns:
        FastAPI router
    """
    router = APIRouter(prefix="/api/auth", tags=["Authentication"])

    @router.post("/verify", response_model=AuthVerifyResponse)
    async def verify_token(request: AuthVerifyRequest) -> AuthVerifyResponse:
        """Verify an API token.

        Returns whether the token is valid and when it expires (if applicable).
        """
        is_valid = request.token in valid_tokens

        return AuthVerifyResponse(
            valid=is_valid,
            expires_at=None,  # Tokens don't expire in this implementation
        )

    return router
