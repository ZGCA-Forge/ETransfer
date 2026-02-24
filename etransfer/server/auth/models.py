"""Data models for the user system.

SQLModel table models double as both SQLAlchemy ORM models and Pydantic models.
The same engine works with SQLite, MySQL, PostgreSQL, etc.
"""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel
from pydantic import Field as PydanticField
from sqlalchemy import BigInteger, Column, Index, String, Text
from sqlmodel import Field, SQLModel

# ── Enums ─────────────────────────────────────────────────────


class Role(str, Enum):
    """User roles with ascending privilege levels."""

    GUEST = "guest"
    USER = "user"
    ADMIN = "admin"


# ── SQLModel table models (ORM) ──────────────────────────────


class UserTable(SQLModel, table=True):
    """User table — synced from OIDC provider on login."""

    __tablename__ = "users"

    id: Optional[int] = Field(default=None, primary_key=True)
    oidc_sub: str = Field(
        sa_column=Column(String(255), unique=True, nullable=False, index=True),
        description="OIDC subject identifier (unique per provider)",
    )
    username: str = Field(sa_column=Column(String(255), nullable=False))
    display_name: Optional[str] = Field(default=None, sa_column=Column(String(255)))
    email: Optional[str] = Field(default=None, sa_column=Column(String(255)))
    avatar_url: Optional[str] = Field(default=None, sa_column=Column(Text))
    role: str = Field(default="user", sa_column=Column(String(32), nullable=False))
    is_active: bool = Field(default=True)
    is_admin: bool = Field(default=False)
    storage_used: int = Field(default=0, sa_column=Column(BigInteger, nullable=False, default=0))
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class GroupTable(SQLModel, table=True):
    """Group table — auto-created when synced from OIDC provider.

    Quota settings are managed locally by admin.
    """

    __tablename__ = "groups"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(sa_column=Column(String(255), unique=True, nullable=False))
    description: Optional[str] = Field(default=None, sa_column=Column(Text))
    quota_json: str = Field(
        default="{}",
        sa_column=Column(Text, nullable=False),
        description="JSON-encoded RoleQuota",
    )
    created_at: datetime = Field(default_factory=datetime.utcnow)


class UserGroupLink(SQLModel, table=True):
    """Many-to-many link between users and groups."""

    __tablename__ = "user_groups"

    user_id: int = Field(foreign_key="users.id", primary_key=True)
    group_id: int = Field(foreign_key="groups.id", primary_key=True)


class SessionTable(SQLModel, table=True):
    """Active user session."""

    __tablename__ = "sessions"
    __table_args__ = (
        Index("idx_sessions_user", "user_id"),
        Index("idx_sessions_expires", "expires_at"),
    )

    token: str = Field(
        sa_column=Column(String(255), primary_key=True),
    )
    user_id: int = Field(foreign_key="users.id")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = Field(default=None)


class PendingLoginTable(SQLModel, table=True):
    """Pending CLI login waiting for OAuth callback."""

    __tablename__ = "pending_logins"
    __table_args__ = (Index("idx_pending_created", "created_at"),)

    state: str = Field(
        sa_column=Column(String(255), primary_key=True),
    )
    created_at: datetime = Field(default_factory=datetime.utcnow)
    session_token: Optional[str] = Field(default=None, sa_column=Column(String(255)))
    completed: bool = Field(default=False)
    redirect_uri: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="The redirect_uri used in the authorize request, "
        "so the callback can use the same value for code exchange.",
    )
    authorize_url: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Full OIDC authorize URL, stored so /login/go/{state} " "can redirect to it (short-link for CLI).",
    )


# ── Pydantic models (API request/response) ────────────────────


class RoleQuota(BaseModel):
    """Quota and speed limits for a role or group."""

    max_storage_size: Optional[int] = PydanticField(None, description="Max total storage in bytes (None = unlimited)")
    max_upload_size: Optional[int] = PydanticField(None, description="Max single file upload size in bytes")
    upload_speed_limit: Optional[int] = PydanticField(
        None, description="Upload speed limit in bytes/sec (None = unlimited)"
    )
    download_speed_limit: Optional[int] = PydanticField(
        None, description="Download speed limit in bytes/sec (None = unlimited)"
    )
    default_retention: str = PydanticField("permanent", description="Default retention policy")
    default_retention_ttl: Optional[int] = PydanticField(None, description="Default TTL for ttl retention (seconds)")


class UserPublic(BaseModel):
    """Public user info (returned by API)."""

    id: int
    username: str
    display_name: Optional[str] = None
    email: Optional[str] = None
    avatar_url: Optional[str] = None
    role: str
    is_active: bool
    is_admin: bool = False
    storage_used: int = 0
    groups: list[str] = PydanticField(default_factory=list)
    effective_quota: Optional[RoleQuota] = None


class GroupCreate(BaseModel):
    """Request to create/update a group's quota config."""

    name: str
    description: Optional[str] = None
    max_storage_size: Optional[int] = None
    max_upload_size: Optional[int] = None
    upload_speed_limit: Optional[int] = None
    download_speed_limit: Optional[int] = None
    default_retention: str = "permanent"
    default_retention_ttl: Optional[int] = None


class GroupPublic(BaseModel):
    """Public group info."""

    id: int
    name: str
    description: Optional[str] = None
    quota: RoleQuota
    member_count: int = 0
