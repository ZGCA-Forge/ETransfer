"""Server configuration."""

import logging
import os
from pathlib import Path
from typing import Literal, Optional

from pydantic import Field
from pydantic_settings import BaseSettings

from etransfer.common.constants import DEFAULT_CHUNK_SIZE, DEFAULT_REDIS_URL, DEFAULT_SERVER_PORT

logger = logging.getLogger("etransfer.server.config")

# Fields that can be hot-reloaded without a server restart.
HOT_RELOADABLE_FIELDS = frozenset(
    {
        "role_quotas",
        "auth_tokens",
        "token_retention_policies",
        "default_retention",
        "default_retention_ttl",
        "advertised_endpoints",
        "max_storage_size",
    }
)


class ServerSettings(BaseSettings):
    """Server settings loaded from environment or config file."""

    # Server
    host: str = Field("0.0.0.0", description="Server bind host")  # nosec B104
    port: int = Field(DEFAULT_SERVER_PORT, description="Server port")
    workers: int = Field(1, description="Number of uvicorn workers")

    # Storage
    storage_path: Path = Field(Path("./storage"), description="Path to store uploaded files")
    chunk_size: int = Field(DEFAULT_CHUNK_SIZE, description="Default chunk size")
    max_upload_size: Optional[int] = Field(None, description="Maximum single upload size (None = unlimited)")
    max_storage_size: Optional[int] = Field(
        None,
        description="Maximum total storage size in bytes (None = unlimited). "
        "When exceeded, new uploads are rejected with 507 until space is freed. "
        "Env: ETRANSFER_MAX_STORAGE_SIZE (supports suffixes: 100MB, 1GB, etc.)",
    )

    # State Backend - no longer requires Redis by default
    state_backend: Literal["memory", "file", "redis"] = Field(
        "file", description="State backend type: memory (test), file (default), redis (multi-worker)"  # noqa: E501
    )
    redis_url: str = Field(DEFAULT_REDIS_URL, description="Redis connection URL (only used if state_backend=redis)")

    # Auth
    auth_enabled: bool = Field(True, description="Enable token authentication")
    auth_tokens: list[str] = Field(default_factory=list, description="Valid API tokens")

    # Retention policies per token
    # Format: {"token1": {"default_retention": "download_once", "max_ttl": 86400}}
    # If not set, all tokens use "permanent" by default
    token_retention_policies: dict = Field(
        default_factory=dict,
        description="Per-token retention policy overrides. "
        "Keys are tokens, values are dicts with "
        "'default_retention' (permanent/download_once/ttl) and "
        "'default_ttl' (seconds, for ttl policy). "
        "Env: ETRANSFER_TOKEN_RETENTION_POLICIES as JSON.",
    )

    # Global default retention
    default_retention: str = Field(
        "permanent",
        description="Global default retention policy: permanent, download_once, ttl",
    )
    default_retention_ttl: Optional[int] = Field(
        None,
        description="Global default TTL in seconds (for ttl retention). " "Env: ETRANSFER_DEFAULT_RETENTION_TTL",
    )

    # Network
    interfaces: list[str] = Field(
        default_factory=list,
        description="Network interfaces to use (empty = auto-detect)",
    )
    prefer_ipv4: bool = Field(True, description="Prefer IPv4 addresses")
    advertised_endpoints: list[str] = Field(
        default_factory=list,
        description="Advertised IP addresses for clients (e.g., ['192.168.1.1', '10.0.0.1']). "
        "If empty, auto-detect from interfaces. "
        "Env: ETRANSFER_ADVERTISED_ENDPOINTS as JSON array.",
    )

    @classmethod
    def parse_list_env(cls, key: str, default: list = None) -> list:  # type: ignore[assignment]
        """Parse list from environment variable (JSON format)."""
        import json

        value = os.environ.get(key, "")
        if not value:
            return default or []
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
            return [str(parsed)]
        except json.JSONDecodeError:
            # Try comma-separated
            return [v.strip() for v in value.split(",") if v.strip()]

    # ── User system (OIDC + roles/groups) ──
    user_system_enabled: bool = Field(
        False,
        description="Enable the user system (OIDC login, roles, groups, per-user quotas). "
        "Env: ETRANSFER_USER_SYSTEM_ENABLED",
    )

    # OIDC configuration
    oidc_issuer_url: str = Field(
        "",
        description="OIDC issuer URL (e.g. https://auth.example.com). "
        "Endpoints are auto-discovered from .well-known/openid-configuration. "
        "Env: ETRANSFER_OIDC_ISSUER_URL",
    )
    oidc_client_id: str = Field(
        "",
        description="OIDC client ID. Env: ETRANSFER_OIDC_CLIENT_ID",
    )
    oidc_client_secret: str = Field(
        "",
        description="OIDC client secret. Env: ETRANSFER_OIDC_CLIENT_SECRET",
    )
    oidc_callback_url: str = Field(
        "",
        description="OIDC callback URL prefix (scheme + host, no path). "
        "The fixed path /api/users/callback is appended automatically. "
        "Env: ETRANSFER_OIDC_CALLBACK_URL. "
        "Example: http://localhost:8765",
    )
    oidc_scope: str = Field(
        "openid profile email",
        description="OIDC scopes. Env: ETRANSFER_OIDC_SCOPE",
    )

    # Database backend for user system
    user_db_backend: str = Field(
        "sqlite",
        description="User database backend: sqlite (default) or mysql. " "Env: ETRANSFER_USER_DB_BACKEND",
    )
    user_db_path: str = Field(
        "",
        description="Path to user SQLite database (when backend=sqlite). "
        "Default: <storage_path>/users.db. "
        "Env: ETRANSFER_USER_DB_PATH",
    )
    # MySQL connection settings (when user_db_backend=mysql)
    mysql_host: str = Field("127.0.0.1", description="MySQL host. Env: ETRANSFER_MYSQL_HOST")
    mysql_port: int = Field(3306, description="MySQL port. Env: ETRANSFER_MYSQL_PORT")
    mysql_user: str = Field("root", description="MySQL user. Env: ETRANSFER_MYSQL_USER")
    mysql_password: str = Field("", description="MySQL password. Env: ETRANSFER_MYSQL_PASSWORD")
    mysql_database: str = Field(
        "etransfer",
        description="MySQL database name. Env: ETRANSFER_MYSQL_DATABASE",
    )

    # Role quota defaults (JSON dict)
    role_quotas: dict = Field(
        default_factory=lambda: {
            "admin": {
                "max_storage_size": None,
                "max_upload_size": None,
                "upload_speed_limit": None,
                "download_speed_limit": None,
            },
            "user": {
                "max_storage_size": 10 * 1024**3,  # 10 GB
                "max_upload_size": 5 * 1024**3,  # 5 GB
                "upload_speed_limit": None,
                "download_speed_limit": None,
            },
            "guest": {
                "max_storage_size": 1 * 1024**3,  # 1 GB
                "max_upload_size": 512 * 1024**2,  # 512 MB
                "upload_speed_limit": 10 * 1024**2,  # 10 MB/s
                "download_speed_limit": 10 * 1024**2,
            },
        },
        description="Per-role quota defaults. Env: ETRANSFER_ROLE_QUOTAS as JSON.",
    )

    # CORS
    cors_origins: list[str] = Field(["*"], description="CORS allowed origins")

    # Cleanup
    cleanup_interval: int = Field(3600, description="Cleanup interval in seconds")
    upload_expiration_hours: int = Field(24, description="Hours before incomplete uploads expire")

    # Config file watching (hot-reload)
    config_watch: bool = Field(
        False,
        description="Watch config file for changes and auto-reload hot settings. " "Env: ETRANSFER_CONFIG_WATCH",
    )
    config_watch_interval: int = Field(
        30,
        description="Config file watch interval in seconds. " "Env: ETRANSFER_CONFIG_WATCH_INTERVAL",
    )

    class Config:
        env_prefix = "ETRANSFER_"
        env_nested_delimiter = "__"


def parse_size(value: str) -> int:
    """Parse human-readable size string to bytes.

    Examples: '100MB', '1GB', '500kb', '1073741824'
    """
    value = value.strip().upper()
    multipliers = {
        "B": 1,
        "KB": 1024,
        "MB": 1024**2,
        "GB": 1024**3,
        "TB": 1024**4,
        "K": 1024,
        "M": 1024**2,
        "G": 1024**3,
        "T": 1024**4,
    }
    for suffix, mult in sorted(multipliers.items(), key=lambda x: -len(x[0])):
        if value.endswith(suffix):
            num = value[: -len(suffix)].strip()
            return int(float(num) * mult)
    return int(value)


def _parse_yaml_to_settings_dict(config: dict) -> dict:
    """Convert a parsed YAML config dict into a flat settings dict.

    This is the single source of truth for YAML -> settings field mapping.
    Used by both initial load and hot-reload.
    """
    d: dict = {}

    if "server" in config:
        d.update(config["server"])
    if "state" in config:
        if "backend" in config["state"]:
            d["state_backend"] = config["state"]["backend"]
        if "redis_url" in config["state"]:
            d["redis_url"] = config["state"]["redis_url"]
    if "auth" in config:
        if "enabled" in config["auth"]:
            d["auth_enabled"] = config["auth"]["enabled"]
        if "tokens" in config["auth"]:
            d["auth_tokens"] = config["auth"]["tokens"]
    if "network" in config:
        if "interfaces" in config["network"]:
            d["interfaces"] = config["network"]["interfaces"]
        if "prefer_ipv4" in config["network"]:
            d["prefer_ipv4"] = config["network"]["prefer_ipv4"]
        if "advertised_endpoints" in config["network"]:
            d["advertised_endpoints"] = config["network"]["advertised_endpoints"]
    if "storage" in config:
        st = config["storage"]
        if "path" in st:
            d["storage_path"] = st["path"]
        if "max_storage_size" in st:
            v = st["max_storage_size"]
            d["max_storage_size"] = parse_size(str(v)) if isinstance(v, str) else v
    if "retention" in config:
        rt = config["retention"]
        if "default" in rt:
            d["default_retention"] = rt["default"]
        if "default_ttl" in rt:
            d["default_retention_ttl"] = rt["default_ttl"]
        if "token_policies" in rt:
            d["token_retention_policies"] = rt["token_policies"]
    if "user_system" in config:
        us = config["user_system"]
        if "enabled" in us:
            d["user_system_enabled"] = us["enabled"]
        if "role_quotas" in us:
            d["role_quotas"] = us["role_quotas"]
        oidc = us.get("oidc", {})
        if "issuer_url" in oidc:
            d["oidc_issuer_url"] = oidc["issuer_url"]
        if "client_id" in oidc:
            d["oidc_client_id"] = oidc["client_id"]
        if "client_secret" in oidc:
            d["oidc_client_secret"] = oidc["client_secret"]
        if "callback_url" in oidc:
            d["oidc_callback_url"] = oidc["callback_url"]
        if "scope" in oidc:
            d["oidc_scope"] = oidc["scope"]
        db = us.get("database", {})
        if "backend" in db:
            d["user_db_backend"] = db["backend"]
        if "path" in db:
            d["user_db_path"] = db["path"]
        mysql = db.get("mysql", {})
        for key in ("host", "port", "user", "password", "database"):
            if key in mysql:
                d[f"mysql_{key}"] = mysql[key]

    return d


def _apply_env_overrides(settings_dict: dict) -> None:
    """Apply environment variable overrides to a settings dict (in-place)."""
    import json

    env_endpoints = os.environ.get("ETRANSFER_ADVERTISED_ENDPOINTS", "")
    if env_endpoints:
        try:
            parsed = json.loads(env_endpoints)
            if isinstance(parsed, list):
                settings_dict["advertised_endpoints"] = parsed
            else:
                settings_dict["advertised_endpoints"] = [str(parsed)]
        except json.JSONDecodeError:
            settings_dict["advertised_endpoints"] = [v.strip() for v in env_endpoints.split(",") if v.strip()]

    env_max_storage = os.environ.get("ETRANSFER_MAX_STORAGE_SIZE", "")
    if env_max_storage:
        settings_dict["max_storage_size"] = parse_size(env_max_storage)


# ── Config file auto-discovery ────────────────────────────────

# Paths searched in order when no explicit config is given.
_CONFIG_SEARCH_PATHS = [
    Path("./config.yaml"),
    Path("./config/config.yaml"),
    Path.home() / ".etransfer" / "server.yaml",
]


def discover_config_path() -> Optional[Path]:
    """Find a config file using auto-discovery.

    Search order:
      1. ``$ETRANSFER_CONFIG`` environment variable
      2. ``./config.yaml``
      3. ``./config/config.yaml``
      4. ``~/.etransfer/server.yaml``

    Returns:
        Path to the discovered config file, or None.
    """
    env_path = os.environ.get("ETRANSFER_CONFIG", "")
    if env_path:
        p = Path(env_path)
        if p.exists():
            return p
        logger.warning("$ETRANSFER_CONFIG=%s does not exist", env_path)

    for candidate in _CONFIG_SEARCH_PATHS:
        if candidate.exists():
            return candidate
    return None


def load_server_settings(config_path: Optional[Path] = None) -> ServerSettings:
    """Load server settings from config file + environment variables.

    Args:
        config_path: Explicit path to config file.  When ``None``,
            auto-discovery is used (see :func:`discover_config_path`).

    Returns:
        Tuple of (ServerSettings, resolved_config_path or None).
        The resolved path is stored so hot-reload can re-read it later.
    """
    import yaml

    # Auto-discover config file
    resolved_path = config_path
    if resolved_path is None:
        resolved_path = discover_config_path()

    settings_dict: dict = {}

    if resolved_path and resolved_path.exists():
        with open(resolved_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
        settings_dict = _parse_yaml_to_settings_dict(config)
        logger.info("Loaded config from %s", resolved_path.resolve())
    else:
        logger.info("No config file found, using defaults + environment variables")

    _apply_env_overrides(settings_dict)

    settings = ServerSettings(**settings_dict)
    # Stash the resolved path so callers / hot-reload can reference it.
    settings._config_path = resolved_path  # type: ignore[attr-defined]
    return settings


def reload_hot_settings(settings: ServerSettings) -> dict[str, tuple]:
    """Re-read the config file and update hot-reloadable fields in place.

    Only fields listed in ``HOT_RELOADABLE_FIELDS`` are updated.
    Static fields (host, port, storage_path, oidc_*, etc.) are ignored.

    Args:
        settings: The live ServerSettings instance to update.

    Returns:
        A dict of ``{field: (old_value, new_value)}`` for every field
        that actually changed.
    """
    import yaml

    config_path: Optional[Path] = getattr(settings, "_config_path", None)
    if not config_path or not config_path.exists():
        return {}

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    fresh_dict = _parse_yaml_to_settings_dict(config)
    _apply_env_overrides(fresh_dict)

    changes: dict[str, tuple] = {}
    for field in HOT_RELOADABLE_FIELDS:
        if field not in fresh_dict:
            continue
        old = getattr(settings, field)
        new = fresh_dict[field]
        if old != new:
            changes[field] = (old, new)
            object.__setattr__(settings, field, new)

    return changes
