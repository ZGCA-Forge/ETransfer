"""Constants for EasyTransfer."""

# TUS Protocol Constants
TUS_VERSION = "1.0.0"
TUS_EXTENSIONS = [
    "creation",
    "creation-with-upload",
    "termination",
    "checksum",
    "expiration",
    "parallel-writes",  # EasyTransfer extension: out-of-order PATCH
]

# Default chunk size: 32MB
DEFAULT_CHUNK_SIZE = 32 * 1024 * 1024  # 32MB

# Maximum chunk size: 64MB
MAX_CHUNK_SIZE = 64 * 1024 * 1024  # 64MB

# Minimum chunk size: 256KB
MIN_CHUNK_SIZE = 256 * 1024  # 256KB

# Default server port (like Minecraft's 25565, we use 8765)
DEFAULT_SERVER_PORT = 8765

# Default Redis URL
DEFAULT_REDIS_URL = "redis://localhost:6379/0"

# Cache directory name
CACHE_DIR_NAME = ".etransfer"

# HTTP Headers for TUS


class TusHeaders:
    TUS_RESUMABLE = "Tus-Resumable"
    TUS_VERSION = "Tus-Version"
    TUS_EXTENSION = "Tus-Extension"
    TUS_MAX_SIZE = "Tus-Max-Size"
    UPLOAD_OFFSET = "Upload-Offset"
    UPLOAD_LENGTH = "Upload-Length"
    UPLOAD_METADATA = "Upload-Metadata"
    UPLOAD_DEFER_LENGTH = "Upload-Defer-Length"
    UPLOAD_CHECKSUM = "Upload-Checksum"
    UPLOAD_EXPIRES = "Upload-Expires"
    LOCATION = "Location"
    CONTENT_TYPE = "Content-Type"
    CONTENT_LENGTH = "Content-Length"


# Content Types
CONTENT_TYPE_OFFSET = "application/offset+octet-stream"

# Token header name
AUTH_HEADER = "X-API-Token"

# Traffic monitoring interval (seconds)
TRAFFIC_MONITOR_INTERVAL = 1.0

# Max concurrent uploads per client
MAX_CONCURRENT_UPLOADS = 10

# Max concurrent downloads per client
MAX_CONCURRENT_DOWNLOADS = 10

# Upload expiration time (seconds) - 24 hours
UPLOAD_EXPIRATION_SECONDS = 24 * 60 * 60

# Redis key prefixes


class RedisKeys:
    UPLOAD_PREFIX = "et:upload:"
    FILE_PREFIX = "et:file:"
    LOCK_PREFIX = "et:lock:"
    TRAFFIC_PREFIX = "et:traffic:"
    QUOTA_PREFIX = "et:quota:"  # per-user reserved quota
    CHUNK_PREFIX = "et:chunk:"


# Redis key TTL values (seconds)
class RedisTTL:
    UPLOAD = 86400 * 7  # 7 days — incomplete upload record
    UPLOAD_SIZE = 86400 * 7  # 7 days — fast size lookup
    CHUNK = 86400 * 7  # 7 days — chunk availability flag (refreshed on write)
    FILE_PERMANENT = 86400 * 365  # 365 days — completed permanent file
    FILE_DOWNLOAD_ONCE = 86400 * 7  # 7 days — download_once file (deleted after download)
    LOCK = 30  # 30 seconds — distributed lock auto-expire
    QUOTA = 86400 * 7  # 7 days — per-user quota reservation
    TRAFFIC = 15  # 15 seconds — instance traffic snapshot
