"""Client for ModelScope's public hub APIs.

Two distinct API families are involved:

* ``/openapi/v1/*`` — public discovery (models, datasets, collections).
  Response envelope: ``{"success": true, "data": {...}}``.
* ``/api/v1/*`` — repo file listings used for batch expansion.
  Response envelope: ``{"Code": 200, "Data": {...}}``.

The per-repo file endpoints are **asymmetric**, which is the single easiest
thing to get wrong here:

    model   -> /api/v1/models/{owner}/{name}/repo/files    (200)
    dataset -> /api/v1/datasets/{owner}/{name}/repo/tree   (200)
    model   -> /api/v1/models/{owner}/{name}/repo/tree     (404)
    dataset -> /api/v1/datasets/{owner}/{name}/repo/files  (405)

The listing is never paginated — the whole tree comes back in one response,
and dataset trees include ``Type == "tree"`` directory entries that callers
must filter out.

Download URLs are ``/{models,datasets}/{owner}/{name}/resolve/{revision}/
{path}`` and honour HTTP Range requests, so the existing chunked downloader
and TUS pipeline work against them unchanged.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import unquote, urlparse

import httpx

logger = logging.getLogger("etransfer.modelscope")

MS_HOST = "modelscope.cn"
MS_HOSTS = ("modelscope.cn", "www.modelscope.cn")
MS_BASE = f"https://{MS_HOST}"

DEFAULT_REVISION = "master"

#: Values accepted by the upstream ``sort`` parameter. It is case sensitive
#: upstream (``Downloads`` triggers a server error), so we normalise here.
SORT_VALUES = ("default", "downloads", "likes", "last_modified")

#: Path segments that lead to a repo page rather than a single file.
_REPO_PAGE_SEGMENTS = ("", "summary", "files", "download", "model", "dataset")

#: Actions ModelScope exposes for direct file access. ``tree`` is the web UI's
#: file-browser path (``/datasets/{o}/{n}/tree/{rev}/{path}``) and is accepted
#: so pasted browser URLs work.
_FILE_ACTIONS = ("resolve", "blob", "file", "raw", "tree")

_MAX_PAGE_SIZE = 100
_DEFAULT_TIMEOUT = 30.0


class ModelScopeError(RuntimeError):
    """Raised when the hub returns an error or an unexpected payload."""


@dataclass(frozen=True)
class RepoRef:
    """A ModelScope repository reference."""

    kind: str  # "model" | "dataset"
    owner: str
    name: str
    revision: str = DEFAULT_REVISION

    @property
    def repo_id(self) -> str:
        return f"{self.owner}/{self.name}"

    @property
    def web_prefix(self) -> str:
        """URL segment used in web / resolve paths."""
        return "models" if self.kind == "model" else "datasets"

    @property
    def api_kind(self) -> str:
        """URL segment used in the ``/api/v1`` file endpoints."""
        return self.kind

    def web_url(self) -> str:
        return f"{MS_BASE}/{self.web_prefix}/{self.owner}/{self.name}"


def _make_client(token: Optional[str] = None) -> httpx.AsyncClient:
    """Create the HTTP client used for hub calls.

    Tests monkeypatch this to inject an ``httpx.MockTransport``.
    """
    headers = {"User-Agent": "etransfer"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT, follow_redirects=True, headers=headers)


def is_modelscope_url(url: str) -> bool:
    """Return True when *url* points at a ModelScope host."""
    try:
        host = urlparse(url).hostname or ""
    except Exception:
        return False
    return any(host == h or host.endswith(f".{h}") for h in MS_HOSTS)


def parse_url(url: str) -> Optional[tuple[RepoRef, str]]:
    """Split a ModelScope URL into ``(RepoRef, file_path)``.

    ``file_path`` is empty for repo pages. Returns ``None`` when the URL is
    not a ModelScope model/dataset URL at all.

    Accepted shapes::

        https://modelscope.cn/models/{owner}/{name}
        https://modelscope.cn/models/{owner}/{name}/summary
        https://modelscope.cn/models/{owner}/{name}/resolve/{rev}/{path}
        https://modelscope.cn/models/{owner}/{name}/blob/{rev}/{path}
        https://modelscope.cn/models/{owner}/{name}/file/{rev}/{path}
        https://modelscope.cn/datasets/{owner}/{name}/{...}
    """
    try:
        parsed = urlparse(url)
    except Exception:
        return None

    if not is_modelscope_url(url):
        return None

    m = re.match(r"^/(?P<seg>models|datasets)/(?P<owner>[^/]+)/(?P<name>[^/]+)(?P<rest>/.*)?$", parsed.path or "")
    if not m:
        return None

    kind = "model" if m.group("seg") == "models" else "dataset"
    owner = m.group("owner")
    name = m.group("name")
    rest = (m.group("rest") or "").strip("/")

    if not rest or rest in _REPO_PAGE_SEGMENTS:
        return RepoRef(kind=kind, owner=owner, name=name), ""

    parts = rest.split("/", 2)
    action = parts[0]
    if action not in _FILE_ACTIONS:
        # Unknown sub-page (e.g. /models/{o}/{n}/something) — not a file URL,
        # but still a reproducible repo reference.
        return None

    # ``/resolve/{rev}`` with no trailing path is a repo reference at that revision.
    if len(parts) == 1:
        return RepoRef(kind=kind, owner=owner, name=name), ""
    revision = unquote(parts[1]) or DEFAULT_REVISION
    file_path = unquote(parts[2]) if len(parts) > 2 else ""
    return RepoRef(kind=kind, owner=owner, name=name, revision=revision), file_path


def build_resolve_url(ref: RepoRef, file_path: str) -> str:
    """Build the direct-download URL for one file inside a repo."""
    path = file_path.lstrip("/")
    return f"{MS_BASE}/{ref.web_prefix}/{ref.owner}/{ref.name}/resolve/{ref.revision}/{path}"


def derive_repo_object_key(ref: RepoRef, file_path: str, prefix: str = "") -> str:
    """Deterministic object-storage key for a repo file.

    Keying on repository identity (rather than a timestamp) is what makes
    "has this model already been pushed?" answerable by a single
    ``list_objects(prefix=...)`` call, and what stops a re-download from
    creating a second 15 GB copy::

        modelscope/models/Qwen/Qwen3.5-0.8B/master/config.json
        modelscope/datasets/AI-ModelScope/gsm8k/master/main/train.parquet

    The key deliberately carries no user component — the bucket acts as a
    shared cache across users, which is the point of the dedup check.
    """
    path = file_path.lstrip("/")
    base = f"modelscope/{ref.web_prefix}/{ref.owner}/{ref.name}/{ref.revision}/{path}"
    clean_prefix = (prefix or "").strip("/")
    return f"{clean_prefix}/{base}" if clean_prefix else base


def _check_status(resp: httpx.Response, what: str) -> None:
    if resp.status_code == 404:
        raise ModelScopeError(f"{what}: not found (404)")
    if resp.status_code == 405:
        raise ModelScopeError(f"{what}: method/endpoint mismatch (405)")
    if resp.status_code == 401:
        raise ModelScopeError(f"{what}: authentication required (401) — pass a ModelScope token")
    if resp.status_code >= 400:
        raise ModelScopeError(f"{what}: HTTP {resp.status_code}")


def _json(resp: httpx.Response, what: str) -> dict[str, Any]:
    _check_status(resp, what)
    try:
        payload = resp.json()
    except Exception as exc:  # pragma: no cover - defensive
        raise ModelScopeError(f"{what}: invalid JSON response") from exc
    if not isinstance(payload, dict):
        raise ModelScopeError(f"{what}: unexpected payload type {type(payload).__name__}")
    return payload


# ── Repo file listing ────────────────────────────────────────


def _files_endpoint(ref: RepoRef) -> str:
    """Return the correct (asymmetric) file-listing endpoint for *ref*."""
    if ref.kind == "dataset":
        return f"{MS_BASE}/api/v1/datasets/{ref.owner}/{ref.name}/repo/tree"
    return f"{MS_BASE}/api/v1/models/{ref.owner}/{ref.name}/repo/files"


async def list_repo_files(
    ref: RepoRef,
    *,
    root: str = "",
    recursive: bool = True,
    token: Optional[str] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> list[dict[str, Any]]:
    """List every blob in a repo at ``ref.revision``.

    Directory entries (``Type == "tree"``) are filtered out. The upstream
    endpoint ignores pagination parameters and returns the full listing.

    Returns:
        ``[{"path", "size", "sha256", "lfs"}, ...]``
    """
    url = _files_endpoint(ref)
    params = {"Revision": ref.revision, "Root": root}
    if recursive:
        params["Recursive"] = "True"

    what = f"list {ref.kind} {ref.repo_id}@{ref.revision}"
    own_client = client is None
    http = client or _make_client(token)
    try:
        resp = await http.get(url, params=params)
        payload = _json(resp, what)
    finally:
        if own_client:
            await http.aclose()

    code = payload.get("Code")
    if code not in (None, 200):
        message = payload.get("Message") or payload.get("message") or "unknown error"
        raise ModelScopeError(f"{what}: {message} (Code={code})")

    data = payload.get("Data")
    if not isinstance(data, dict):
        raise ModelScopeError(f"{what}: missing Data object")
    entries = data.get("Files")
    if not isinstance(entries, list):
        raise ModelScopeError(f"{what}: missing Files array")

    files: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        # Dataset trees mix directory nodes into the recursive listing.
        if entry.get("Type") not in (None, "blob"):
            continue
        path = str(entry.get("Path") or entry.get("Name") or "").strip("/")
        if not path or path in seen:
            continue
        seen.add(path)
        files.append(
            {
                "path": path,
                "size": int(entry.get("Size") or 0),
                "sha256": str(entry.get("Sha256") or ""),
                "lfs": bool(entry.get("IsLFS")),
            }
        )
    files.sort(key=lambda f: f["path"])
    return files


async def get_repo_info(
    ref: RepoRef,
    *,
    token: Optional[str] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> dict[str, Any]:
    """Fetch repo metadata from ``/openapi/v1/{models,datasets}/{owner}/{name}``."""
    url = f"{MS_BASE}/openapi/v1/{ref.web_prefix}/{ref.owner}/{ref.name}"
    what = f"info {ref.kind} {ref.repo_id}"
    own_client = client is None
    http = client or _make_client(token)
    try:
        payload = _json(await http.get(url), what)
    finally:
        if own_client:
            await http.aclose()

    if payload.get("success") is False:
        raise ModelScopeError(f"{what}: {payload.get('message') or 'request failed'}")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise ModelScopeError(f"{what}: missing data object")
    return dict(data)


# ── Discovery ────────────────────────────────────────────────


def _normalise_sort(sort: Optional[str]) -> str:
    value = (sort or "default").strip().lower()
    if value not in SORT_VALUES:
        raise ValueError(f"sort must be one of {', '.join(SORT_VALUES)} (got {sort!r})")
    return value


def _normalise_paging(page_number: int, page_size: int) -> tuple[int, int]:
    page = max(1, int(page_number or 1))
    size = int(page_size or 20)
    if size < 1:
        size = 1
    return page, min(size, _MAX_PAGE_SIZE)


async def _discover(
    endpoint: str,
    data_key: str,
    params: dict[str, Any],
    *,
    what: str,
    token: Optional[str] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> dict[str, Any]:
    """Shared implementation for the ``/openapi/v1`` list endpoints.

    ``endpoint`` is the URL path segment while ``data_key`` is the key the
    items live under inside ``data`` — they are not always identical
    (``collections`` → ``collection_list``).
    """
    url = f"{MS_BASE}/openapi/v1/{endpoint}"
    own_client = client is None
    http = client or _make_client(token)
    try:
        payload = _json(await http.get(url, params=params), what)
    finally:
        if own_client:
            await http.aclose()

    if payload.get("success") is False:
        raise ModelScopeError(f"{what}: {payload.get('message') or 'request failed'}")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise ModelScopeError(f"{what}: missing data object")

    items = data.get(data_key) or []
    return {
        "items": [dict(item) for item in items if isinstance(item, dict)],
        "total_count": int(data.get("total_count") or 0),
        "page_number": int(data.get("page_number") or params.get("page_number") or 1),
        "page_size": int(data.get("page_size") or params.get("page_size") or len(items)),
    }


async def list_models(
    *,
    search: str = "",
    sort: Optional[str] = None,
    page_number: int = 1,
    page_size: int = 20,
    task: str = "",
    token: Optional[str] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> dict[str, Any]:
    """List / search public models.

    Only ``search`` performs a keyword filter — ``name``, ``query`` and
    ``keyword`` are silently ignored upstream, so never add them.
    """
    page, size = _normalise_paging(page_number, page_size)
    params: dict[str, Any] = {"page_number": page, "page_size": size}
    if search:
        params["search"] = search
    sort_value = _normalise_sort(sort)
    if sort_value != "default":
        params["sort"] = sort_value
    if task:
        params["task"] = task
    return await _discover("models", "models", params, what="list models", token=token, client=client)


async def list_datasets(
    *,
    search: str = "",
    sort: Optional[str] = None,
    page_number: int = 1,
    page_size: int = 20,
    task: str = "",
    token: Optional[str] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> dict[str, Any]:
    """List / search public datasets."""
    page, size = _normalise_paging(page_number, page_size)
    params: dict[str, Any] = {"page_number": page, "page_size": size}
    if search:
        params["search"] = search
    sort_value = _normalise_sort(sort)
    if sort_value != "default":
        params["sort"] = sort_value
    if task:
        params["task"] = task
    return await _discover("datasets", "datasets", params, what="list datasets", token=token, client=client)


async def list_collections(
    *,
    search: str = "",
    page_number: int = 1,
    page_size: int = 20,
    token: Optional[str] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> dict[str, Any]:
    """List public collections (合集)."""
    page, size = _normalise_paging(page_number, page_size)
    params: dict[str, Any] = {"page_number": page, "page_size": size}
    if search:
        params["search"] = search
    return await _discover(
        "collections", "collection_list", params, what="list collections", token=token, client=client
    )


async def get_collection(
    owner: str,
    slug: str,
    *,
    token: Optional[str] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> dict[str, Any]:
    """Fetch one collection, including its ``items`` member list."""
    url = f"{MS_BASE}/openapi/v1/collections/{owner}/{slug}"
    what = f"get collection {owner}/{slug}"
    own_client = client is None
    http = client or _make_client(token)
    try:
        payload = _json(await http.get(url), what)
    finally:
        if own_client:
            await http.aclose()

    if payload.get("success") is False:
        raise ModelScopeError(f"{what}: {payload.get('message') or 'request failed'}")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise ModelScopeError(f"{what}: missing data object")
    return dict(data)


def collection_members(collection: dict[str, Any]) -> list[dict[str, str]]:
    """Extract ``[{"kind", "repo_id"}]`` from a collection payload.

    ``item_type`` is ``model`` or ``dataset``; ``item_object_id`` is the
    ``owner/name`` pair.
    """
    members: list[dict[str, str]] = []
    for item in collection.get("items") or []:
        if not isinstance(item, dict):
            continue
        item_type = str(item.get("item_type") or "").strip().lower()
        object_id = str(item.get("item_object_id") or "").strip()
        if not object_id or "/" not in object_id:
            continue
        if item_type not in ("model", "dataset"):
            continue
        members.append({"kind": item_type, "repo_id": object_id})
    return members
