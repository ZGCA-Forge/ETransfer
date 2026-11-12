# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

ETransfer is a TUS-based large-file transfer tool (Python 3.9+) distributed as one package containing both a
client and a server. Primary deploy target is `pip install etransfer`; the server lives behind the `server` extra.
CLI entry points are `et` and `etransfer` (both → `etransfer.client.cli:app`); `python -m etransfer` works too.

## Commands

```bash
# Install for development (the greenlet flag avoids C-build failures on some platforms)
pip install -e ".[dev]" --only-binary greenlet

# Tests — conftest spins up a real uvicorn server in a thread, so these are integration-ish
pytest
pytest tests/test_cli_upload_download.py            # single file
pytest tests/test_cli_upload_download.py::test_retention_policies_exposed_in_api_info   # single test
pytest -k retention                                  # by keyword

# Format / lint / type / security — all config lives in pyproject.toml
pre-commit run --all-files        # black, isort, flake8, mypy, bandit, prettier, file hygiene
pre-commit run mypy --all-files   # one hook only

# Run the server
et server start                                    # config auto-discovered
et server start --config config/config.yaml --backend redis
uvicorn etransfer.server.main:app --port 8765      # module-level app object

# CLI smoke use
et setup http://127.0.0.1:8765
et upload ./file.bin -j 8 -c 33554432
et --help                                          # grouped command panel

# Version bump — updates pyproject.toml + etransfer/__init__.py, commits, tags, pushes
bump-my-version bump patch
```

`scripts/` holds standalone manual test drivers (`test_large_partial.py`, `test_quota_resume.py`,
`test_multi_ip.py`, `test_hot_reload.py`, `test_user_system.py`, `test_retention.py`) plus
`start_test_env.sh` (starts a server with `test-token-12345`). They take `--server/--token` args and are not
part of pytest discovery.

## Architecture

One distribution, four layers that all import from `etransfer.common`:

- `etransfer/common/` — shared pydantic models, `constants.py` (TUS headers, chunk sizes, Redis key prefixes and
  TTLs), `fileutil.py` (`pwrite`/`ftruncate` primitives, `derive_sink_object_key`).
- `etransfer/client/` — Typer CLI (`cli.py`, ~3.3k lines, the bulk of client surface), TUS client wrapper,
  `parallel_uploader.py`, `downloader.py`, `cache.py`, Tkinter `gui.py`.
- `etransfer/server/` — FastAPI app, TUS handler/storage, REST routes, auth, tasks, migrations.
- `etransfer/plugins/` — Source (download providers) and Sink (push targets) plugin framework.

### Server app wiring (`etransfer/server/main.py`)

`create_app(settings)` builds everything; `app = create_app()` at module scope is what uvicorn serves. The
routers are registered at import/creation time, but every real service (`TusStorage`, `InstanceTrafficTracker`,
`UserDB`, `TaskManager`, migration controller) is constructed in the `@app.on_event("startup")` handler and
stashed in module globals plus `app.state`. Routers therefore receive **lazy proxy objects**
(`_StorageProxy`, `_TrackerProxy`, `_UserDBProxy`) that resolve globals on attribute access — a `RuntimeError:
Storage not initialized` means you touched one before startup or after shutdown. Tests rely on this: they call
`create_app(ServerSettings(...))` and run it on a thread.

Startup order matters: guard multi-worker (`workers > 1` requires `state_backend=redis`, otherwise it raises) →
connect `StateManager` → build `TusStorage` → `reconcile_state()` (rebuild chunk keys from disk, drop orphans) →
traffic tracker → user DB + quota reconciliation → `plugin_registry.discover()` + `TaskManager` → resume
interrupted tasks → config watcher / cleanup loop. The SPA mount at `/` is registered last and catches everything.

### TUS core (`etransfer/server/tus/`)

`storage.py` (`TusStorage`) is the state machine; `handler.py` holds the HTTP verbs. It implements TUS 1.0.0 plus
extensions `creation-with-upload`, `termination`, `checksum`, `expiration`, and a non-standard **parallel-writes**
extension: PATCHes may arrive out of order, so the server tracks `received_ranges` and exposes them on HEAD as
`X-Received-Ranges` (`"start-end,..."`, end-exclusive). The client's `ParallelUploader` uses that header to resume
precisely instead of falling back to a contiguous `Upload-Offset`. Files may also be stored chunked, in which case
HEAD reports `X-Chunk-Size` / `X-Available-Chunks` and downloads accept `?chunk=N`.

State is abstracted behind `services/state.py::StateManager` with `memory` / `file` / `redis` backends
(`services/backends/`), so the same storage code runs single-process and multi-worker.

All read/write paths go through `common/fileutil.pwrite` off the request path where possible; disk I/O is
submitted to `server/io_pool.py`. Preserve the "stream body straight to disk, then take the local asyncio lock"
shape in PATCH — it is the hot path.

### Quota and retention

Two distinct quota layers:

- **Global storage** — `TusStorage.max_storage_size`; over-limit returns HTTP 507 and the CLI polls/waits
  (`--wait-on-quota`).
- **Per-user** — `tus/quota.py::QuotaService` reserves bytes in the state backend at upload CREATE, releases on
  finalize (committing `user.storage_used` in the user DB) or delete. Reservation happens before any DB write so
  concurrent creates can't overshoot.

Retention (`permanent` / `download_once` / `ttl`) is resolved by precedence client metadata > token policy >
server default. `auth/privilege.py` is the single place deciding who is privileged: a static `X-API-Token` holder
or an admin role. **A plain API token is treated as admin** — that is intentional and load-bearing for
`retention.allow_permanent: false`. Don't duplicate this check in new routes; import
`is_privileged_caller` / `enforce_retention_policy`.

### Plugin system (`etransfer/plugins/`)

Entry-point groups `etransfer.sources` and `etransfer.sinks` are discovered by `registry.py::plugin_registry`
(a module singleton, `discover()` is idempotent). Built-in sinks additionally self-register by import so
source-tree installs work without refreshed metadata; built-in sources only come from entry points.

- `BaseSource`: declare `supported_hosts` + `priority`; the registry matches by hostname then calls
  `can_handle`. `DirectLinkSource` has `priority = -1` and empty hosts, so it is the fallback. Token-based
  access goes through `auth_headers(token)`.
- `BaseSink`: S3-style multipart lifecycle `initialize_upload` → `upload_part`* → `complete_upload` /
  `abort_upload`. Credentials normally come from server-side `sinks.presets.<sink>.<name>` (resolved by
  `resolve_config`: explicit `sink_preset` > group > role > `default`), so clients never send secrets; a
  client-supplied `sink_config` is merged on top.

To add a plugin: subclass the base, add an entry point in `pyproject.toml` (`[project.entry-points."etransfer.sinks"]`),
reinstall editable, and it appears in `et plugins/sinks/sources` and `/api/plugins/*`. Tests register plugins via
`plugin_registry.register_sink(...)` instead (see `tests/conftest.py`).

### Transfer tasks (`etransfer/server/tasks/`)

`TaskManager` runs offline-download jobs: resolve source plugin for the URL → download (streaming or to the local
download dir) → register the data as a TUS file → optionally push to a sink. Statuses are `pending`,
`downloading`, `pushing`, `completed`, `failed`, `cancelled`. Tasks are persisted as JSON in the state backend
(`et:task:*`), including `uploaded_parts` and `sink_session_id`, so retry/resume/restart continue mid-push rather
than restarting. `source_config` (e.g. a HuggingFace token) is persisted but must never be echoed by
`TaskResponse` — see `_sanitize_task_dict`. Concurrency is bounded by a resizable gate
(`max_concurrent_tasks`, hot-reloadable).

### Bucket migration (`etransfer/server/routes/migrations.py`)

A large controller that expands one S3-compatible source bucket into per-object transfer tasks and pushes them to a
TOS/S3 target, with presigned GETs, range-based source reads, retry accounting, progress persisted under
`et:migration:*`, and JSONL object logs under `local_deploy/`. It is self-contained — keep it that way rather than
threading it through `TaskManager`.

### ModelScope source (`etransfer/common/modelscope_hub.py` + `plugins/sources/modelscope.py`)

Public hub APIs, no auth needed for public repos. Two API families with different envelopes:
`/openapi/v1/*` for discovery (`{"success", "data"}`) and `/api/v1/*` for file listings (`{"Code", "Data"}`).

Upstream quirks that are easy to reintroduce as bugs — all of them are covered by tests:

- **The file endpoints are asymmetric.** Models use `/api/v1/models/{o}/{n}/repo/files`; datasets use
  `/api/v1/datasets/{o}/{n}/repo/tree`. Swapping them yields 404/405, not an empty list.
- Dataset recursive listings include `Type: "tree"` directory nodes — `list_repo_files` filters to `blob`.
- Only `search=` filters discovery; `name`/`query`/`keyword` are silently ignored upstream. `sort` is
  case-sensitive (`Downloads` errors), so `_normalise_sort` lowercases and validates.
- The default branch is **`master`**, not `main`.
- **HEAD returns 200 with no `Content-Length`**, so size must fall back to a `Range: bytes=0-0` GET and read
  `Content-Range`. `get_file_info` does this; don't "simplify" it back to a bare HEAD.
- `/openapi/v1/collections` maps to a `collection_list` data key — endpoint segment and payload key differ.

`ModelScopeSource` implements `is_repo_url` / `list_repo_files` / `build_file_url`, which is all the existing batch
expansion in `routes/tasks.py::_create_batch_from_repo` needs. It also adds `derive_object_key`, which produces
`modelscope/{models,datasets}/{owner}/{name}/{revision}/{path}` — deterministic and repo-identity-based, so a
re-download overwrites instead of duplicating, and dedup is answerable from a key prefix. Tasks carry the result in
`TransferTask.sink_object_key`; when empty the old timestamped `derive_sink_object_key` path is used unchanged.

Dedup has two levels: the discovery routes annotate results as `downloaded` from persisted `et:task:*` state
(cheap, no network), while `sink_object_exists` does a real `head_object` via the optional
`BaseSink.object_exists` / `list_objects` hooks. Those hooks default to returning `None` (= unknown) — never treat
`None` as "absent" in a way that skips a download. Sink config for these checks must be resolved through
`resolve_sink_config` with the preset, exactly like `create_task`, or presets (where credentials actually live)
are bypassed and every check silently reports "missing".

HTTP clients in both modules are built through module-level factories (`_make_client`, `_http_client`) rather than
calling `httpx.AsyncClient` inline, so tests can inject `httpx.MockTransport` without monkeypatching the shared
`httpx` module attribute (which would leak into unrelated sinks).

### Configuration

`server/config.py::ServerSettings` is pydantic-settings (`ETRANSFER_` prefix). Discovery order for the YAML:
`$ETRANSFER_CONFIG` → `./config.yaml` → `./config/config.yaml` → `~/.etransfer/server.yaml`. YAML is mapped to flat
settings by `_parse_yaml_to_settings_dict` (nested sections: `server`, `storage`, `state`, `auth`, `retention`,
`tasks`, `network`, `user_system`, `sinks.presets`, `logging`), then env vars override. `config/` is git-ignored
except its README; `config.example.yaml` is the template.

Hot reload is explicit: only `HOT_RELOADABLE_FIELDS` change in place, and `main._propagate_hot_changes` must be
extended to push a field into live runtime objects (e.g. `max_storage_size` → `TusStorage`). Anything else
(host/port/workers, OIDC, DB backend, storage path) requires a restart. Retention defaults are read from
`request.app.state.settings` at request time precisely so hot reload takes effect; follow that pattern instead of
capturing settings at router construction.

### Client (`etransfer/client/`)

- Config is `~/.etransfer/client.json` (server URL + cached session token); `ETRANSFER_SERVER` and
  `ETRANSFER_TOKEN` override. `CLIENT_CONFIG_DIR` / `CLIENT_CONFIG_FILE` are module-level constants that tests
  monkeypatch — keep them that way rather than inlining paths.
- `cli.py` holds all commands with `rich_help_panel=` groups (Setup / Files / Folders / Offline / Plugins /
  Server). Add new commands there; sub-apps are `server`, `tasks`, `folders`.
- Endpoint selection: `/api/endpoints` reports per-IP load, and the chosen endpoint is cached per server
  (`_load_endpoint_cache`) so subsequent uploads skip probing. `_invalidate_endpoint_cache` on failure.
- `ParallelUploader` prefetches and pushes chunks across threads; `ChunkDownloader` writes chunk files into
  `~/.etransfer/cache` with a part-dir/meta sidecar to resume, and also supports follow-mode streaming downloads
  of a still-uploading file.

## Testing

`tests/conftest.py` is the whole harness: it strips every `ETRANSFER_*` var before importing etransfer, starts an
in-memory S3 mock (`_fake_bucket.py`) and the real app with `state_backend="memory"`, registers `FakeBucketSink`,
and isolates client config. Fixtures: `memory_server` (dict with `url`/`token`/`settings`/`storage_path`),
`cli_env`, `cli_runner`, `run_cli("upload", "f.txt")`, `host_file`. Mutating `memory_server["settings"]` affects the
live server, which is how policy tests (e.g. disabling `permanent`) work. Files starting with `_` are helpers, not
tests.

## Conventions

- Formatting is black (line length 120) + isort (black profile); flake8 allows 200 chars and ignores a broad set
  (unused imports E402/F401/F841 are intentionally tolerated). mypy requires annotations
  (`disallow_untyped_defs`) on `etransfer/` only, with `client/gui` exempted per-code.
- Prettier runs on Markdown/YAML/JSON through pre-commit — edits to docs and config templates will be reformatted.
- The CI matrix runs Python 3.13 on Ubuntu/macOS/Windows; the full 3.9–3.13 matrix is manual via `workflow_dispatch`.
  So avoid 3.10+-only syntax (no `match`, no `X | Y` annotations at runtime-evaluated sites) despite
  `requires-python >= 3.9`.
- Comments and docstrings in the codebase are English; user-facing CLI text and docs mix English and Chinese.
  Keep new user-facing strings consistent with the surrounding command.
- Version lives in two places (`pyproject.toml`, `etransfer/__init__.py`) and `bump-my-version` keeps them in sync.
  The publish workflow fails if the git tag doesn't match `etransfer.__version__`.
