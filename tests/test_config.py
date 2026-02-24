#!/usr/bin/env python3
"""
单元测试 — 配置自动发现、YAML 解析、热重载。

不需要启动服务端，所有测试在临时目录中完成。

运行方式:
    python -m pytest tests/test_config.py -v
    # 或者直接执行:
    python tests/test_config.py
"""

import sys
import textwrap
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from etransfer.server.config import (  # noqa: E402
    HOT_RELOADABLE_FIELDS,
    ServerSettings,
    _apply_env_overrides,
    _parse_yaml_to_settings_dict,
    discover_config_path,
    load_server_settings,
    parse_size,
    reload_hot_settings,
)

# ── parse_size ────────────────────────────────────────────────


def test_parse_size_bytes():
    assert parse_size("1024") == 1024


def test_parse_size_kb():
    assert parse_size("10KB") == 10 * 1024


def test_parse_size_mb():
    assert parse_size("100MB") == 100 * 1024**2


def test_parse_size_gb():
    assert parse_size("2GB") == 2 * 1024**3


def test_parse_size_tb():
    assert parse_size("1TB") == 1024**4


def test_parse_size_case_insensitive():
    assert parse_size("50mb") == 50 * 1024**2


def test_parse_size_short_suffix():
    assert parse_size("5G") == 5 * 1024**3


def test_parse_size_with_spaces():
    assert parse_size("  100 MB  ") == 100 * 1024**2


def test_parse_size_float():
    assert parse_size("1.5GB") == int(1.5 * 1024**3)


# ── _parse_yaml_to_settings_dict ──────────────────────────────


def test_parse_yaml_server_section():
    cfg = {"server": {"host": "1.2.3.4", "port": 9999, "workers": 4}}
    d = _parse_yaml_to_settings_dict(cfg)
    assert d["host"] == "1.2.3.4"
    assert d["port"] == 9999
    assert d["workers"] == 4


def test_parse_yaml_state_section():
    cfg = {"state": {"backend": "redis", "redis_url": "redis://r:6379/1"}}
    d = _parse_yaml_to_settings_dict(cfg)
    assert d["state_backend"] == "redis"
    assert d["redis_url"] == "redis://r:6379/1"


def test_parse_yaml_auth_section():
    cfg = {"auth": {"enabled": False, "tokens": ["aaa", "bbb"]}}
    d = _parse_yaml_to_settings_dict(cfg)
    assert d["auth_enabled"] is False
    assert d["auth_tokens"] == ["aaa", "bbb"]


def test_parse_yaml_network_section():
    cfg = {
        "network": {
            "interfaces": ["eth0"],
            "prefer_ipv4": False,
            "advertised_endpoints": ["10.0.0.1"],
        }
    }
    d = _parse_yaml_to_settings_dict(cfg)
    assert d["interfaces"] == ["eth0"]
    assert d["prefer_ipv4"] is False
    assert d["advertised_endpoints"] == ["10.0.0.1"]


def test_parse_yaml_storage_max_size_int():
    cfg = {"storage": {"max_storage_size": 1073741824}}
    d = _parse_yaml_to_settings_dict(cfg)
    assert d["max_storage_size"] == 1073741824


def test_parse_yaml_storage_max_size_string():
    cfg = {"storage": {"max_storage_size": "2GB"}}
    d = _parse_yaml_to_settings_dict(cfg)
    assert d["max_storage_size"] == 2 * 1024**3


def test_parse_yaml_retention_section():
    cfg = {
        "retention": {
            "default": "download_once",
            "default_ttl": 3600,
            "token_policies": {"tok1": {"default_retention": "ttl", "default_ttl": 600}},
        }
    }
    d = _parse_yaml_to_settings_dict(cfg)
    assert d["default_retention"] == "download_once"
    assert d["default_retention_ttl"] == 3600
    assert d["token_retention_policies"]["tok1"]["default_ttl"] == 600


def test_parse_yaml_user_system_section():
    cfg = {
        "user_system": {
            "enabled": True,
            "role_quotas": {
                "admin": {"max_storage_size": None},
                "user": {"max_storage_size": 5368709120},
            },
            "oidc": {
                "issuer_url": "https://auth.example.com",
                "client_id": "cid",
                "client_secret": "csec",
                "callback_url": "http://localhost:8765/cb",
                "scope": "openid email",
            },
            "database": {
                "backend": "mysql",
                "path": "/tmp/users.db",
                "mysql": {
                    "host": "db.local",
                    "port": 3307,
                    "user": "et",
                    "password": "pass",
                    "database": "etdb",
                },
            },
        }
    }
    d = _parse_yaml_to_settings_dict(cfg)
    assert d["user_system_enabled"] is True
    assert d["role_quotas"]["admin"]["max_storage_size"] is None
    assert d["oidc_issuer_url"] == "https://auth.example.com"
    assert d["oidc_client_id"] == "cid"
    assert d["oidc_client_secret"] == "csec"
    assert d["oidc_scope"] == "openid email"
    assert d["user_db_backend"] == "mysql"
    assert d["mysql_host"] == "db.local"
    assert d["mysql_port"] == 3307
    assert d["mysql_database"] == "etdb"


def test_parse_yaml_empty():
    assert _parse_yaml_to_settings_dict({}) == {}


# ── _apply_env_overrides ──────────────────────────────────────


def test_env_override_advertised_endpoints_json(monkeypatch):
    monkeypatch.setenv("ETRANSFER_ADVERTISED_ENDPOINTS", '["1.1.1.1","2.2.2.2"]')
    d: dict = {}
    _apply_env_overrides(d)
    assert d["advertised_endpoints"] == ["1.1.1.1", "2.2.2.2"]


def test_env_override_advertised_endpoints_csv(monkeypatch):
    monkeypatch.setenv("ETRANSFER_ADVERTISED_ENDPOINTS", "3.3.3.3, 4.4.4.4")
    d: dict = {}
    _apply_env_overrides(d)
    assert d["advertised_endpoints"] == ["3.3.3.3", "4.4.4.4"]


def test_env_override_max_storage_size(monkeypatch):
    monkeypatch.setenv("ETRANSFER_MAX_STORAGE_SIZE", "500MB")
    d: dict = {}
    _apply_env_overrides(d)
    assert d["max_storage_size"] == 500 * 1024**2


def test_env_override_not_set():
    d: dict = {"advertised_endpoints": ["keep"]}
    _apply_env_overrides(d)
    assert d["advertised_endpoints"] == ["keep"]


# ── discover_config_path ──────────────────────────────────────


def test_discover_env_var(monkeypatch, tmp_path):
    cfg = tmp_path / "custom.yaml"
    cfg.write_text("server:\n  port: 1234\n")
    monkeypatch.setenv("ETRANSFER_CONFIG", str(cfg))
    monkeypatch.chdir(tmp_path)
    result = discover_config_path()
    assert result == cfg


def test_discover_env_var_missing(monkeypatch, tmp_path):
    monkeypatch.setenv("ETRANSFER_CONFIG", "/nonexistent/path.yaml")
    monkeypatch.chdir(tmp_path)
    result = discover_config_path()
    # Should fall through to other candidates (which also don't exist)
    assert result is None


def test_discover_cwd_config_yaml(monkeypatch, tmp_path):
    monkeypatch.delenv("ETRANSFER_CONFIG", raising=False)
    cfg = tmp_path / "config.yaml"
    cfg.write_text("server:\n  port: 2222\n")
    monkeypatch.chdir(tmp_path)
    result = discover_config_path()
    assert result is not None
    assert result.name == "config.yaml"


def test_discover_config_subfolder(monkeypatch, tmp_path):
    monkeypatch.delenv("ETRANSFER_CONFIG", raising=False)
    sub = tmp_path / "config"
    sub.mkdir()
    cfg = sub / "config.yaml"
    cfg.write_text("server:\n  port: 3333\n")
    monkeypatch.chdir(tmp_path)
    result = discover_config_path()
    assert result is not None
    assert str(result).endswith("config/config.yaml")


def test_discover_priority_cwd_over_subfolder(monkeypatch, tmp_path):
    """./config.yaml should win over ./config/config.yaml."""
    monkeypatch.delenv("ETRANSFER_CONFIG", raising=False)

    top = tmp_path / "config.yaml"
    top.write_text("server:\n  port: 1111\n")

    sub = tmp_path / "config"
    sub.mkdir()
    (sub / "config.yaml").write_text("server:\n  port: 2222\n")

    monkeypatch.chdir(tmp_path)
    result = discover_config_path()
    assert result is not None
    assert result.name == "config.yaml"
    # discover returns the relative Path("./config.yaml"); resolve to compare
    assert result.resolve().parent == tmp_path


def test_discover_none_when_nothing_exists(monkeypatch, tmp_path):
    monkeypatch.delenv("ETRANSFER_CONFIG", raising=False)
    monkeypatch.chdir(tmp_path)
    result = discover_config_path()
    assert result is None


# ── load_server_settings ──────────────────────────────────────


def test_load_explicit_path(tmp_path):
    cfg = tmp_path / "test.yaml"
    cfg.write_text(textwrap.dedent("""\
        server:
          port: 7777
          workers: 2
        auth:
          enabled: false
          tokens:
            - "tok-abc"
        storage:
          max_storage_size: 5GB
        retention:
          default: ttl
          default_ttl: 600
        network:
          advertised_endpoints:
            - "10.0.0.5"
    """))
    settings = load_server_settings(cfg)
    assert settings.port == 7777
    assert settings.workers == 2
    assert settings.auth_enabled is False
    assert settings.auth_tokens == ["tok-abc"]
    assert settings.max_storage_size == 5 * 1024**3
    assert settings.default_retention == "ttl"
    assert settings.default_retention_ttl == 600
    assert settings.advertised_endpoints == ["10.0.0.5"]
    assert getattr(settings, "_config_path", None) == cfg


def test_load_auto_discover(monkeypatch, tmp_path):
    monkeypatch.delenv("ETRANSFER_CONFIG", raising=False)
    cfg = tmp_path / "config.yaml"
    cfg.write_text("server:\n  port: 4444\n")
    monkeypatch.chdir(tmp_path)
    settings = load_server_settings()  # no explicit path
    assert settings.port == 4444
    assert getattr(settings, "_config_path") is not None


def test_load_defaults_when_no_config(monkeypatch, tmp_path):
    monkeypatch.delenv("ETRANSFER_CONFIG", raising=False)
    monkeypatch.chdir(tmp_path)
    settings = load_server_settings()
    assert settings.port == 8765
    assert settings.state_backend == "file"
    assert settings.default_retention == "permanent"


def test_load_config_watch_fields(tmp_path):
    cfg = tmp_path / "test.yaml"
    cfg.write_text(textwrap.dedent("""\
        server:
          config_watch: true
          config_watch_interval: 10
    """))
    settings = load_server_settings(cfg)
    assert settings.config_watch is True
    assert settings.config_watch_interval == 10


# ── reload_hot_settings ───────────────────────────────────────


def _make_config_and_settings(tmp_path, yaml_text: str) -> ServerSettings:
    cfg = tmp_path / "reload_test.yaml"
    cfg.write_text(textwrap.dedent(yaml_text))
    return load_server_settings(cfg)


def test_reload_no_changes(tmp_path):
    settings = _make_config_and_settings(
        tmp_path,
        """\
        auth:
          tokens:
            - "aaa"
        storage:
          max_storage_size: 1GB
    """,
    )
    assert settings.auth_tokens == ["aaa"]
    changes = reload_hot_settings(settings)
    assert changes == {}


def test_reload_detects_token_change(tmp_path):
    cfg = tmp_path / "reload_test.yaml"
    cfg.write_text(textwrap.dedent("""\
        auth:
          tokens:
            - "token-v1"
    """))
    settings = load_server_settings(cfg)
    assert settings.auth_tokens == ["token-v1"]

    # Modify the file
    cfg.write_text(textwrap.dedent("""\
        auth:
          tokens:
            - "token-v2"
            - "token-v3"
    """))
    changes = reload_hot_settings(settings)
    assert "auth_tokens" in changes
    assert changes["auth_tokens"][0] == ["token-v1"]
    assert changes["auth_tokens"][1] == ["token-v2", "token-v3"]
    # Settings object should be updated in place
    assert settings.auth_tokens == ["token-v2", "token-v3"]


def test_reload_detects_quota_change(tmp_path):
    cfg = tmp_path / "reload_test.yaml"
    cfg.write_text(textwrap.dedent("""\
        storage:
          max_storage_size: 1GB
    """))
    settings = load_server_settings(cfg)
    assert settings.max_storage_size == 1 * 1024**3

    cfg.write_text(textwrap.dedent("""\
        storage:
          max_storage_size: 5GB
    """))
    changes = reload_hot_settings(settings)
    assert "max_storage_size" in changes
    assert settings.max_storage_size == 5 * 1024**3


def test_reload_detects_retention_change(tmp_path):
    cfg = tmp_path / "reload_test.yaml"
    cfg.write_text(textwrap.dedent("""\
        retention:
          default: permanent
    """))
    settings = load_server_settings(cfg)
    assert settings.default_retention == "permanent"

    cfg.write_text(textwrap.dedent("""\
        retention:
          default: download_once
    """))
    changes = reload_hot_settings(settings)
    assert "default_retention" in changes
    assert settings.default_retention == "download_once"


def test_reload_detects_advertised_endpoints_change(tmp_path):
    cfg = tmp_path / "reload_test.yaml"
    cfg.write_text(textwrap.dedent("""\
        network:
          advertised_endpoints:
            - "10.0.0.1"
    """))
    settings = load_server_settings(cfg)

    cfg.write_text(textwrap.dedent("""\
        network:
          advertised_endpoints:
            - "10.0.0.1"
            - "10.0.0.2"
    """))
    changes = reload_hot_settings(settings)
    assert "advertised_endpoints" in changes
    assert settings.advertised_endpoints == ["10.0.0.1", "10.0.0.2"]


def test_reload_detects_role_quotas_change(tmp_path):
    cfg = tmp_path / "reload_test.yaml"
    cfg.write_text(textwrap.dedent("""\
        user_system:
          role_quotas:
            user:
              max_storage_size: 1073741824
    """))
    settings = load_server_settings(cfg)

    cfg.write_text(textwrap.dedent("""\
        user_system:
          role_quotas:
            user:
              max_storage_size: 5368709120
    """))
    changes = reload_hot_settings(settings)
    assert "role_quotas" in changes
    assert settings.role_quotas["user"]["max_storage_size"] == 5368709120


def test_reload_ignores_static_fields(tmp_path):
    cfg = tmp_path / "reload_test.yaml"
    cfg.write_text(textwrap.dedent("""\
        server:
          port: 8765
        auth:
          tokens:
            - "t1"
    """))
    settings = load_server_settings(cfg)
    assert settings.port == 8765

    # Change both a hot field and a static field
    cfg.write_text(textwrap.dedent("""\
        server:
          port: 9999
        auth:
          tokens:
            - "t2"
    """))
    changes = reload_hot_settings(settings)
    # Only auth_tokens should change, not port
    assert "auth_tokens" in changes
    assert settings.auth_tokens == ["t2"]
    # Port should remain unchanged
    assert settings.port == 8765


def test_reload_multiple_fields_at_once(tmp_path):
    cfg = tmp_path / "reload_test.yaml"
    cfg.write_text(textwrap.dedent("""\
        auth:
          tokens:
            - "old-token"
        storage:
          max_storage_size: 1GB
        retention:
          default: permanent
          default_ttl: 3600
        network:
          advertised_endpoints:
            - "1.1.1.1"
    """))
    settings = load_server_settings(cfg)

    cfg.write_text(textwrap.dedent("""\
        auth:
          tokens:
            - "new-token"
        storage:
          max_storage_size: 10GB
        retention:
          default: ttl
          default_ttl: 7200
        network:
          advertised_endpoints:
            - "2.2.2.2"
            - "3.3.3.3"
    """))
    changes = reload_hot_settings(settings)
    assert len(changes) == 5
    assert settings.auth_tokens == ["new-token"]
    assert settings.max_storage_size == 10 * 1024**3
    assert settings.default_retention == "ttl"
    assert settings.default_retention_ttl == 7200
    assert settings.advertised_endpoints == ["2.2.2.2", "3.3.3.3"]


def test_reload_no_config_path():
    """reload_hot_settings should return {} if no config path is stashed."""
    settings = ServerSettings()
    changes = reload_hot_settings(settings)
    assert changes == {}


def test_reload_config_file_deleted(tmp_path):
    """reload_hot_settings should return {} if config file was deleted."""
    cfg = tmp_path / "gone.yaml"
    cfg.write_text("auth:\n  tokens:\n    - t\n")
    settings = load_server_settings(cfg)
    cfg.unlink()
    changes = reload_hot_settings(settings)
    assert changes == {}


def test_reload_token_retention_policies(tmp_path):
    cfg = tmp_path / "reload_test.yaml"
    cfg.write_text(textwrap.dedent("""\
        retention:
          token_policies:
            guest-tok:
              default_retention: download_once
    """))
    settings = load_server_settings(cfg)
    assert "guest-tok" in settings.token_retention_policies

    cfg.write_text(textwrap.dedent("""\
        retention:
          token_policies:
            guest-tok:
              default_retention: ttl
              default_ttl: 3600
            vip-tok:
              default_retention: permanent
    """))
    changes = reload_hot_settings(settings)
    assert "token_retention_policies" in changes
    assert "vip-tok" in settings.token_retention_policies
    assert settings.token_retention_policies["guest-tok"]["default_ttl"] == 3600


# ── HOT_RELOADABLE_FIELDS completeness ───────────────────────


def test_hot_reloadable_fields_are_valid():
    """Every field in HOT_RELOADABLE_FIELDS must exist on ServerSettings."""
    all_fields = set(ServerSettings.model_fields.keys())
    for field in HOT_RELOADABLE_FIELDS:
        assert field in all_fields, f"{field} is in HOT_RELOADABLE_FIELDS but not on ServerSettings"


def test_static_fields_not_in_hot():
    """Critical static fields must NOT be hot-reloadable."""
    static = {
        "host",
        "port",
        "workers",
        "storage_path",
        "state_backend",
        "redis_url",
        "oidc_issuer_url",
        "oidc_client_id",
        "oidc_client_secret",
        "user_db_backend",
    }
    for field in static:
        assert field not in HOT_RELOADABLE_FIELDS, f"{field} should NOT be hot-reloadable"


# ── run_all_tests (standalone runner) ─────────────────────────


def run_all_tests():
    """Run all tests when executed standalone (without pytest)."""
    import inspect

    print("=" * 60)
    print("EasyTransfer — Config / Hot-Reload 单元测试")
    print("=" * 60)

    # Collect test functions
    tests = [obj for name, obj in globals().items() if name.startswith("test_") and callable(obj)]
    tests.sort(key=lambda f: f.__name__)

    passed = failed = skipped = 0

    for test_fn in tests:
        sig = inspect.signature(test_fn)
        params = list(sig.parameters.keys())

        # Provide required fixtures for standalone mode
        kwargs: dict = {}
        if "tmp_path" in params:
            import tempfile as _tf

            td = _tf.mkdtemp()
            kwargs["tmp_path"] = Path(td)
        if "monkeypatch" in params:
            # Simplified monkeypatch for standalone
            try:
                import pytest

                skipped += 1
                print(f"  ⊘ {test_fn.__name__} (需要 pytest monkeypatch，跳过)")
                continue
            except ImportError:
                skipped += 1
                print(f"  ⊘ {test_fn.__name__} (需要 pytest，跳过)")
                continue

        try:
            test_fn(**kwargs)
            passed += 1
            print(f"  ✓ {test_fn.__name__}")
        except Exception as e:
            failed += 1
            print(f"  ✗ {test_fn.__name__}: {e}")

    print(f"\n{'=' * 60}")
    print(f"结果: {passed} 通过, {failed} 失败, {skipped} 跳过")
    print(f"{'=' * 60}")
    if skipped:
        print("提示: 使用 pytest 运行以获得完整测试覆盖:")
        print("  python -m pytest tests/test_config.py -v")
    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
