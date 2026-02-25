#!/usr/bin/env python3
"""
简单的单元测试 - 不需要启动服务端。

测试各个模块的基本功能。
"""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def test_constants():
    """测试常量模块。"""
    print("测试常量模块...")

    from etransfer.common.constants import DEFAULT_CHUNK_SIZE, TUS_EXTENSIONS, TUS_VERSION, TusHeaders

    assert TUS_VERSION == "1.0.0"
    assert DEFAULT_CHUNK_SIZE == 32 * 1024 * 1024
    assert "creation" in TUS_EXTENSIONS
    assert TusHeaders.TUS_RESUMABLE == "Tus-Resumable"

    print("  ✓ 常量模块测试通过")


def test_models():
    """测试数据模型。"""
    print("测试数据模型...")

    from etransfer.common.models import FileInfo, ServerInfo

    # 测试 FileInfo
    file_info = FileInfo(
        file_id="test123",
        filename="test.txt",
        size=1024,
        chunk_size=256,
        total_chunks=4,
    )

    assert file_info.file_id == "test123"
    assert file_info.progress == 0.0
    assert not file_info.is_complete

    file_info.uploaded_size = 1024
    assert file_info.progress == 100.0
    assert file_info.is_complete

    # 测试 ServerInfo
    server_info = ServerInfo(
        version="0.1.0",
        tus_version="1.0.0",
        tus_extensions=["creation"],
        chunk_size=4194304,
    )

    assert server_info.version == "0.1.0"

    print("  ✓ 数据模型测试通过")


def test_config():
    """测试配置模块。"""
    print("测试配置模块...")

    from etransfer.server.config import (
        HOT_RELOADABLE_FIELDS,
        ServerSettings,
        _parse_yaml_to_settings_dict,
        load_server_settings,
        parse_size,
        reload_hot_settings,
    )

    # 测试默认配置
    settings = ServerSettings()
    assert settings.port == 8765
    assert settings.chunk_size == 32 * 1024 * 1024
    assert settings.state_backend == "file"
    assert settings.default_retention == "permanent"
    assert settings.config_watch is False
    assert settings.config_watch_interval == 30

    # 测试自定义配置
    custom = ServerSettings(port=9000, max_upload_size=1024 * 1024)
    assert custom.port == 9000
    assert custom.max_upload_size == 1024 * 1024

    # 测试 parse_size
    assert parse_size("1GB") == 1024**3
    assert parse_size("100MB") == 100 * 1024**2
    assert parse_size("1024") == 1024

    # 测试 YAML 解析
    d = _parse_yaml_to_settings_dict(
        {
            "server": {"port": 7777},
            "auth": {"tokens": ["t1"]},
            "storage": {"max_storage_size": "2GB"},
        }
    )
    assert d["port"] == 7777
    assert d["auth_tokens"] == ["t1"]
    assert d["max_storage_size"] == 2 * 1024**3

    # 测试 HOT_RELOADABLE_FIELDS 完整性
    all_fields = set(ServerSettings.model_fields.keys())
    for f in HOT_RELOADABLE_FIELDS:
        assert f in all_fields, f"{f} not on ServerSettings"
    for f in ("host", "port", "workers", "storage_path"):
        assert f not in HOT_RELOADABLE_FIELDS, f"{f} should not be hot-reloadable"

    # 测试热重载
    import yaml

    cfg = Path(tempfile.mkdtemp()) / "test.yaml"
    cfg.write_text(yaml.dump({"auth": {"tokens": ["old"]}}))
    s = load_server_settings(cfg)
    assert s.auth_tokens == ["old"]
    cfg.write_text(yaml.dump({"auth": {"tokens": ["new"]}}))
    changes = reload_hot_settings(s)
    assert "auth_tokens" in changes
    assert s.auth_tokens == ["new"]

    print("  ✓ 配置模块测试通过（含自动发现 + 热重载）")


def test_cache():
    """测试本地缓存模块。"""
    print("测试本地缓存模块...")

    from etransfer.client.cache import LocalCache

    with tempfile.TemporaryDirectory() as tmpdir:
        cache = LocalCache(Path(tmpdir))

        # 测试写入和读取
        file_id = "test_file_123"
        chunk_data = b"Hello, World!" * 1000

        cache.put_chunk(file_id, 0, chunk_data)
        assert cache.has_chunk(file_id, 0)

        retrieved = cache.get_chunk(file_id, 0)
        assert retrieved == chunk_data

        # 测试获取已缓存的分片列表
        cache.put_chunk(file_id, 1, b"Chunk 1")
        cache.put_chunk(file_id, 2, b"Chunk 2")

        cached_chunks = cache.get_cached_chunks(file_id)
        assert 0 in cached_chunks
        assert 1 in cached_chunks
        assert 2 in cached_chunks

        # 测试清理
        cache.clear_file(file_id)
        assert not cache.has_chunk(file_id, 0)

    print("  ✓ 本地缓存测试通过")


def test_tus_metadata():
    """测试 TUS 元数据解析。"""
    print("测试 TUS 元数据...")

    import base64

    from etransfer.server.tus.models import RetentionPolicy, TusMetadata

    # 构造 Upload-Metadata 头
    filename = base64.b64encode(b"test.txt").decode()
    filetype = base64.b64encode(b"text/plain").decode()
    header_value = f"filename {filename},filetype {filetype}"

    metadata = TusMetadata.from_header(header_value)

    assert metadata.filename == "test.txt"
    assert metadata.filetype == "text/plain"
    assert metadata.retention is None  # 未指定时为 None
    assert metadata.retention_ttl is None

    # 测试转换回 header
    header = metadata.to_header()
    assert "filename" in header

    # 测试带 retention 的元数据解析
    retention_val = base64.b64encode(b"download_once").decode()
    header_with_retention = f"filename {filename}," f"retention {retention_val}"
    meta2 = TusMetadata.from_header(header_with_retention)
    assert meta2.retention == "download_once"
    assert meta2.retention_ttl is None

    # 测试带 TTL 的元数据解析
    ttl_val = base64.b64encode(b"3600").decode()
    retention_ttl_val = base64.b64encode(b"ttl").decode()
    header_with_ttl = f"filename {filename}," f"retention {retention_ttl_val}," f"retention_ttl {ttl_val}"
    meta3 = TusMetadata.from_header(header_with_ttl)
    assert meta3.retention == "ttl"
    assert meta3.retention_ttl == 3600

    # 测试 retention 序列化到 header
    meta4 = TusMetadata(filename="test.bin", retention="ttl", retention_ttl=7200)
    header4 = meta4.to_header()
    assert "retention" in header4
    assert "retention_ttl" in header4

    # 再解析回来验证往返一致性
    meta5 = TusMetadata.from_header(header4)
    assert meta5.filename == "test.bin"
    assert meta5.retention == "ttl"
    assert meta5.retention_ttl == 7200

    # 测试 RetentionPolicy 枚举
    assert RetentionPolicy.PERMANENT.value == "permanent"
    assert RetentionPolicy.DOWNLOAD_ONCE.value == "download_once"
    assert RetentionPolicy.TTL.value == "ttl"

    print("  ✓ TUS 元数据测试通过（含 retention）")


def test_instance_traffic_tracker():
    """测试实例流量追踪器。"""
    print("测试实例流量追踪器...")

    from etransfer.server.services.instance_traffic import InstanceTrafficTracker

    tracker = InstanceTrafficTracker(host="127.0.0.1", port=8765)
    print(f"  endpoint: {tracker.endpoint}")
    assert tracker.endpoint == "127.0.0.1:8765"

    # 记录流量
    tracker.record_upload(1024)
    tracker.record_download(2048)
    assert tracker.total_bytes_recv == 1024
    assert tracker.total_bytes_sent == 2048

    # 获取快照
    snap = tracker.get_snapshot()
    print(f"  snapshot: {snap}")
    assert snap["endpoint"] == "127.0.0.1:8765"
    assert snap["url"] == "http://127.0.0.1:8765"
    assert snap["bytes_recv"] == 1024
    assert snap["bytes_sent"] == 2048

    # 提交样本后获取速率
    tracker._commit_sample()
    tracker.record_upload(1024)
    tracker.record_download(2048)
    tracker._commit_sample()
    up, down = tracker.get_rates()
    print(f"  rates: up={up} B/s, down={down} B/s")
    assert up > 0 or down > 0

    print("  ✓ 实例流量追踪器测试通过")


def run_all_tests():
    """运行所有测试。"""
    print("=" * 50)
    print("EasyTransfer 单元测试")
    print("=" * 50)

    tests = [
        test_constants,
        test_models,
        test_config,
        test_cache,
        test_tus_metadata,
        test_instance_traffic_tracker,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"  ✗ 测试失败: {e}")
            failed += 1

    print("\n" + "=" * 50)
    print(f"测试结果: {passed} 通过, {failed} 失败")
    print("=" * 50)

    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
