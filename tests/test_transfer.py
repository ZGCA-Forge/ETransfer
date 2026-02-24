#!/usr/bin/env python3
"""
EasyTransfer 传输测试脚本

测试场景：
1. 启动服务端 (支持多种后端)
2. 生成多个大文件（可配置大小）
3. 并发上传文件
4. 并发下载文件（包括边上传边下载测试）
5. 验证文件完整性

使用方式：
    python tests/test_transfer.py              # 使用内存后端 (默认)
    python tests/test_transfer.py --backend file   # 使用文件后端
    python tests/test_transfer.py --backend redis  # 使用 Redis 后端 (需要 Redis)
    python tests/test_transfer.py --quick      # 快速测试模式
    python tests/test_transfer.py --large      # 大文件测试模式 (1GB)
"""

import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))


# 测试配置
class TestConfig:
    # 服务端配置
    SERVER_HOST = "127.0.0.1"
    SERVER_PORT = 18080

    # 状态后端配置
    STATE_BACKEND = "file"  # memory, file, or redis
    REDIS_URL = "redis://localhost:6379/0"

    # 测试文件配置
    NUM_FILES = 3  # 测试文件数量
    FILE_SIZE_MB = 100  # 每个文件大小 (MB)

    # 并发配置
    UPLOAD_WORKERS = 2  # 并发上传线程
    DOWNLOAD_WORKERS = 2  # 并发下载线程

    # Token
    API_TOKEN = "test-token-12345"

    # 超时配置
    SERVER_STARTUP_TIMEOUT = 10  # 服务端启动超时(秒)
    TRANSFER_TIMEOUT = 600  # 传输超时(秒)


def generate_test_file(path: Path, size_mb: int) -> str:
    """生成测试文件并返回其 MD5 哈希值。

    Args:
        path: 文件路径
        size_mb: 文件大小 (MB)

    Returns:
        文件的 MD5 哈希值
    """
    print(f"  生成测试文件: {path.name} ({size_mb} MB)")

    chunk_size = 1024 * 1024  # 1MB chunks
    total_chunks = size_mb

    md5 = hashlib.md5()

    with open(path, "wb") as f:
        for i in range(total_chunks):
            # 生成随机数据（使用索引作为种子确保可重复）
            data = os.urandom(chunk_size)
            f.write(data)
            md5.update(data)

            # 显示进度
            if (i + 1) % 100 == 0 or i == total_chunks - 1:
                progress = (i + 1) / total_chunks * 100
                print(f"    进度: {progress:.1f}%", end="\r")

    print()
    return md5.hexdigest()


def calculate_file_hash(path: Path) -> str:
    """计算文件的 MD5 哈希值。"""
    md5 = hashlib.md5()
    with open(path, "rb") as f:
        while chunk := f.read(8192 * 1024):
            md5.update(chunk)
    return md5.hexdigest()


def wait_for_server(host: str, port: int, timeout: int = 10) -> bool:
    """等待服务端启动。"""
    import socket

    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def check_backend_available(backend: str) -> bool:
    """检查后端是否可用。"""
    if backend == "memory":
        return True
    elif backend == "file":
        return True
    elif backend == "redis":
        try:
            import redis

            r = redis.from_url(TestConfig.REDIS_URL)
            r.ping()
            return True
        except Exception:
            return False
    return False


def start_server(storage_path: Path) -> subprocess.Popen:
    """启动测试服务端。"""
    print("\n[1] 启动服务端...")
    print(f"  后端类型: {TestConfig.STATE_BACKEND}")

    env = os.environ.copy()

    # 确保存储目录存在
    storage_dir = storage_path / "storage"
    storage_dir.mkdir(parents=True, exist_ok=True)

    # 设置环境变量
    import json

    env["ETRANSFER_STORAGE_PATH"] = str(storage_dir)
    env["ETRANSFER_STATE_BACKEND"] = TestConfig.STATE_BACKEND
    env["ETRANSFER_REDIS_URL"] = TestConfig.REDIS_URL
    env["ETRANSFER_AUTH_ENABLED"] = "true"
    env["ETRANSFER_AUTH_TOKENS"] = json.dumps([TestConfig.API_TOKEN])

    # 使用 uvicorn 直接启动
    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "etransfer.server.main:app",
        "--host",
        TestConfig.SERVER_HOST,
        "--port",
        str(TestConfig.SERVER_PORT),
    ]

    print(f"  命令: {' '.join(cmd)}")

    # 启动进程，不捕获输出以便调试
    process = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    # 等待服务端启动
    print(f"  等待服务端启动 (最多 {TestConfig.SERVER_STARTUP_TIMEOUT} 秒)...")
    if wait_for_server(TestConfig.SERVER_HOST, TestConfig.SERVER_PORT, TestConfig.SERVER_STARTUP_TIMEOUT):
        print(f"  ✓ 服务端已启动: http://{TestConfig.SERVER_HOST}:{TestConfig.SERVER_PORT}")
        return process
    else:
        # 尝试读取错误输出
        try:
            output, _ = process.communicate(timeout=2)
            if output:
                print(f"  服务端输出:\n{output.decode()[:1000]}")
        except Exception:
            pass
        process.terminate()
        raise RuntimeError("服务端启动超时")


def upload_file(file_path: Path, server_url: str, token: str) -> Optional[str]:
    """上传文件并返回文件ID。"""
    try:
        from etransfer.client.tus_client import EasyTransferClient

        start_time = time.time()
        file_size = file_path.stat().st_size

        with EasyTransferClient(server_url, token=token) as client:
            uploader = client.create_uploader(str(file_path))
            location = uploader.upload()

            # 从 location URL 提取 file_id
            file_id = location.split("/")[-1] if location else None

        elapsed = time.time() - start_time
        speed = file_size / elapsed / 1024 / 1024  # MB/s

        print(f"  ✓ 上传完成: {file_path.name} -> {file_id[:8]}... ({speed:.2f} MB/s)")
        return file_id

    except Exception as e:
        print(f"  ✗ 上传失败: {file_path.name} - {e}")
        return None


def download_file(
    file_id: str,
    output_path: Path,
    server_url: str,
    token: str,
) -> bool:
    """下载文件。"""
    try:
        from etransfer.client.downloader import ChunkDownloader

        start_time = time.time()

        downloader = ChunkDownloader(server_url, token=token)
        _ = downloader.get_file_info(file_id)

        success = downloader.download_file(file_id, output_path)

        if success:
            elapsed = time.time() - start_time
            file_size = output_path.stat().st_size
            speed = file_size / elapsed / 1024 / 1024  # MB/s
            print(f"  ✓ 下载完成: {file_id[:8]}... -> {output_path.name} ({speed:.2f} MB/s)")

        return success

    except Exception as e:
        print(f"  ✗ 下载失败: {file_id[:8]}... - {e}")
        return False


def run_concurrent_upload(
    files: list[tuple[Path, str]],
    server_url: str,
    token: str,
) -> dict[str, str]:
    """并发上传测试。

    Args:
        files: [(文件路径, 原始哈希), ...]
        server_url: 服务端URL
        token: API token

    Returns:
        {原文件名: file_id}
    """
    print(f"\n[3] 并发上传测试 ({len(files)} 个文件, {TestConfig.UPLOAD_WORKERS} 线程)...")

    results = {}

    with ThreadPoolExecutor(max_workers=TestConfig.UPLOAD_WORKERS) as executor:
        futures = {executor.submit(upload_file, path, server_url, token): (path, hash_val) for path, hash_val in files}

        for future in as_completed(futures):
            path, hash_val = futures[future]
            file_id = future.result()
            if file_id:
                results[path.name] = file_id

    print(f"  上传成功: {len(results)}/{len(files)}")
    return results


def run_concurrent_download(
    file_ids: dict[str, str],
    original_hashes: dict[str, str],
    download_dir: Path,
    server_url: str,
    token: str,
) -> dict[str, bool]:
    """并发下载测试。

    Args:
        file_ids: {原文件名: file_id}
        original_hashes: {原文件名: 原始哈希}
        download_dir: 下载目录
        server_url: 服务端URL
        token: API token

    Returns:
        {原文件名: 是否成功且哈希匹配}
    """
    print(f"\n[4] 并发下载测试 ({len(file_ids)} 个文件, {TestConfig.DOWNLOAD_WORKERS} 线程)...")

    results = {}

    with ThreadPoolExecutor(max_workers=TestConfig.DOWNLOAD_WORKERS) as executor:
        futures = {}
        for filename, file_id in file_ids.items():
            output_path = download_dir / f"downloaded_{filename}"
            future = executor.submit(download_file, file_id, output_path, server_url, token)
            futures[future] = (filename, file_id, output_path)

        for future in as_completed(futures):
            filename, file_id, output_path = futures[future]
            success = future.result()

            if success and output_path.exists():
                # 验证哈希
                downloaded_hash = calculate_file_hash(output_path)
                original_hash = original_hashes.get(filename)

                if downloaded_hash == original_hash:
                    print(f"  ✓ 哈希验证通过: {filename}")
                    results[filename] = True
                else:
                    print(f"  ✗ 哈希不匹配: {filename}")
                    print(f"    原始: {original_hash}")
                    print(f"    下载: {downloaded_hash}")
                    results[filename] = False
            else:
                results[filename] = False

    passed = sum(1 for v in results.values() if v)
    print(f"  下载验证通过: {passed}/{len(file_ids)}")
    return results


def run_upload_while_download(
    file_path: Path,
    download_dir: Path,
    server_url: str,
    token: str,
) -> bool:
    """测试边上传边下载。

    上传一个大文件，同时尝试下载已上传的部分。
    """
    print("\n[5] 边上传边下载测试...")

    import threading

    file_id = None
    upload_done = threading.Event()
    download_results = []

    def uploader():
        nonlocal file_id
        from etransfer.client.tus_client import EasyTransferClient

        with EasyTransferClient(server_url, token=token) as client:
            uploader = client.create_uploader(str(file_path))
            location = uploader.upload()
            file_id = location.split("/")[-1] if location else None

        upload_done.set()
        print(f"  上传完成: {file_id[:8] if file_id else 'N/A'}...")

    def downloader():
        import httpx

        from etransfer.client.downloader import ChunkDownloader

        # 等待上传开始
        time.sleep(2)

        # 尝试获取文件列表
        try:
            _ = ChunkDownloader(server_url, token=token)

            # 轮询等待文件出现
            for _ in range(30):  # 最多等待30秒
                try:
                    with httpx.Client(timeout=5) as client:
                        headers = {"X-API-Token": token}
                        response = client.get(
                            f"{server_url}/api/files",
                            headers=headers,
                            params={"include_partial": True},
                        )
                        if response.status_code == 200:
                            files = response.json().get("files", [])
                            for f in files:
                                if f.get("status") == "partial":
                                    available = f.get("uploaded_size", 0)
                                    total = f.get("size", 0)
                                    progress = (available / total * 100) if total else 0
                                    download_results.append(
                                        f"发现上传中文件: {f['file_id'][:8]}... "
                                        f"({available / 1024 / 1024:.1f}/{total / 1024 / 1024:.1f} MB, {progress:.1f}%)"
                                    )
                                    return
                except Exception:
                    pass
                time.sleep(1)

        except Exception as e:
            download_results.append(f"下载检测错误: {e}")

    # 启动上传和下载线程
    upload_thread = threading.Thread(target=uploader)
    download_thread = threading.Thread(target=downloader)

    upload_thread.start()
    download_thread.start()

    upload_thread.join(timeout=TestConfig.TRANSFER_TIMEOUT)
    download_thread.join(timeout=30)

    # 输出结果
    for result in download_results:
        print(f"  {result}")

    if file_id:
        print("  ✓ 边上传边下载测试完成")
        return True
    else:
        print("  ✗ 边上传边下载测试失败")
        return False


def run_retention_policies(
    upload_dir: Path,
    download_dir: Path,
    server_url: str,
    token: str,
) -> bool:
    """测试缓存策略 (permanent / download_once / ttl)。"""
    print("\n[6] 缓存策略测试...")
    import httpx

    all_ok = True
    headers = {"X-API-Token": token}

    # --- 6a: permanent ---
    print("  6a) permanent: 多次下载不删除...")
    perm_file = upload_dir / "retention_permanent.bin"
    perm_file.write_bytes(os.urandom(64 * 1024))

    from etransfer.client.tus_client import EasyTransferClient

    with EasyTransferClient(server_url, token=token, chunk_size=64 * 1024) as client:
        up = client.create_uploader(str(perm_file), retention="permanent")
        up.upload()
        perm_id = up.url.split("/")[-1]

    for i in range(2):
        r = httpx.get(f"{server_url}/api/files/{perm_id}/download", headers=headers, timeout=10)
        if r.status_code != 200:
            print(f"    ✗ 下载 #{i+1} 失败: {r.status_code}")
            all_ok = False
    r = httpx.get(f"{server_url}/api/files/{perm_id}", headers=headers, timeout=10)
    if r.status_code == 200:
        print("    ✓ permanent: 多次下载后文件仍在")
    else:
        print(f"    ✗ permanent: 文件丢失 ({r.status_code})")
        all_ok = False

    # --- 6b: download_once ---
    print("  6b) download_once: 下载一次后自动删除...")
    once_file = upload_dir / "retention_once.bin"
    once_file.write_bytes(os.urandom(64 * 1024))

    with EasyTransferClient(server_url, token=token, chunk_size=64 * 1024) as client:
        up = client.create_uploader(str(once_file), retention="download_once")
        up.upload()
        once_id = up.url.split("/")[-1]

    r = httpx.get(f"{server_url}/api/files/{once_id}/download", headers=headers, timeout=10)
    if r.status_code == 200 and r.headers.get("x-retention-policy") == "download_once":
        print("    ✓ 首次下载成功，策略 header 正确")
    else:
        print(f"    ✗ 首次下载异常: status={r.status_code}")
        all_ok = False

    time.sleep(1)
    r = httpx.get(f"{server_url}/api/files/{once_id}", headers=headers, timeout=10)
    if r.status_code == 404:
        print("    ✓ download_once: 下载后文件已删除")
    else:
        print(f"    ✗ download_once: 文件未删除 ({r.status_code})")
        all_ok = False

    # --- 6c: ttl ---
    print("  6c) ttl: 到期后自动清理...")
    ttl_file = upload_dir / "retention_ttl.bin"
    ttl_file.write_bytes(os.urandom(64 * 1024))

    with EasyTransferClient(server_url, token=token, chunk_size=64 * 1024) as client:
        up = client.create_uploader(str(ttl_file), retention="ttl", retention_ttl=3)
        up.upload()
        ttl_id = up.url.split("/")[-1]

    r = httpx.get(f"{server_url}/api/files/{ttl_id}", headers=headers, timeout=10)
    if r.status_code == 200:
        meta = r.json().get("metadata", {})
        print(f"    过期时间: {meta.get('retention_expires_at', 'N/A')}")
    else:
        print("    ✗ TTL 文件创建异常")
        all_ok = False

    print("    等待 TTL 过期 (5秒)...")
    time.sleep(5)

    # 触发清理
    httpx.post(f"{server_url}/api/files/cleanup", headers=headers, timeout=10)
    time.sleep(0.5)

    r = httpx.get(f"{server_url}/api/files/{ttl_id}", headers=headers, timeout=10)
    if r.status_code == 404:
        print("    ✓ ttl: 过期后文件已清理")
    else:
        print(f"    ✗ ttl: 文件未清理 ({r.status_code})")
        all_ok = False

    if all_ok:
        print("  ✓ 所有缓存策略测试通过")
    else:
        print("  ✗ 缓存策略测试部分失败")

    return all_ok


def run_tests():
    """运行所有测试。"""
    print("=" * 60)
    print("EasyTransfer 传输测试")
    print("=" * 60)
    print("配置:")
    print(f"  - 测试文件数量: {TestConfig.NUM_FILES}")
    print(f"  - 每个文件大小: {TestConfig.FILE_SIZE_MB} MB")
    print(f"  - 服务端地址: http://{TestConfig.SERVER_HOST}:{TestConfig.SERVER_PORT}")
    print(f"  - 状态后端: {TestConfig.STATE_BACKEND}")

    # 检查后端可用性
    print(f"\n[0] 检查后端: {TestConfig.STATE_BACKEND}...")
    if not check_backend_available(TestConfig.STATE_BACKEND):
        if TestConfig.STATE_BACKEND == "redis":
            print("  ✗ Redis 连接失败")
            print("  请先启动 Redis: redis-server")
            print("  或使用其他后端: --backend memory 或 --backend file")
        else:
            print(f"  ✗ 后端 {TestConfig.STATE_BACKEND} 不可用")
        return False
    print("  ✓ 后端可用")

    # 创建临时目录
    test_dir = Path(tempfile.mkdtemp(prefix="etransfer_test_"))
    upload_dir = test_dir / "upload"
    download_dir = test_dir / "download"
    storage_dir = test_dir / "storage"

    upload_dir.mkdir(parents=True)
    download_dir.mkdir(parents=True)
    storage_dir.mkdir(parents=True)

    print(f"  - 测试目录: {test_dir}")

    server_process = None

    try:
        # 启动服务端
        server_process = start_server(test_dir)
        time.sleep(2)  # 给服务端一些初始化时间

        # 生成测试文件
        print(f"\n[2] 生成 {TestConfig.NUM_FILES} 个测试文件...")
        test_files = []  # [(path, hash), ...]
        original_hashes = {}  # {filename: hash}

        for i in range(TestConfig.NUM_FILES):
            file_path = upload_dir / f"test_file_{i + 1}.bin"
            file_hash = generate_test_file(file_path, TestConfig.FILE_SIZE_MB)
            test_files.append((file_path, file_hash))
            original_hashes[file_path.name] = file_hash

        server_url = f"http://{TestConfig.SERVER_HOST}:{TestConfig.SERVER_PORT}"

        # 并发上传测试
        file_ids = run_concurrent_upload(test_files, server_url, TestConfig.API_TOKEN)

        if not file_ids:
            print("\n✗ 上传测试失败，跳过下载测试")
            return False

        # 并发下载测试
        download_results = run_concurrent_download(
            file_ids,
            original_hashes,
            download_dir,
            server_url,
            TestConfig.API_TOKEN,
        )

        # 边上传边下载测试（使用一个新文件）
        stream_test_file = upload_dir / "stream_test.bin"
        generate_test_file(stream_test_file, TestConfig.FILE_SIZE_MB)
        run_upload_while_download(
            stream_test_file,
            download_dir,
            server_url,
            TestConfig.API_TOKEN,
        )

        # 缓存策略测试
        retention_passed = run_retention_policies(
            upload_dir,
            download_dir,
            server_url,
            TestConfig.API_TOKEN,
        )

        # 总结
        print("\n" + "=" * 60)
        print("测试结果汇总")
        print("=" * 60)

        upload_success = len(file_ids)
        download_success = sum(1 for v in download_results.values() if v)

        print(f"  上传: {upload_success}/{TestConfig.NUM_FILES} 成功")
        print(f"  下载: {download_success}/{len(file_ids)} 成功且哈希验证通过")
        print(f"  缓存策略: {'通过' if retention_passed else '失败'}")

        all_passed = upload_success == TestConfig.NUM_FILES and download_success == len(file_ids) and retention_passed

        if all_passed:
            print("\n✓ 所有测试通过!")
        else:
            print("\n✗ 部分测试失败")

        return all_passed

    except Exception as e:
        print(f"\n测试异常: {e}")
        import traceback

        traceback.print_exc()
        return False

    finally:
        # 清理
        print("\n[清理] 停止服务端...")
        if server_process:
            server_process.terminate()
            server_process.wait(timeout=5)

        # 询问是否删除测试文件
        print(f"[清理] 测试目录: {test_dir}")
        try:
            response = input("是否删除测试文件? [y/N]: ").strip().lower()
            if response == "y":
                shutil.rmtree(test_dir)
                print("  已删除")
            else:
                print(f"  保留在: {test_dir}")
        except (KeyboardInterrupt, EOFError):
            print(f"\n  保留在: {test_dir}")


def quick_test():
    """快速测试（使用较小的文件）。"""
    TestConfig.NUM_FILES = 2
    TestConfig.FILE_SIZE_MB = 10  # 10MB
    run_tests()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="EasyTransfer 传输测试")
    parser.add_argument(
        "--quick",
        "-q",
        action="store_true",
        help="快速测试模式 (小文件)",
    )
    parser.add_argument(
        "--files",
        "-n",
        type=int,
        default=3,
        help="测试文件数量",
    )
    parser.add_argument(
        "--size",
        "-s",
        type=int,
        default=100,
        help="每个文件大小 (MB)",
    )
    parser.add_argument(
        "--large",
        "-l",
        action="store_true",
        help="大文件测试模式 (1GB文件)",
    )
    parser.add_argument(
        "--backend",
        "-b",
        type=str,
        choices=["memory", "file", "redis"],
        default="file",
        help="状态后端类型 (默认: file)",
    )

    args = parser.parse_args()

    # 设置后端
    TestConfig.STATE_BACKEND = args.backend

    if args.quick:
        TestConfig.NUM_FILES = 2
        TestConfig.FILE_SIZE_MB = 10
    elif args.large:
        TestConfig.NUM_FILES = 3
        TestConfig.FILE_SIZE_MB = 1024  # 1GB
    else:
        TestConfig.NUM_FILES = args.files
        TestConfig.FILE_SIZE_MB = args.size

    success = run_tests()
    sys.exit(0 if success else 1)
