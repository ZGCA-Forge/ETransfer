#!/usr/bin/env python3
"""
EasyTransfer 客户端演示脚本

演示上传、下载、缓存策略功能。
"""

import hashlib
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from etransfer.common.constants import DEFAULT_SERVER_PORT  # noqa: E402


def generate_test_file(path: Path, size_mb: int = 10) -> str:
    """生成测试文件。"""
    print(f"生成测试文件: {path} ({size_mb} MB)")

    md5 = hashlib.md5()
    chunk_size = 1024 * 1024  # 1MB

    with open(path, "wb") as f:
        for i in range(size_mb):
            data = os.urandom(chunk_size)
            f.write(data)
            md5.update(data)
            print(f"  进度: {i + 1}/{size_mb} MB", end="\r")

    print()
    return md5.hexdigest()


def demo_upload(server_url: str, token: str, file_path: Path, retention: str = None, retention_ttl: int = None):
    """演示上传。"""
    from etransfer.client.tus_client import EasyTransferClient

    print("\n=== 上传演示 ===")
    print(f"文件: {file_path}")
    print(f"服务器: {server_url}")
    if retention:
        print(f"缓存策略: {retention}" + (f" (TTL={retention_ttl}s)" if retention_ttl else ""))

    def progress_callback(uploaded, total):
        progress = uploaded / total * 100
        print(f"  上传进度: {progress:.1f}% ({uploaded}/{total} bytes)", end="\r")

    with EasyTransferClient(server_url, token=token) as client:
        uploader = client.create_uploader(
            str(file_path),
            progress_callback=progress_callback,
            retention=retention,
            retention_ttl=retention_ttl,
        )
        location = uploader.upload()
        file_id = location.split("/")[-1] if location else None

    print(f"\n✓ 上传完成! File ID: {file_id}")
    if retention == "download_once":
        print("  注意: 此文件为「阅后即焚」，下载一次后自动删除")
    elif retention == "ttl" and retention_ttl:
        print(f"  注意: 此文件将在 {retention_ttl} 秒后过期")
    return file_id


def demo_download(server_url: str, token: str, file_id: str, output_dir: Path):
    """演示下载。"""
    from etransfer.client.downloader import ChunkDownloader

    print("\n=== 下载演示 ===")
    print(f"File ID: {file_id}")
    print(f"输出目录: {output_dir}")

    downloader = ChunkDownloader(server_url, token=token)

    # 获取文件信息
    info = downloader.get_file_info(file_id)
    print(f"文件名: {info.filename}")
    print(f"大小: {info.size / 1024 / 1024:.1f} MB")

    output_path = output_dir / info.filename

    def progress_callback(downloaded, total):
        progress = downloaded / total * 100
        print(f"  下载进度: {progress:.1f}% ({downloaded}/{total} bytes)", end="\r")

    success = downloader.download_file(
        file_id,
        output_path,
        progress_callback=progress_callback,
    )

    if success:
        print(f"\n✓ 下载完成! 保存到: {output_path}")
    else:
        print("\n✗ 下载失败")

    return success, output_path


def demo_list_files(server_url: str, token: str):
    """演示列出文件。"""
    from etransfer.client.tus_client import EasyTransferClient

    print("\n=== 文件列表 ===")

    with EasyTransferClient(server_url, token=token) as client:
        files = client.list_files()

    if not files:
        print("没有文件")
        return

    print(f"共 {len(files)} 个文件:")
    for f in files:
        progress = f.progress
        status = "完成" if f.status.value == "complete" else f"上传中 ({progress:.1f}%)"
        retention = f.metadata.get("retention", "permanent") if f.metadata else "permanent"
        retention_label = {
            "permanent": "",
            "download_once": " [阅后即焚]",
            "ttl": " [定时过期]",
        }.get(retention, "")
        print(f"  - {f.file_id[:8]}... | {f.filename} | " f"{f.size / 1024 / 1024:.1f} MB | {status}{retention_label}")


def demo_server_info(server_url: str, token: str):
    """演示获取服务器信息。"""
    from etransfer.client.tus_client import EasyTransferClient

    print("\n=== 服务器信息 ===")

    with EasyTransferClient(server_url, token=token) as client:
        info = client.get_server_info()

    print(f"版本: {info.version}")
    print(f"TUS 版本: {info.tus_version}")
    print(f"分片大小: {info.chunk_size / 1024 / 1024:.1f} MB")
    print(f"文件总数: {info.total_files}")
    print(f"存储使用: {info.total_size / 1024 / 1024:.1f} MB")

    if info.interfaces:
        print("网络接口:")
        for iface in info.interfaces:
            print(f"  - {iface.name}: {iface.ip_address}")

    # 存储状态
    try:
        storage = client.get_storage_status()
        print("\n存储状态:")
        print(f"  已用: {storage.get('used_formatted', 'N/A')}")
        print(f"  限额: {storage.get('max_formatted', 'unlimited')}")
        print(f"  可用: {storage.get('available_formatted', 'unlimited')}")
    except Exception:
        pass


def demo_retention(server_url: str, token: str, tmpdir: Path):
    """演示缓存策略。"""
    print(f"\n{'=' * 50}")
    print("=== 缓存策略演示 ===")
    print(f"{'=' * 50}")

    # 1. 阅后即焚
    print("\n--- 1) 阅后即焚 (download_once) ---")
    file_path = tmpdir / "secret.bin"
    generate_test_file(file_path, 1)
    file_id = demo_upload(server_url, token, file_path, retention="download_once")
    if file_id:
        print("  第一次下载...")
        success, _ = demo_download(server_url, token, file_id, tmpdir)
        if success:
            import time

            time.sleep(1)
            print("  尝试第二次下载 (应该失败)...")
            try:
                demo_download(server_url, token, file_id, tmpdir)
            except Exception as e:
                print(f"  ✓ 预期失败: {e}")

    # 2. TTL 定时过期
    print("\n--- 2) TTL 定时过期 (10秒) ---")
    file_path = tmpdir / "temp.bin"
    generate_test_file(file_path, 1)
    file_id = demo_upload(server_url, token, file_path, retention="ttl", retention_ttl=10)
    if file_id:
        print("  立即下载 (TTL 未到期)...")
        demo_download(server_url, token, file_id, tmpdir)
        print("  文件将在 10 秒后过期...")

    # 3. 永久
    print("\n--- 3) 永久保存 (permanent) ---")
    file_path = tmpdir / "permanent.bin"
    generate_test_file(file_path, 1)
    demo_upload(server_url, token, file_path, retention="permanent")
    print("  文件将永久保存，直到手动删除")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="EasyTransfer 客户端演示")
    parser.add_argument(
        "--server",
        "-s",
        default=f"http://localhost:{DEFAULT_SERVER_PORT}",
        help="服务器地址",
    )
    parser.add_argument(
        "--token",
        "-t",
        default="test-token-12345",
        help="API Token",
    )
    parser.add_argument(
        "--action",
        "-a",
        choices=["upload", "download", "list", "info", "retention", "full"],
        default="full",
        help="执行的操作",
    )
    parser.add_argument(
        "--file",
        "-f",
        help="要上传的文件路径",
    )
    parser.add_argument(
        "--file-id",
        "-i",
        help="要下载的文件 ID",
    )
    parser.add_argument(
        "--size",
        type=int,
        default=10,
        help="生成测试文件的大小 (MB)",
    )
    parser.add_argument(
        "--retention",
        "-r",
        choices=["permanent", "download_once", "ttl"],
        default=None,
        help="上传时指定缓存策略",
    )
    parser.add_argument(
        "--retention-ttl",
        type=int,
        default=None,
        help="TTL 秒数 (仅 --retention ttl 时生效)",
    )

    args = parser.parse_args()

    print("=" * 50)
    print("EasyTransfer 客户端演示")
    print("=" * 50)
    print(f"服务器: {args.server}")
    print(f"Token: {args.token[:10]}...")

    if args.action == "info":
        demo_server_info(args.server, args.token)

    elif args.action == "list":
        demo_list_files(args.server, args.token)

    elif args.action == "upload":
        if args.file:
            file_path = Path(args.file)
        else:
            # 生成测试文件
            file_path = Path(tempfile.gettempdir()) / "etransfer_test.bin"
            generate_test_file(file_path, args.size)

        demo_upload(args.server, args.token, file_path, retention=args.retention, retention_ttl=args.retention_ttl)

    elif args.action == "download":
        if not args.file_id:
            print("请指定 --file-id")
            return

        output_dir = Path(tempfile.gettempdir())
        demo_download(args.server, args.token, args.file_id, output_dir)

    elif args.action == "retention":
        with tempfile.TemporaryDirectory() as tmpdir:
            demo_retention(args.server, args.token, Path(tmpdir))

    elif args.action == "full":
        # 完整演示
        demo_server_info(args.server, args.token)

        # 生成测试文件
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            file_path = tmpdir / "test_upload.bin"
            original_hash = generate_test_file(file_path, args.size)

            # 上传
            file_id = demo_upload(args.server, args.token, file_path)

            if file_id:
                # 列出文件
                demo_list_files(args.server, args.token)

                # 下载
                success, output_path = demo_download(args.server, args.token, file_id, tmpdir)

                if success:
                    # 验证哈希
                    downloaded_hash = hashlib.md5(output_path.read_bytes()).hexdigest()
                    if downloaded_hash == original_hash:
                        print("\n✓ 哈希验证通过!")
                    else:
                        print("\n✗ 哈希不匹配!")
                        print(f"  原始: {original_hash}")
                        print(f"  下载: {downloaded_hash}")

            # 缓存策略演示
            demo_retention(args.server, args.token, tmpdir)

    print("\n演示完成!")


if __name__ == "__main__":
    main()
