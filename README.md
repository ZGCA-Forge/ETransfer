# ETransfer

面向大文件场景的高性能传输工具，基于 [TUS 协议](https://tus.io)。支持断点续传、切片上传下载、流式中继（上传未完成即可下载）、阅后即焚，适用于 AI 训练数据集、模型权重等大文件的可靠分发。

## 特性

- 基于 TUS 协议的断点续传，网络中断自动恢复
- 切片上传/下载，支持部分下载（流式中继）
- 阅后即焚 / TTL 过期 / 永久保存三种文件策略
- 存储配额管理，超限自动等待并恢复
- 多 IP 负载均衡，自动选择最优节点
- OIDC 用户系统，基于角色/群组的配额控制
- CLI + GUI + Python API

## 安装

```bash
pip install etransfer
```

## 快速开始

```bash
# 配置服务器地址
etransfer setup your-server

# 登录（如果服务器启用了 OIDC）
etransfer login

# 上传
etransfer upload ./model-weights.bin

# 阅后即焚上传
etransfer upload ./dataset.tar.gz --retention download_once

# 下载
etransfer download <file_id> -o ./output/

# 文件列表
etransfer list

# 服务器信息
etransfer info
```

## 进阶使用（Python API）

```python
from etransfer.client.tus_client import EasyTransferClient
from etransfer.client.downloader import ChunkDownloader

# 上传
with EasyTransferClient("http://your-server:8765", token="your-token") as client:
    uploader = client.create_uploader("./large-model.bin", retention="download_once")
    uploader.upload(wait_on_quota=True)
    file_id = uploader.url.split("/")[-1]

# 下载
downloader = ChunkDownloader("http://your-server:8765", token="your-token")
downloader.download_file(file_id, "./output/large-model.bin")
```

## 文档

| 文档                             | 说明                          |
| -------------------------------- | ----------------------------- |
| [私有化部署](docs/deployment.md) | 服务端安装、配置、Docker 部署 |
| [通信设计](docs/api-design.md)   | API 端点与协议说明            |

## License

[Apache-2.0](LICENSE)
