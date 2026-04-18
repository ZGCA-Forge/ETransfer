# ETransfer

面向大文件场景的高性能传输工具，基于 [TUS 协议](https://tus.io)。支持断点续传、切片上传下载、流式中继（上传未完成即可下载）、阅后即焚，适用于 AI 训练数据集、模型权重等大文件的可靠分发。

## 特性

- 基于 TUS 协议的断点续传，网络中断自动恢复
- 切片上传/下载，支持部分下载（流式中继）
- 阅后即焚 / TTL 过期 / 永久保存三种文件策略，且永久保存可由服务端策略关闭
- 离线下载（remote-download）：服务端代下载 HuggingFace / 直链 / GDrive 等
- 插件化的 Source（下载源）+ Sink（推送目标，支持 S3 / TOS / 自建对象存储）
- 存储配额管理，超限自动等待并恢复
- 多 IP 负载均衡，自动选择最优节点
- OIDC 用户系统，基于角色/群组的配额控制
- CLI + GUI + Python API

## 安装

```bash
pip install etransfer
```

如需部署服务端（含 SQLAlchemy async 依赖）：

```bash
pip install "etransfer[server]" --only-binary greenlet
```

> `--only-binary greenlet` 确保只安装预编译 wheel，避免 Windows 等平台上的 C 编译问题。

## 快速开始

> CLI 入口同时提供 `etransfer` 与短别名 `et`。下文统一使用 `et`。

```bash
# 配置服务器地址（也可通过 ETRANSFER_SERVER 环境变量）
et setup your-server

# 登录（如果服务器启用了 OIDC）
et login

# 上传：默认 download_once（阅后即焚）
et upload ./model-weights.bin

# 上传保留：永久保存（如果服务端开启了该策略）
et upload ./dataset.tar.gz --retention permanent

# TTL 过期
et upload ./snapshot.tar --retention ttl --retention-ttl 3600

# 多线程加速 + 自定义切片
et upload ./large.iso -j 8 -c $((16*1024*1024))

# 上传到本地，同时流式推送到对象存储 sink
et upload ./model.bin --sink tos

# 下载
et download <file_id> -p ./output/

# 文件 / 目录管理
et list
et folders list
et folders download <folder_id> -p ./out/   # 整目录打包 zip
et folders delete <folder_id>

# 离线下载（服务端代下载）
et remote-download https://example.com/file.zip
et remote-download --urls-file urls.txt --sink tos
cat urls.txt | et remote-download -f -          # 从 stdin 批量提交

# 任务管理
et tasks list
et tasks cancel <task_prefix>
et tasks retry  <task_prefix> --wait
et tasks watch  <task_prefix>                   # 实时查看进度
et tasks wait   <task_prefix>

# 插件 / Sink / Source 浏览
et plugins
et sinks
et sources

# 服务器信息 / 热重载（admin token）
et info
et server reload
```

完整命令面板见 `et --help`，按功能分组：Setup / Files / Folders / Offline / Plugins / Server。

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

## 服务端策略：关闭永久保存

如果你的服务端只想把磁盘当成中转，可以在 `config.yaml` 中关闭 `permanent` 策略：

```yaml
retention:
  default: download_once
  allow_permanent: false   # 普通用户被拒绝；API token / admin 可继续使用
```

- `/api/info` 会自动从 `retention_policies` 中剔除 `permanent`
- 前端 Upload 页 / Guide 页会隐藏该选项并提示「已被服务器策略禁用」
- 普通会话用户提交 `retention=permanent` 会拿到 HTTP 403
- 静态 API token 调用方（包括 CLI `--token`）按管理员处理，仍可强制使用

该字段属于热重载字段，`et server reload` 即时生效。

## 插件架构

ETransfer 自带 *Source*（离线下载源）+ *Sink*（推送目标）两类插件：

- 内置 Source：`direct`、`huggingface`、`gdrive`
- 内置 Sink：`local`、`tos`（火山引擎 TOS）
- 通过 `entry_points` 注册第三方插件，开发请参考 `etransfer/plugins/base_*.py`

## 文档

| 文档                             | 说明                          |
| -------------------------------- | ----------------------------- |
| [私有化部署](docs/deployment.md) | 服务端安装、配置、Docker 部署 |
| [通信设计](docs/api-design.md)   | API 端点与协议说明            |

## License

[Apache-2.0](LICENSE)
