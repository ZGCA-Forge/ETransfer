# 私有化部署

## 安装

```bash
# 基础安装
pip install etransfer

# Redis 后端（多 Worker 生产环境）
pip install "etransfer[redis]"

# MySQL 用户数据库
pip install "etransfer[mysql]"

# 开发依赖
pip install "etransfer[dev]"
```

## 启动服务端

配置文件自动发现顺序：`./config.yaml` → `./config/config.yaml` → `~/.etransfer/server.yaml`，也可通过 `$ETRANSFER_CONFIG` 或 `--config` 指定。

```bash
# CLI 启动（推荐）
et server start

# 指定配置文件
et server start --config /path/to/config.yaml

# uvicorn 直接启动
uvicorn etransfer.server.main:app --host 0.0.0.0 --port 8765

# 环境变量指定
ETRANSFER_CONFIG=/path/to/config.yaml uvicorn etransfer.server.main:app
```

## 配置文件

项目提供了 `config.example.yaml` 模板：

```bash
cp config.example.yaml config/config.yaml
vim config/config.yaml
```

`config/` 目录已 git-ignored，密钥不会被提交。

### 完整配置项

```yaml
server:
  host: 0.0.0.0
  port: 8765
  workers: 1
  # config_watch: true        # 自动监听配置变更
  # config_watch_interval: 30

storage:
  path: ./storage
  # max_storage_size: 10737418240  # 10 GB（null = 不限）

state:
  backend: file # memory / file / redis
  redis_url: redis://localhost:6379/0

auth:
  enabled: true
  tokens:
    - "your-api-token-here"

retention:
  default: permanent # permanent / download_once / ttl
  # default_ttl: 86400
  # token_policies:
  #   guest-token:
  #     default_retention: download_once

network:
  interfaces: [] # 空 = 自动检测
  prefer_ipv4: true
  advertised_endpoints: [] # 显式指定客户端可见 IP

user_system:
  enabled: false
  oidc:
    issuer_url: "https://your-oidc-provider.example.com"
    client_id: "your-client-id"
    client_secret: "" # 建议用环境变量 ETRANSFER_OIDC_CLIENT_SECRET
    callback_url: "" # 公网 URL 前缀，如 https://transfer.example.com
    scope: "openid profile email"
  database:
    backend: sqlite # sqlite / mysql
    path: "" # SQLite 路径（默认 <storage>/users.db）

cors:
  origins: ["*"]
```

### 环境变量

所有配置项均可通过 `ETRANSFER_` 前缀的环境变量覆盖：

| 变量                             | 说明               | 默认值                     |
| -------------------------------- | ------------------ | -------------------------- |
| `ETRANSFER_HOST`                 | 绑定地址           | `0.0.0.0`                  |
| `ETRANSFER_PORT`                 | 端口               | `8765`                     |
| `ETRANSFER_STORAGE_PATH`         | 文件存储路径       | `./storage`                |
| `ETRANSFER_STATE_BACKEND`        | 状态后端           | `file`                     |
| `ETRANSFER_REDIS_URL`            | Redis 地址         | `redis://localhost:6379/0` |
| `ETRANSFER_AUTH_ENABLED`         | 启用鉴权           | `true`                     |
| `ETRANSFER_AUTH_TOKENS`          | Token 列表（JSON） | `[]`                       |
| `ETRANSFER_MAX_UPLOAD_SIZE`      | 单文件大小限制     | 不限                       |
| `ETRANSFER_MAX_STORAGE_SIZE`     | 总存储配额         | 不限                       |
| `ETRANSFER_CHUNK_SIZE`           | 切片大小           | `4194304` (4MB)            |
| `ETRANSFER_ADVERTISED_ENDPOINTS` | 广播 IP 列表       | 自动检测                   |
| `ETRANSFER_DEFAULT_RETENTION`    | 默认文件策略       | `permanent`                |
| `ETRANSFER_CORS_ORIGINS`         | CORS 来源          | `["*"]`                    |

## 配置热重载

部分配置项支持不重启即时生效：`role_quotas`、`auth_tokens`、`max_storage_size`、`advertised_endpoints`、`retention` 相关。

```bash
# CLI
et server reload

# API
curl -X POST http://localhost:8765/api/admin/reload-config \
  -H "X-API-Token: your-token"
```

也可在 `config.yaml` 中设置 `server.config_watch: true` 开启自动监听。

> 不可热重载：`host`/`port`/`workers`、OIDC 配置、数据库后端。

## 文件策略

| 策略            | 说明                       | 场景                 |
| --------------- | -------------------------- | -------------------- |
| `permanent`     | 永久保存                   | 默认                 |
| `download_once` | 下载后自动删除（阅后即焚） | 一次性分享、敏感文件 |
| `ttl`           | 按时间自动过期             | 临时文件、限时分享   |

优先级：客户端显式指定 > Token 级策略 > 全局默认

## 存储配额

```bash
export ETRANSFER_MAX_STORAGE_SIZE=10GB
```

超限时服务端返回 `HTTP 507`，客户端自动轮询等待空间释放后恢复上传。

## OIDC 用户系统

支持任意标准 OIDC 提供者（Casdoor、Keycloak、Auth0 等）。

配额优先级：群组配额（最大值）> 角色配额 > 全局默认。

## 技术栈

- 服务端：FastAPI + Uvicorn + aiofiles + psutil
- 客户端：tus-py-client + httpx + Typer + Rich + Tkinter
- 协议：TUS 1.0.0（creation, creation-with-upload, termination, checksum, expiration）
- 认证：OIDC + API Token
- 数据库：SQLModel + SQLAlchemy async（SQLite / MySQL / PostgreSQL）
- 状态管理：Memory / File / Redis
