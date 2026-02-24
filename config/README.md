# EasyTransfer 部署配置

本文件夹存放实际部署所用的配置文件，已加入 `.gitignore`，不会被提交到仓库。

> **仓库中的模板**: 项目根目录下的 `config.example.yaml` 是配置模板，仅包含示例值，
> 可安全提交。本文件夹中的 `config.yaml` 是你的真实部署配置，包含密钥等敏感信息。

---

## 快速开始

```bash
# 1. 从模板复制一份配置
cp config.example.yaml config/config.yaml

# 2. 编辑配置（填入真实密钥等）
vim config/config.yaml

# 3. 启动服务端（自动发现 config/config.yaml，无需 --config）
et server start
```

---

## 配置文件自动发现

服务端启动时会按以下顺序自动搜索配置文件（先找到先用）：

| 优先级 | 路径                       | 说明               |
| ------ | -------------------------- | ------------------ |
| 1      | `$ETRANSFER_CONFIG`        | 环境变量，最高优先 |
| 2      | `./config.yaml`            | 当前工作目录       |
| 3      | `./config/config.yaml`     | config 子文件夹    |
| 4      | `~/.etransfer/server.yaml` | 用户 home 目录     |

这意味着：

- `et server start` — 自动发现，无需 `--config`
- `uvicorn etransfer.server.main:app` — 同样自动发现
- `ETRANSFER_CONFIG=/path/to/config.yaml uvicorn ...` — 显式指定
- `et server start --config /path/to/config.yaml` — 手动覆盖

启动时会打印加载了哪个配置文件：

```
[Config] Loaded config from /home/user/project/config/config.yaml
```

---

## 配置流程

### 1. 基础服务配置

编辑 `config/config.yaml` 中的 `server` 段：

```yaml
server:
  host: 0.0.0.0
  port: 8765
  workers: 1
  # config_watch: true        # 开启后自动监听配置变更
  # config_watch_interval: 30 # 检查间隔（秒）
```

### 2. 存储配置

```yaml
storage:
  path: ./storage
  # max_storage_size: 10737418240  # 10 GB，到达限额后新上传返回 507
```

### 3. Token 认证（简单场景）

不启用用户系统时，可以使用静态 token 认证：

```yaml
auth:
  enabled: true
  tokens:
    - "your-secure-token-here"
```

### 4. OIDC 用户系统配置（推荐）

支持接入任何标准 OIDC 提供商（Casdoor、Keycloak、Auth0 等）。

#### 4.1 在 OIDC 提供商中创建应用

以 Casdoor 为例：

1. 登录 Casdoor 管理后台
2. 创建一个新应用（Application）
3. 记录 `Client ID` 和 `Client Secret`
4. 设置回调地址（Redirect URL）为你的服务地址 + `/api/users/callback`

   例如：`http://localhost:8765/api/users/callback`

5. 配置 Groups：在 Casdoor 中创建所需的用户组（如 `vip`、`team-a` 等），
   用于权限和配额管理

#### 4.2 填写 OIDC 配置

```yaml
user_system:
  enabled: true

  oidc:
    issuer_url: "https://your-oidc-provider.example.com"
    client_id: "your-client-id"
    client_secret: "your-client-secret" # 敏感！建议用环境变量
    callback_url: "" # 留空 = 自动从请求推导（推荐）
    scope: "openid profile email"
```

> **安全提示**: `client_secret` 建议通过环境变量传入，而非写在配置文件中：
>
> ```bash
> export ETRANSFER_OIDC_CLIENT_SECRET="your-client-secret"
> ```

> **callback_url 说明**:
>
> - **留空（推荐）**：服务端自动从客户端请求的 Host 头推导回调地址。
>   例如客户端通过 `http://192.168.1.100:8765` 访问，回调自动变为
>   `http://192.168.1.100:8765/api/users/callback`。SSH 远程场景下自动生效。
> - **显式设置**：仅在反向代理/域名与实际访问地址不同时需要，
>   例如 `https://transfer.example.com/api/users/callback`。
> - **在 OIDC 提供商中注册**：无论哪种方式，都需要在 OIDC 提供商
>   （如 Casdoor）的应用设置中，将对应的回调地址添加为 Redirect URI。

#### 4.3 配置数据库

默认使用 SQLite（零配置），也支持 MySQL：

```yaml
database:
  backend: sqlite # 默认，数据保存在 storage/users.db
  # backend: mysql      # 生产环境推荐 MySQL

  # MySQL 配置（backend=mysql 时生效）
  # mysql:
  #   host: 127.0.0.1
  #   port: 3306
  #   user: etransfer
  #   password: ""      # 建议用环境变量 ETRANSFER_MYSQL_PASSWORD
  #   database: etransfer
```

#### 4.4 配置角色配额

```yaml
role_quotas:
  admin:
    max_storage_size: null # 无限制
    max_upload_size: null
    upload_speed_limit: null
    download_speed_limit: null
  user:
    max_storage_size: 10737418240 # 10 GB
    max_upload_size: 5368709120 # 5 GB
  guest:
    max_storage_size: 1073741824 # 1 GB
    max_upload_size: 536870912 # 512 MB
    upload_speed_limit: 10485760 # 10 MB/s
    download_speed_limit: 10485760 # 10 MB/s
```

用户最终配额 = max(角色配额, 所属各组配额)，取最宽松值。

### 5. 网络 / 多 IP 配置

```yaml
network:
  interfaces: [] # 空 = 自动检测
  prefer_ipv4: true
  advertised_endpoints: # 显式指定客户端可用的 IP
    - "192.168.1.100"
    - "10.0.0.100"
```

### 6. 文件保留策略

```yaml
retention:
  default: permanent # permanent / download_once / ttl
  # default_ttl: 86400      # TTL 策略的默认过期时间（秒）
  # token_policies:
  #   guest-token:
  #     default_retention: download_once
  #   temp-token:
  #     default_retention: ttl
  #     default_ttl: 3600
```

---

## 客户端使用流程

### 第一步：配置服务器地址

所有命令共享同一个服务器地址，只需配置一次：

```bash
# 配置服务器（域名+端口 或 IP+端口）
etransfer setup 192.168.1.100:8765
etransfer setup myserver.example.com:8765

# 查看当前配置
etransfer status
```

`setup` 命令会自动探测连通性，并检查服务器是否需要登录。
配置保存在 `~/.etransfer/client.json`。

### 第二步：登录（如果服务器要求）

如果服务器启用了 OIDC 用户系统，需要登录：

```bash
# 发起登录 — CLI 会输出一个 URL，手动在浏览器中打开
etransfer login

# CLI 输出类似：
# ┌─ Login ──────────────────────────────────────────────┐
# │ Open this URL in your browser to login:              │
# │ https://ca.auth.example.com/login/oauth/authorize?...│
# └──────────────────────────────────────────────────────┘
# 在浏览器中完成登录后，CLI 自动获取 token 并缓存
```

> CLI 不会自动打开浏览器，需要你手动复制 URL 到浏览器。
> GUI 客户端会自动打开浏览器。
> 所有命令支持简写 `et`（如 `et setup`、`et login`、`et upload`）。

### SSH 远程登录场景

通过 SSH 连接到远程服务器时，OIDC 登录同样可以正常工作：

```bash
# 在远程服务器上
ssh user@192.168.1.100

# 配置服务器地址（用服务器自身的 IP/域名，不是 localhost）
et setup 192.168.1.100:8765

# 发起登录
et login
# CLI 输出的 URL 中 callback 会自动使用 192.168.1.100:8765
# 而不是 localhost:8765

# 在你的本地浏览器中打开该 URL，完成 OIDC 认证
# 认证完成后浏览器会跳转到 http://192.168.1.100:8765/api/users/callback
# 远程服务器上的 CLI 自动获取 token
```

**原理**：服务端根据请求的 Host 头自动推导 callback 地址。
当 CLI 通过 `192.168.1.100:8765` 访问 `login/start`，
服务端生成的 authorize URL 中 redirect_uri 即为
`http://192.168.1.100:8765/api/users/callback`，
浏览器认证后可以正确回调到服务端。

**前提**：需要在 OIDC 提供商（如 Casdoor）中注册
`http://192.168.1.100:8765/api/users/callback` 为合法的 Redirect URI。

### 第三步：正常使用

登录成功后，所有命令自动使用缓存的 token，无需额外参数：

```bash
# 查看用户信息和配额
etransfer whoami

# 上传文件
etransfer upload ./myfile.zip

# 下载文件
etransfer download <file-id>

# 查看文件列表
etransfer list

# 查看服务器信息
etransfer info

# 登出
etransfer logout
```

---

## 环境变量覆盖

所有配置项均可通过环境变量覆盖，前缀为 `ETRANSFER_`：

| 环境变量                         | 说明                               |
| -------------------------------- | ---------------------------------- |
| `ETRANSFER_OIDC_ISSUER_URL`      | OIDC 提供商地址                    |
| `ETRANSFER_OIDC_CLIENT_ID`       | OIDC Client ID                     |
| `ETRANSFER_OIDC_CLIENT_SECRET`   | OIDC Client Secret                 |
| `ETRANSFER_OIDC_CALLBACK_URL`    | OIDC 回调地址                      |
| `ETRANSFER_OIDC_SCOPE`           | OIDC Scope                         |
| `ETRANSFER_USER_SYSTEM_ENABLED`  | 是否启用用户系统                   |
| `ETRANSFER_USER_DB_BACKEND`      | 数据库后端 (sqlite/mysql)          |
| `ETRANSFER_MYSQL_PASSWORD`       | MySQL 密码                         |
| `ETRANSFER_MAX_STORAGE_SIZE`     | 最大存储限额 (支持 100MB, 1GB 等)  |
| `ETRANSFER_ADVERTISED_ENDPOINTS` | 广播 IP 列表 (JSON 数组或逗号分隔) |

| `ETRANSFER_CONFIG` | 配置文件路径（优先于自动发现） |
| `ETRANSFER_CONFIG_WATCH` | 是否监听配置变更 (true/false) |

---

## 配置热重载

部分配置项支持**不重启服务**即时生效：

| 可热重载                                      | 需要重启                        |
| --------------------------------------------- | ------------------------------- |
| `role_quotas` 角色配额                        | `host`, `port`, `workers`       |
| `auth_tokens` API Token                       | `storage_path`, `state_backend` |
| `token_retention_policies`                    | `oidc_*` OIDC 配置              |
| `default_retention` / `default_retention_ttl` | `user_db_backend`, `mysql_*`    |
| `advertised_endpoints`                        |                                 |
| `max_storage_size` 存储限额                   |                                 |

> **注意**：
>
> - 群组配额 (Group Quota) 存储在数据库中，通过 API `PUT /api/groups/{id}/quota` 管理，天然即时生效。
> - `advertised_endpoints` 是客户端可见的 IP 列表（可热重载），但 **监听 IP/端口** 由 uvicorn 在启动时绑定，无法在线变更。如需监听新 IP，必须重启服务。

### 手动触发重载

**推荐：使用 CLI 命令**

```bash
# 使用已配置的服务器地址和 token（需先 et setup + et login）
et server reload

# 或者显式指定
et server reload 192.168.1.100:8765 --token your-api-token
```

**或直接调用 API**

```bash
curl -X POST http://localhost:8765/api/admin/reload-config \
  -H "X-API-Token: your-token"

# 返回变更内容 + 可热重载/需重启的字段列表
# {"reloaded": true, "changes": {...}, "hot_reloadable": [...], "requires_restart": [...]}
```

### 自动监听文件变更

在配置中开启 `config_watch`，服务端会定期检查配置文件是否修改，自动热重载：

```yaml
server:
  config_watch: true
  config_watch_interval: 30 # 检查间隔（秒）
```

### 查看配置状态

```bash
curl http://localhost:8765/api/admin/config-status \
  -H "X-API-Token: your-token"

# {"config_file": "/path/to/config.yaml", "config_watch": true, "config_watch_interval": 30}
```

---

## 文件说明

| 文件                               | 用途                             | 是否提交到 Git |
| ---------------------------------- | -------------------------------- | -------------- |
| `config.example.yaml` (项目根目录) | 配置模板，包含所有可用选项及注释 | 是             |
| `config/config.yaml`               | 实际部署配置，含真实密钥         | **否**         |
| `config/README.md`                 | 本文件，配置流程说明             | 是             |

---

## 注意事项

- **不要将 `config/config.yaml` 提交到 Git**，该文件包含敏感信息（密钥等）
- 如需多环境配置，可创建 `config/config.dev.yaml`、`config/config.prod.yaml` 等
- 所有 `config/*.yaml` 文件均已在 `.gitignore` 中排除
