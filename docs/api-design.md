# 通信设计

## 认证

所有需认证的请求在 Header 中携带：

- `X-API-Token: <token>` — API Token 认证
- `Authorization: Bearer <session_token>` — OIDC Session Token

公开端点（无需认证）：`/api/health`、`/api/users/login`、`/api/users/callback`、`/l/`

## TUS 协议端点

基于 [TUS 1.0.0](https://tus.io/protocols/resumable-upload) 实现，支持扩展：creation, creation-with-upload, termination, checksum, expiration, parallel-writes。

| 方法      | 路径             | 说明                                  |
| --------- | ---------------- | ------------------------------------- |
| `OPTIONS` | `/tus`           | 获取服务端能力                        |
| `POST`    | `/tus`           | 创建上传（支持 creation-with-upload） |
| `HEAD`    | `/tus/{file_id}` | 获取上传进度                          |
| `PATCH`   | `/tus/{file_id}` | 上传切片                              |
| `DELETE`  | `/tus/{file_id}` | 终止上传                              |

### TUS Metadata

上传时通过 `Upload-Metadata` 头传递元数据：

| Key             | 说明                                            |
| --------------- | ----------------------------------------------- |
| `filename`      | 文件名（Base64 编码）                           |
| `content_type`  | MIME 类型                                       |
| `retention`     | 文件策略：`permanent` / `download_once` / `ttl` |
| `retention_ttl` | TTL 秒数（仅 `ttl` 策略）                       |

## 文件管理

| 方法     | 路径                                 | 说明                   |
| -------- | ------------------------------------ | ---------------------- |
| `GET`    | `/api/files`                         | 文件列表（分页）       |
| `GET`    | `/api/files/{file_id}`               | 文件详情               |
| `GET`    | `/api/files/{file_id}/download`      | 下载文件（支持 Range） |
| `GET`    | `/api/files/{file_id}/info/download` | 下载信息（切片元数据） |
| `DELETE` | `/api/files/{file_id}`               | 删除文件               |
| `POST`   | `/api/files/cleanup`                 | 手动触发清理           |

### 下载信息 (`/info/download`)

返回切片下载所需的元数据：

```json
{
  "file_id": "abc123",
  "filename": "model.bin",
  "size": 1073741824,
  "chunked": true,
  "chunk_size": 4194304,
  "total_chunks": 256,
  "available_size": 1073741824,
  "retention": "permanent",
  "download_count": 0
}
```

### 切片下载

当文件使用切片存储时，通过 `chunk` 查询参数下载指定切片：

```
GET /api/files/{file_id}/download?chunk=0
GET /api/files/{file_id}/download?chunk=1
...
```

也支持标准 HTTP Range 头进行范围下载。

### 下载响应头

| Header                | 说明         |
| --------------------- | ------------ |
| `X-Retention-Policy`  | 当前文件策略 |
| `X-Retention-Expires` | TTL 到期时间 |
| `X-Retention-Warning` | 阅后即焚提醒 |
| `X-Download-Count`    | 下载次数     |

## 服务端信息

| 方法  | 路径             | 说明                               |
| ----- | ---------------- | ---------------------------------- |
| `GET` | `/api/health`    | 健康检查                           |
| `GET` | `/api/info`      | 服务器信息（版本、能力、存储统计） |
| `GET` | `/api/stats`     | 详细统计                           |
| `GET` | `/api/endpoints` | 可用端点（含流量负载）             |
| `GET` | `/api/storage`   | 存储配额与使用状态                 |
| `GET` | `/api/traffic`   | 实时流量数据                       |

## 认证验证

| 方法   | 路径               | 说明           |
| ------ | ------------------ | -------------- |
| `POST` | `/api/auth/verify` | 验证 API Token |

## 用户系统（OIDC）

| 方法   | 路径                            | 说明                            |
| ------ | ------------------------------- | ------------------------------- |
| `GET`  | `/api/users/login-info`         | 获取登录配置                    |
| `POST` | `/api/users/login/start`        | CLI 登录：返回 state 和授权 URL |
| `GET`  | `/api/users/login/poll/{state}` | CLI 轮询登录结果                |
| `GET`  | `/api/users/login`              | 浏览器跳转 OIDC 登录            |
| `GET`  | `/api/users/callback`           | OIDC 回调                       |
| `GET`  | `/api/users/me`                 | 当前用户信息 + 配额             |
| `GET`  | `/api/users/me/quota`           | 当前用户配额使用                |
| `POST` | `/api/users/logout`             | 注销                            |

### 管理员接口

| 方法     | 路径                              | 说明            |
| -------- | --------------------------------- | --------------- |
| `GET`    | `/api/users`                      | 用户列表        |
| `PUT`    | `/api/users/{id}/role`            | 设置角色        |
| `PUT`    | `/api/users/{id}/active`          | 启用/禁用用户   |
| `GET`    | `/api/groups`                     | 群组列表        |
| `POST`   | `/api/groups`                     | 创建群组 + 配额 |
| `PUT`    | `/api/groups/{id}/quota`          | 更新群组配额    |
| `DELETE` | `/api/groups/{id}`                | 删除群组        |
| `POST`   | `/api/groups/{gid}/members/{uid}` | 添加成员        |
| `DELETE` | `/api/groups/{gid}/members/{uid}` | 移除成员        |

### 管理接口

| 方法   | 路径                       | 说明       |
| ------ | -------------------------- | ---------- |
| `POST` | `/api/admin/reload-config` | 热重载配置 |

## CLI 登录流程

```
客户端                          服务端                          OIDC 提供者
  │                               │                               │
  │  POST /api/users/login/start  │                               │
  │──────────────────────────────>│                               │
  │  { state, auth_url }          │                               │
  │<──────────────────────────────│                               │
  │                               │                               │
  │  用户在浏览器打开 auth_url     │                               │
  │  ─────────────────────────────────────────────────────────────>│
  │                               │  GET /api/users/callback      │
  │                               │<──────────────────────────────│
  │                               │  (code + state)               │
  │                               │                               │
  │  GET /api/users/login/poll/{state}                            │
  │──────────────────────────────>│                               │
  │  { token }                    │                               │
  │<──────────────────────────────│                               │
```
