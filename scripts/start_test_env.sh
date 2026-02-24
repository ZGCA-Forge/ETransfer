#!/bin/bash
# EasyTransfer 测试环境启动脚本
#
# 使用方法:
#   ./scripts/start_test_env.sh           # 使用默认的 file 后端
#   ./scripts/start_test_env.sh memory    # 使用 memory 后端
#   ./scripts/start_test_env.sh redis     # 使用 redis 后端 (需要 Redis)
#
# 环境变量:
#   SERVER_PORT     服务端口 (默认: 8765)
#   STORAGE_PATH    存储路径 (默认: ./test_storage)
#   API_TOKEN       API 令牌 (默认: test-token-12345)
#   MAX_STORAGE     最大存储限额 (默认: 不限, 示例: 1GB)
#   DEFAULT_RETENTION 默认缓存策略 (默认: permanent, 可选: download_once / ttl)
#   USER_SYSTEM     启用用户系统 (默认: false)
#   OIDC_ISSUER     OIDC Issuer URL
#   OIDC_CLIENT_ID  OIDC Client ID
#   OIDC_CLIENT_SEC OIDC Client Secret

set -e

# 配置
STATE_BACKEND=${1:-file}  # 第一个参数指定后端类型，默认 file
SERVER_PORT=${SERVER_PORT:-8765}
STORAGE_PATH=${STORAGE_PATH:-./test_storage}
REDIS_URL=${REDIS_URL:-redis://localhost:6379/0}
API_TOKEN=${API_TOKEN:-test-token-12345}
MAX_STORAGE=${MAX_STORAGE:-}
DEFAULT_RETENTION=${DEFAULT_RETENTION:-permanent}
USER_SYSTEM=${USER_SYSTEM:-false}
OIDC_ISSUER=${OIDC_ISSUER:-}
OIDC_CLIENT_ID=${OIDC_CLIENT_ID:-}
OIDC_CLIENT_SEC=${OIDC_CLIENT_SEC:-}

# 颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${GREEN}================================${NC}"
echo -e "${GREEN}EasyTransfer 测试环境${NC}"
echo -e "${GREEN}================================${NC}"

# 根据后端类型进行检查
echo -e "\n${YELLOW}[1] 检查后端: $STATE_BACKEND${NC}"
case $STATE_BACKEND in
    memory)
        echo -e "${GREEN}  ✓ 使用内存后端 (无需外部依赖)${NC}"
        ;;
    file)
        echo -e "${GREEN}  ✓ 使用文件后端 (状态将持久化)${NC}"
        ;;
    redis)
        echo -e "${CYAN}  检查 Redis...${NC}"
        if redis-cli ping > /dev/null 2>&1; then
            echo -e "${GREEN}  ✓ Redis 已运行${NC}"
        else
            echo -e "${YELLOW}  正在启动 Redis...${NC}"
            if command -v redis-server &> /dev/null; then
                redis-server --daemonize yes
                sleep 1
                if redis-cli ping > /dev/null 2>&1; then
                    echo -e "${GREEN}  ✓ Redis 已启动${NC}"
                else
                    echo -e "${RED}  ✗ Redis 启动失败${NC}"
                    exit 1
                fi
            else
                echo -e "${RED}  ✗ Redis 未安装${NC}"
                echo -e "${YELLOW}  提示: 可以改用 memory 或 file 后端${NC}"
                exit 1
            fi
        fi
        ;;
    *)
        echo -e "${RED}  ✗ 未知后端类型: $STATE_BACKEND${NC}"
        echo -e "${YELLOW}  支持的后端: memory, file, redis${NC}"
        exit 1
        ;;
esac

# 创建存储目录
echo -e "\n${YELLOW}[2] 创建存储目录...${NC}"
mkdir -p "$STORAGE_PATH"
echo -e "${GREEN}  ✓ 存储目录: $STORAGE_PATH${NC}"

# 生成配置文件
echo -e "\n${YELLOW}[3] 生成配置文件...${NC}"
CONFIG_FILE="$STORAGE_PATH/config.yaml"
cat > "$CONFIG_FILE" << EOF
server:
  host: 0.0.0.0
  port: $SERVER_PORT
  storage_path: $STORAGE_PATH
  chunk_size: 4194304

state:
  backend: $STATE_BACKEND
  redis_url: $REDIS_URL

auth:
  enabled: true
  tokens:
    - "$API_TOKEN"

retention:
  default: $DEFAULT_RETENTION

network:
  interfaces: []
  prefer_ipv4: true
EOF
echo -e "${GREEN}  ✓ 配置文件: $CONFIG_FILE${NC}"

# 启动服务端
echo -e "\n${YELLOW}[4] 启动 EasyTransfer 服务端...${NC}"
echo -e "  后端: $STATE_BACKEND"
echo -e "  端口: $SERVER_PORT"
echo -e "  Token: $API_TOKEN"
echo -e "  缓存策略: $DEFAULT_RETENTION"

# 设置环境变量
export ETRANSFER_STORAGE_PATH="$STORAGE_PATH"
export ETRANSFER_STATE_BACKEND="$STATE_BACKEND"
export ETRANSFER_REDIS_URL="$REDIS_URL"
export ETRANSFER_AUTH_ENABLED="true"
export ETRANSFER_AUTH_TOKENS="[\"$API_TOKEN\"]"
export ETRANSFER_DEFAULT_RETENTION="$DEFAULT_RETENTION"

if [ -n "$MAX_STORAGE" ]; then
    export ETRANSFER_MAX_STORAGE_SIZE="$MAX_STORAGE"
    echo -e "  存储限额: $MAX_STORAGE"
fi

if [ "$USER_SYSTEM" = "true" ]; then
    export ETRANSFER_USER_SYSTEM_ENABLED="true"
    export ETRANSFER_USER_DB_PATH="$STORAGE_PATH/users.db"
    echo -e "  用户系统: ${GREEN}已启用${NC}"
    if [ -n "$OIDC_ISSUER" ]; then
        export ETRANSFER_OIDC_ISSUER_URL="$OIDC_ISSUER"
        export ETRANSFER_OIDC_CLIENT_ID="$OIDC_CLIENT_ID"
        export ETRANSFER_OIDC_CLIENT_SECRET="$OIDC_CLIENT_SEC"
        export ETRANSFER_OIDC_CALLBACK_URL="http://localhost:$SERVER_PORT/api/users/callback"
        echo -e "  OIDC Issuer: $OIDC_ISSUER"
    fi
fi

echo -e "\n${GREEN}================================${NC}"
echo -e "${GREEN}服务端启动中...${NC}"
echo -e "${GREEN}================================${NC}"
echo -e "  URL: http://localhost:$SERVER_PORT"
echo -e "  后端: ${CYAN}$STATE_BACKEND${NC}"
echo -e "  API Token: $API_TOKEN"
echo -e "  存储路径: $STORAGE_PATH"
echo -e "  缓存策略: $DEFAULT_RETENTION"
echo -e "\n按 Ctrl+C 停止服务端\n"

# 启动
python -m uvicorn etransfer.server.main:app \
    --host 0.0.0.0 \
    --port "$SERVER_PORT" \
    --reload
