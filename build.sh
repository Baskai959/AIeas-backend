#!/usr/bin/env bash
# 一键打包脚本：在本机交叉编译 Linux 二进制，并产出可直接上传到服务器的部署目录。
#
# 用法：
#   ./build.sh                       # 默认 linux/amd64，输出到 ./dist/aieas
#   GOARCH=arm64 ./build.sh          # 编译 arm64
#   OUTPUT_DIR=/tmp/aieas ./build.sh # 自定义输出目录
#
# 产出结构：
#   <OUTPUT_DIR>/
#     ├── bin/aieas              # 主二进制
#     ├── configs/config.yaml    # 配置（仓库默认值，密钥走 .env 覆盖）
#     ├── scripts/lua/           # 运行时加载的 Redis Lua 脚本
#     ├── start.sh               # 启动脚本：加载 .env 后运行二进制
#     └── .env                   # 从 .env.example 拷贝，请上传后填入真实值

set -euo pipefail

# ---------- 可配置参数 ----------
GOOS="${GOOS:-linux}"
GOARCH="${GOARCH:-amd64}"
CGO_ENABLED="${CGO_ENABLED:-0}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-${SCRIPT_DIR}/dist/aieas}"

BINARY_NAME="aieas"

# ---------- 颜色输出 ----------
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log()  { echo -e "${GREEN}[build]${NC} $*"; }
warn() { echo -e "${YELLOW}[build]${NC} $*"; }
err()  { echo -e "${RED}[build]${NC} $*" >&2; }

# ---------- 前置检查 ----------
cd "${SCRIPT_DIR}"

if ! command -v go >/dev/null 2>&1; then
  err "未找到 go 命令，请先安装 Go"
  exit 1
fi

log "Go 版本: $(go version)"
log "目标平台: ${GOOS}/${GOARCH}  CGO_ENABLED=${CGO_ENABLED}"
log "输出目录: ${OUTPUT_DIR}"

# ---------- 清理并创建目录 ----------
if [[ -d "${OUTPUT_DIR}" ]]; then
  warn "输出目录已存在，先清空: ${OUTPUT_DIR}"
  rm -rf "${OUTPUT_DIR}"
fi

mkdir -p "${OUTPUT_DIR}/bin"
mkdir -p "${OUTPUT_DIR}/configs"
mkdir -p "${OUTPUT_DIR}/scripts/lua"
mkdir -p "${OUTPUT_DIR}/log"

# ---------- 编译主二进制 ----------
log "开始编译主二进制 ..."
GOOS="${GOOS}" GOARCH="${GOARCH}" CGO_ENABLED="${CGO_ENABLED}" \
  go build -ldflags="-s -w" -o "${OUTPUT_DIR}/bin/${BINARY_NAME}" .

chmod +x "${OUTPUT_DIR}/bin/${BINARY_NAME}"
log "编译完成: ${OUTPUT_DIR}/bin/${BINARY_NAME}"

# ---------- 拷贝配置 ----------
log "拷贝 configs/config.yaml ..."
cp "${SCRIPT_DIR}/configs/config.yaml" "${OUTPUT_DIR}/configs/config.yaml"

# ---------- 拷贝 Lua 脚本 ----------
log "拷贝 scripts/lua/*.lua ..."
cp "${SCRIPT_DIR}/scripts/lua/"*.lua "${OUTPUT_DIR}/scripts/lua/"

# ---------- 生成 .env 模板 ----------
if [[ -f "${SCRIPT_DIR}/.env.example" ]]; then
  log "基于 .env.example 生成 .env 模板（请上传后填入真实密钥）"
  cp "${SCRIPT_DIR}/.env.example" "${OUTPUT_DIR}/.env"
else
  warn ".env.example 不存在，生成最小化 .env 模板"
  cat > "${OUTPUT_DIR}/.env" <<'EOF'
# 上线前请替换为真实值；含特殊字符（() & ? 空格 等）的值请用单引号包裹
MYSQL_DSN='auction:PASSWORD@tcp(rds-host:3306)/auction?charset=utf8mb4&parseTime=true&loc=Local'
REDIS_RT_PRIMARY_ADDR='redis-rt-host:6379'
REDIS_RT_PRIMARY_PASSWORD=''
REDIS_CACHE_ADDR='redis-cache-host:6379'
REDIS_CACHE_PASSWORD=''
JWT_SECRET='CHANGE_ME'
OBSERVABILITY_FORMAT='json'
EOF
fi

# ---------- 生成 start.sh 启动脚本 ----------
log "生成 start.sh 启动脚本 ..."
cat > "${OUTPUT_DIR}/start.sh" <<'STARTEOF'
#!/usr/bin/env bash
# 启动脚本：加载 .env 并运行 aieas 二进制。
#
# 用法：
#   ./start.sh           # 前台运行（Ctrl+C 退出，方便调试）
#   ./start.sh -d        # 后台运行（nohup，日志写到 log/app.log）
#   ./start.sh stop      # 停止后台进程
#   ./start.sh status    # 查看后台进程状态
#   ./start.sh restart   # 重启后台进程
#
# 注意：生产环境推荐用 systemd 托管，本脚本主要用于快速调试 / 临时部署。

set -euo pipefail

CURDIR="$(cd "$(dirname "$0")" && pwd)"
cd "${CURDIR}"

BINARY_NAME="aieas"
BINARY="${CURDIR}/bin/${BINARY_NAME}"
ENV_FILE="${CURDIR}/.env"
CONFIG_FILE="${CURDIR}/configs/config.yaml"
LOG_DIR="${CURDIR}/log"
LOG_FILE="${LOG_DIR}/app.log"
PID_FILE="${CURDIR}/${BINARY_NAME}.pid"

mkdir -p "${LOG_DIR}"

# ---------- 校验 ----------
if [[ ! -x "${BINARY}" ]]; then
  echo "[start] 二进制不存在或无可执行权限: ${BINARY}" >&2
  exit 1
fi

if [[ ! -f "${CONFIG_FILE}" ]]; then
  echo "[start] 缺少配置文件: ${CONFIG_FILE}" >&2
  exit 1
fi

# ---------- 加载环境变量（可选） ----------
# .env 存在则加载用于覆盖 config.yaml；不存在则完全使用 config.yaml 的值。
if [[ -f "${ENV_FILE}" ]]; then
  echo "[start] 加载 .env: ${ENV_FILE}"
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
else
  echo "[start] 未发现 .env，跳过；将完全使用 ${CONFIG_FILE} 的配置"
fi

# ---------- 子命令 ----------
ACTION="${1:-foreground}"

is_running() {
  [[ -f "${PID_FILE}" ]] || return 1
  local pid
  pid="$(cat "${PID_FILE}")"
  [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null
}

start_background() {
  if is_running; then
    echo "[start] 进程已在运行: PID=$(cat "${PID_FILE}")"
    exit 0
  fi
  echo "[start] 后台启动 ${BINARY} ..."
  nohup "${BINARY}" >> "${LOG_FILE}" 2>&1 &
  echo $! > "${PID_FILE}"
  sleep 1
  if is_running; then
    echo "[start] 启动成功: PID=$(cat "${PID_FILE}")  日志=${LOG_FILE}"
  else
    echo "[start] 启动失败，请查看日志: ${LOG_FILE}" >&2
    exit 1
  fi
}

stop_background() {
  if ! is_running; then
    echo "[start] 进程未运行"
    rm -f "${PID_FILE}"
    return
  fi
  local pid
  pid="$(cat "${PID_FILE}")"
  echo "[start] 停止进程 PID=${pid} ..."
  kill "${pid}"
  for _ in $(seq 1 20); do
    if ! kill -0 "${pid}" 2>/dev/null; then
      break
    fi
    sleep 0.5
  done
  if kill -0 "${pid}" 2>/dev/null; then
    echo "[start] 进程未能优雅退出，发送 SIGKILL"
    kill -9 "${pid}" || true
  fi
  rm -f "${PID_FILE}"
  echo "[start] 已停止"
}

status() {
  if is_running; then
    echo "[start] 运行中: PID=$(cat "${PID_FILE}")"
    echo "[start] 日志: ${LOG_FILE}"
  else
    echo "[start] 未运行"
    exit 1
  fi
}

case "${ACTION}" in
  foreground|fg|"")
    echo "[start] 前台启动 ${BINARY} (Ctrl+C 退出)"
    exec "${BINARY}"
    ;;
  -d|daemon|start)
    start_background
    ;;
  stop)
    stop_background
    ;;
  restart)
    stop_background
    start_background
    ;;
  status)
    status
    ;;
  *)
    echo "用法: $0 [foreground|-d|stop|restart|status]" >&2
    exit 1
    ;;
esac
STARTEOF
chmod +x "${OUTPUT_DIR}/start.sh"

# ---------- 输出摘要 ----------
log "打包完成，目录结构如下:"
if command -v tree >/dev/null 2>&1; then
  tree -a "${OUTPUT_DIR}"
else
  find "${OUTPUT_DIR}" -print | sed -e "s|${OUTPUT_DIR}|.|"
fi

cat <<EOF

${GREEN}下一步建议:${NC}
  1. 修改 ${OUTPUT_DIR}/.env 中的 MYSQL_DSN / REDIS_ADDR / JWT_SECRET 等密钥
     （含特殊字符的值要用单引号包裹）
  2. 上传到服务器:
       rsync -avz ${OUTPUT_DIR}/ root@your-server:/opt/aieas/
  3. 服务器上启动:
       cd /opt/aieas
       ./start.sh            # 前台调试
       ./start.sh -d         # 后台运行
       ./start.sh status     # 查看状态
       ./start.sh stop       # 停止
       ./start.sh restart    # 重启
EOF
