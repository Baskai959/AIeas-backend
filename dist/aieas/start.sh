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
