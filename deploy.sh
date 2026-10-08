#!/usr/bin/env bash
# chat-bridge 一键部署脚本（在目标服务器上以 root 运行）
# 用法: BRIDGE_PASSWORD=你的网页密码 BRIDGE_TOKEN=你的API令牌 bash deploy.sh
#   （两个值也可以后面再改 /opt/chat-bridge/.env）
set -euo pipefail

INSTALL_DIR=/opt/chat-bridge
PORT="${BRIDGE_PORT:-8090}"

if [ "$(id -u)" != "0" ]; then echo "请用 root 运行"; exit 1; fi
if [ -z "${BRIDGE_PASSWORD:-}" ]; then echo "ERROR: 请设置 BRIDGE_PASSWORD"; exit 1; fi
if [ -z "${BRIDGE_TOKEN:-}" ]; then echo "ERROR: 请设置 BRIDGE_TOKEN"; exit 1; fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

mkdir -p "$INSTALL_DIR/data"
cp "$SCRIPT_DIR/bridge.py" "$INSTALL_DIR/bridge.py"
chmod 755 "$INSTALL_DIR/bridge.py"

cat > "$INSTALL_DIR/.env" <<EOF
BRIDGE_PORT=$PORT
BRIDGE_DATA=$INSTALL_DIR/data
BRIDGE_PASSWORD=$BRIDGE_PASSWORD
BRIDGE_TOKEN=$BRIDGE_TOKEN
EOF
chmod 600 "$INSTALL_DIR/.env"

cp "$SCRIPT_DIR/chat-bridge.service" /etc/systemd/system/chat-bridge.service
systemctl daemon-reload
systemctl enable --now chat-bridge
sleep 2
systemctl is-active chat-bridge
echo "OK: chat-bridge 已启动，端口 $PORT"
curl -s -o /dev/null -w "health: %{http_code}\n" http://127.0.0.1:$PORT/api/agent/health
