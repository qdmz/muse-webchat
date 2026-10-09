#!/usr/bin/env bash
# feishu_adapter 启动包装
# token 与 bridge 主服务同源（server.env，和 run-bridge.sh 同一份），永不漂移；
# 飞书凭据单独放 feishu.env（600 权限），缺失则拒绝启动。
set -euo pipefail
SRV_CONF="/home/hatch/.config/chat-bridge/server.env"  # BRIDGE_TOKEN/BRIDGE_PORT/BRIDGE_DATA/BRIDGE_PASSWORD
BRI_CONF="/home/hatch/.config/chat-bridge/bridge.conf"  # BRIDGE_URL/PUBLIC_BASE/...
FEI_CONF="/home/hatch/.config/chat-bridge/feishu.env"   # FEISHU_APP_ID/FEISHU_APP_SECRET
[ -f "$SRV_CONF" ] || { echo "missing $SRV_CONF" >&2; exit 1; }
[ -f "$FEI_CONF" ] || { echo "missing $FEI_CONF（先填好飞书 App ID/Secret）" >&2; exit 1; }
set -a
source "$SRV_CONF"
[ -f "$BRI_CONF" ] && source "$BRI_CONF"
source "$FEI_CONF"
set +a
# 沙盒 egress 代理（WebSocket 必须走代理，见 feishu_adapter.py 的 monkey-patch）
export https_proxy="${https_proxy:-http://hatch-egress-proxy:3128}"
export HTTPS_PROXY="${HTTPS_PROXY:-$https_proxy}"
export http_proxy="${http_proxy:-$https_proxy}"
export HTTP_PROXY="${HTTP_PROXY:-$https_proxy}"
# 本地桥 API 不走代理
export no_proxy="${no_proxy:-127.0.0.1,localhost}"
export NO_PROXY="${NO_PROXY:-$no_proxy}"
exec /home/hatch/workspace/chat-bridge/venv-feishu/bin/python \
  /home/hatch/workspace/chat-bridge/feishu_adapter.py
