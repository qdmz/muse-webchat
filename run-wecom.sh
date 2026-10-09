#!/usr/bin/env bash
# wecom_adapter 启动包装
# token 与 bridge 主服务同源（server.env）；企业微信凭据单独放 wecom.env（600 权限）。
set -euo pipefail
SRV_CONF="/home/hatch/.config/chat-bridge/server.env"  # BRIDGE_TOKEN/BRIDGE_PORT/...
BRI_CONF="/home/hatch/.config/chat-bridge/bridge.conf"  # BRIDGE_URL/PUBLIC_BASE/...
WEC_CONF="/home/hatch/.config/chat-bridge/wecom.env"    # WECOM_CORP_ID/AGENT_ID/SECRET/...
[ -f "$SRV_CONF" ] || { echo "missing $SRV_CONF" >&2; exit 1; }
[ -f "$WEC_CONF" ] || { echo "missing $WEC_CONF（先填好企业微信 CorpID/AgentID/Secret）" >&2; exit 1; }
set -a
source "$SRV_CONF"
[ -f "$BRI_CONF" ] && source "$BRI_CONF"
source "$WEC_CONF"
set +a
# 沙盒 egress 代理（企业微信 API 走公网）
export https_proxy="${https_proxy:-http://hatch-egress-proxy:3128}"
export HTTPS_PROXY="${HTTPS_PROXY:-$https_proxy}"
export http_proxy="${http_proxy:-$https_proxy}"
export HTTP_PROXY="${HTTP_PROXY:-$https_proxy}"
export no_proxy="${no_proxy:-127.0.0.1,localhost}"
export NO_PROXY="${NO_PROXY:-$no_proxy}"
exec /usr/bin/python3 /home/hatch/workspace/chat-bridge/wecom_adapter.py
