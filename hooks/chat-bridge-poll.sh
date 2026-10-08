#!/usr/bin/env bash
# chat-bridge 轮询脚本：检查桥接服务器上有无用户新消息，有则唤醒 agent
set -euo pipefail
source "$HATCH_HOOK_RUNTIME"

CONF="$HOME/.config/chat-bridge/bridge.conf"
CURSOR="$HOME/hooks/state/chat-bridge-cursor"
TOKEN_FILE="$HOME/.config/chat-bridge/agent_token"

# 未配置好之前保持安静
[ -f "$CONF" ] || { silent "bridge not configured yet" '{}'; }
# shellcheck disable=SC1090
source "$CONF"
[ -n "${BRIDGE_URL:-}" ] || { silent "bridge url not set" '{}'; }
[ -f "$TOKEN_FILE" ] || { silent "bridge token not set" '{}'; }

TOKEN="$(cat "$TOKEN_FILE")"
AFTER="0"
[ -f "$CURSOR" ] && AFTER="$(cat "$CURSOR" 2>/dev/null || echo 0)"

RESP="$(curl --fail --silent --show-error --max-time 10 \
  -H "Authorization: Bearer $TOKEN" \
  "$BRIDGE_URL/api/agent/inbox?after=$AFTER" 2>&1)" \
  || { log "bridge poll failed" "{\"err\": $(printf '%s' "$RESP" | head -c 200 | jq -Rs .)}"; silent "bridge unreachable" '{}'; }

COUNT="$(printf '%s' "$RESP" | jq '.messages | length')"
if [ "$COUNT" -gt 0 ]; then
  # 把新消息原样带给 worker（最多 5 条防刷屏）
  PAYLOAD="$(printf '%s' "$RESP" | jq -c "{messages: [.messages[:5] | .[] | {id, text: .text[:2000], ts}]}")"
  # 先推进游标再唤醒：worker 处理慢（>轮询间隔）时，下一轮不会重复喊醒。
  # dry_run 时不写真实游标。
  if [ "${HATCH_HOOK_DRY_RUN:-0}" != "1" ]; then
    printf '%s' "$RESP" | jq -r '[.messages[].id] | max' > "$CURSOR"
  fi
  wake "chat-bridge: $COUNT 条用户新消息" "$PAYLOAD"
else
  silent "no new chat messages" '{}'
fi
