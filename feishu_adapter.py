#!/usr/bin/env python3
"""
feishu_adapter: 飞书通道适配器（chat-bridge v3 多通道架构）

参考 https://github.com/caichengle666/omp-feishu-lark 的核心思路：
飞书 bot 用 WebSocket 长连接主动向外连飞书服务器收消息，无需公网 IP/入站端口，
非常适合沙盒环境。

工作流：
  收：飞书 WebSocket (im.message.receive_v1)
      → POST {BRIDGE_URL}/api/channel/incoming {channel:"feishu", channel_ctx, text}
  发：轮询 {BRIDGE_URL}/api/agent/outbox?channel=feishu&after=<cursor>
      → 经飞书 API 发到对应 chat → 推进游标

配置（环境变量）：
  FEISHU_APP_ID      飞书开放平台应用的 App ID
  FEISHU_APP_SECRET  飞书开放平台应用的 App Secret
  BRIDGE_URL         bridge.py 地址，如 http://127.0.0.1:8090
  BRIDGE_TOKEN       与 bridge.py 相同的 bearer token
  FEISHU_DOMAIN      默认 https://open.feishu.cn（Lark 用 https://open.larksuite.com）

飞书应用需开通权限：im:message（接收消息）、im:message:send_as_bot（发消息），
事件订阅选"使用长连接接收事件"。

依赖：pip install lark-oapi（建议 venv）
"""

import json
import logging
import os
import sys
import threading
import time
import urllib.request

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [feishu] %(levelname)s %(message)s")
log = logging.getLogger(__name__)

APP_ID = os.environ.get("FEISHU_APP_ID", "")
APP_SECRET = os.environ.get("FEISHU_APP_SECRET", "")
BRIDGE_URL = os.environ.get("BRIDGE_URL", "http://127.0.0.1:8090").rstrip("/")
BRIDGE_TOKEN = os.environ.get("BRIDGE_TOKEN", "")
DOMAIN = os.environ.get("FEISHU_DOMAIN", "https://open.feishu.cn")

CHANNEL = "feishu"
CURSOR_FILE = "/home/hatch/.config/chat-bridge/feishu-cursor"
POLL_INTERVAL = 3

_seen_msg_ids = set()


def bridge_api(method, path, body=None):
    req = urllib.request.Request(
        BRIDGE_URL + path,
        data=json.dumps(body).encode() if body else None,
        method=method,
        headers={"Authorization": f"Bearer {BRIDGE_TOKEN}",
                 "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode())


def load_cursor():
    try:
        return int(open(CURSOR_FILE).read().strip())
    except (FileNotFoundError, ValueError):
        return 0


def save_cursor(n):
    with open(CURSOR_FILE, "w") as f:
        f.write(str(n))


def extract_text(msg):
    """从飞书消息事件提纯文本（只处理 text 类型）"""
    try:
        if (msg.message_type or "") != "text":
            return None
        content = json.loads(msg.content or "{}")
        text = (content.get("text") or "").strip()
        # 去掉 @bot 的 mention 残留
        return text or None
    except Exception:
        return None


def on_feishu_message(event):
    """im.message.receive_v1 回调（lark-oapi 收到的是 P2ImMessageReceiveV1 对象）"""
    log.info("收到飞书事件回调，开始处理")
    try:
        data = event.event
        msg = data.message
        msg_id = msg.message_id or ""
        if msg_id in _seen_msg_ids:
            return
        _seen_msg_ids.add(msg_id)
        if len(_seen_msg_ids) > 1000:
            _seen_msg_ids.clear()

        # 只处理发给 bot 的私聊，或群里 @bot 的消息
        # （私聊 chat_type=p2p；群聊需 mention 才处理）
        if (msg.chat_type or "") != "p2p" and not (msg.mentions or []):
            return

        text = extract_text(msg)
        if not text:
            return

        open_id = ""
        sender = data.sender
        if sender is not None and sender.sender_id is not None:
            open_id = sender.sender_id.open_id or ""
        ctx = {
            "chat_id": msg.chat_id or "",
            "open_id": open_id,
            "msg_id": msg_id,
        }
        r = bridge_api("POST", "/api/channel/incoming", {
            "channel": CHANNEL, "channel_ctx": ctx, "text": text,
        })
        log.info("收到飞书消息 id=%s 已入桥: %s", r.get("id"), text[:40])
    except Exception as e:
        log.exception("处理飞书消息失败: %s", e)


def feishu_send_text(client, chat_id, text):
    """经飞书 API 发文本消息（超长截断）"""
    from lark_oapi.api.im.v1 import (
        CreateMessageRequest, CreateMessageRequestBody)
    # 飞书文本消息上限约 150KB，这里保守截断
    body = CreateMessageRequestBody.builder() \
        .receive_id(chat_id).msg_type("text") \
        .content(json.dumps({"text": text[:30000]},
                            ensure_ascii=False)).build()
    req = CreateMessageRequest.builder() \
        .receive_id_type("chat_id").request_body(body).build()
    resp = client.im.v1.message.create(req)
    if not resp.success():
        log.warning("飞书发送失败: %s %s", resp.code, resp.msg)
        return False
    return True


def outbox_loop(client):
    """轮询 bridge 取本渠道回复，发回飞书"""
    cursor = load_cursor()
    while True:
        try:
            r = bridge_api(
                "GET", f"/api/agent/outbox?channel={CHANNEL}&after={cursor}")
            for m in r.get("messages", []):
                ctx = m.get("channel_ctx") or {}
                chat_id = ctx.get("chat_id", "")
                if not chat_id:
                    log.warning("回复 id=%s 无 chat_id，跳过", m["id"])
                elif feishu_send_text(client, chat_id, m["text"]):
                    log.info("已发回飞书 id=%s chat=%s",
                             m["id"], chat_id[:12])
                cursor = max(cursor, m["id"])
            save_cursor(cursor)
        except Exception as e:
            log.warning("outbox 轮询失败: %s", e)
        time.sleep(POLL_INTERVAL)


def main():
    if not APP_ID or not APP_SECRET:
        log.error("必须设置 FEISHU_APP_ID / FEISHU_APP_SECRET")
        sys.exit(1)
    if not BRIDGE_TOKEN:
        log.error("必须设置 BRIDGE_TOKEN")
        sys.exit(1)

    import lark_oapi as lark
    from lark_oapi.ws import Client as WsClient
    from lark_oapi.event.dispatcher_handler import EventDispatcherHandler

    # 沙盒必须走 egress 代理：SDK 默认 ws 直连（proxy=None），这里 patch 让它走代理
    import lark_oapi.ws.client as _ws_client_mod
    _proxy_url = os.environ.get("https_proxy") or os.environ.get("HTTPS_PROXY") \
        or os.environ.get("http_proxy") or os.environ.get("HTTP_PROXY")
    if _proxy_url:
        _ws_client_mod._ws_connect_kwargs = lambda: {"proxy": _proxy_url}
        log.info("WebSocket 将经代理连接")

    # API client（发消息用）
    api_client = lark.Client.builder() \
        .app_id(APP_ID).app_secret(APP_SECRET).domain(DOMAIN) \
        .log_level(lark.LogLevel.WARNING).build()

    # 事件分发（lark-oapi 1.7.3：用 builder 注册，回调用的是 P2ImMessageReceiveV1）
    handler = EventDispatcherHandler.builder("", "") \
        .register_p2_im_message_receive_v1(on_feishu_message) \
        .build()

    # outbox 发送线程
    t = threading.Thread(target=outbox_loop, args=(api_client,),
                         daemon=True)
    t.start()

    # WebSocket 长连接（阻塞）
    log.info("启动飞书 WebSocket 长连接…")
    ws = WsClient(APP_ID, APP_SECRET, event_handler=handler,
                  domain=DOMAIN,
                  log_level=lark.LogLevel.WARNING)
    ws.start()


if __name__ == "__main__":
    main()
