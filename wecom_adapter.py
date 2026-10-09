#!/usr/bin/env python3
"""企业微信 adapter：轮询 bridge outbox，经企业微信 API 发回用户

架构：
  [企业微信服务器] --回调--> bridge.py:/api/channel/wecom/callback（收消息入桥）
  本 adapter: 轮询 /api/agent/outbox?channel=wecom → 调企业微信 message/send 发回

配置（~/.config/chat-bridge/wecom.env，600 权限）：
  WECOM_CORP_ID        企业 ID
  WECOM_AGENT_ID       应用 AgentID
  WECOM_SECRET         应用 Secret
  # WECOM_TOKEN / WECOM_ENCODING_AES_KEY 给 bridge.py 回调用（同一文件可共存）

bridge 配置（server.env）：BRIDGE_TOKEN
"""
import json
import logging
import os
import sys
import time
import urllib.request

CHANNEL = "wecom"
CURSOR_FILE = "/home/hatch/.config/chat-bridge/wecom-cursor"

WECOM_CORP_ID = os.environ.get("WECOM_CORP_ID", "")
WECOM_AGENT_ID = os.environ.get("WECOM_AGENT_ID", "")
WECOM_SECRET = os.environ.get("WECOM_SECRET", "")
BRIDGE_TOKEN = os.environ.get("BRIDGE_TOKEN", "")
BRIDGE_URL = os.environ.get("BRIDGE_URL", "http://127.0.0.1:8090")

log = logging.getLogger("wecom")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [wecom] %(levelname)s %(message)s")


def bridge_api(method, path, body=None):
    req = urllib.request.Request(
        BRIDGE_URL + path,
        data=json.dumps(body).encode() if body is not None else None,
        method=method,
        headers={"Authorization": f"Bearer {BRIDGE_TOKEN}",
                 "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode())


def load_cursor():
    try:
        return int(open(CURSOR_FILE).read().strip())
    except Exception:
        return 0


def save_cursor(cid):
    try:
        with open(CURSOR_FILE, "w") as f:
            f.write(str(cid))
    except Exception as e:
        log.warning("cursor 保存失败: %s", e)


# access_token 缓存（企业微信 token 有效期 2 小时）
_token_cache = {"token": "", "expires": 0}


def get_access_token():
    now = time.time()
    if _token_cache["token"] and now < _token_cache["expires"] - 60:
        return _token_cache["token"]
    url = ("https://qyapi.weixin.qq.com/cgi-bin/gettoken"
           f"?corpid={WECOM_CORP_ID}&corpsecret={WECOM_SECRET}")
    with urllib.request.urlopen(url, timeout=15) as r:
        data = json.loads(r.read().decode())
    if data.get("errcode") != 0:
        raise RuntimeError(f"gettoken 失败: {data}")
    _token_cache["token"] = data["access_token"]
    _token_cache["expires"] = now + int(data.get("expires_in", 7200))
    return _token_cache["token"]


def wecom_send_text(user_id, text):
    """经企业微信 API 发文本消息"""
    token = get_access_token()
    url = ("https://qyapi.weixin.qq.com/cgi-bin/message/send"
           f"?access_token={token}")
    # 企业微信文本上限 2048 字节，超长截断
    payload = {
        "touser": user_id,
        "msgtype": "text",
        "agentid": int(WECOM_AGENT_ID),
        "text": {"content": text[:2000]},
    }
    req = urllib.request.Request(
        url, data=json.dumps(payload, ensure_ascii=False).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=15) as r:
        data = json.loads(r.read().decode())
    if data.get("errcode") != 0:
        raise RuntimeError(f"message/send 失败: {data}")
    return data


def outbox_loop():
    cursor = load_cursor()
    log.info("outbox 轮询启动，cursor=%d", cursor)
    while True:
        try:
            resp = bridge_api(
                "GET",
                f"/api/agent/outbox?channel={CHANNEL}&after={cursor}")
            for m in resp.get("messages", []):
                ctx = m.get("channel_ctx") or {}
                user_id = ctx.get("user_id", "")
                if not user_id:
                    log.warning("消息 %d 缺 user_id，跳过", m["id"])
                    cursor = m["id"]
                    continue
                try:
                    wecom_send_text(user_id, m.get("text", ""))
                    log.info("已发回企业微信 id=%d user=%s",
                             m["id"], user_id[:8])
                except Exception as e:
                    log.error("发送失败 id=%d: %s", m["id"], e)
                    break  # 下轮重试，不推进 cursor
                cursor = m["id"]
                save_cursor(cursor)
        except Exception as e:
            log.warning("outbox 轮询失败: %s", e)
        time.sleep(3)


def main():
    if not all([WECOM_CORP_ID, WECOM_AGENT_ID, WECOM_SECRET]):
        log.error("必须设置 WECOM_CORP_ID / WECOM_AGENT_ID / WECOM_SECRET")
        sys.exit(1)
    if not BRIDGE_TOKEN:
        log.error("必须设置 BRIDGE_TOKEN")
        sys.exit(1)
    log.info("企业微信 adapter 启动（ corp=%s agent=%s ）",
             WECOM_CORP_ID[:6], WECOM_AGENT_ID)
    outbox_loop()


if __name__ == "__main__":
    main()
