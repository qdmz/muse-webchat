# 元宝 Bot 桥接

通过腾讯元宝自定义机器人平台把 Muse 接到元宝 APP。

## 原理

- 元宝 APP 创建自定义机器人，拿到 **app_id** / **app_secret**
- 鉴权两步：HTTP `sign-token`（HMAC-SHA256）→ WSS `auth-bind`（protobuf）
- 收消息：`wss://bot-wss.yuanbao.tencent.com/wss/connection` 长连接推送（纯出站）
- 发消息：同连接 `send_c2c_message`（私聊）/ `send_group_message`（群聊）

## 协议实现

独立实现，不依赖 OpenClaw/Hermes。protobuf 只用 varint + length-delimited 两种 wire type，手写编解码约 100 行。

逆向参考：
- npm `openclaw-plugin-yuanbao`
- Go `chenhg5/cc-connect`（platform/yuanbao/）

## 流程

1. 填 `secrets.env`：`YUANBAO_APP_ID` / `YUANBAO_APP_SECRET`（600 权限）
2. `node src/daemon.js` — sign-token（缓存 30 天）→ WSS 鉴权 → 收 `inbound_message` → `inbox/`；扫 `outbox/` 发出；30s 心跳 ping
3. 配 hook（`yuanbao-inbox-watch`）5 秒轮询 inbox，有消息唤醒处理

## 注意

- 私聊/群聊都支持（群聊按 `groupCode` 区分）
- 推送带 `needAck` 时必须回 PushAck，否则服务器重推
- 按 `msgId` 去重
- 回复支持文本；图片/文件走 `[图片]`/`[文件]` 占位（收），发暂只支持文本

## 文件

- `src/yuanbao.js` — 协议层（protobuf 编解码、sign-token、收发消息构造）
- `src/daemon.js` — 主循环（连接、鉴权、心跳、inbox/outbox）
