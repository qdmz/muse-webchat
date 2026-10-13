# Telegram Bot 桥接（双 bot）

通过 Telegram Bot API 把 Muse 接到 Telegram，支持多 bot 同时在线。

## 原理

- Telegram Bot API：`https://api.telegram.org`
- 收消息：`getUpdates` 长轮询（纯出站，无需公网入站、无需固定 IP）
- 发消息：`sendMessage` / `sendPhoto` / `sendDocument`
- 下文件：`getFile` → `https://api.telegram.org/file/bot<token>/<path>`

## 前置

1. 找 [@BotFather](https://t.me/BotFather) 建机器人，拿到 token
2. 多个 bot 用分号分隔，填入 `secrets.env`：
   ```
   TELEGRAM_BOTS=name1:<id1>:<secret1>;name2:<id2>:<secret2>
   ```
   （600 权限）

## 流程

1. `node src/daemon.js` — 每个 bot 独立长轮询 → `inbox/`；扫 `outbox/` 发出
2. 配 hook（`telegram-inbox-watch`）5 秒轮询 inbox，有消息唤醒处理

## 功能

- 收：文字/图片/文件/语音/视频/贴纸，媒体自动下载到 `media/`
- 发：文字（reply 引用原消息）、图片/文件（公网 URL）
- 群组：只回复 @bot 的消息，私聊全收
- 去重：按 update_id，offset 持久化到 `<bot名>.offset`

## 注意

- 几个通道里适配成本最低的：无需扫码、无需固定 IP、无 protobuf、无 AES
- 发图片/文件需要公网 URL（本地文件需先传到可访问的位置）

## 文件

- `src/telegram.js` — Bot API 客户端（收发、下载、解析）
- `src/daemon.js` — 主循环（多 bot 轮询、inbox/outbox）
