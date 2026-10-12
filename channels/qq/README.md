# QQ Bot 桥接

通过 QQ 官方开放平台把 Muse 接到 QQ。

## 原理

- QQ 官方 Bot API（q.qq.com）
- 收消息：`wss://api.bot.qq.com/websocket/` 网关长连接（纯出站）
- 发消息：REST API `POST /v2/users/{openid}/messages`

## 前置

1. 去 [q.qq.com](https://q.qq.com) 用手机 QQ 扫码注册，创建机器人
2. 拿到 **AppID** 和 **AppSecret**（离页即重置，当场保存）
3. 申请 `1<<25` 权限（GROUP_AND_C2C，私聊+群@消息）
4. 配服务器 IP 白名单（50 个上限）

## 流程

1. 填 `secrets.env`：`QQ_APP_ID` / `QQ_APP_SECRET`（600 权限）
2. `node src/daemon.js` — 连网关收 `C2C_MESSAGE_CREATE` / `GROUP_AT_MESSAGE_CREATE` → `inbox/`；扫 `outbox/` 经 REST 发出
3. 配 hook 轮询 inbox，有消息唤醒处理

## 注意

- 沙盒出口 IP 轮换，白名单覆盖不了 → daemon 需跑在固定 IP 服务器上
- access_token 2 小时有效，daemon 内自动刷新
- 被动回复 5 分钟窗口（带 msg_id），过期自动转主动消息重试
