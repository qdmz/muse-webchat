# 微信 ClawBot 桥接

通过微信官方 ClawBot 插件（iLink 协议）把 Muse 接到个人微信。

## 原理

- 微信官方插件（2026 年 3 月上线），走官方通道，不封号
- iLink 协议：纯 HTTP/JSON，无需 WebSocket
- 接入域名：`https://ilinkai.weixin.qq.com`

## 流程

1. `node src/qrcode.js` — 生成二维码，微信扫码绑定，token 存 `bot_token.json`（600 权限）
2. `node src/daemon.js` — 长轮询 `getupdates` 收消息 → `inbox/`；扫 `outbox/` 经 `sendmessage` 发出 → 归档 `sent/`
3. 配 hook（`wechat-inbox-watch`）5 秒轮询 inbox，有消息唤醒处理

## 限制

- 仅私聊，不支持群聊
- 不能主动推送，必须用户先发消息（需 `context_token`）
- 回复要在 24 小时窗口内

## 文件

- `src/ilink.js` — iLink API 客户端
- `src/qrcode.js` — 扫码登录
- `src/daemon.js` — 主循环
- `src/proxy.js` — 出网代理（沙盒环境）
