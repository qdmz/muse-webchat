# chat-bridge：Muse Web 聊天桥接（v2）

muse.ai 打不开时，用自建 Web 页面跟 Muse 聊天。纯 Python 标准库，单文件，无依赖。

## 架构

```
[浏览器] ──(密码登录)──→ [本机 :8090] ←─(15s 轮询, bearer token)─→ [Muse hook worker]
   │                         │                                            │
   │                    bridge.py                                    处理消息后 POST 回复
   │                    消息队列/文件                                  ↓
   │                                                          /api/agent/reply
   └──────── chat.qdmzctyun.dpdns.org (Cloudflare Tunnel) ──────────┘
```

## 功能

- 文字聊天（Markdown 渲染：代码块/粗体/列表/链接）
- 文件上传（📎，≤20MB）：图片内联显示，其他文件可下载；Muse 可直接读取
- Muse 发文件：写到 `$DATA_DIR/outbox/`，回复里带 `$PUBLIC_BASE/files/o_<文件名>` 链接
- 多轮上下文：worker 自动取最近 20 条历史
- 长任务进度更新：>1 分钟的任务先回进度，再回结果
- 通知推送：`POST /api/agent/notify {"text"}` 可把定时任务/提醒推到桥上（🔔 样式）
- 输入中提示、15 秒轮询

## 部署

本机 systemd（已上线）：`cfb-ctyun-sales-tunnel-ac5ea8-svc-chat-bridge`
隧道注册：`cfbridge add-service --name chat-bridge ...`（见 config.json）

## 配置

`~/.config/chat-bridge/`（全部 600）：
- `server.env`：BRIDGE_PORT/PASSWORD/TOKEN/DATA（systemd 启动用）
- `bridge.conf`：BRIDGE_URL / PUBLIC_BASE / DATA_DIR（hook 用）
- `agent_token`、`web_password`

## 安全

- Web UI 密码登录（sha256+salt，HttpOnly session，7 天）
- Agent API 用 bearer token；上传文件名消毒防路径穿越；/files 需登录
- 消息只存 500 条；上传 ≤20MB
