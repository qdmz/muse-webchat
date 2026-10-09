# chat-bridge：Muse 多通道聊天桥接（v4）

muse.ai 打不开时，用自建 Web 页面 / 飞书 / 企业微信跟 Muse 聊天。
bridge.py 纯 Python 标准库单文件；各平台经独立 adapter 接入，不动核心。

## 架构

```
[Web UI] ────────┐
[飞书 Bot] ─ adapter ─┼→ bridge.py (:8090，消息中枢) ←→ Muse（hook 15s 轮询）
[企业微信应用] ───────┘         │
                          消息带 channel 字段路由，
                          回复自动继承原渠道
```

- **Web**：浏览器直连，`channel=web`
- **飞书**：`feishu_adapter.py` 经官方 SDK WebSocket 长连接收消息（纯出站，无需公网入站），轮询 outbox 发回
- **企业微信**：bridge 内置 `/api/channel/wecom/callback` 收微信服务器回调（需公网 HTTPS），`wecom_adapter.py` 轮询 outbox 经企业微信 API 发回

## 功能（v2 全部保留）

- 文字聊天（Markdown 渲染）、文件上传（≤20MB，图片内联）、Muse 发文件
- 多轮上下文（最近 20 条）、长任务进度更新、通知推送、输入中提示
- **v3**：多通道消息路由（`channel`/`channel_ctx`），回复自动回原渠道
- **v4**：企业微信回调接入

## Agent / Adapter API

需要 `Authorization: Bearer <BRIDGE_TOKEN>`（企业微信回调除外，靠微信签名验签）：

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/agent/inbox?after=<id>` | 取用户消息（全渠道，含 channel 字段） |
| GET | `/api/agent/outbox?channel=<c>&after=<id>` | adapter 取某渠道待发送的回复 |
| GET | `/api/agent/history?limit=<n>` | 最近 n 条（补上下文） |
| POST | `/api/agent/reply` | `{reply_to, text}`，自动继承渠道 |
| POST | `/api/agent/notify` | `{text}` 推送通知 |
| POST | `/api/channel/incoming` | adapter 提交消息 `{channel, channel_ctx, text}` |
| GET/POST | `/api/channel/wecom/callback` | 企业微信服务器回调（验签，不用 bearer） |

## 部署

### 1. 主桥（已上线）

systemd：`cfb-ctyun-sales-tunnel-ac5ea8-svc-chat-bridge`
公网：`https://chat.qdmzctyun.dpdns.org/`（Cloudflare Tunnel）

### 2. 飞书 adapter（已联调通过）

1. [open.feishu.cn](https://open.feishu.cn) 创建企业自建应用，记下 App ID / App Secret
2. 权限：`im:message`、`im:message:send_as_bot`；事件订阅选**使用长连接接收事件**，添加 `im.message.receive_v1`
3. 发布应用
4. 写 `~/.config/chat-bridge/feishu.env`（600）：
   ```
   FEISHU_APP_ID=cli_xxx
   FEISHU_APP_SECRET=xxx
   ```
5. `systemctl enable --now cfb-ctyun-sales-tunnel-ac5ea8-svc-feishu-adapter`

注意：沙盒出网走代理，`run-feishu.sh` 已处理（含 lark-oapi 的 WebSocket 代理 monkey-patch）。

### 3. 企业微信 adapter（待配置）

1. 企业微信管理后台 → 应用管理 → 创建自建应用，记下 CorpID / AgentID / Secret
2. 接收消息设置回调：
   - URL：`https://chat.qdmzctyun.dpdns.org/api/channel/wecom/callback`
   - Token / EncodingAESKey：随机生成，保存好
3. 写 `~/.config/chat-bridge/wecom.env`（600）：
   ```
   WECOM_CORP_ID=wwxxx
   WECOM_AGENT_ID=1000001
   WECOM_SECRET=xxx
   WECOM_TOKEN=yyy          # 给 bridge 回调用
   WECOM_ENCODING_AES_KEY=zzz
   ```
4. bridge 需装 `pycryptodome`（加解密用）：`pip install pycryptodome`
5. 让 bridge 进程加载 wecom.env（加到 run-bridge.sh 或 systemd EnvironmentFile）
6. 启动 adapter：`run-wecom.sh`（或配 systemd 服务）

## 配置文件

`~/.config/chat-bridge/`（全部 600 权限）：
- `server.env`：BRIDGE_PORT / BRIDGE_PASSWORD / BRIDGE_TOKEN / BRIDGE_DATA
- `bridge.conf`：BRIDGE_URL / PUBLIC_BASE（hook 用）
- `feishu.env`：FEISHU_APP_ID / FEISHU_APP_SECRET
- `wecom.env`：WECOM_CORP_ID / WECOM_AGENT_ID / WECOM_SECRET / WECOM_TOKEN / WECOM_ENCODING_AES_KEY

## 安全

- Web UI 密码登录（sha256+salt，HttpOnly session，7 天）
- Agent/Adapter API 用 bearer token；上传文件名消毒；/files 需登录
- 企业微信回调靠微信 SHA1 签名验签，无需 bearer
- 凭据只放 `~/.config/chat-bridge/*.env`（600），不进代码仓库
- 消息只存 500 条；上传 ≤20MB
