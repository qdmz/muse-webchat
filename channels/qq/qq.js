/**
 * QQ 官方 Bot API 客户端
 * - 鉴权: AppID + AppSecret → access_token（2h 有效，自动刷新）
 * - 收消息: wss://api.bot.qq.com/websocket/ 网关（Identify + 心跳 + Dispatch）
 * - 发消息: POST /v2/users/{openid}/messages
 */
import https from "node:https";
import { HttpsProxyAgent } from "https-proxy-agent";
import WebSocket from "ws";

const API_BASE = "https://api.bot.qq.com";
const WS_URL = "wss://api.bot.qq.com/websocket/";

function proxyAgent() {
  const url = process.env.HTTPS_PROXY || process.env.https_proxy ||
    process.env.ALL_PROXY || process.env.all_proxy || "";
  return url ? new HttpsProxyAgent(url, { keepAlive: true }) : undefined;
}
const AGENT = proxyAgent();
if (AGENT) console.log("[proxy] 经代理出网");

function httpsJson(urlStr, { method = "GET", headers = {}, body = null, timeoutMs = 15000 }) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const data = body ? JSON.stringify(body) : null;
    const req = https.request({
      hostname: u.hostname, port: u.port || 443, path: u.pathname + u.search,
      method,
      headers: {
        "Content-Type": "application/json",
        ...(data ? { "Content-Length": Buffer.byteLength(data) } : {}),
        ...headers,
      },
      agent: AGENT, timeout: timeoutMs,
    }, (res) => {
      const chunks = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => {
        try { resolve(JSON.parse(Buffer.concat(chunks).toString("utf8"))); }
        catch { reject(new Error(`非 JSON 响应: ${res.statusCode}`)); }
      });
    });
    req.on("timeout", () => req.destroy(new Error("请求超时")));
    req.on("error", reject);
    if (data) req.write(data);
    req.end();
  });
}

export class QQBot {
  constructor({ appId, appSecret }) {
    this.appId = appId;
    this.appSecret = appSecret;
    this.accessToken = null;
    this.tokenExpiry = 0;
    this.ws = null;
    this.seq = null;
    this.sessionId = null;
    this.heartbeatTimer = null;
    this.onMessage = null; // (event) => void
    this.onReady = null;
  }

  async getAccessToken(force = false) {
    if (!force && this.accessToken && Date.now() < this.tokenExpiry - 60000) {
      return this.accessToken;
    }
    const r = await httpsJson(`${API_BASE}/app/getAppAccessToken`, {
      method: "POST",
      body: { appId: this.appId, clientSecret: this.appSecret },
    });
    if (r.code) throw new Error(`取 token 失败: code=${r.code} ${r.message || ""}`);
    this.accessToken = r.access_token;
    this.tokenExpiry = Date.now() + (parseInt(r.expires_in) || 7200) * 1000;
    return this.accessToken;
  }

  authHeaders(token) {
    return { "Authorization": `QQBot ${token}` };
  }

  /** 发私聊消息 */
  async sendC2C({ userOpenid, content, msgId = null, msgSeq = 1 }) {
    const token = await this.getAccessToken();
    const body = { content, msg_type: 0, msg_seq: msgSeq };
    if (msgId) body.msg_id = msgId; // 被动回复（5分钟窗口）
    return httpsJson(`${API_BASE}/v2/users/${userOpenid}/messages`, {
      method: "POST",
      headers: this.authHeaders(token),
      body,
    });
  }

  /** 发群聊消息 */
  async sendGroup({ groupOpenid, content, msgId = null, msgSeq = 1 }) {
    const token = await this.getAccessToken();
    const body = { content, msg_type: 0, msg_seq: msgSeq };
    if (msgId) body.msg_id = msgId;
    return httpsJson(`${API_BASE}/v2/groups/${groupOpenid}/messages`, {
      method: "POST",
      headers: this.authHeaders(token),
      body,
    });
  }

  /** 连接网关 */
  async connect({ intents = (1 << 25) | (1 << 30) } = {}) {
    const token = await this.getAccessToken();
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(WS_URL, { agent: AGENT, handshakeTimeout: 15000 });
      this.ws = ws;
      let helloReceived = false;

      ws.on("open", () => console.log("[qq] 网关连接已建立，等待 Hello…"));
      ws.on("message", async (raw) => {
        let p;
        try { p = JSON.parse(raw.toString()); } catch { return; }
        const op = p.op;

        if (op === 10) { // Hello
          helloReceived = true;
          const hb = p.d.heartbeat_interval;
          this._startHeartbeat(hb);
          // Identify 或 Resume
          const payload = this.sessionId
            ? { op: 6, d: { token: `QQBot ${token}`, session_id: this.sessionId, seq: this.seq } }
            : {
                op: 2,
                d: {
                  token: `QQBot ${token}`,
                  intents,
                  shard: [0, 1],
                  properties: { os: "linux", browser: "muse-qq-bridge", device: "muse-qq-bridge" },
                },
              };
          ws.send(JSON.stringify(payload));
        } else if (op === 0) { // Dispatch
          if (p.s) this.seq = p.s;
          this._handleDispatch(p);
        } else if (op === 11) { // Heartbeat ACK
          // ok
        } else if (op === 9) { // Invalid Session
          console.log("[qq] session 无效，重新 Identify");
          this.sessionId = null; this.seq = null;
          ws.close(4000, "invalid session");
        } else if (op === 7) { // Reconnect
          console.log("[qq] 服务端要求重连");
          ws.close(4000, "reconnect");
        }
      });

      ws.on("close", (code, reason) => {
        console.log(`[qq] 网关断开: ${code} ${reason}`);
        this._stopHeartbeat();
        if (!helloReceived) reject(new Error(`网关连接失败: ${code}`));
        // 由 daemon 外层重连
        this.onClose?.(code);
      });
      ws.on("error", (e) => {
        console.error("[qq] 网关错误:", e.message);
        if (!helloReceived) reject(e);
      });

      // 等 Ready 事件后 resolve
      const readyTimeout = setTimeout(() => reject(new Error("等待 Ready 超时")), 30000);
      this._resolveReady = () => { clearTimeout(readyTimeout); resolve(); };
    });
  }

  _startHeartbeat(interval) {
    this._stopHeartbeat();
    this.heartbeatTimer = setInterval(() => {
      if (this.ws?.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({ op: 1, d: this.seq }));
      }
    }, interval);
  }
  _stopHeartbeat() {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = null;
  }

  _handleDispatch(p) {
    const t = p.t;
    if (t === "READY") {
      this.sessionId = p.d.session_id;
      console.log(`[qq] ✅ 网关 Ready, session=${this.sessionId.slice(0, 8)}…`);
      this._resolveReady?.();
      this.onReady?.(p.d);
      return;
    }
    if (t === "RESUMED") {
      console.log("[qq] ✅ 会话已恢复");
      this._resolveReady?.();
      return;
    }
    // C2C 私聊消息
    if (t === "C2C_MESSAGE_CREATE") {
      const d = p.d;
      this.onMessage?.({
        kind: "c2c",
        messageId: d.id,
        userOpenid: d.author?.user_openid || d.user_openid,
        content: d.content?.trim() || "",
        timestamp: d.timestamp,
        msgSeq: d.msg_seq || 1,
        raw: d,
      });
      return;
    }
    // 群@消息
    if (t === "GROUP_AT_MESSAGE_CREATE") {
      const d = p.d;
      this.onMessage?.({
        kind: "group_at",
        messageId: d.id,
        groupOpenid: d.group_openid,
        userOpenid: d.author?.user_openid,
        content: d.content?.trim() || "",
        timestamp: d.timestamp,
        msgSeq: d.msg_seq || 1,
        raw: d,
      });
      return;
    }
  }

  disconnect() {
    this._stopHeartbeat();
    try { this.ws?.close(); } catch {}
    this.ws = null;
  }
}

/** QQ 消息 content 可能带 @bot 前缀，清理一下 */
export function cleanContent(s) {
  return String(s || "").replace(/<@![^>]+>/g, "").trim();
}
