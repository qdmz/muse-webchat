/**
 * iLink 协议客户端（微信 ClawBot 官方插件，纯 HTTP/JSON）。
 * 参考 x1ah/wechat-ilink-demo 与 @tencent-weixin/openclaw-weixin 源码。
 */
import crypto from "node:crypto";
import https from "node:https";
import { HttpsProxyAgent } from "https-proxy-agent";

const CHANNEL_VERSION = "1.0.2";
const DEFAULT_BASE = "https://ilinkai.weixin.qq.com";

function getProxyAgent() {
  const url =
    process.env.HTTPS_PROXY || process.env.https_proxy ||
    process.env.ALL_PROXY || process.env.all_proxy || "";
  if (!url) return null;
  return new HttpsProxyAgent(url, { keepAlive: true });
}
const PROXY_AGENT = getProxyAgent();
if (PROXY_AGENT) console.log("[proxy] 经代理出网");

function httpsJson(urlStr, { method = "GET", headers = {}, body = null, timeoutMs = 15000 }) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const data = body ? JSON.stringify(body) : null;
    const req = https.request(
      {
        hostname: u.hostname,
        port: u.port || 443,
        path: u.pathname + u.search,
        method,
        headers: {
          "Content-Type": "application/json",
          ...(data ? { "Content-Length": Buffer.byteLength(data) } : {}),
          ...headers,
        },
        agent: PROXY_AGENT || undefined,
        timeout: timeoutMs,
      },
      (res) => {
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () => {
          const text = Buffer.concat(chunks).toString("utf8");
          const safe = text.replace(
            /"(message_id|msg_id|svr_id|seq)"\s*:\s*(\d{16,})/g,
            '"$1":"$2"'
          );
          try { resolve(JSON.parse(safe)); }
          catch { reject(new Error(`iLink 返回非 JSON: ${text.slice(0, 120)}`)); }
        });
      }
    );
    req.on("timeout", () => req.destroy(new Error("请求超时")));
    req.on("error", reject);
    if (data) req.write(data);
    req.end();
  });
}

function randomUin() {
  const n = crypto.randomBytes(4).readUInt32BE(0);
  return Buffer.from(String(n), "utf8").toString("base64");
}

function clientVersion() {
  // "1.0.2" → 0x00010002 = 65538
  const [a = 0, b = 0, c = 0] = CHANNEL_VERSION.split(".").map(Number);
  return (a << 16) | (b << 8) | c;
}

export class ILink {
  constructor({ baseUrl = DEFAULT_BASE, token = null } = {}) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
    this.token = token;
  }

  setToken(t) { this.token = t; }
  setBaseUrl(u) { this.baseUrl = u.replace(/\/$/, ""); }

  headers() {
    const h = {
      "AuthorizationType": "ilink_bot_token",
      "X-WECHAT-UIN": randomUin(),
      "iLink-App-Id": "bot",
      "iLink-App-ClientVersion": String(clientVersion()),
    };
    if (this.token) h["Authorization"] = `Bearer ${this.token}`;
    return h;
  }

  async _fetch(path, { method = "GET", body = null, timeoutMs = 15000 } = {}) {
    return httpsJson(this.baseUrl + path, {
      method, headers: this.headers(), body, timeoutMs,
    });
  }

  baseInfo() { return { channel_version: CHANNEL_VERSION }; }

  /** ① 获取扫码二维码 → { qrcode(轮询key), qrcode_img_content(二维码URL) } */
  async getBotQrcode() {
    return this._fetch(`/ilink/bot/get_bot_qrcode?bot_type=3`, { timeoutMs: 15000 });
  }

  /** ② 轮询扫码状态 → { status, bot_token?, baseurl?, ilink_bot_id?, ilink_user_id? } */
  async getQrcodeStatus(qrcodeKey) {
    return this._fetch(
      `/ilink/bot/get_qrcode_status?qrcode=${encodeURIComponent(qrcodeKey)}`,
      { timeoutMs: 40000 }
    );
  }

  /** ③ 长轮询收消息（服务端 hold 35s） */
  async getUpdates(cursor) {
    return this._fetch(`/ilink/bot/getupdates`, {
      method: "POST",
      body: { get_updates_buf: cursor || "", base_info: this.baseInfo() },
      timeoutMs: 45000,
    });
  }

  /** ④ 发消息（必须带 context_token，只能回复） */
  async sendMessage({ toUserId, contextToken, text }) {
    const clientId = `muse-${crypto.randomUUID()}`;
    return this._fetch(`/ilink/bot/sendmessage`, {
      method: "POST",
      body: {
        msg: {
          from_user_id: "",
          to_user_id: toUserId,
          client_id: clientId,
          message_type: 2,
          message_state: 2,
          context_token: contextToken,
          item_list: [{ type: 1, text_item: { text } }],
        },
        base_info: this.baseInfo(),
      },
      timeoutMs: 15000,
    });
  }

  /** typing 状态 */
  async getTypingTicket({ userId, contextToken }) {
    const r = await this._fetch(`/ilink/bot/getconfig`, {
      method: "POST",
      body: { ilink_user_id: userId, context_token: contextToken, base_info: this.baseInfo() },
      timeoutMs: 10000,
    });
    return r.typing_ticket;
  }
  async sendTyping({ userId, ticket, status = 1 }) {
    return this._fetch(`/ilink/bot/sendtyping`, {
      method: "POST",
      body: { ilink_user_id: userId, typing_ticket: ticket, status, base_info: this.baseInfo() },
      timeoutMs: 10000,
    });
  }
}

/** 从入站消息提取纯文本（跳过 bot 自己的回声） */
export function extractText(msg) {
  if (msg.message_type === 2) return null; // bot 自己发的
  if (String(msg.from_user_id || "").endsWith("@im.bot")) return null;
  const parts = [];
  for (const it of msg.item_list || []) {
    if (it.type === 1 && it.text_item?.text) parts.push(it.text_item.text);
    else if (it.type === 3 && it.voice_item?.text) parts.push(`[语音转写] ${it.voice_item.text}`);
    else if (it.type === 2) parts.push("[图片]");
    else if (it.type === 4) parts.push(`[文件] ${it.file_item?.file_name || ""}`);
    else if (it.type === 5) parts.push("[视频]");
  }
  const t = parts.join("\n").trim();
  return t || null;
}

/** markdown 转微信纯文本 */
export function mdToPlain(s) {
  return String(s)
    .replace(/```[\s\S]*?```/g, (m) => m.replace(/```\w*\n?/g, "").trim())
    .replace(/`([^`]+)`/g, "$1")
    .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
    .replace(/^#{1,6}\s+/gm, "")
    .replace(/\*\*([^*]+)\*\*/g, "$1")
    .replace(/__([^_]+)__/g, "$1")
    .replace(/^\s*[-*]\s+/gm, "• ")
    .trim();
}
