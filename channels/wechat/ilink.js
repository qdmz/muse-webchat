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

  /**
   * 上传一个媒体文件到微信 CDN（2026-10-25 由 nanoMuse wechat 渠道实测版回灌）。
   * mediaType: 1=图片 2=视频 3=文件。返回 sendmessage 用的 media 对象与密文长度。
   */
  async uploadMedia({ toUserId, data, mediaType }) {
    const filekey = crypto.randomBytes(16).toString("hex");
    const aesKey = crypto.randomBytes(16);
    const rawMd5 = crypto.createHash("md5").update(data).digest("hex");
    const r = await this._fetch(`/ilink/bot/getuploadurl`, {
      method: "POST",
      body: {
        filekey,
        media_type: mediaType,
        to_user_id: toUserId,
        rawsize: data.length,
        rawfilemd5: rawMd5,
        filesize: paddedSize(data.length),
        no_need_thumb: true,
        aeskey: aesKey.toString("hex"),
      },
      timeoutMs: 30000,
    });
    if (r.errcode) throw new Error(`getuploadurl 失败: ${r.errcode} ${r.errmsg || ""}`);
    let url = r.upload_full_url;
    if (!url && r.upload_param) {
      url = `https://novac2c.cdn.weixin.qq.com/c2c/upload?encrypted_query_param=${encodeURIComponent(r.upload_param)}&filekey=${encodeURIComponent(filekey)}`;
    }
    if (!url) throw new Error(`getuploadurl 无上传地址: ${JSON.stringify(r).slice(0, 160)}`);
    const cipher = encryptAesEcb(data, aesKey);
    let param = null;
    let lastErr = null;
    for (let i = 0; i < 3 && !param; i++) {
      try {
        const res = await httpsRawPost(url, cipher);
        if (res.status >= 500) { lastErr = new Error(`CDN ${res.status}`); continue; }
        param = res.headers["x-encrypted-param"];
        if (!param) lastErr = new Error(`CDN 未返回 x-encrypted-param (HTTP ${res.status})`);
      } catch (e) { lastErr = e; }
    }
    if (!param) throw lastErr || new Error("CDN 上传失败");
    return {
      media: {
        encrypt_query_param: param,
        aes_key: Buffer.from(aesKey.toString("hex"), "ascii").toString("base64"),
        encrypt_type: 1,
      },
      cipherSize: cipher.length,
    };
  }

  /** 发送一条带 item_list 的消息（文字/媒体/混合） */
  async sendItems({ toUserId, contextToken, items }) {
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
          item_list: items,
        },
        base_info: this.baseInfo(),
      },
      timeoutMs: 30000,
    });
  }

  /** 发图片（caption 非空时与图片同条发送） */
  async sendImage({ toUserId, contextToken, data, caption = "" }) {
    const up = await this.uploadMedia({ toUserId, data, mediaType: 1 });
    const items = [];
    if (caption) items.push({ type: 1, text_item: { text: caption } });
    items.push({ type: 2, image_item: { media: up.media, mid_size: up.cipherSize } });
    return this.sendItems({ toUserId, contextToken, items });
  }

  /** 发文件 */
  async sendFile({ toUserId, contextToken, data, fileName, caption = "" }) {
    const up = await this.uploadMedia({ toUserId, data, mediaType: 3 });
    const items = [];
    if (caption) items.push({ type: 1, text_item: { text: caption } });
    items.push({ type: 4, file_item: { media: up.media, file_name: fileName, len: String(data.length) } });
    return this.sendItems({ toUserId, contextToken, items });
  }

  /** 发视频 */
  async sendVideo({ toUserId, contextToken, data, caption = "" }) {
    const up = await this.uploadMedia({ toUserId, data, mediaType: 2 });
    const items = [];
    if (caption) items.push({ type: 1, text_item: { text: caption } });
    items.push({ type: 5, video_item: { media: up.media, video_size: up.cipherSize } });
    return this.sendItems({ toUserId, contextToken, items });
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

// ---------- 媒体加解密（AES-128-ECB + PKCS#7；Node crypto 默认带填充） ----------
function encryptAesEcb(plain, key) {
  const cipher = crypto.createCipheriv("aes-128-ecb", key, null);
  return Buffer.concat([cipher.update(plain), cipher.final()]);
}
function paddedSize(n) { return (Math.floor(n / 16) + 1) * 16; }

/** 原始字节 POST（CDN 上传用），返回 { status, headers } */
function httpsRawPost(urlStr, data, timeoutMs = 60000) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const req = https.request(
      {
        hostname: u.hostname, port: u.port || 443, path: u.pathname + u.search,
        method: "POST",
        headers: { "Content-Type": "application/octet-stream", "Content-Length": data.length },
        agent: PROXY_AGENT || undefined,
        timeout: timeoutMs,
      },
      (res) => {
        res.resume();
        res.on("end", () => resolve({ status: res.statusCode, headers: res.headers }));
      }
    );
    req.on("timeout", () => req.destroy(new Error("CDN 上传超时")));
    req.on("error", reject);
    req.write(data);
    req.end();
  });
}

// ---------- 图片下载（CDN + AES-128-ECB 解密） ----------
import { createDecipheriv } from "node:crypto";

function decryptAesEcb(ciphertext, key) {
  const decipher = createDecipheriv("aes-128-ecb", key, null);
  return Buffer.concat([decipher.update(ciphertext), decipher.final()]);
}

function parseAesKey(aesKeyBase64) {
  const decoded = Buffer.from(aesKeyBase64, "base64");
  if (decoded.length === 16) return decoded;
  if (decoded.length === 32 && /^[0-9a-fA-F]{32}$/.test(decoded.toString("ascii"))) {
    return Buffer.from(decoded.toString("ascii"), "hex");
  }
  throw new Error(`aes_key 格式不对: ${decoded.length} bytes`);
}

async function fetchBytes(url, timeoutMs = 30000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal: ctrl.signal,
      agent: PROXY_AGENT || undefined,
    });
    if (!res.ok) throw new Error(`CDN 下载失败: ${res.status}`);
    return Buffer.from(await res.arrayBuffer());
  } finally {
    clearTimeout(t);
  }
}

/**
 * 从图片 item 下载并解密图片
 * @param {object} imageItem - it.image_item
 * @param {string} cdnBaseUrl - CDN 基础地址（从 bot_token.json 的 base_url 推导或配置）
 * @returns {Buffer|null} 解密后的图片数据
 */
export async function downloadImage(imageItem, cdnBaseUrl) {
  const media = imageItem?.media;
  if (!media?.encrypt_query_param && !media?.full_url) return null;

  const url = media.full_url ||
    `${cdnBaseUrl}/download?encrypted_query_param=${encodeURIComponent(media.encrypt_query_param)}`;

  const raw = await fetchBytes(url);

  // 有 AES key 则解密，否则直接返回
  const aesKeyBase64 = imageItem.aeskey
    ? Buffer.from(imageItem.aeskey, "hex").toString("base64")
    : media.aes_key;
  if (!aesKeyBase64) return raw;

  const key = parseAesKey(aesKeyBase64);
  return decryptAesEcb(raw, key);
}

/** 从消息中提取所有图片 item */
export function extractImages(msg) {
  const images = [];
  for (const it of msg.item_list || []) {
    if (it.type === 2 && it.image_item?.media) {
      images.push(it.image_item);
    }
  }
  return images;
}
