/**
 * Telegram Bot API 客户端（双 bot）
 * 收：getUpdates 长轮询（纯出站，无需公网入站）
 * 发：sendMessage / sendPhoto / sendDocument / sendVoice / sendVideo
 */
import https from "node:https";
import fs from "node:fs";
import path from "node:path";
import { HttpsProxyAgent } from "https-proxy-agent";

const API_BASE = "https://api.telegram.org";

function proxyAgent() {
  const url = process.env.HTTPS_PROXY || process.env.https_proxy ||
    process.env.ALL_PROXY || process.env.all_proxy || "";
  return url ? new HttpsProxyAgent(url, { keepAlive: true }) : undefined;
}
const AGENT = proxyAgent();

function apiJson(token, method, params = {}, timeoutMs = 40000) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(params);
    const req = https.request({
      hostname: "api.telegram.org",
      port: 443,
      path: `/bot${token}/${method}`,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(data),
      },
      agent: AGENT,
      timeout: timeoutMs,
    }, (res) => {
      const chunks = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => {
        try {
          const j = JSON.parse(Buffer.concat(chunks).toString("utf8"));
          if (!j.ok) reject(new Error(`Telegram API: ${j.description || "unknown"}`));
          else resolve(j.result);
        } catch (e) { reject(new Error(`非 JSON 响应: ${res.statusCode}`)); }
      });
    });
    req.on("timeout", () => req.destroy(new Error("请求超时")));
    req.on("error", reject);
    req.write(data);
    req.end();
  });
}

/** 下载文件：先 getFile 拿 path，再从 api.telegram.org/file/bot<token>/<path> 拉 */
export async function downloadFile(token, fileId, destPath, timeoutMs = 60000) {
  const info = await apiJson(token, "getFile", { file_id: fileId }, 15000);
  const filePath = info.file_path;
  const url = `${API_BASE}/file/bot${token}/${filePath}`;
  return new Promise((resolve, reject) => {
    const req = https.request(url, { agent: AGENT, timeout: timeoutMs }, (res) => {
      if (res.statusCode !== 200) {
        reject(new Error(`下载失败: ${res.statusCode}`));
        return;
      }
      const ws = fs.createWriteStream(destPath);
      res.pipe(ws);
      ws.on("finish", () => resolve(destPath));
      ws.on("error", reject);
    });
    req.on("timeout", () => req.destroy(new Error("下载超时")));
    req.on("error", reject);
    req.end();
  });
}

export class TelegramBot {
  constructor({ name, token }) {
    this.name = name;   // 如 myheyunbot
    this.token = token;
    this.offset = 0;
    this.me = null;
  }

  async init() {
    this.me = await apiJson(this.token, "getMe", {}, 15000);
    try {
      const saved = JSON.parse(fs.readFileSync(
        path.join(process.env.TG_STATE_DIR || ".", `${this.name}.offset`), "utf8"));
      this.offset = saved.offset || 0;
    } catch {}
    return this.me;
  }

  saveOffset() {
    try {
      fs.writeFileSync(
        path.join(process.env.TG_STATE_DIR || ".", `${this.name}.offset`),
        JSON.stringify({ offset: this.offset }));
    } catch {}
  }

  /** 长轮询一批更新（timeout 30s） */
  async getUpdates() {
    const updates = await apiJson(this.token, "getUpdates", {
      offset: this.offset,
      timeout: 30,
      allowed_updates: ["message", "edited_message", "channel_post", "callback_query"],
    }, 45000);
    for (const u of updates) {
      this.offset = Math.max(this.offset, u.update_id + 1);
    }
    if (updates.length) this.saveOffset();
    return updates;
  }

  async sendMessage(chatId, text, { replyTo = null } = {}) {
    const params = { chat_id: chatId, text: text.slice(0, 4000) };
    if (replyTo) params.reply_to_message_id = replyTo;
    return apiJson(this.token, "sendMessage", params, 15000);
  }

  /** 发图片：支持本地路径（先走 file_id 复用，无则提示用 URL）或公网 URL */
  async sendPhoto(chatId, photo, { caption = "" } = {}) {
    return apiJson(this.token, "sendPhoto", {
      chat_id: chatId, photo, caption: caption.slice(0, 1000),
    }, 30000);
  }

  async sendDocument(chatId, document, { caption = "" } = {}) {
    return apiJson(this.token, "sendDocument", {
      chat_id: chatId, document, caption: caption.slice(0, 1000),
    }, 60000);
  }

  async sendChatAction(chatId, action = "typing") {
    try { await apiJson(this.token, "sendChatAction", { chat_id: chatId, action }, 8000); }
    catch {}
  }
}

/** 从 update 提取统一消息结构 */
export function parseUpdate(u) {
  const msg = u.message || u.edited_message || u.channel_post;
  if (!msg) return null;
  const chat = msg.chat || {};
  const from = msg.from || {};
  const out = {
    updateId: u.update_id,
    messageId: msg.message_id,
    chatId: chat.id,
    chatType: chat.type, // private / group / supergroup / channel
    chatTitle: chat.title || "",
    fromId: from.id,
    fromName: [from.first_name, from.last_name].filter(Boolean).join(" ") || from.username || "",
    fromUsername: from.username || "",
    date: msg.date,
    text: "",
    media: [], // [{kind: photo|document|voice|video, fileId, fileName, mimeType}]
  };
  if (msg.text) out.text = msg.text;
  else if (msg.caption) out.text = msg.caption;
  // 图片取最大尺寸
  if (msg.photo?.length) {
    const best = msg.photo[msg.photo.length - 1];
    out.media.push({ kind: "photo", fileId: best.file_id, fileSize: best.file_size });
    if (!out.text) out.text = "[图片]";
  }
  if (msg.document) {
    out.media.push({
      kind: "document", fileId: msg.document.file_id,
      fileName: msg.document.file_name || "file", mimeType: msg.document.mime_type || "",
      fileSize: msg.document.file_size,
    });
    if (!out.text) out.text = `[文件] ${msg.document.file_name || ""}`;
  }
  if (msg.voice) {
    out.media.push({ kind: "voice", fileId: msg.voice.file_id, fileSize: msg.voice.file_size, duration: msg.voice.duration });
    if (!out.text) out.text = "[语音]";
  }
  if (msg.video) {
    out.media.push({ kind: "video", fileId: msg.video.file_id, fileName: msg.video.file_name || "video.mp4", fileSize: msg.video.file_size });
    if (!out.text) out.text = "[视频]";
  }
  if (msg.sticker) {
    out.text = `[贴纸] ${msg.sticker.emoji || ""}`;
  }
  return out.text || out.media.length ? out : null;
}
