/**
 * muse-yuanbao-bridge daemon
 * 腾讯元宝自定义 Bot → inbox/outbox/hook 模式
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import WebSocket from "ws";
import { HttpsProxyAgent } from "https-proxy-agent";
import {
  getSignToken, buildAuthBind, buildPushAck, buildPing,
  buildSendC2C, buildSendGroup, parseHead, parseInbound, WS_URL,
} from "./yuanbao.js";

const ROOT = path.resolve(new URL(".", import.meta.url).pathname, "..");
const SECRETS_FILE = path.join(ROOT, "secrets.env");
const TOKEN_CACHE = path.join(ROOT, ".token-cache.json");
const dirs = {
  inbox: path.join(ROOT, "inbox"),
  outbox: path.join(ROOT, "outbox"),
  sent: path.join(ROOT, "sent"),
  expired: path.join(ROOT, "expired"),
};
for (const d of Object.values(dirs)) fs.mkdirSync(d, { recursive: true });

function loadSecrets() {
  const env = Object.fromEntries(
    fs.readFileSync(SECRETS_FILE, "utf8").split("\n")
      .filter((l) => l.includes("=") && !l.trim().startsWith("#"))
      .map((l) => { const i = l.indexOf("="); return [l.slice(0, i).trim(), l.slice(i + 1).trim()]; })
  );
  if (!env.YUANBAO_APP_ID || !env.YUANBAO_APP_SECRET) {
    console.error("❌ secrets.env 缺少 YUANBAO_APP_ID / YUANBAO_APP_SECRET");
    process.exit(1);
  }
  return env;
}
function loadTokenCache() {
  try {
    const c = JSON.parse(fs.readFileSync(TOKEN_CACHE, "utf8"));
    if (c.expiresAt > Date.now() + 600000) return c;
  } catch {}
  return null;
}
function saveTokenCache(c) {
  fs.writeFileSync(TOKEN_CACHE, JSON.stringify(c), { mode: 0o600 });
}
function atomicWrite(dir, name, obj) {
  const tmp = path.join(dir, name + ".tmp");
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 2));
  fs.renameSync(tmp, path.join(dir, name));
}

const secrets = loadSecrets();
const proxyUrl = process.env.HTTPS_PROXY || process.env.https_proxy ||
  process.env.ALL_PROXY || process.env.all_proxy || "";
const agent = proxyUrl ? new HttpsProxyAgent(proxyUrl, { keepAlive: true }) : undefined;
if (agent) console.log("[proxy] 经代理出网");

let botId = null;
let ws = null;
let running = true;
let pingTimer = null;
let authed = false;
const seenMsgIds = new Set();

process.on("SIGINT", () => { running = false; try { ws?.close(); } catch {} process.exit(0); });
process.on("SIGTERM", () => { running = false; try { ws?.close(); } catch {} process.exit(0); });

async function getToken() {
  const cached = loadTokenCache();
  if (cached) { botId = cached.botId; return cached; }
  console.log("[yb] 获取 sign-token…");
  const t = await getSignToken(secrets.YUANBAO_APP_ID, secrets.YUANBAO_APP_SECRET);
  botId = t.botId;
  const c = { ...t, expiresAt: Date.now() + 29 * 24 * 3600 * 1000 };
  saveTokenCache(c);
  console.log(`[yb] ✅ sign-token ok, bot_id=${botId.slice(0, 12)}…`);
  return c;
}

function sendPing() {
  if (ws?.readyState === WebSocket.OPEN) {
    try { ws.send(buildPing()); } catch {}
  }
}

async function drainOutbox() {
  const files = fs.readdirSync(dirs.outbox).filter((f) => f.endsWith(".json"));
  for (const f of files) {
    const p = path.join(dirs.outbox, f);
    let rep;
    try { rep = JSON.parse(fs.readFileSync(p, "utf8")); }
    catch { fs.renameSync(p, path.join(dirs.expired, f + ".badjson")); continue; }
    if (!rep.text?.trim() || !authed) continue;
    const msgId = `muse_${crypto.randomUUID().replace(/-/g, "").slice(0, 16)}`;
    try {
      const buf = rep.chatType === "group"
        ? buildSendGroup({ msgId, groupCode: rep.targetId, fromAccount: botId, text: rep.text.slice(0, 4000) })
        : buildSendC2C({ msgId, toAccount: rep.targetId, fromAccount: botId, text: rep.text.slice(0, 4000) });
      ws.send(buf);
      fs.renameSync(p, path.join(dirs.sent, f));
      console.log(`[send] ✅ 已发出 (${rep.messageId?.slice(0, 8)}…)`);
    } catch (e) {
      console.error(`[send] ❌ 失败: ${e.message}`);
    }
  }
}

function handleMessage(raw) {
  const h = parseHead(raw);
  if (!h) return;
  // 鉴权响应
  if (h.cmd === "auth-bind" && h.cmdType === 1) {
    const { parseFields } = { parseFields: null }; // 用下面直接解析
    console.log(`[yb] auth-bind 响应: cmdType=${h.cmdType}`);
    authed = true;
    console.log("[yb] ✅ WSS 魳权成功，开始收消息");
    return;
  }
  // ping 响应
  if (h.cmd === "ping") return;
  // 业务推送
  if (h.cmdType === 2 && (h.cmd === "inbound_message" || h.cmd === "InboundMessagePush")) {
    if (h.needAck && ws?.readyState === WebSocket.OPEN) {
      try { ws.send(buildPushAck(h.cmd, h.msgId)); } catch {}
    }
    const m = parseInbound(h.data);
    if (!m.msgId || seenMsgIds.has(m.msgId)) return;
    seenMsgIds.add(m.msgId);
    if (seenMsgIds.size > 1000) { const it = seenMsgIds.values(); seenMsgIds.delete(it.next().value); }
    if (m.fromAccount === botId) return; // 自己的回声
    if (!m.text) return;
    const isGroup = !!m.groupCode;
    const item = {
      messageId: m.msgId,
      chatId: isGroup ? `group:${m.groupCode}` : `dm:${m.fromAccount}`,
      chatType: isGroup ? "group" : "single",
      senderUserId: m.fromAccount,
      senderNickname: m.nickname,
      groupName: m.groupName || null,
      targetId: isGroup ? m.groupCode : m.fromAccount,
      text: m.text,
      ts: new Date().toISOString(),
    };
    const dest = path.join(dirs.inbox, `${m.msgId}.json`);
    if (fs.existsSync(dest) || fs.existsSync(dest + ".claimed")) return;
    atomicWrite(dirs.inbox, `${m.msgId}.json`, item);
    console.log(`[recv] 📩 [${isGroup ? "群" : "私"}] ${m.nickname || m.fromAccount}: ${m.text.slice(0, 50)}`);
    return;
  }
  // 其他推送也尝试解析
  if (h.cmdType === 2 && h.data.length > 0) {
    if (h.needAck && ws?.readyState === WebSocket.OPEN) {
      try { ws.send(buildPushAck(h.cmd, h.msgId)); } catch {}
    }
    try {
      const m = parseInbound(h.data);
      if (m.msgId && m.text && !seenMsgIds.has(m.msgId) && m.fromAccount !== botId) {
        seenMsgIds.add(m.msgId);
        const isGroup = !!m.groupCode;
        atomicWrite(dirs.inbox, `${m.msgId}.json`, {
          messageId: m.msgId,
          chatId: isGroup ? `group:${m.groupCode}` : `dm:${m.fromAccount}`,
          chatType: isGroup ? "group" : "single",
          senderUserId: m.fromAccount,
          senderNickname: m.nickname,
          targetId: isGroup ? m.groupCode : m.fromAccount,
          text: m.text,
          ts: new Date().toISOString(),
        });
        console.log(`[recv] 📩 ${m.text.slice(0, 50)}`);
      }
    } catch {}
  }
}

async function connect() {
  const tok = await getToken();
  return new Promise((resolve, reject) => {
    console.log("[yb] 连接 WSS…");
    const w = new WebSocket(WS_URL, { agent, handshakeTimeout: 15000 });
    ws = w;
    let opened = false;

    w.on("open", () => {
      opened = true;
      console.log("[yb] WSS 已连接，发送 auth-bind…");
      w.send(buildAuthBind(tok.botId, tok.source, tok.token));
      // 鉴权后 resolve（handleMessage 里 authed=true 时）
      const checkAuth = setInterval(() => {
        if (authed) { clearInterval(checkAuth); resolve(); }
      }, 500);
      setTimeout(() => { clearInterval(checkAuth); if (!authed) reject(new Error("鉴权超时")); }, 20000);
    });
    w.on("message", (raw) => {
      try { handleMessage(raw); } catch (e) { console.error("[yb] 解析错误:", e.message); }
    });
    w.on("close", (code, reason) => {
      console.log(`[yb] 连接断开: ${code} ${reason.toString().slice(0, 60)}`);
      authed = false;
      if (pingTimer) clearInterval(pingTimer);
      if (!opened) reject(new Error(`连接失败: ${code}`));
      else if (running) scheduleReconnect();
    });
    w.on("error", (e) => {
      console.error("[yb] WS 错误:", e.message);
      if (!opened) reject(e);
    });
  });
}

let reconnectDelay = 5000;
function scheduleReconnect() {
  if (!running) return;
  console.log(`[yb] ${reconnectDelay / 1000}s 后重连…`);
  setTimeout(async () => {
    if (!running) return;
    try {
      authed = false;
      await connect();
      reconnectDelay = 5000;
      startLoops();
    } catch (e) {
      console.error("[yb] 重连失败:", e.message);
      reconnectDelay = Math.min(reconnectDelay * 2, 60000);
      scheduleReconnect();
    }
  }, reconnectDelay);
}

function startLoops() {
  if (pingTimer) clearInterval(pingTimer);
  pingTimer = setInterval(sendPing, 30000);
  // outbox 轮询
  (async () => {
    while (running && authed) {
      await drainOutbox();
      await new Promise((r) => setTimeout(r, 2000));
    }
  })();
}

async function main() {
  console.log("muse-yuanbao-bridge daemon 启动中…");
  await connect();
  startLoops();
  console.log("muse-yuanbao-bridge daemon 已启动，等待元宝消息…");
}

main().catch((e) => { console.error("daemon 异常:", e.message); process.exit(1); });
