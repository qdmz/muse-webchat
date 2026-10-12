/**
 * 腾讯元宝自定义 Bot 协议客户端（独立实现，无需 OpenClaw/Hermes）
 * 协议逆向自 openclaw-plugin-yuanbao + chenhg5/cc-connect
 *
 * 流程:
 *  1. POST bot.yuanbao.tencent.com/api/v5/robotLogic/sign-token → {bot_id, token, source}
 *     签名: HMAC-SHA256(key=appSecret, msg=nonce+timestamp+appKey+appSecret)
 *  2. WSS wss://bot-wss.yuanbao.tencent.com/wss/connection → 发 auth-bind (protobuf)
 *  3. 收 inbound_message 推送 → 回 PushAck → 写 inbox/
 *  4. 发消息: send_c2c_message / send_group_message (protobuf)
 *  5. 心跳: 每 30s ping
 */
import crypto from "node:crypto";
import https from "node:https";
import { HttpsProxyAgent } from "https-proxy-agent";
import WebSocket from "ws";

const API_BASE = "https://bot.yuanbao.tencent.com";
const WS_URL = "wss://bot-wss.yuanbao.tencent.com/wss/connection";

function proxyAgent() {
  const url = process.env.HTTPS_PROXY || process.env.https_proxy ||
    process.env.ALL_PROXY || process.env.all_proxy || "";
  return url ? new HttpsProxyAgent(url, { keepAlive: true }) : undefined;
}
const AGENT = proxyAgent();

// ---------- 手写 protobuf (varint + length-delimited) ----------
let _seq = 1;
const nextSeq = () => _seq++;

function wv(b, v) {
  v = BigInt(v);
  while (true) {
    const x = Number(v & 0x7fn);
    v >>= 7n;
    if (v) b.push(x | 0x80);
    else { b.push(x); break; }
  }
}
function wf(b, n, wt) { wv(b, BigInt((n << 3) | wt)); }
function wstr(b, n, s) {
  if (!s) return;
  wf(b, n, 2);
  const u = Buffer.from(s, "utf8");
  wv(b, BigInt(u.length));
  for (const x of u) b.push(x);
}
function wbytes(b, n, d) {
  if (!d || !d.length) return;
  wf(b, n, 2); wv(b, BigInt(d.length));
  for (const x of d) b.push(x);
}
function wvar(b, n, v) { if (!v && v !== 0) return; wf(b, n, 0); wv(b, BigInt(v)); }

function mkHead(cmdType, cmd, msgId, module, needAck = false) {
  const h = [];
  wvar(h, 1, cmdType); wstr(h, 2, cmd); wvar(h, 3, nextSeq());
  wstr(h, 4, msgId); wstr(h, 5, module);
  if (needAck) wvar(h, 6, 1);
  return Buffer.from(h);
}
function connMsg(cmdType, cmd, msgId, module, data, needAck = false) {
  const b = [];
  wbytes(b, 1, mkHead(cmdType, cmd, msgId, module, needAck));
  wbytes(b, 2, data || Buffer.alloc(0));
  return Buffer.from(b);
}

// 解析
function pv(buf) {
  let r = 0n, s = 0n, n = 0;
  for (const x of buf) { n++; r |= BigInt(x & 0x7f) << s; s += 7n; if (!(x & 0x80)) break; }
  return { v: Number(r), n };
}
export function parseFields(buf) {
  const out = [];
  let p = 0;
  buf = Buffer.from(buf);
  while (p < buf.length) {
    const t = pv(buf.slice(p)); p += t.n;
    const fn = t.v >> 3, wt = t.v & 7;
    if (wt === 2) {
      const l = pv(buf.slice(p)); p += l.n;
      out.push([fn, buf.slice(p, p + l.v)]); p += l.v;
    } else if (wt === 0) {
      const v = pv(buf.slice(p)); p += v.n;
      out.push([fn, v.v]);
    } else break;
  }
  return out;
}
const fieldStr = (fields, n) => {
  const f = fields.find((x) => x[0] === n);
  return f ? (Buffer.isBuffer(f[1]) ? f[1].toString("utf8") : String(f[1])) : "";
};

// ---------- sign-token ----------
function httpsPost(urlStr, body, timeoutMs = 15000) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const data = JSON.stringify(body);
    const req = https.request({
      hostname: u.hostname, port: 443, path: u.pathname,
      method: "POST",
      headers: { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(data) },
      agent: AGENT, timeout: timeoutMs,
    }, (res) => {
      const chunks = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => {
        try { resolve(JSON.parse(Buffer.concat(chunks).toString("utf8"))); }
        catch (e) { reject(new Error("sign-token 返回非 JSON")); }
      });
    });
    req.on("timeout", () => req.destroy(new Error("超时")));
    req.on("error", reject);
    req.write(data); req.end();
  });
}

export async function getSignToken(appKey, appSecret) {
  const nonce = crypto.randomBytes(16).toString("hex");
  const ts = new Date(Date.now() + 8 * 3600 * 1000).toISOString()
    .replace("Z", "+08:00").replace(/\.\d{3}/, "");
  const signature = crypto.createHmac("sha256", appSecret)
    .update(nonce + ts + appKey + appSecret).digest("hex");
  const r = await httpsPost(`${API_BASE}/api/v5/robotLogic/sign-token`, {
    app_key: appKey, nonce, signature, timestamp: ts,
  });
  if (r.code !== 0) throw new Error(`sign-token 失败: code=${r.code} ${r.message || ""}`);
  return { botId: r.data.bot_id, token: r.data.token, source: r.data.source || "web" };
}

// ---------- 消息编解码 ----------
function buildAuthBind(uid, source, token) {
  const ai = []; wstr(ai, 1, uid); wstr(ai, 2, source); wstr(ai, 3, token);
  const di = [];
  wstr(di, 1, "muse-yb/1.0"); wstr(di, 2, "linux"); wstr(di, 10, "17"); wstr(di, 24, "muse-yb/1.0");
  const rb = [];
  wstr(rb, 1, "ybBot"); wbytes(rb, 2, Buffer.from(ai)); wbytes(rb, 3, Buffer.from(di));
  return connMsg(0, "auth-bind", `auth_${Date.now()}`, "conn_access", Buffer.from(rb));
}

function buildPushAck(cmd, msgId) {
  return connMsg(3, cmd, msgId, "conn_access", Buffer.alloc(0));
}

function buildPing() {
  return connMsg(0, "ping", `ping_${Date.now()}`, "conn_access", Buffer.alloc(0));
}

// MsgBodyElement{1: msgType, 2: MsgContent{1: text}}
function textBody(text) {
  const mc = []; wstr(mc, 1, text);
  const el = []; wstr(el, 1, "TIMTextElem"); wbytes(el, 2, Buffer.from(mc));
  return Buffer.from(el);
}

export function buildSendC2C({ msgId, toAccount, fromAccount, text }) {
  const b = [];
  wstr(b, 1, msgId); wstr(b, 2, toAccount); wstr(b, 3, fromAccount);
  wbytes(b, 5, textBody(text));
  return connMsg(0, "send_c2c_message", msgId, "yuanbao_openclaw_proxy", Buffer.from(b));
}

export function buildSendGroup({ msgId, groupCode, fromAccount, text }) {
  const b = [];
  wstr(b, 1, msgId); wstr(b, 2, groupCode); wstr(b, 3, fromAccount);
  wbytes(b, 6, textBody(text));
  return connMsg(0, "send_group_message", msgId, "yuanbao_openclaw_proxy", Buffer.from(b));
}

/** 解析 inbound push → {msgId, fromAccount, nickname, groupCode, groupName, text} */
export function parseInbound(data) {
  const f = parseFields(data);
  const msgId = fieldStr(f, 12);
  const fromAccount = fieldStr(f, 2);
  const nickname = fieldStr(f, 4);
  const groupCode = fieldStr(f, 6);
  const groupName = fieldStr(f, 7);
  // msgBody repeated @13
  const texts = [];
  for (const [fn, val] of f) {
    if (fn !== 13) continue;
    const el = parseFields(val);
    const msgType = fieldStr(el, 1);
    const mcBuf = (el.find((x) => x[0] === 2) || [])[1];
    if (!mcBuf) continue;
    const mc = parseFields(mcBuf);
    if (msgType === "TIMTextElem") {
      const t = fieldStr(mc, 1);
      if (t) texts.push(t);
    } else if (msgType === "TIMImageElem") texts.push("[图片]");
    else if (msgType === "TIMFileElem") texts.push(`[文件] ${fieldStr(mc, 12)}`);
    else if (msgType === "TIMSoundElem") texts.push("[语音]");
  }
  return { msgId, fromAccount, nickname, groupCode, groupName, text: texts.join("\n").trim() };
}

export function parseHead(raw) {
  const cm = parseFields(raw);
  const headB = (cm.find((f) => f[0] === 1) || [])[1];
  const dataB = (cm.find((f) => f[0] === 2) || [])[1];
  if (!headB) return null;
  const hf = parseFields(headB);
  const g = (n) => fieldStr(hf, n);
  const needAck = hf.some(([fn, v]) => fn === 6 && v === 1);
  return {
    cmdType: parseInt(g(1) || "0"),
    cmd: g(2), msgId: g(4), module: g(5), needAck,
    data: dataB || Buffer.alloc(0),
  };
}

export { buildAuthBind, buildPushAck, buildPing, WS_URL };
