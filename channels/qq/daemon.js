/**
 * muse-qq-bridge daemon
 * 主循环: QQ 网关 WebSocket 收 C2C/群@消息 → inbox/
 *         扫 outbox/ → REST 发出 → 归档 sent/
 */
import fs from "node:fs";
import path from "node:path";
import { QQBot, cleanContent } from "./qq.js";

const ROOT = path.resolve(new URL(".", import.meta.url).pathname, "..");
const SECRETS_FILE = path.join(ROOT, "secrets.env");
const dirs = {
  inbox: path.join(ROOT, "inbox"),
  outbox: path.join(ROOT, "outbox"),
  sent: path.join(ROOT, "sent"),
  expired: path.join(ROOT, "expired"),
};
for (const d of Object.values(dirs)) fs.mkdirSync(d, { recursive: true });

function loadSecrets() {
  if (!fs.existsSync(SECRETS_FILE)) {
    console.error(`❌ 找不到 ${SECRETS_FILE}`);
    console.error(`   格式:\n   QQ_APP_ID=...\n   QQ_APP_SECRET=...`);
    process.exit(1);
  }
  const env = Object.fromEntries(
    fs.readFileSync(SECRETS_FILE, "utf8").split("\n")
      .filter((l) => l.includes("=") && !l.trim().startsWith("#"))
      .map((l) => { const i = l.indexOf("="); return [l.slice(0, i).trim(), l.slice(i + 1).trim()]; })
  );
  if (!env.QQ_APP_ID || !env.QQ_APP_SECRET) {
    console.error("❌ secrets.env 缺少 QQ_APP_ID / QQ_APP_SECRET");
    process.exit(1);
  }
  return env;
}

function atomicWrite(dir, name, obj) {
  const tmp = path.join(dir, name + ".tmp");
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 2));
  fs.renameSync(tmp, path.join(dir, name));
}

const secrets = loadSecrets();
const bot = new QQBot({ appId: secrets.QQ_APP_ID, appSecret: secrets.QQ_APP_SECRET });

bot.onMessage = (ev) => {
  const text = cleanContent(ev.content);
  if (!text) return;
  const id = ev.messageId;
  const dest = path.join(dirs.inbox, `${id}.json`);
  if (fs.existsSync(dest) || fs.existsSync(dest + ".claimed")) return;
  atomicWrite(dirs.inbox, `${id}.json`, {
    messageId: id,
    chatId: ev.kind === "c2c" ? ev.userOpenid : ev.groupOpenid,
    chatType: ev.kind === "c2c" ? "single" : "group",
    senderUserId: ev.userOpenid,
    kind: ev.kind,
    groupOpenid: ev.groupOpenid || null,
    text,
    ts: ev.timestamp,
    msgSeq: ev.msgSeq,
  });
  console.log(`[recv] 📩 [${ev.kind}] ${text.slice(0, 60)}`);
};

// 被动回复窗口（官方 60 分钟/5 分钟，保守取值；2026-10-25 nanoMuse 实测口径）
const PASSIVE_WINDOW_S = { c2c: 3300, group_at: 280 };
const MAX_SEND_ATTEMPTS = 3;

function saveRep(p, rep) {
  fs.writeFileSync(p, JSON.stringify(rep, null, 2));
}

/**
 * 发送一条回复。rep 可带：
 *  - imageFile: 130 本地图片路径 → 走官方富媒体两步流程真发图（file_info 缓存在 rep._fileInfo）
 *  - imageUrl : 旧方式的公网图片 URL（msg_type=1，保留兼容）
 * 窗口内带 msg_id 被动回复；msg_id 被判过期（40034005/304103）时在同一次调用内降级主动一次。
 */
async function sendReply(rep) {
  const isGroup = rep.kind === "group_at";
  const send = (opts) =>
    isGroup
      ? bot.sendGroup({ groupOpenid: rep.chatId, ...opts })
      : bot.sendC2C({ userOpenid: rep.chatId, ...opts });

  if (rep.imageFile && !rep._fileInfo) {
    const buf = fs.readFileSync(rep.imageFile);
    rep._fileInfo = await bot.uploadRichFile({
      kind: rep.kind, targetId: rep.chatId, fileType: 1, dataB64: buf.toString("base64"),
    });
    console.log(`[send] 📎 富媒体已上传 (${rep.imageFile})`);
  }
  const media = rep._fileInfo || null;
  const text = (rep.text || "").slice(0, 4000);
  const ageS = rep.ts ? (Date.now() - new Date(rep.ts).getTime()) / 1000 : 0;
  const inWindow = ageS <= (PASSIVE_WINDOW_S[rep.kind] ?? 280);
  let seq = rep.msgSeq || 1;

  const attempt = async (passive) => {
    const msgId = passive && rep.messageId ? rep.messageId : null;
    if (media) {
      const r = await send({ content: text.slice(0, 3000) || undefined, media, msgId, msgSeq: seq });
      let rest = text.slice(3000);
      while (rest) {
        seq += 1;
        await send({ content: rest.slice(0, 3000), msgId, msgSeq: seq });
        rest = rest.slice(3000);
      }
      return r;
    }
    return send({ content: text, imageUrl: rep.imageUrl || null, msgId, msgSeq: seq });
  };

  try {
    return await attempt(inWindow);
  } catch (e) {
    if (inWindow && /40034005|304103/.test(e.message)) {
      console.log("[send] msg_id 已过期，降级为主动消息重试一次");
      return await attempt(false);
    }
    throw e;
  }
}

async function drainOutbox() {
  const files = fs.readdirSync(dirs.outbox).filter((f) => f.endsWith(".json"));
  for (const f of files) {
    const p = path.join(dirs.outbox, f);
    let rep;
    try { rep = JSON.parse(fs.readFileSync(p, "utf8")); }
    catch { fs.renameSync(p, path.join(dirs.expired, f + ".badjson")); continue; }
    if (!rep.text?.trim() && !rep.imageFile && !rep.imageUrl) { fs.renameSync(p, path.join(dirs.sent, f)); continue; }
    try {
      const r = await sendReply(rep);
      saveRep(p, rep); // 把 _fileInfo 等状态随文件一起归档，便于追溯
      fs.renameSync(p, path.join(dirs.sent, f));
      console.log(`[send] ✅ 已发出 (${rep.messageId?.slice(0, 8)}…) qq_msg_id=${r?.id || "?"}`);
    } catch (e) {
      // 尝试计数落盘：跨轮次/重启都算数，绝不再出现"同一个失败每 2 秒重演一遍"
      rep._attempts = (rep._attempts || 0) + 1;
      rep._lastError = e.message;
      rep._lastAttemptAt = new Date().toISOString();
      console.error(`[send] ❌ 失败(第 ${rep._attempts} 次) ${rep.messageId}: ${e.message}`);
      if (rep._attempts >= MAX_SEND_ATTEMPTS) {
        saveRep(p, rep);
        fs.renameSync(p, path.join(dirs.expired, f + ".failed"));
        console.error(`[send] 🗄 已归档为永久失败: ${f}`);
      } else {
        saveRep(p, rep);
      }
    }
  }
}

let running = true;
process.on("SIGINT", () => { running = false; bot.disconnect(); });
process.on("SIGTERM", () => { running = false; bot.disconnect(); });

console.log("muse-qq-bridge daemon 启动中…");

async function main() {
  // token 预检
  try {
    await bot.getAccessToken();
    console.log("✅ QQ access_token 获取成功");
  } catch (e) {
    console.error(`❌ QQ 鉴权失败: ${e.message}`);
    console.error("   检查 secrets.env 的 QQ_APP_ID / QQ_APP_SECRET 是否正确");
    process.exit(1);
  }

  // 网关连接（带 intent 降级）
  const intentSets = [
    { name: "完整 (C2C+群@+频道)", v: (1 << 25) | (1 << 30) },
    { name: "降级 (仅频道)", v: (1 << 30) },
  ];
  let connected = false;
  for (const it of intentSets) {
    try {
      console.log(`[qq] 尝试连接网关 (${it.name})…`);
      bot.onClose = () => {};
      await bot.connect({ intents: it.v });
      connected = true;
      break;
    } catch (e) {
      console.error(`[qq] ${it.name} 连接失败: ${e.message}`);
      bot.disconnect();
    }
  }
  if (!connected) {
    console.error("❌ 网关连接失败，退出");
    process.exit(2);
  }

  // 断线重连
  bot.onClose = async (code) => {
    if (!running) return;
    console.log(`[qq] 断线重连中…`);
    await new Promise((r) => setTimeout(r, 5000));
    if (!running) return;
    try { await bot.connect({ intents: (1 << 25) | (1 << 30) }); }
    catch (e) { console.error("[qq] 重连失败:", e.message); process.exit(2); }
  };

  console.log("muse-qq-bridge daemon 已启动，等待 QQ 消息…");
  // outbox 轮询
  while (running) {
    await drainOutbox();
    await new Promise((r) => setTimeout(r, 2000));
  }
}

main().catch((e) => { console.error("daemon 异常:", e.message); process.exit(1); });
