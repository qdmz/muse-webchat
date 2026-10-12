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

async function drainOutbox() {
  const files = fs.readdirSync(dirs.outbox).filter((f) => f.endsWith(".json"));
  for (const f of files) {
    const p = path.join(dirs.outbox, f);
    let rep;
    try { rep = JSON.parse(fs.readFileSync(p, "utf8")); }
    catch { fs.renameSync(p, path.join(dirs.expired, f + ".badjson")); continue; }
    if (!rep.text?.trim()) { fs.renameSync(p, path.join(dirs.sent, f)); continue; }
    try {
      const text = rep.text.slice(0, 4000);
      if (rep.kind === "group_at") {
        await bot.sendGroup({ groupOpenid: rep.chatId, content: text, msgId: rep.messageId, msgSeq: rep.msgSeq });
      } else {
        await bot.sendC2C({ userOpenid: rep.chatId, content: text, msgId: rep.messageId, msgSeq: rep.msgSeq });
      }
      fs.renameSync(p, path.join(dirs.sent, f));
      console.log(`[send] ✅ 已发出 (${rep.messageId?.slice(0, 8)}…)`);
    } catch (e) {
      console.error(`[send] ❌ 失败 ${rep.messageId}: ${e.message}`);
      // 40034005/304103 = msg_id 过期，转主动消息重试一次
      if (/40034005|304103/.test(e.message) && !rep._retriedActive) {
        try {
          rep._retriedActive = true;
          if (rep.kind === "group_at") {
            await bot.sendGroup({ groupOpenid: rep.chatId, content: rep.text.slice(0, 4000) });
          } else {
            await bot.sendC2C({ userOpenid: rep.chatId, content: rep.text.slice(0, 4000) });
          }
          fs.renameSync(p, path.join(dirs.sent, f));
          console.log(`[send] ✅ 主动消息重试成功`);
          continue;
        } catch (e2) { console.error(`[send] ❌ 重试也失败: ${e2.message}`); }
      }
      // 留在 outbox 下轮重试
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
