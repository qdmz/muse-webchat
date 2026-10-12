/**
 * muse-wechat-bridge daemon
 * 主循环: getupdates 长轮询 → 新消息写 inbox/ → hook 唤醒 worker
 *         扫 outbox/ → sendmessage 发出 → 归档 sent/
 */
import fs from "node:fs";
import path from "node:path";
import { ILink, extractText, mdToPlain } from "./ilink.js";


const ROOT = path.resolve(new URL(".", import.meta.url).pathname, "..");
const TOKEN_FILE = path.join(ROOT, "bot_token.json");
const CURSOR_FILE = path.join(ROOT, "cursor.txt");
const dirs = {
  inbox: path.join(ROOT, "inbox"),
  outbox: path.join(ROOT, "outbox"),
  sent: path.join(ROOT, "sent"),
  expired: path.join(ROOT, "expired"),
};
for (const d of Object.values(dirs)) fs.mkdirSync(d, { recursive: true });

function loadToken() {
  if (!fs.existsSync(TOKEN_FILE)) {
    console.error(`❌ 找不到 ${TOKEN_FILE}，先运行: node src/qrcode.js 扫码登录`);
    process.exit(1);
  }
  return JSON.parse(fs.readFileSync(TOKEN_FILE, "utf8"));
}
function loadCursor() {
  try { return fs.readFileSync(CURSOR_FILE, "utf8").trim(); } catch { return ""; }
}
function saveCursor(c) {
  fs.writeFileSync(CURSOR_FILE, c);
}

const tok = loadToken();
const api = new ILink({ baseUrl: tok.base_url, token: tok.bot_token });
let cursor = loadCursor();
let running = true;
process.on("SIGINT", () => { running = false; });
process.on("SIGTERM", () => { running = false; });

console.log(`muse-wechat-bridge daemon 已启动，iLink 长轮询收消息中…`);

// 上线通知（可选，失败不影响）
api._fetch(`/ilink/bot/msg/notifystart`, {
  method: "POST", body: { base_info: api.baseInfo() }, timeoutMs: 8000,
}).catch(() => {});

function atomicWrite(dir, name, obj) {
  const tmp = path.join(dir, name + ".tmp");
  const fin = path.join(dir, name);
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 2));
  fs.renameSync(tmp, fin);
}

/** 处理 outbox：发出所有待发回复 */
async function drainOutbox() {
  const files = fs.readdirSync(dirs.outbox).filter((f) => f.endsWith(".json"));
  for (const f of files) {
    const id = f.replace(/\.json$/, "");
    const p = path.join(dirs.outbox, f);
    let rep;
    try { rep = JSON.parse(fs.readFileSync(p, "utf8")); }
    catch { fs.renameSync(p, path.join(dirs.expired, f + ".badjson")); continue; }
    if (!rep.text?.trim()) {
      // 空回复 = 不回复，直接归档
      fs.renameSync(p, path.join(dirs.sent, f));
      continue;
    }
    try {
      await api.sendMessage({
        toUserId: rep.chatId,
        contextToken: rep.contextToken,
        text: mdToPlain(rep.text).slice(0, 4000),
      });
      fs.renameSync(p, path.join(dirs.sent, f));
      console.log(`[send] ✅ 已发出回复 (${id.slice(0, 8)}…)`);
    } catch (e) {
      console.error(`[send] ❌ 发送失败 ${id}: ${e.message}`);
      // 留在 outbox 下轮重试；超过 24h 窗口的由 hook 侧超时处理
    }
  }
}

let pollFail = 0;
while (running) {
  // 先把积压的回复发掉
  await drainOutbox();

  let r;
  try {
    r = await api.getUpdates(cursor);
    pollFail = 0;
  } catch (e) {
    // 超时 AbortError 是正常情况（35s hold 无消息）
    if (e.name === "AbortError" || /aborted/i.test(e.message)) continue;
    pollFail++;
    console.error(`[poll] 错误 (${pollFail}): ${e.message}`);
    if (pollFail > 10) {
      console.error("[poll] 连续失败 10 次，退出由 keepalive 拉起");
      process.exit(2);
    }
    await new Promise((r2) => setTimeout(r2, 3000));
    continue;
  }
  if (r.errcode) {
    if (r.errcode === -14) {
      console.error("❌ token 失效 (errcode -14)，需重新扫码: node src/qrcode.js");
      process.exit(3);
    }
    console.error(`[poll] errcode=${r.errcode} ${r.errmsg || ""}`);
    await new Promise((r2) => setTimeout(r2, 5000));
    continue;
  }
  // 成功时不一定有 ret 字段；用 get_updates_buf / sync_buf 推进游标
  if (r.get_updates_buf) { cursor = r.get_updates_buf; saveCursor(cursor); }
  else if (r.sync_buf) { cursor = r.sync_buf; saveCursor(cursor); }

  for (const m of r.msgs || []) {
    const text = extractText(m);
    if (!text) continue;
    const id = String(m.message_id);
    const item = {
      messageId: id,
      chatId: m.from_user_id,
      chatType: "single",
      senderUserId: m.from_user_id,
      text,
      ts: new Date(m.create_time_ms || Date.now()).toISOString(),
      contextToken: m.context_token, // 回复时必须带上
    };
    const dest = path.join(dirs.inbox, `${id}.json`);
    if (fs.existsSync(dest) || fs.existsSync(dest + ".claimed")) continue;
    atomicWrite(dirs.inbox, `${id}.json`, item);
    console.log(`[recv] 📩 ${text.slice(0, 60)}`);
  }
}
console.log("daemon 退出");
