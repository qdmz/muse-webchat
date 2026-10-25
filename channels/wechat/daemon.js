/**
 * muse-wechat-bridge daemon
 * 主循环: getupdates 长轮询 → 新消息写 inbox/ → hook 唤醒 worker
 *         扫 outbox/ → sendmessage 发出 → 归档 sent/
 */
import fs from "node:fs";
import path from "node:path";
import { ILink, extractText, extractImages, downloadImage, mdToPlain } from "./ilink.js";


const ROOT = path.resolve(new URL(".", import.meta.url).pathname, "..");
const TOKEN_FILE = path.join(ROOT, "bot_token.json");
const CURSOR_FILE = path.join(ROOT, "cursor.txt");
const MEDIA_DIR = path.join(ROOT, "media");
const dirs = {
  inbox: path.join(ROOT, "inbox"),
  outbox: path.join(ROOT, "outbox"),
  sent: path.join(ROOT, "sent"),
  expired: path.join(ROOT, "expired"),
};
for (const d of Object.values(dirs)) fs.mkdirSync(d, { recursive: true });
fs.mkdirSync(MEDIA_DIR, { recursive: true });

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
    const mediaPath = rep.imageFile || rep.videoFile || rep.filePath || null;
    if (!rep.text?.trim() && !mediaPath) {
      // 空回复 = 不回复，直接归档
      fs.renameSync(p, path.join(dirs.sent, f));
      continue;
    }
    try {
      const caption = rep.text ? mdToPlain(rep.text).slice(0, 2000) : "";
      if (mediaPath) {
        // 2026-10-25 回灌：出站媒体（图片/视频/文件），走 iLink 加密 CDN 上传
        if (!fs.existsSync(mediaPath)) throw new Error(`媒体文件不存在: ${mediaPath}`);
        const data = fs.readFileSync(mediaPath);
        if (rep.imageFile) {
          await api.sendImage({ toUserId: rep.chatId, contextToken: rep.contextToken, data, caption });
        } else if (rep.videoFile) {
          await api.sendVideo({ toUserId: rep.chatId, contextToken: rep.contextToken, data, caption });
        } else {
          await api.sendFile({ toUserId: rep.chatId, contextToken: rep.contextToken, data, fileName: path.basename(mediaPath), caption });
        }
        console.log(`[send] 📎 已发出媒体 (${id.slice(0, 8)}…) ${path.basename(mediaPath)}`);
      } else {
        await api.sendMessage({
          toUserId: rep.chatId,
          contextToken: rep.contextToken,
          text: mdToPlain(rep.text).slice(0, 4000),
        });
      }
      fs.renameSync(p, path.join(dirs.sent, f));
      console.log(`[send] ✅ 已发出回复 (${id.slice(0, 8)}…)`);
    } catch (e) {
      console.error(`[send] ❌ 发送失败 ${id}: ${e.message}`);
      // 尝试计数落盘：确定性失败（如媒体不存在）直接归档，其余留 outbox 下轮重试
      rep._attempts = (rep._attempts || 0) + 1;
      rep._lastError = e.message;
      try { fs.writeFileSync(p, JSON.stringify(rep, null, 2)); } catch {}
      if (/不存在/.test(e.message) || rep._attempts >= 5) {
        fs.renameSync(p, path.join(dirs.expired, f + ".failed"));
        console.error(`[send] 🗄 已归档为永久失败: ${f}`);
      }
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
    const images = extractImages(m);
    if (!text && images.length === 0) continue;
    const id = String(m.message_id);

    // 下载图片
    const imagePaths = [];
    for (let i = 0; i < images.length; i++) {
      try {
        // CDN 地址从 base_url 推导：ilinkai.weixin.qq.com
        const cdnBase = tok.base_url.replace("ilinkai.weixin.qq.com", "cdn.weixin.qq.com");
        const buf = await downloadImage(images[i], cdnBase);
        if (buf) {
          const ext = buf[0] === 0xff && buf[1] === 0xd8 ? "jpg" :
                      buf[0] === 0x89 ? "png" : "bin";
          const imgPath = path.join(MEDIA_DIR, `${id}_${i}.${ext}`);
          fs.writeFileSync(imgPath, buf);
          imagePaths.push(imgPath);
          console.log(`[recv] 🖼️ 图片已下载: ${imgPath} (${buf.length} bytes)`);
        }
      } catch (e) {
        console.error(`[recv] 🖼️ 图片下载失败: ${e.message}`);
      }
    }

    const item = {
      messageId: id,
      chatId: m.from_user_id,
      chatType: "single",
      senderUserId: m.from_user_id,
      text: text || (imagePaths.length ? "[图片]" : ""),
      images: imagePaths,
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
