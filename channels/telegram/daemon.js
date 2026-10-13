/**
 * muse-telegram-bridge daemon（双 bot）
 * getUpdates 长轮询 → inbox/ → hook 唤醒
 * 扫 outbox/ → sendMessage/sendPhoto/sendDocument → sent/
 */
import fs from "node:fs";
import path from "node:path";
import { TelegramBot, parseUpdate, downloadFile } from "./telegram.js";

const ROOT = path.resolve(new URL(".", import.meta.url).pathname, "..");
process.env.TG_STATE_DIR = ROOT;
const SECRETS_FILE = path.join(ROOT, "secrets.env");
const MEDIA_DIR = path.join(ROOT, "media");
const dirs = {
  inbox: path.join(ROOT, "inbox"),
  outbox: path.join(ROOT, "outbox"),
  sent: path.join(ROOT, "sent"),
  expired: path.join(ROOT, "expired"),
};
for (const d of Object.values(dirs)) fs.mkdirSync(d, { recursive: true });
fs.mkdirSync(MEDIA_DIR, { recursive: true });

function loadBots() {
  const env = Object.fromEntries(
    fs.readFileSync(SECRETS_FILE, "utf8").split("\n")
      .filter((l) => l.includes("=") && !l.trim().startsWith("#"))
      .map((l) => { const i = l.indexOf("="); return [l.slice(0, i).trim(), l.slice(i + 1).trim()]; })
  );
  const raw = env.TELEGRAM_BOTS || "";
  return raw.split(";").filter(Boolean).map((s) => {
    const [name, id, secret] = s.split(":");
    return { name, token: `${id}:${secret}` };
  });
}

function atomicWrite(dir, name, obj) {
  const tmp = path.join(dir, name + ".tmp");
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 2));
  fs.renameSync(tmp, path.join(dir, name));
}

const bots = loadBots().map((b) => new TelegramBot(b));
const botByName = Object.fromEntries(bots.map((b) => [b.name, b]));
let running = true;
process.on("SIGINT", () => { running = false; });
process.on("SIGTERM", () => { running = false; });

const seenUpdates = new Set();

async function pollBot(bot) {
  while (running) {
    let updates;
    try {
      updates = await bot.getUpdates();
    } catch (e) {
      console.error(`[tg:${bot.name}] 轮询失败: ${e.message}`);
      await new Promise((r) => setTimeout(r, 5000));
      continue;
    }
    for (const u of updates) {
      if (seenUpdates.has(u.update_id)) continue;
      seenUpdates.add(u.update_id);
      if (seenUpdates.size > 2000) {
        const it = seenUpdates.values();
        seenUpdates.delete(it.next().value);
      }
      const m = parseUpdate(u);
      if (!m) continue;
      // 群组消息只处理 @bot 或回复 bot 的（私聊全收）
      if (m.chatType === "group" || m.chatType === "supergroup") {
        const txt = m.text || "";
        const mentioned = txt.includes(`@${bot.me.username}`);
        if (!mentioned) continue;
        m.text = txt.replace(`@${bot.me.username}`, "").trim();
      }

      // 下载媒体
      const mediaPaths = [];
      for (const media of m.media) {
        try {
          const ext = media.kind === "photo" ? "jpg"
            : media.kind === "voice" ? "ogg"
            : media.kind === "video" ? "mp4"
            : (media.fileName?.split(".").pop() || "bin");
          const dest = path.join(MEDIA_DIR, `${bot.name}_${u.update_id}_${media.kind}.${ext}`);
          await downloadFile(bot.token, media.fileId, dest);
          mediaPaths.push({ kind: media.kind, path: dest, fileName: media.fileName || "" });
          console.log(`[tg:${bot.name}] 📎 媒体已下载: ${dest}`);
        } catch (e) {
          console.error(`[tg:${bot.name}] 📎 下载失败: ${e.message}`);
        }
      }

      const id = `${bot.name}_${u.update_id}`;
      const item = {
        messageId: id,
        botName: bot.name,
        chatId: m.chatId,
        chatType: m.chatType === "private" ? "single" : "group",
        chatTitle: m.chatTitle,
        senderUserId: m.fromId,
        senderNickname: m.fromName,
        senderUsername: m.fromUsername,
        replyToMessageId: m.messageId,
        text: m.text,
        media: mediaPaths,
        ts: new Date(m.date * 1000).toISOString(),
      };
      const dest = path.join(dirs.inbox, `${id}.json`);
      if (fs.existsSync(dest) || fs.existsSync(dest + ".claimed")) continue;
      atomicWrite(dirs.inbox, `${id}.json`, item);
      const label = m.chatType === "private" ? "私聊" : `群 ${m.chatTitle || ""}`;
      console.log(`[tg:${bot.name}] 📩 [${label}] ${m.fromName}: ${(m.text || "").slice(0, 50)}`);
    }
  }
}

async function drainOutbox() {
  while (running) {
    const files = fs.readdirSync(dirs.outbox).filter((f) => f.endsWith(".json"));
    for (const f of files) {
      const p = path.join(dirs.outbox, f);
      let rep;
      try { rep = JSON.parse(fs.readFileSync(p, "utf8")); }
      catch { fs.renameSync(p, path.join(dirs.expired, f + ".badjson")); continue; }
      const bot = botByName[rep.botName];
      if (!bot) {
        console.error(`[send] ❌ 未知 bot: ${rep.botName}`);
        fs.renameSync(p, path.join(dirs.expired, f + ".nobots"));
        continue;
      }
      try {
        // 先发媒体（如果有），再发文字
        if (rep.photoUrl) {
          await bot.sendPhoto(rep.chatId, rep.photoUrl, { caption: rep.caption || "" });
        } else if (rep.documentUrl) {
          await bot.sendDocument(rep.chatId, rep.documentUrl, { caption: rep.caption || "" });
        }
        if (rep.text?.trim()) {
          await bot.sendMessage(rep.chatId, rep.text.slice(0, 4000),
            rep.replyToMessageId ? { replyTo: rep.replyToMessageId } : {});
        }
        fs.renameSync(p, path.join(dirs.sent, f));
        console.log(`[tg:${rep.botName}] ✅ 已发出 (${rep.messageId?.slice(0, 20)}…)`);
      } catch (e) {
        console.error(`[tg:${rep.botName}] ❌ 发送失败: ${e.message}`);
        // 留在 outbox 下轮重试
      }
    }
    await new Promise((r) => setTimeout(r, 2000));
  }
}

async function main() {
  const defs = loadBots();
  if (!defs.length) {
    console.error("❌ secrets.env 里没有 TELEGRAM_BOTS");
    process.exit(1);
  }
  for (const bot of bots) {
    const me = await bot.init();
    console.log(`[tg:${bot.name}] ✅ 上线 @${me.username} (${me.first_name})`);
  }
  console.log(`muse-telegram-bridge daemon 已启动，${bots.length} 个 bot 长轮询中…`);
  // 每个 bot 独立轮询协程 + 一个 outbox 发送协程
  bots.forEach((b) => pollBot(b));
  await drainOutbox();
}

main().catch((e) => { console.error("daemon 异常:", e.message); process.exit(1); });
