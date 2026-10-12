/**
 * 生成微信 ClawBot 扫码二维码，轮询等待扫码确认，成功后写 token.json。
 * 用法: node src/qrcode.js
 */
import fs from "node:fs";
import path from "node:path";
import qrcode from "qrcode-terminal";
import { ILink } from "./ilink.js";


const ROOT = path.resolve(new URL(".", import.meta.url).pathname, "..");
const TOKEN_FILE = path.join(ROOT, "bot_token.json");

const api = new ILink({ proxyAgent: getProxyAgent() });

console.log("正在获取微信 ClawBot 扫码二维码…");
const qr = await api.getBotQrcode();
if (!qr.qrcode || !qr.qrcode_img_content) {
  console.error("获取二维码失败:", JSON.stringify(qr).slice(0, 300));
  process.exit(1);
}
console.log("\n请用微信「扫一扫」扫描下方二维码（约 5 分钟有效）：\n");
qrcode.generate(qr.qrcode_img_content, { small: true });
console.log(`\n二维码链接: ${qr.qrcode_img_content}`);
console.log("等待扫码确认中（Ctrl+C 取消）…");

const t0 = Date.now();
while (Date.now() - t0 < 5 * 60 * 1000) {
  let st;
  try {
    st = await api.getQrcodeStatus(qr.qrcode);
  } catch (e) {
    console.log(`  轮询出错: ${e.message}，重试…`);
    await new Promise((r) => setTimeout(r, 3000));
    continue;
  }
  const s = st.status;
  if (s === "confirmed" && st.bot_token) {
    if (st.baseurl) api.setBaseUrl(st.baseurl);
    const data = {
      bot_token: st.bot_token,
      base_url: st.baseurl || "https://ilinkai.weixin.qq.com",
      ilink_bot_id: st.ilink_bot_id || "",
      ilink_user_id: st.ilink_user_id || "",
      created_at: new Date().toISOString(),
    };
    fs.writeFileSync(TOKEN_FILE, JSON.stringify(data, null, 2), { mode: 0o600 });
    fs.chmodSync(TOKEN_FILE, 0o600);
    console.log("\n✅ 扫码成功！token 已写入 bot_token.json（600 权限）");
    console.log(`   bot_id: ${data.ilink_bot_id}`);
    process.exit(0);
  }
  if (s === "expired") { console.error("\n❌ 二维码已过期，请重新运行"); process.exit(2); }
  if (s === "scaned") console.log("  已扫码，等待手机上确认…");
  else if (s === "need_verifycode" || s === "verify_code_blocked") {
    console.error("\n❌ 需要验证码/被风控，本次无法继续"); process.exit(3);
  }
  await new Promise((r) => setTimeout(r, 2000));
}
console.error("\n❌ 5 分钟超时未扫码");
process.exit(4);
