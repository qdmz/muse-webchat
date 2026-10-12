/**
 * 出网代理：沙盒只能经代理出网。node 原生 fetch 不读代理变量，
 * 这里用 undici ProxyAgent（node 18+ 内置 undici，可直接 import）。
 */
import { ProxyAgent } from "undici";

let cached = null;

export function getProxyAgent() {
  if (cached !== undefined) return cached;
  const url =
    process.env.HTTPS_PROXY || process.env.https_proxy ||
    process.env.ALL_PROXY || process.env.all_proxy ||
    process.env.HTTP_PROXY || process.env.http_proxy || "";
  if (!url) { cached = null; return null; }
  // no_proxy 检查
  const np = process.env.NO_PROXY || process.env.no_proxy || "";
  if (np) {
    const host = "ilinkai.weixin.qq.com";
    const hit = np.split(",").some((p) => {
      p = p.trim().toLowerCase().replace(/:\d+$/, "");
      return p && (host === p || host.endsWith("." + p.replace(/^\./, "")));
    });
    if (hit) { cached = null; return null; }
  }
  console.log(`[proxy] 经代理出网: ${url.replace(/:\/\/[^@]+@/, "://***@")}`);
  cached = new ProxyAgent(url);
  return cached;
}
