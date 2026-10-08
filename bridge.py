#!/usr/bin/env python3
"""
chat-bridge: 极简 Web 聊天桥接后端（纯 Python 标准库，无第三方依赖）

架构：
  [用户浏览器] ←→ 本服务(8090) ←→ Muse 沙盒(hook 定时轮询)
  - 用户在 Web UI 发消息/传文件 → 存入队列
  - Muse 的 hook 每 N 秒 GET /api/agent/inbox 取走未处理的用户消息
  - Muse 处理完 POST /api/agent/reply 写回回复（可带文件链接）
  - Web UI 轮询 /api/poll 展示新消息

配置（环境变量）：
  BRIDGE_PORT      监听端口，默认 8090
  BRIDGE_DATA      数据目录，默认 /opt/chat-bridge/data
  BRIDGE_PASSWORD  Web UI 登录密码
  BRIDGE_TOKEN     Agent API 的 bearer token

Web UI（需要登录 session）：
  GET  /                  聊天页
  POST /api/login         {password}
  POST /api/send          {text, attachments?: [fid]}
  POST /api/upload        multipart/form-data file=... → {fid, name, size, mime}
  GET  /api/poll?after=<id>
  GET  /files/<key>       下载文件（u_<fid> 用户上传，o_<name> Muse 发出）

Agent API（需要 Authorization: Bearer <token>）：
  GET  /api/agent/inbox?after=<id>   取 id 之后的用户消息（含附件元数据）
  GET  /api/agent/history?limit=<n>  取最近 n 条全部消息（给 worker 补上下文）
  POST /api/agent/reply              {reply_to, text} 写回回复
  POST /api/agent/notify             {text} 推送通知（显示为系统通知样式）
  GET  /api/agent/health             健康检查

文件约定（worker 在本机，直接读写磁盘）：
  用户上传：$DATA_DIR/uploads/<fid>_<safe_name>
  Muse 发出：worker 写到 $DATA_DIR/outbox/<name>，回复文本里带
             {PUBLIC_BASE}/files/o_<name> 链接即可
"""

import hashlib
import hmac
import json
import mimetypes
import os
import re
import secrets
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, unquote

PORT = int(os.environ.get("BRIDGE_PORT", "8090"))
DATA_DIR = os.environ.get("BRIDGE_DATA", "/opt/chat-bridge/data")
UPLOAD_DIR = os.path.join(DATA_DIR, "uploads")
OUTBOX_DIR = os.path.join(DATA_DIR, "outbox")
MSG_FILE = os.path.join(DATA_DIR, "messages.json")
PASSWORD = os.environ.get("BRIDGE_PASSWORD", "")
AGENT_TOKEN = os.environ.get("BRIDGE_TOKEN", "")
MAX_UPLOAD = 20 * 1024 * 1024  # 20MB

SALT = secrets.token_hex(16)
PW_HASH = hashlib.sha256((SALT + PASSWORD).encode()).hexdigest() if PASSWORD else ""
SESSIONS = {}
SESSION_TTL = 7 * 24 * 3600

_lock = threading.Lock()


def _load():
    try:
        with open(MSG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"next_id": 1, "messages": []}


def _save(db):
    tmp = MSG_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False)
    os.replace(tmp, MSG_FILE)


def add_message(frm, text, reply_to=None, attachments=None, kind="chat"):
    with _lock:
        db = _load()
        mid = db["next_id"]
        db["next_id"] += 1
        msg = {"id": mid, "from": frm, "text": text, "ts": int(time.time()),
               "kind": kind}
        if reply_to:
            msg["reply_to"] = reply_to
        if attachments:
            msg["attachments"] = attachments
        db["messages"].append(msg)
        db["messages"] = db["messages"][-500:]
        _save(db)
        return msg


def get_messages(after=0, frm=None, limit=None):
    with _lock:
        db = _load()
        out = [m for m in db["messages"] if m["id"] > after]
        if frm:
            out = [m for m in out if m["from"] == frm]
        if limit:
            out = out[-limit:]
        return out


def check_password(pw):
    if not PW_HASH:
        return False
    h = hashlib.sha256((SALT + pw).encode()).hexdigest()
    return hmac.compare_digest(h, PW_HASH)


def new_session():
    tok = secrets.token_hex(32)
    SESSIONS[tok] = time.time() + SESSION_TTL
    return tok


def check_session(cookie_header):
    if not cookie_header:
        return False
    for part in cookie_header.split(";"):
        part = part.strip()
        if part.startswith("cb_session="):
            tok = part[len("cb_session="):]
            exp = SESSIONS.get(tok)
            if exp and exp > time.time():
                return True
            SESSIONS.pop(tok, None)
    return False


def check_agent_token(auth_header):
    if not AGENT_TOKEN or not auth_header:
        return False
    if not auth_header.startswith("Bearer "):
        return False
    return hmac.compare_digest(auth_header[7:].strip(), AGENT_TOKEN)


def safe_filename(name):
    name = os.path.basename(unquote(name or ""))
    name = re.sub(r"[^a-zA-Z0-9._\-]", "_", name)
    name = name.strip("._")[:80] or "file"
    return name


def parse_multipart(body, boundary):
    """极简 multipart 解析，返回 [(filename, content_type, data)]"""
    parts = []
    sep = b"--" + boundary
    for chunk in body.split(sep)[1:]:
        if chunk.startswith(b"--"):
            break
        if chunk.startswith(b"\r\n"):
            chunk = chunk[2:]
        if chunk.endswith(b"\r\n"):
            chunk = chunk[:-2]
        if b"\r\n\r\n" not in chunk:
            continue
        hblob, data = chunk.split(b"\r\n\r\n", 1)
        filename, ctype = None, "application/octet-stream"
        for line in hblob.decode("latin-1").split("\r\n"):
            kl = line.lower()
            if kl.startswith("content-disposition:"):
                for p in line.split(";"):
                    p = p.strip()
                    if p.lower().startswith("filename="):
                        filename = p[9:].strip().strip('"')
            elif kl.startswith("content-type:"):
                ctype = line.split(":", 1)[1].strip()
        if filename:
            parts.append((filename, ctype, data))
    return parts


CHAT_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
<title>Muse 聊天桥接</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei",sans-serif;background:#f0f2f5;height:100dvh;display:flex;flex-direction:column}
header{background:#1a1d29;color:#fff;padding:12px 16px;font-size:16px;font-weight:600;display:flex;justify-content:space-between;align-items:center}
header .dot{font-size:12px;font-weight:400;color:#8b93a7}
header .dot.on{color:#4ade80}
#msgs{flex:1;overflow-y:auto;padding:16px;display:flex;flex-direction:column;gap:10px}
.msg{max-width:88%;padding:10px 14px;border-radius:14px;font-size:15px;line-height:1.6;word-break:break-word}
.msg.user{align-self:flex-end;background:#2563eb;color:#fff;border-bottom-right-radius:4px}
.msg.asst{align-self:flex-start;background:#fff;color:#1a1d29;border-bottom-left-radius:4px;box-shadow:0 1px 2px rgba(0,0,0,.08)}
.msg.notify{align-self:center;background:#fef3c7;color:#92400e;font-size:13px;max-width:95%;text-align:center}
.msg .meta{font-size:11px;opacity:.55;margin-top:6px}
.msg pre{background:#f3f4f6;border-radius:8px;padding:10px;overflow-x:auto;margin:8px 0;font-size:13px}
.msg.user pre{background:rgba(255,255,255,.15)}
.msg code{background:#f3f4f6;border-radius:4px;padding:1px 5px;font-size:13px;font-family:ui-monospace,Menlo,monospace}
.msg.user code{background:rgba(255,255,255,.2)}
.msg pre code{background:none;padding:0}
.msg ul{margin:6px 0 6px 18px}
.msg h2,.msg h3,.msg h4{margin:8px 0 4px}
.msg a{color:#2563eb}
.msg.user a{color:#bfdbfe}
.msg img.att{max-width:100%;border-radius:8px;margin-top:8px;display:block}
.msg .filelink{display:inline-block;margin-top:8px;padding:6px 12px;background:#eef2ff;border-radius:8px;font-size:13px;text-decoration:none}
.msg.user .filelink{background:rgba(255,255,255,.2);color:#fff}
#typing{align-self:flex-start;color:#8b93a7;font-size:13px;padding:4px 8px;display:none}
#inputbar{display:flex;gap:8px;padding:12px;background:#fff;border-top:1px solid #e5e7eb;padding-bottom:calc(12px + env(safe-area-inset-bottom));align-items:center}
#inputbar input[type=text]{flex:1;border:1px solid #d1d5db;border-radius:20px;padding:10px 16px;font-size:15px;outline:none;min-width:0}
#inputbar input[type=text]:focus{border-color:#2563eb}
#inputbar button{background:#2563eb;color:#fff;border:none;border-radius:20px;padding:10px 18px;font-size:15px;cursor:pointer;white-space:nowrap}
#inputbar button:disabled{opacity:.5}
#attachbtn{background:#e5e7eb !important;color:#374151 !important;padding:10px 12px !important}
#login{position:fixed;inset:0;background:#1a1d29;display:flex;align-items:center;justify-content:center;z-index:10}
#login .box{background:#fff;border-radius:16px;padding:32px 28px;width:320px;text-align:center}
#login h2{margin-bottom:20px;font-size:18px}
#login input{width:100%;border:1px solid #d1d5db;border-radius:10px;padding:12px;font-size:15px;margin-bottom:12px;outline:none}
#login button{width:100%;background:#2563eb;color:#fff;border:none;border-radius:10px;padding:12px;font-size:15px;cursor:pointer}
#login .err{color:#dc2626;font-size:13px;margin-top:8px;min-height:18px}
#uploading{font-size:12px;color:#8b93a7;padding:0 16px 4px;display:none}
</style>
</head>
<body>
<header><span>Muse 聊天桥接</span><span class="dot" id="status">● 连接中</span></header>
<div id="msgs"></div>
<div id="typing">Muse 正在思考…</div>
<div id="uploading">上传中…</div>
<div id="inputbar">
<button id="attachbtn" title="发送文件">📎</button>
<input id="fileinp" type="file" style="display:none" multiple>
<input id="inp" type="text" placeholder="输入消息…" autocomplete="off" disabled>
<button id="sendbtn" disabled>发送</button>
</div>
<div id="login"><div class="box">
<h2>请输入访问密码</h2>
<input id="pw" type="password" placeholder="密码" autocomplete="current-password">
<button id="loginbtn">进入</button>
<div class="err" id="loginerr"></div>
</div></div>
<script>
let lastId=0,loggedIn=false,pendingFiles=[];
const msgsEl=document.getElementById('msgs');
const inp=document.getElementById('inp');
const sendbtn=document.getElementById('sendbtn');
const statusEl=document.getElementById('status');
const typingEl=document.getElementById('typing');

function fmtTs(ts){const d=new Date(ts*1000);return d.toLocaleString('zh-CN',{hour12:false});}
function esc(s){return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function renderMd(src){
  const codes=[];
  let h=esc(src);
  h=h.replace(/```(\\w*)\\n?([\\s\\S]*?)```/g,(m,l,c)=>{codes.push('<pre><code>'+c.replace(/\\n$/,'')+'</code></pre>');return '\\x00'+(codes.length-1)+'\\x00';});
  h=h.replace(/`([^`\\n]+)`/g,'<code>$1</code>');
  h=h.replace(/\\*\\*([^*]+)\\*\\*/g,'<strong>$1</strong>');
  h=h.replace(/(^|[^*])\\*([^\\*\\n]+)\\*/g,'$1<em>$2</em>');
  h=h.replace(/^### (.*)$/gm,'<h4>$1</h4>').replace(/^## (.*)$/gm,'<h3>$1</h3>').replace(/^# (.*)$/gm,'<h2>$1</h2>');
  h=h.replace(/^(?:- |\\* )(.*)$/gm,'<li>$1</li>');
  h=h.replace(/(<li>.*<\\/li>\\n?)+/g,'<ul>$&</ul>');
  h=h.replace(/(https?:\\/\\/[^\\s<]+)/g,'<a href="$1" target="_blank" rel="noopener">$1</a>');
  h=h.replace(/\\n/g,'<br>');
  h=h.replace(/\\x00(\\d+)\\x00/g,(m,i)=>codes[+i]);
  return h;
}
function renderAtt(a,from){
  const url='/files/u_'+a.fid;
  if(a.mime&&a.mime.startsWith('image/'))return '<a href="'+url+'" target="_blank"><img class="att" src="'+url+'" alt="'+esc(a.name)+'"></a>';
  return '<a class="filelink" href="'+url+'" target="_blank">📄 '+esc(a.name)+' ('+fmtSize(a.size)+')</a>';
}
function fmtSize(n){if(n>1048576)return (n/1048576).toFixed(1)+'MB';if(n>1024)return (n/1024).toFixed(0)+'KB';return n+'B';}
function addMsg(m){
  const div=document.createElement('div');
  div.className='msg '+(m.kind==='notify'?'notify':(m.from==='user'?'user':'asst'));
  if(m.kind==='notify')div.innerHTML='🔔 '+renderMd(m.text);
  else{
    const body=document.createElement('div');
    body.innerHTML=m.from==='user'?esc(m.text).replace(/\\n/g,'<br>'):renderMd(m.text);
    div.appendChild(body);
    (m.attachments||[]).forEach(a=>{const s=document.createElement('span');s.innerHTML=renderAtt(a);div.appendChild(s);});
  }
  const meta=document.createElement('div');meta.className='meta';meta.textContent=fmtTs(m.ts);div.appendChild(meta);
  msgsEl.appendChild(div);msgsEl.scrollTop=msgsEl.scrollHeight;
}
async function poll(){
  if(!loggedIn)return;
  try{
    const r=await fetch('/api/poll?after='+lastId);
    if(r.status===401){location.reload();return;}
    const j=await r.json();
    if(j.messages&&j.messages.length){
      j.messages.forEach(m=>{addMsg(m);lastId=Math.max(lastId,m.id);if(m.from==='assistant')typingEl.style.display='none';});
    }
    statusEl.textContent='● 在线';statusEl.classList.add('on');
  }catch(e){statusEl.textContent='● 断开';statusEl.classList.remove('on');}
}
async function send(){
  const t=inp.value.trim();
  if(!t&&!pendingFiles.length)return;
  inp.value='';sendbtn.disabled=true;
  typingEl.style.display='block';
  try{
    await fetch('/api/send',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({text:t,attachments:pendingFiles})});
    pendingFiles=[];
  }catch(e){}
  sendbtn.disabled=false;inp.focus();poll();
}
document.getElementById('attachbtn').onclick=()=>{if(loggedIn)document.getElementById('fileinp').click();};
document.getElementById('fileinp').onchange=async e=>{
  const files=e.target.files;if(!files.length)return;
  const up=document.getElementById('uploading');up.style.display='block';
  for(const f of files){
    if(f.size>20*1024*1024){alert('文件太大（>20MB）：'+f.name);continue;}
    const fd=new FormData();fd.append('file',f);
    try{
      const r=await fetch('/api/upload',{method:'POST',body:fd});
      const j=await r.json();
      if(j.ok)pendingFiles.push(j.fid);
      else alert('上传失败：'+f.name);
    }catch(err){alert('上传失败：'+f.name);}
  }
  e.target.value='';up.style.display='none';
  inp.placeholder=pendingFiles.length?('已选 '+pendingFiles.length+' 个文件，输入说明后发送…'):'输入消息…';
  inp.focus();
};
async function doLogin(){
  const pw=document.getElementById('pw').value;
  const r=await fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:pw})});
  if(r.ok){document.getElementById('login').style.display='none';loggedIn=true;inp.disabled=false;sendbtn.disabled=false;inp.focus();poll();}
  else{document.getElementById('loginerr').textContent='密码错误';}
}
document.getElementById('loginbtn').onclick=doLogin;
document.getElementById('pw').onkeydown=e=>{if(e.key==='Enter')doLogin();};
sendbtn.onclick=send;
inp.onkeydown=e=>{if(e.key==='Enter')send();};
setInterval(poll,3000);
fetch('/api/poll?after=0').then(r=>{if(r.ok){document.getElementById('login').style.display='none';loggedIn=true;inp.disabled=false;sendbtn.disabled=false;poll();}});
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    server_version = "chat-bridge/2.0"

    def log_message(self, *a):
        pass

    def _json(self, code, obj, cookie=None):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        if cookie:
            self.send_header("Set-Cookie", cookie)
        self.end_headers()
        self.wfile.write(body)

    def _html(self, code, html):
        body = html.encode()
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self, limit=65536):
        n = int(self.headers.get("Content-Length", 0) or 0)
        if n <= 0 or n > limit:
            return None
        return self.rfile.read(n)

    def _json_body(self):
        raw = self._body()
        if raw is None:
            return {}
        try:
            return json.loads(raw.decode("utf-8") or "{}")
        except (json.JSONDecodeError, UnicodeDecodeError):
            return {}

    def _serve_file(self, path, download_name=None):
        if not os.path.isfile(path):
            return self._json(404, {"error": "not found"})
        ctype = mimetypes.guess_type(path)[0] or "application/octet-stream"
        size = os.path.getsize(path)
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(size))
        if download_name and not ctype.startswith(("image/", "text/")):
            self.send_header("Content-Disposition",
                             f'attachment; filename="{download_name}"')
        self.end_headers()
        with open(path, "rb") as f:
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                self.wfile.write(chunk)

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)

        if u.path == "/api/agent/health":
            return self._json(200, {"ok": True, "ts": int(time.time())})

        if u.path == "/api/agent/inbox":
            if not check_agent_token(self.headers.get("Authorization")):
                return self._json(401, {"error": "unauthorized"})
            after = int(q.get("after", ["0"])[0] or 0)
            return self._json(200, {"messages": get_messages(after=after, frm="user")})

        if u.path == "/api/agent/history":
            if not check_agent_token(self.headers.get("Authorization")):
                return self._json(401, {"error": "unauthorized"})
            limit = min(int(q.get("limit", ["20"])[0] or 20), 100)
            return self._json(200, {"messages": get_messages(after=0, limit=limit)})

        if u.path == "/api/poll":
            if not check_session(self.headers.get("Cookie")):
                return self._json(401, {"error": "login required"})
            after = int(q.get("after", ["0"])[0] or 0)
            return self._json(200, {"messages": get_messages(after=after)})

        if u.path.startswith("/files/"):
            if not check_session(self.headers.get("Cookie")):
                return self._json(401, {"error": "login required"})
            key = u.path[len("/files/"):]
            if key.startswith("u_"):
                fid = key[2:]
                if not re.fullmatch(r"[A-Za-z0-9_\-]+", fid):
                    return self._json(400, {"error": "bad key"})
                for fn in os.listdir(UPLOAD_DIR):
                    if fn.startswith(fid + "_"):
                        return self._serve_file(os.path.join(UPLOAD_DIR, fn))
                return self._json(404, {"error": "not found"})
            if key.startswith("o_"):
                name = safe_filename(key[2:])
                return self._serve_file(os.path.join(OUTBOX_DIR, name),
                                        download_name=name)
            return self._json(404, {"error": "not found"})

        if u.path in ("/", "/index.html"):
            return self._html(200, CHAT_HTML)

        return self._json(404, {"error": "not found"})

    def do_POST(self):
        u = urlparse(self.path)

        if u.path == "/api/login":
            body = self._json_body()
            if check_password(body.get("password", "")):
                tok = new_session()
                return self._json(200, {"ok": True},
                                  cookie=f"cb_session={tok}; Path=/; HttpOnly; Max-Age={SESSION_TTL}")
            return self._json(401, {"error": "bad password"})

        if u.path == "/api/send":
            if not check_session(self.headers.get("Cookie")):
                return self._json(401, {"error": "login required"})
            body = self._json_body()
            text = (body.get("text") or "").strip()
            atts = []
            for fid in (body.get("attachments") or [])[:5]:
                if not re.fullmatch(r"[A-Za-z0-9_\-]+", str(fid)):
                    continue
                for fn in os.listdir(UPLOAD_DIR):
                    if fn.startswith(fid + "_"):
                        fp = os.path.join(UPLOAD_DIR, fn)
                        atts.append({"fid": fid, "name": fn[len(fid) + 1:],
                                     "size": os.path.getsize(fp),
                                     "mime": mimetypes.guess_type(fp)[0] or
                                             "application/octet-stream"})
                        break
            if not text and not atts:
                return self._json(400, {"error": "empty"})
            msg = add_message("user", text[:4000], attachments=atts or None)
            return self._json(200, {"ok": True, "id": msg["id"]})

        if u.path == "/api/upload":
            if not check_session(self.headers.get("Cookie")):
                return self._json(401, {"error": "login required"})
            ctype = self.headers.get("Content-Type", "")
            m = re.search(r"boundary=([^\s;]+)", ctype)
            if not m:
                return self._json(400, {"error": "not multipart"})
            raw = self._body(limit=MAX_UPLOAD + 1024)
            if raw is None:
                return self._json(413, {"error": "too large"})
            parts = parse_multipart(raw, m.group(1).encode())
            if not parts:
                return self._json(400, {"error": "no file"})
            fname, fctype, data = parts[0]
            if len(data) > MAX_UPLOAD:
                return self._json(413, {"error": "too large"})
            fid = f"{int(time.time())}_{secrets.token_hex(4)}"
            sname = safe_filename(fname)
            with open(os.path.join(UPLOAD_DIR, f"{fid}_{sname}"), "wb") as f:
                f.write(data)
            return self._json(200, {"ok": True, "fid": fid, "name": sname,
                                    "size": len(data), "mime": fctype})

        if u.path == "/api/agent/reply":
            if not check_agent_token(self.headers.get("Authorization")):
                return self._json(401, {"error": "unauthorized"})
            body = self._json_body()
            text = (body.get("text") or "").strip()
            if not text:
                return self._json(400, {"error": "empty"})
            msg = add_message("assistant", text[:20000],
                              reply_to=body.get("reply_to"))
            return self._json(200, {"ok": True, "id": msg["id"]})

        if u.path == "/api/agent/notify":
            if not check_agent_token(self.headers.get("Authorization")):
                return self._json(401, {"error": "unauthorized"})
            body = self._json_body()
            text = (body.get("text") or "").strip()
            if not text:
                return self._json(400, {"error": "empty"})
            msg = add_message("assistant", text[:4000], kind="notify")
            return self._json(200, {"ok": True, "id": msg["id"]})

        return self._json(404, {"error": "not found"})


def main():
    if not PASSWORD:
        print("ERROR: 必须设置 BRIDGE_PASSWORD 环境变量", flush=True)
        raise SystemExit(1)
    if not AGENT_TOKEN:
        print("ERROR: 必须设置 BRIDGE_TOKEN 环境变量", flush=True)
        raise SystemExit(1)
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    os.makedirs(OUTBOX_DIR, exist_ok=True)
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"chat-bridge v2 listening on 0.0.0.0:{PORT}, data={DATA_DIR}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
