#!/usr/bin/env python3
"""企业微信回调消息加解密（WXBizMsgCrypt 精简实现）

只实现 bridge 需要的两件事：
1. verify_url() — GET 回调验证：验签 + 解密 echostr
2. decrypt_msg() — POST 消息解密：验签 + 解密 XML 明文

算法来源：企业微信官方 WXBizMsgCrypt（SHA1 字典序验签 + AES-CBC）。
"""
import base64
import hashlib
import struct
import xml.etree.ElementTree as ET

try:
    from Crypto.Cipher import AES
except ImportError:
    AES = None


class WeComCryptError(Exception):
    pass


def _sha1_sign(token, timestamp, nonce, encrypted):
    """字典序排序后 SHA1"""
    lst = sorted([token, timestamp, nonce, encrypted])
    return hashlib.sha1("".join(lst).encode()).hexdigest()


def verify_signature(token, timestamp, nonce, encrypted, signature):
    return _sha1_sign(token, timestamp, nonce, encrypted) == signature


def _get_aes_key(encoding_aes_key):
    if AES is None:
        raise WeComCryptError("需要 pycryptodome: pip install pycryptodome")
    # EncodingAESKey 是 43 位，去掉末尾补的 = 后 base64 解码得 32 字节
    return base64.b64decode(encoding_aes_key + "=")


def _pkcs7_unpad(data):
    pad = data[-1]
    if pad < 1 or pad > 32:
        raise WeComCryptError("padding 无效")
    return data[:-pad]


def decrypt_echostr(encoding_aes_key, echostr):
    """解密 URL 验证时的 echostr，返回明文 str"""
    key = _get_aes_key(encoding_aes_key)
    cipher = AES.new(key, AES.MODE_CBC, key[:16])
    plain = _pkcs7_unpad(cipher.decrypt(base64.b64decode(echostr)))
    # 结构：16 字节随机 + 4 字节长度 + 消息 + corp_id
    msg_len = struct.unpack(">I", plain[16:20])[0]
    return plain[20:20 + msg_len].decode("utf-8")


def decrypt_msg(encoding_aes_key, encrypted_xml):
    """解密 POST 过来的消息 XML，返回 (明文 XML str, ToUserName)"""
    key = _get_aes_key(encoding_aes_key)
    root = ET.fromstring(encrypted_xml)
    encrypted = root.findtext("Encrypt")
    if not encrypted:
        raise WeComCryptError("XML 里没有 Encrypt 节点")
    cipher = AES.new(key, AES.MODE_CBC, key[:16])
    plain = _pkcs7_unpad(cipher.decrypt(base64.b64decode(encrypted)))
    msg_len = struct.unpack(">I", plain[16:20])[0]
    msg_xml = plain[20:20 + msg_len].decode("utf-8")
    to_user = root.findtext("ToUserName") or ""
    return msg_xml, to_user


def parse_text_message(msg_xml):
    """从解密后的消息 XML 提纯文本。只处理 text 类型，返回 (from_user, text)"""
    root = ET.fromstring(msg_xml)
    if (root.findtext("MsgType") or "") != "text":
        return None, None
    from_user = root.findtext("FromUserName") or ""
    # 企业微信自建应用收到的用户消息：FromUserName 是成员 UserID
    text = (root.findtext("Content") or "").strip()
    # AgentID 校验（可选）：ToUserName 是企业 CorpID
    return from_user, (text or None)
