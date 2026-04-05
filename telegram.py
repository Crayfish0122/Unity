"""
Unified Telegram utilities for DailyReport & WeeklyReport.

Handles HTML sanitization, message splitting, send/delete,
and status.json management.
"""
from __future__ import annotations

import html
import json
import re
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError

from Unity.config import get_telegram_bot_token, get_telegram_chat_id


# =========================
# Report directory
# =========================
def build_report_dir(base_dir: Path, report_date: str) -> Path:
    return base_dir / report_date


def load_report_files(report_dir: Path, build_tg_func) -> tuple[str, str, dict]:
    """
    Load report.html, telegram.html, meta.json from a report directory.

    If telegram.html doesn't exist, it is built on the fly using
    ``build_tg_func(report_html, meta)``.
    """
    report_path = report_dir / "report.html"
    telegram_path = report_dir / "telegram.html"
    meta_path = report_dir / "meta.json"

    if not report_path.exists():
        raise FileNotFoundError(f"找不到报告文件: {report_path}")
    if not meta_path.exists():
        raise FileNotFoundError(f"找不到元数据文件: {meta_path}")

    report_html = report_path.read_text(encoding="utf-8")
    meta = json.loads(meta_path.read_text(encoding="utf-8"))

    if telegram_path.exists():
        telegram_html = telegram_path.read_text(encoding="utf-8")
    else:
        telegram_html = build_tg_func(report_html, meta)

    return report_html, telegram_html, meta


# =========================
# HTML sanitization
# =========================
def sanitize_html_for_telegram(text: str) -> str:
    text = text.strip()
    text = re.sub(r"<h[1-6]>(.*?)</h[1-6]>", r"<b>\1</b>", text, flags=re.I | re.S)
    text = re.sub(r"<li>\s*", "• ", text, flags=re.I)
    text = re.sub(r"</li>", "\n", text, flags=re.I)
    text = re.sub(r"</?(ul|ol)>", "", text, flags=re.I)
    text = re.sub(r"\n{3,}", "\n\n", text)

    allowed = {"b", "strong", "i", "em", "u", "ins", "s", "strike", "del", "code", "pre", "a"}
    pattern = re.compile(r"</?([a-zA-Z0-9]+)(\s+[^>]*)?>")

    def repl(match):
        tag = match.group(1).lower()
        return match.group(0) if tag in allowed else ""

    text = pattern.sub(repl, text)
    return text.strip()


# =========================
# Telegram text builder
# =========================
def build_telegram_text(
    report_html: str,
    meta: dict,
    *,
    title: str = "报告",
    emoji: str = "📊",
) -> str:
    report_date = str(meta.get("report_date", ""))
    model_used = str(meta.get("model_used", ""))
    generated_at = str(meta.get("generated_at", ""))

    header = (
        f"<b>{emoji} {html.escape(title)}</b>\n"
        f"执行时间：{html.escape(generated_at)}\n"
        f"报告日期：{html.escape(report_date)}\n"
        f"模型：{html.escape(model_used)}"
    )

    clean_body = sanitize_html_for_telegram(report_html)
    return header + "\n\n" + clean_body


# =========================
# Message splitting
# =========================
def split_telegram_html(text: str, limit: int = 4000) -> list[str]:
    text = text.strip()
    if len(text) <= limit:
        return [text]

    parts: list[str] = []
    chunks = text.split("\n\n")
    current = ""

    def flush():
        nonlocal current
        if current.strip():
            parts.append(current.strip())
        current = ""

    for chunk in chunks:
        candidate = chunk if not current else current + "\n\n" + chunk
        if len(candidate) <= limit:
            current = candidate
            continue

        if current:
            flush()

        if len(chunk) <= limit:
            current = chunk
            continue

        lines = chunk.split("\n")
        line_buf = ""
        for line in lines:
            candidate2 = line if not line_buf else line_buf + "\n" + line
            if len(candidate2) <= limit:
                line_buf = candidate2
            else:
                if line_buf:
                    parts.append(line_buf.strip())
                if len(line) <= limit:
                    line_buf = line
                else:
                    start = 0
                    while start < len(line):
                        parts.append(line[start:start + limit])
                        start += limit
                    line_buf = ""

        if line_buf.strip():
            current = line_buf.strip()

    if current.strip():
        parts.append(current.strip())

    return parts


# =========================
# Telegram API
# =========================
def telegram_api_post(method: str, payload: dict) -> dict:
    bot_token = get_telegram_bot_token()

    data = urllib.parse.urlencode(payload).encode("utf-8")
    req = urllib.request.Request(
        url=f"https://api.telegram.org/bot{bot_token}/{method}",
        data=data,
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            parsed = json.loads(body)
            if not parsed.get("ok"):
                raise RuntimeError(f"Telegram API 失败: {parsed}")
            return parsed
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Telegram HTTPError {e.code}: {body}") from e


def send_telegram_message_html(text: str) -> list[int]:
    chat_id = get_telegram_chat_id()

    parts = split_telegram_html(text, limit=4000)
    print(f"[Telegram] 准备发送 {len(parts)} 段消息")

    message_ids: list[int] = []

    for idx, part in enumerate(parts, start=1):
        parsed = telegram_api_post(
            "sendMessage",
            {
                "chat_id": chat_id,
                "text": part,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
        )

        result = parsed.get("result", {})
        message_id = result.get("message_id")
        if message_id is None:
            raise RuntimeError(f"Telegram 返回里没有 message_id: {parsed}")

        message_ids.append(int(message_id))
        print(f"[Telegram] 第 {idx}/{len(parts)} 段发送成功，message_id={message_id}")

    return message_ids


def delete_telegram_messages(message_ids: list[int]) -> None:
    chat_id = get_telegram_chat_id()

    for message_id in message_ids:
        try:
            telegram_api_post(
                "deleteMessage",
                {
                    "chat_id": chat_id,
                    "message_id": message_id,
                },
            )
            print(f"[Telegram] 已删除旧消息 message_id={message_id}")
        except Exception as e:
            print(f"[Telegram] 删除旧消息失败 message_id={message_id} -> {e}")


# =========================
# Status management
# =========================
def datetime_now_str() -> str:
    return datetime.now().isoformat(timespec="seconds")


def load_status(report_dir: Path) -> dict:
    status_path = report_dir / "status.json"
    if not status_path.exists():
        return {}
    try:
        return json.loads(status_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_status(report_dir: Path, status: dict) -> None:
    status_path = report_dir / "status.json"
    status_path.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")


def find_previous_report_dir(base_dir: Path, current_date: str) -> Path | None:
    if not base_dir.exists():
        return None
    candidates = sorted(
        [d for d in base_dir.iterdir() if d.is_dir() and d.name != current_date],
        reverse=True,
    )
    for d in candidates:
        status = load_status(d)
        if status.get("message_ids"):
            return d
    return None


# =========================
# High-level send workflow
# =========================
def send_and_rotate(
    base_dir: Path,
    report_date: str,
    telegram_html: str,
) -> list[int]:
    """
    Send telegram_html, delete previous report's messages, save status.
    Returns new message_ids.
    """
    report_dir = build_report_dir(base_dir, report_date)

    new_message_ids = send_telegram_message_html(telegram_html)

    prev_dir = find_previous_report_dir(base_dir, report_date)
    if prev_dir:
        prev_status = load_status(prev_dir)
        old_ids = prev_status.get("message_ids", [])
        if old_ids:
            print(f"[Telegram] 准备删除旧消息（{prev_dir.name}），共 {len(old_ids)} 条")
            delete_telegram_messages([int(x) for x in old_ids])
            prev_status["message_ids"] = []
            prev_status["deleted_at"] = datetime_now_str()
            save_status(prev_dir, prev_status)

    save_status(report_dir, {
        "message_ids": new_message_ids,
        "sent_at": datetime_now_str(),
    })

    return new_message_ids
