from __future__ import annotations
from datetime import datetime

import argparse
import html
import json
import os
import re
import urllib.parse
import urllib.request
from pathlib import Path
from urllib.error import HTTPError

from dotenv import load_dotenv

load_dotenv()


def load_report_files(output_dir: Path, report_key: str) -> tuple[str, dict, Path, Path]:
    report_path = output_dir / "report.html"
    meta_path = output_dir / "meta.json"

    if not report_path.exists():
        raise FileNotFoundError(f"找不到报告文件: {report_path}")
    if not meta_path.exists():
        raise FileNotFoundError(f"找不到元数据文件: {meta_path}")

    report_html = report_path.read_text(encoding="utf-8")
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    return report_html, meta, report_path, meta_path


def sanitize_html_for_telegram(text: str) -> str:
    """
    Telegram HTML sanitizer — 3-phase approach.

    Phase 1: Convert known HTML structures (headings, lists) and save
             whitelisted Telegram tags as placeholders.
    Phase 2: Escape ALL remaining <, >, & so that stray patterns like
             ``<60min）`` can never be misinterpreted as tags.
    Phase 3: Restore the saved whitelisted tags from placeholders.
    """
    text = text.strip()

    # --- structural conversions ---
    text = re.sub(r"<h[1-6]>(.*?)</h[1-6]>", r"<b>\1</b>", text, flags=re.I | re.S)
    text = re.sub(r"<li>\s*", "• ", text, flags=re.I)
    text = re.sub(r"</li>", "\n", text, flags=re.I)
    text = re.sub(r"</?(ul|ol)>", "", text, flags=re.I)
    text = re.sub(r"\n{3,}", "\n\n", text)

    # --- Phase 1: save whitelisted tags, remove disallowed ones ---
    allowed = {"b", "strong", "i", "em", "u", "ins", "s", "strike", "del", "code", "pre", "a"}
    placeholders: list[str] = []

    def _save_tag(match: re.Match) -> str:
        tag = match.group(1).lower()
        if tag in allowed:
            placeholders.append(match.group(0))
            return f"\x00TAG{len(placeholders) - 1}\x00"
        return ""  # drop disallowed tags

    tag_pattern = re.compile(r"</?([a-zA-Z][a-zA-Z0-9]*)(\s+[^>]*)?>")
    text = tag_pattern.sub(_save_tag, text)

    # --- Phase 2: escape everything left ---
    text = text.replace("&", "&amp;")
    text = text.replace("<", "&lt;")
    text = text.replace(">", "&gt;")

    # --- Phase 3: restore whitelisted tags ---
    for i, original in enumerate(placeholders):
        text = text.replace(f"\x00TAG{i}\x00", original)

    return text.strip()


def build_telegram_text(report_html: str, meta: dict) -> str:
    report_date = str(meta.get("report_date", ""))
    model_used = str(meta.get("model_used", ""))
    generated_at = str(meta.get("generated_at", ""))

    header = (
        f"<b>📈 Alpha Sentinel 股票日报</b>\n"
        f"执行时间：{html.escape(generated_at)}\n"
        f"报告日期：{html.escape(report_date)}\n"
        f"模型：{html.escape(model_used)}"
    )

    clean_body = sanitize_html_for_telegram(report_html)
    return header + "\n\n" + clean_body


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


def telegram_api_post(method: str, payload: dict) -> dict:
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not bot_token:
        raise RuntimeError("未找到 TELEGRAM_BOT_TOKEN，请检查 .env")

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


def load_state(state_path: Path) -> dict:
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(state_path: Path, state: dict) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def send_telegram_message_html(text: str) -> list[int]:
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not chat_id:
        raise RuntimeError("未找到 TELEGRAM_CHAT_ID，请检查 .env")

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
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not chat_id:
        raise RuntimeError("未找到 TELEGRAM_CHAT_ID，请检查 .env")

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


def remove_local_report_files(report_path: Path, meta_path: Path) -> None:
    for p in [report_path, meta_path]:
        try:
            if p.exists():
                p.unlink()
                print(f"[本地清理] 已删除 {p}")
        except Exception as e:
            print(f"[本地清理] 删除失败 {p} -> {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--report-key", default="stock_report", help="报告类型键，例如 stock_report / daily_report / news_summary / mail_summary")
    parser.add_argument("--output-dir", default=r"C:\Users\arash\OneDrive\LifeOps\00_Output\04_StockReport")
    parser.add_argument("--state-file", default="output/telegram_state.json")
    parser.add_argument("--report-date", default=datetime.now().strftime("%Y-%m-%d"), help="报表日期，例如 2026-03-21")
    parser.add_argument("--dry-run", action="store_true", help="只预览，不真正发送")
    args = parser.parse_args()

    report_date = getattr(args, "report_date", datetime.now().strftime("%Y-%m-%d"))
    output_dir = Path(args.output_dir) / report_date
    state_path = Path(args.state_file)

    report_html, meta, report_path, meta_path = load_report_files(output_dir, args.report_key)
    telegram_text = build_telegram_text(report_html, meta)

    if args.dry_run:
        print("=== STEP7: dry-run 预览（未发送） ===")
        print(telegram_text)
        return

    print("=== STEP7: 开始发送 Telegram ===")
    new_message_ids = send_telegram_message_html(telegram_text)

    state = load_state(state_path)
    previous = state.get(args.report_key, {})
    old_message_ids = previous.get("message_ids", [])

    if isinstance(old_message_ids, list) and old_message_ids:
        print(f"[Telegram] 准备删除同类型旧消息，共 {len(old_message_ids)} 条")
        delete_telegram_messages([int(x) for x in old_message_ids])

    state[args.report_key] = {
        "message_ids": new_message_ids,
        "report_date": meta.get("report_date"),
        "sent_at": datetime_now_str(),
    }
    save_state(state_path, state)

    print("=== STEP7: 已发送完成 ===")


def datetime_now_str() -> str:
    from datetime import datetime
    return datetime.now().isoformat(timespec="seconds")


if __name__ == "__main__":
    main()
