"""
Unity/config.py — Single point of .env loading.

Every module that needs env vars imports from here.
No other file should call load_dotenv().

.env location: 06_Python/.env (parent of Unity/)
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env once from 06_Python/.env
_ENV_PATH = os.environ.get(
    "DOTENV_PATH",
    Path(__file__).resolve().parent.parent / ".env",
)
load_dotenv(_ENV_PATH)


# =========================
# API keys
# =========================
def get_gemini_api_key() -> str:
    key = os.getenv("GEMINI_API_KEY", "").strip()
    if not key:
        raise RuntimeError("未找到 GEMINI_API_KEY，请检查 .env")
    return key


def get_telegram_bot_token() -> str:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("未找到 TELEGRAM_BOT_TOKEN，请检查 .env")
    return token


def get_telegram_chat_id() -> str:
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not chat_id:
        raise RuntimeError("未找到 TELEGRAM_CHAT_ID，请检查 .env")
    return chat_id


# =========================
# Output directories
# =========================
def get_daily_output_dir() -> str:
    return os.getenv("DAILY_OUTPUT_DIR", "").strip()


def get_weekly_output_dir() -> str:
    return os.getenv("WEEKLY_OUTPUT_DIR", "").strip()
