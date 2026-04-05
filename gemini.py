"""
Unified Gemini API client with multi-model fallback.

Centralizes model names, thinking config, and token extraction
so both daily and weekly pipelines share the same logic.
"""
from __future__ import annotations

import json
from pathlib import Path

from google import genai
from google.genai import types

from Unity.config import get_gemini_api_key

DEFAULT_MODEL = "gemini-3-flash-preview"
FALLBACK_MODELS = ["gemini-3.1-flash-lite-preview"]
DEFAULT_THINKING_BUDGET = 1024

_GEMINI3_PREFIXES = ("gemini-3",)


def get_genai_client() -> genai.Client:
    return genai.Client(api_key=get_gemini_api_key())


def extract_usage_metadata(response) -> dict[str, int | None]:
    usage = getattr(response, "usage_metadata", None)
    if usage is None:
        usage = getattr(response, "usageMetadata", None)

    if usage is None:
        return {
            "prompt_token_count": None,
            "candidates_token_count": None,
            "thoughts_token_count": None,
            "total_token_count": None,
        }

    def pick(obj, snake: str, camel: str):
        v = getattr(obj, snake, None)
        if v is None:
            v = getattr(obj, camel, None)
        return v

    return {
        "prompt_token_count": pick(usage, "prompt_token_count", "promptTokenCount"),
        "candidates_token_count": pick(usage, "candidates_token_count", "candidatesTokenCount"),
        "thoughts_token_count": pick(usage, "thoughts_token_count", "thoughtsTokenCount"),
        "total_token_count": pick(usage, "total_token_count", "totalTokenCount"),
    }


def _is_gemini3(model_name: str) -> bool:
    return any(model_name.startswith(p) for p in _GEMINI3_PREFIXES)


def build_generation_config(model_name: str, thinking_budget: int) -> types.GenerateContentConfig:
    if _is_gemini3(model_name):
        return types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(thinking_level=types.ThinkingLevel.HIGH)
        )
    return types.GenerateContentConfig(
        thinking_config=types.ThinkingConfig(thinking_budget=thinking_budget)
    )


def call_gemini_with_fallback(
    prompt: str,
    primary_model: str = DEFAULT_MODEL,
    fallback_models: list[str] | None = None,
    thinking_budget: int = DEFAULT_THINKING_BUDGET,
) -> tuple[str, str, dict[str, int | None]]:
    """
    Try primary model, then each fallback model in order.

    Returns (model_used, response_text, usage_metadata).
    Raises RuntimeError if all models fail.
    """
    if fallback_models is None:
        fallback_models = list(FALLBACK_MODELS)

    client = get_genai_client()
    models = [primary_model] + fallback_models
    errors: list[str] = []

    for model_name in models:
        config = build_generation_config(model_name, thinking_budget)
        try:
            print(f"[Gemini] 尝试模型: {model_name}")
            response = client.models.generate_content(
                model=model_name,
                contents=prompt,
                config=config,
            )
            text = getattr(response, "text", None)
            if text and text.strip():
                return model_name, text.strip(), extract_usage_metadata(response)
            errors.append(f"{model_name}: 返回为空")
        except Exception as e:
            errors.append(f"{model_name}: {e}")

    raise RuntimeError("所有模型均失败 -> " + " | ".join(errors))


def save_report_files(
    report_dir: Path,
    report_html: str,
    telegram_html: str,
    meta: dict,
    context_text: str,
    prompt: str,
) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)

    (report_dir / "report.html").write_text(report_html, encoding="utf-8")
    (report_dir / "telegram.html").write_text(telegram_html, encoding="utf-8")
    (report_dir / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    (report_dir / "context.txt").write_text(context_text, encoding="utf-8")
    (report_dir / "prompt.txt").write_text(prompt, encoding="utf-8")

    return report_dir
