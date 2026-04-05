"""
Unified text builders for health data sections.

All functions accept HealthRow lists and produce structured plain-text blocks
consumed by the prompt/context assembly step.
"""
from __future__ import annotations

from typing import Optional

from Unity.health_reader import (
    HealthRow,
    format_avg,
    format_metric,
    parse_nullable_float,
)


# =========================
# Derived metrics
# =========================
def compute_buffer_minutes(end_work: str, sleep_start: str) -> str:
    if not end_work or not sleep_start:
        return ""
    try:
        eh, em = map(int, end_work.split(":"))
        sh, sm = map(int, sleep_start.split(":"))
        end_min = eh * 60 + em
        sleep_min = sh * 60 + sm
        if sleep_min < end_min:
            sleep_min += 24 * 60
        return str(sleep_min - end_min)
    except (ValueError, AttributeError):
        return ""


# =========================
# Sleep
# =========================
def build_sleep_text(rows: list[HealthRow], *, include_status: bool = False) -> str:
    if not rows:
        return "睡眠记录: 该日期范围内无有效记录。"

    header_parts = [
        "字段说明: 日期 | 入睡 | 起床 | InBed(min) | Core(min) | Deep(min) | REM(min)",
        "实际睡眠(min) | 睡眠债务变化(min) | 累计债务(min) | 睡眠状态",
    ]
    if include_status:
        header_parts.append("状态")
    header = " | ".join(header_parts)

    lines = [header]
    for r in rows:
        parts = (
            f"[{r.date}] "
            f"入睡={r.sleep_start} | 起床={r.wake_time} | "
            f"InBed={r.inbed_min} | Core={r.core_min} | Deep={r.deep_min} | REM={r.rem_min} | "
            f"实际睡眠={r.sleep_actual_min} | 债务变化={r.sleep_debt_delta} | "
            f"累计债务={r.sleep_debt_total} | 睡眠状态={r.sleep_status}"
        )
        if include_status:
            parts += f" | 状态={r.status}"
        lines.append(parts)

    return "\n".join(lines)


# =========================
# Work & overtime
# =========================
def build_work_schedule_text(rows: list[HealthRow]) -> str:
    if not rows:
        return "作息与加班: 该日期范围内无有效记录。"

    header = "字段说明: 日期 | 上班 | 下班 | TW | 预计加班(min) | 实际加班(min) | 差异(min) | 入睡 | 下班→入睡缓冲(min)"
    lines = [header]
    for r in rows:
        buffer_min = compute_buffer_minutes(r.end_work, r.sleep_start)
        lines.append(
            f"[{r.date}] "
            f"上班={r.start_work} | 下班={r.end_work} | TW={r.tw} | "
            f"预计加班={r.planned_ot} | 实际加班={r.actual_ot} | 差异={r.ot_diff} | "
            f"入睡={r.sleep_start} | 下班→入睡缓冲={buffer_min}"
        )

    return "\n".join(lines)


# =========================
# Training
# =========================
def build_training_text(rows: list[HealthRow]) -> str:
    if not rows:
        return "健身记录: 该日期范围内无有效记录。"

    lines = []
    for r in rows:
        feedback = r.workout_feedback or "未记录"
        lines.append(f"[{r.date}] 健身反馈={feedback}")

    return "\n".join(lines)


# =========================
# Nutrition
# =========================
def build_nutrition_text(
    target_dates: list[str],
    rows: list[HealthRow],
    *,
    summary_label: str = "统计",
) -> str:
    row_map = {r.date: r for r in rows}

    kcal_values: list[float] = []
    carb_values: list[float] = []
    protein_values: list[float] = []
    fat_values: list[float] = []
    weight_values: list[float] = []

    header = "字段说明: Kcal | C(g) | P(g) | F(g) | 体重(kg) | Delta=与目标偏差(Kcal|C|P|F)"
    lines = [header]

    for d in target_dates:
        row = row_map.get(d)
        kcal    = parse_nullable_float(row.kcal)    if row else None
        carb    = parse_nullable_float(row.carb)    if row else None
        protein = parse_nullable_float(row.protein) if row else None
        fat     = parse_nullable_float(row.fat)     if row else None
        weight  = parse_nullable_float(row.weight)  if row else None

        if kcal    is not None: kcal_values.append(kcal)
        if carb    is not None: carb_values.append(carb)
        if protein is not None: protein_values.append(protein)
        if fat     is not None: fat_values.append(fat)
        if weight  is not None: weight_values.append(weight)

        delta_str = ""
        if row and any([row.nutrition_delta_kcal, row.nutrition_delta_carb,
                        row.nutrition_delta_protein, row.nutrition_delta_fat]):
            delta_str = (
                f" | Delta=Kcal{row.nutrition_delta_kcal}"
                f"/C{row.nutrition_delta_carb}"
                f"/P{row.nutrition_delta_protein}"
                f"/F{row.nutrition_delta_fat}"
            )

        lines.append(
            f"[{d}] "
            f"Kcal={format_metric(kcal, 0, 'kcal')} | "
            f"C={format_metric(carb, 1, 'g')} | "
            f"P={format_metric(protein, 1, 'g')} | "
            f"F={format_metric(fat, 1, 'g')} | "
            f"体重={format_metric(weight, 1, 'kg')}"
            f"{delta_str}"
        )

    lines.append(
        f"{summary_label}: "
        f"Kcal均值={format_avg(kcal_values, 0, 'kcal')} | "
        f"C均值={format_avg(carb_values, 1, 'g')} | "
        f"P均值={format_avg(protein_values, 1, 'g')} | "
        f"F均值={format_avg(fat_values, 1, 'g')} | "
        f"体重均值={format_avg(weight_values, 1, 'kg')}"
    )

    return "\n".join(lines)


# =========================
# Subjective notes
# =========================
def build_subjective_notes_text(rows: list[HealthRow]) -> str:
    has_any = any(r.note for r in rows)
    if not has_any:
        return "主观记录: 该日期范围内无备注。"

    lines = []
    for r in rows:
        note = r.note or "无"
        lines.append(f"[{r.date}] {note}")

    return "\n".join(lines)
