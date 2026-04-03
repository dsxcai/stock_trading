from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Dict, Optional


def calendar_year_block(calendar_cfg: Dict[str, Any], day_value: date) -> Dict[str, Any]:
    years = calendar_cfg.get("years") or {}
    year_block = years.get(f"{day_value.year:04d}")
    return year_block if isinstance(year_block, dict) else {}


def closed_set_for_year_block(year_block: Dict[str, Any]) -> set[str]:
    closed = year_block.get("closed")
    if not isinstance(closed, dict):
        return set()
    return {
        str(date_et).strip()
        for date_et in closed.keys()
        if str(date_et).strip()
    }


def calendar_day_reason(year_block: Dict[str, Any], key: str, day_value: date) -> str:
    payload = year_block.get(key)
    if not isinstance(payload, dict):
        return ""
    return str(payload.get(day_value.isoformat()) or "").strip()


def closed_reason(calendar_cfg: Dict[str, Any], day_value: date) -> str:
    return calendar_day_reason(calendar_year_block(calendar_cfg, day_value), "closed", day_value)


def trade_no_settlement_reason(calendar_cfg: Dict[str, Any], day_value: date) -> str:
    return calendar_day_reason(calendar_year_block(calendar_cfg, day_value), "trade_no_settlement", day_value)


def early_close_payload(calendar_cfg: Dict[str, Any], day_value: date) -> Dict[str, Any]:
    year_block = calendar_year_block(calendar_cfg, day_value)
    early_close = year_block.get("early_close")
    if not isinstance(early_close, dict):
        return {}
    payload = early_close.get(day_value.isoformat()) or {}
    return dict(payload) if isinstance(payload, dict) else {}


def is_weekend_et(day_value: date) -> bool:
    return day_value.weekday() >= 5


def is_trading_day(calendar_cfg: Dict[str, Any], day_value: date) -> bool:
    if is_weekend_et(day_value):
        return False
    return not bool(closed_reason(calendar_cfg, day_value))


def prev_trading_day(calendar_cfg: Dict[str, Any], anchor_day: date, *, max_days: int = 370) -> Optional[date]:
    probe = anchor_day
    for _ in range(max_days):
        probe = probe - timedelta(days=1)
        if is_trading_day(calendar_cfg, probe):
            return probe
    return None


def next_trading_day(calendar_cfg: Dict[str, Any], anchor_day: date, *, max_days: int = 370) -> Optional[date]:
    probe = anchor_day
    for _ in range(max_days):
        probe = probe + timedelta(days=1)
        if is_trading_day(calendar_cfg, probe):
            return probe
    return None


def trading_day_status_text(calendar_cfg: Dict[str, Any], day_value: date) -> str:
    reason = closed_reason(calendar_cfg, day_value)
    if reason:
        return f"Closed ({reason})"
    if is_weekend_et(day_value):
        return "Closed (Weekend)"
    reason = trade_no_settlement_reason(calendar_cfg, day_value)
    if reason:
        return f"Open (Trade, No Settlement: {reason})"
    early_close = early_close_payload(calendar_cfg, day_value)
    close_time_et = str(early_close.get("close_time_et") or "").strip()
    reason = str(early_close.get("reason") or "").strip()
    if close_time_et and reason:
        return f"Early Close {close_time_et} ({reason})"
    if close_time_et:
        return f"Early Close {close_time_et}"
    if reason:
        return f"Early Close ({reason})"
    return "Open"
