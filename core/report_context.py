from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

from core.models import ReportContext
from core.report_meta import _normalize_mode_key
from core.runtime_io import _runtime_config, _runtime_data_config
from utils.config_access import config_trading_calendar
from utils.dates import ET_TZ, _to_yyyy_mm_dd
from utils.trading_calendar import (
    closed_reason,
    closed_set_for_year_block as _closed_set_for_year_block,
    early_close_payload,
    is_weekend_et as _is_weekend_et,
)

_OPEN_TIME_ET = time(9, 30)
_DEFAULT_CLOSE_TIME_ET = time(16, 0)


def _ensure_trading_calendar(runtime: Dict[str, Any]) -> Dict[str, Any]:
    cal = _runtime_data_config(runtime).setdefault("trading_calendar", {})
    if not isinstance(cal, dict):
        cal = {}
        _runtime_data_config(runtime)["trading_calendar"] = cal
    if not isinstance(cal.get("years"), dict):
        cal["years"] = {}
    return cal


def _next_trading_day_et_from_states(runtime: Dict[str, Any], t_et: str) -> Optional[str]:
    t_et = str(t_et or "").strip()
    if not t_et:
        return None
    try:
        years = (config_trading_calendar(_runtime_config(runtime)).get("years") or {})
        d = date.fromisoformat(_to_yyyy_mm_dd(t_et))
        for _ in range(370):
            d += timedelta(days=1)
            if _is_weekend_et(d):
                continue
            year_block = years.get(f"{d.year:04d}")
            if not isinstance(year_block, dict):
                break
            if d.isoformat() not in _closed_set_for_year_block(year_block):
                return d.isoformat()
    except Exception:
        pass
    try:
        import pandas as pd
        import exchange_calendars as xc

        cal = xc.get_calendar("XNYS")
        ts = pd.Timestamp(_to_yyyy_mm_dd(t_et))
        if not cal.is_session(ts):
            ts = cal.previous_session(ts)
        return str(cal.next_session(ts).date())
    except Exception:
        pass
    try:
        d = date.fromisoformat(_to_yyyy_mm_dd(t_et))
        for _ in range(10):
            d += timedelta(days=1)
            if not _is_weekend_et(d):
                return d.isoformat()
    except Exception:
        return None
    return None


def _prev_trading_day_et_from_states(runtime: Dict[str, Any], t_et: str) -> Optional[str]:
    t_et = str(t_et or "").strip()
    if not t_et:
        return None
    try:
        years = (config_trading_calendar(_runtime_config(runtime)).get("years") or {})
        d = date.fromisoformat(_to_yyyy_mm_dd(t_et))
        for _ in range(370):
            d -= timedelta(days=1)
            if _is_weekend_et(d):
                continue
            year_block = years.get(f"{d.year:04d}")
            if not isinstance(year_block, dict):
                break
            if d.isoformat() not in _closed_set_for_year_block(year_block):
                return d.isoformat()
    except Exception:
        pass
    try:
        import pandas as pd
        import exchange_calendars as xc

        cal = xc.get_calendar("XNYS")
        ts = pd.Timestamp(_to_yyyy_mm_dd(t_et))
        if not cal.is_session(ts):
            ts = cal.next_session(ts)
        return str(cal.previous_session(ts).date())
    except Exception:
        pass
    try:
        d = date.fromisoformat(_to_yyyy_mm_dd(t_et))
        for _ in range(10):
            d -= timedelta(days=1)
            if not _is_weekend_et(d):
                return d.isoformat()
    except Exception:
        return None
    return None


def _is_full_day_closed_et(runtime: Dict[str, Any], d: date) -> bool:
    if _is_weekend_et(d):
        return True
    try:
        return bool(closed_reason(config_trading_calendar(_runtime_config(runtime)), d))
    except Exception:
        return False


def _is_trading_day_et(runtime: Dict[str, Any], d: date) -> bool:
    return not _is_full_day_closed_et(runtime, d)


def _close_time_et_from_states(runtime: Dict[str, Any], d: date) -> time:
    try:
        raw = str((early_close_payload(config_trading_calendar(_runtime_config(runtime)), d) or {}).get("close_time_et") or "").strip()
        if raw:
            parts = raw.split(":")
            return time(int(parts[0]), int(parts[1]) if len(parts) > 1 else 0)
    except Exception:
        pass
    return _DEFAULT_CLOSE_TIME_ET


def _session_class_for_now_et(runtime: Dict[str, Any], now_et: datetime) -> str:
    d = now_et.date()
    if not _is_trading_day_et(runtime, d):
        return "closed"
    open_dt = datetime.combine(d, _OPEN_TIME_ET, tzinfo=ZoneInfo(ET_TZ))
    close_dt = datetime.combine(d, _close_time_et_from_states(runtime, d), tzinfo=ZoneInfo(ET_TZ))
    if now_et < open_dt:
        return "premarket"
    if now_et < close_dt:
        return "intraday"
    return "afterclose"


def _version_anchor_day_et(mode: Any, t_et: str, t_plus_1_et: Optional[str]) -> Optional[str]:
    mode_key = _normalize_mode_key(mode)
    if mode_key == "premarket":
        return t_plus_1_et or t_et
    if mode_key in {"intraday", "afterclose"}:
        return t_et or t_plus_1_et
    return t_plus_1_et or t_et


def _resolve_report_context(states: Dict[str, Any], runtime: Dict[str, Any], mode_label: str, now_et: datetime) -> ReportContext:
    del states
    mode_key = _normalize_mode_key(mode_label)
    session = _session_class_for_now_et(runtime, now_et)
    today = now_et.date().isoformat()
    now_iso = now_et.replace(microsecond=0).isoformat()
    if mode_key == "premarket":
        if session == "premarket":
            t_et = _prev_trading_day_et_from_states(runtime, today) or today
            return ReportContext(mode_label, mode_key, session, now_iso, t_et, today, today, t_et, "", "eod", True, "today is a trading day before the open; premarket uses the latest completed trading day as t and today as t+1.", "")
        if session == "afterclose":
            t1 = _next_trading_day_et_from_states(runtime, today) or today
            return ReportContext(mode_label, mode_key, session, now_iso, today, t1, t1, today, "", "eod", True, "market has already closed; premarket can reasonably prepare the next trading day.", "")
        if session == "closed":
            t1 = _next_trading_day_et_from_states(runtime, today) or today
            t_et = _prev_trading_day_et_from_states(runtime, t1) or t1
            return ReportContext(mode_label, mode_key, session, now_iso, t_et, t1, t1, t_et, "", "eod", True, "today is not a trading day; premarket maps to the next trading day.", "")
        t_et = _prev_trading_day_et_from_states(runtime, today) or today
        return ReportContext(mode_label, mode_key, session, now_iso, t_et, today, today, t_et, "", "eod", False, "premarket is defined before the regular session opens.", "current ET session is intraday, so premarket semantics are no longer valid.")
    if mode_key == "intraday":
        next_after_today = _next_trading_day_et_from_states(runtime, today) or today
        if session == "intraday":
            return ReportContext(mode_label, mode_key, session, now_iso, today, next_after_today, today, today, now_iso, "intraday", True, "market is open; intraday uses today as t and the next trading day as t+1.", "")
        if session == "premarket":
            return ReportContext(mode_label, mode_key, session, now_iso, today, next_after_today, today, today, now_iso, "intraday", False, "intraday requires an active regular session.", "current ET session is premarket; the regular session has not started yet.")
        if session == "afterclose":
            return ReportContext(mode_label, mode_key, session, now_iso, today, next_after_today, today, today, now_iso, "intraday", False, "intraday requires an active regular session.", "current ET session is afterclose; the regular session has already ended.")
        t_et = _next_trading_day_et_from_states(runtime, today) or today
        t1 = _next_trading_day_et_from_states(runtime, t_et) or t_et
        return ReportContext(mode_label, mode_key, session, now_iso, t_et, t1, t_et, t_et, now_iso, "intraday", False, "intraday requires an active trading day.", "today is not a trading day, so intraday semantics are unavailable.")
    if mode_key == "afterclose":
        if session == "afterclose":
            t1 = _next_trading_day_et_from_states(runtime, today) or today
            return ReportContext(mode_label, mode_key, session, now_iso, today, t1, today, today, "", "eod", True, "market has closed; afterclose uses today as t and the next trading day as t+1.", "")
        if session == "premarket":
            t_et = _prev_trading_day_et_from_states(runtime, today) or today
            return ReportContext(mode_label, mode_key, session, now_iso, t_et, today, t_et, t_et, "", "eod", True, "before the open, the latest completed trading day is the previous trading day, which is valid for afterclose.", "")
        if session == "closed":
            t_et = _prev_trading_day_et_from_states(runtime, today) or today
            t1 = _next_trading_day_et_from_states(runtime, today) or today
            return ReportContext(mode_label, mode_key, session, now_iso, t_et, t1, t_et, t_et, "", "eod", True, "today is not a trading day; afterclose maps to the latest completed trading day.", "")
        t1 = _next_trading_day_et_from_states(runtime, today) or today
        return ReportContext(mode_label, mode_key, session, now_iso, today, t1, today, today, "", "eod", False, "afterclose requires a completed session close for t.", "current ET session is intraday, so today's close is not finalized yet.")
    raise ValueError(f"unsupported mode: {mode_label}")


def _report_meta_from_mode_dates(mode_label: str, t_et: str, t_plus_1_et: Optional[str], *, generated_at_et: str = "") -> Dict[str, Any]:
    mode_key = _normalize_mode_key(mode_label)
    meta = {
        "mode": str(mode_label or "").strip(),
        "mode_key": mode_key,
        "signal_basis": {"t_et": t_et, "basis": "NYSE Intraday" if mode_key == "intraday" else "NYSE Close"},
        "execution_basis": {"t_plus_1_et": t_plus_1_et, "basis": "NYSE Trading Day"},
    }
    version_anchor_et = _version_anchor_day_et(mode_label, t_et, t_plus_1_et)
    if version_anchor_et:
        meta["version_anchor_et"] = version_anchor_et
    if str(generated_at_et or "").strip():
        meta["generated_at_et"] = str(generated_at_et).strip()
    return meta


def _report_meta_from_context(ctx: ReportContext) -> Dict[str, Any]:
    return _report_meta_from_mode_dates(ctx.mode_label, ctx.t_et, ctx.t_plus_1_et, generated_at_et=ctx.now_et_iso)


def _report_meta_from_report_date(runtime: Dict[str, Any], mode_label: str, report_date: str, *, generated_at_et: str = "") -> Dict[str, Any]:
    anchor_et = _to_yyyy_mm_dd(report_date)
    if _normalize_mode_key(mode_label) == "premarket":
        return _report_meta_from_mode_dates(mode_label, _prev_trading_day_et_from_states(runtime, anchor_et) or anchor_et, anchor_et, generated_at_et=generated_at_et)
    return _report_meta_from_mode_dates(mode_label, anchor_et, _next_trading_day_et_from_states(runtime, anchor_et) or anchor_et, generated_at_et=generated_at_et)


def _resolve_runtime_report_meta(runtime: Dict[str, Any], mode_label: str, report_date: str = "", now_et: Optional[datetime] = None) -> Dict[str, Any]:
    resolved_now_et = now_et.astimezone(ZoneInfo(ET_TZ)) if isinstance(now_et, datetime) and now_et.tzinfo else now_et
    if isinstance(resolved_now_et, datetime) and resolved_now_et.tzinfo is None:
        resolved_now_et = resolved_now_et.replace(tzinfo=ZoneInfo(ET_TZ))
    if not isinstance(resolved_now_et, datetime):
        resolved_now_et = datetime.now(ZoneInfo(ET_TZ))
    report_date = str(report_date or "").strip()
    if report_date:
        return _report_meta_from_report_date(runtime, mode_label, report_date, generated_at_et=resolved_now_et.replace(microsecond=0).isoformat())
    return _report_meta_from_context(_resolve_report_context({}, runtime, mode_label, resolved_now_et))


def _report_date_from_meta(meta: Dict[str, Any]) -> str:
    for value in (
        meta.get("version_anchor_et"),
        (meta.get("execution_basis") or {}).get("t_plus_1_et"),
        (meta.get("signal_basis") or {}).get("t_et"),
    ):
        report_date = str(value or "").strip()
        if report_date:
            return report_date
    return ""


def _parse_broker_asof(states: Dict[str, Any], broker_asof_et: str, broker_asof_et_time: str, broker_asof_et_datetime: str, mode: str = "") -> Tuple[Optional[str], Optional[datetime], str]:
    broker_asof_et = str(broker_asof_et or "").strip()
    broker_asof_et_time = str(broker_asof_et_time or "").strip()
    broker_asof_et_datetime = str(broker_asof_et_datetime or "").strip()
    if (broker_asof_et_time or broker_asof_et_datetime) and not broker_asof_et:
        raise ValueError("--broker-asof-et is required when --broker-asof-et-time or --broker-asof-et-datetime is provided.")
    if not broker_asof_et:
        broker_asof_et = str((((states.get("portfolio") or {}).get("broker") or {}).get("asof_et")) or "").strip()
    if not broker_asof_et:
        broker_asof_et = str((((states.get("market") or {})).get("asof_t_et")) or "").strip()
    if broker_asof_et:
        broker_asof_et = _to_yyyy_mm_dd(broker_asof_et)
    mode_key = _normalize_mode_key(mode)
    snapshot_kind = "intraday" if mode_key == "intraday" else "eod" if mode_key in {"premarket", "afterclose"} else "unknown"
    return broker_asof_et or None, None, snapshot_kind
