from __future__ import annotations
import argparse
import csv
import json
import os
import re
from zoneinfo import ZoneInfo
from datetime import datetime, date, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from core.models import ImportResult, ReportContext
from core.report_bundle import build_report_root, ensure_report_root_fields
from core.reconciliation import (
    _first_token_ticker,
    _normalize_trades_inplace,
    _num_from_cell,
    _trade_key,
    _upsert_trades,
    _verify_holdings_with_broker_investment_total,
)
from core.strategy import (
    _dedupe_by_date_keep_last,
    _fmt_usd,
    _normalize_ma_rule,
    _parse_indicator_window,
    _read_ohlcv_csv,
)
from core.tactical_engine import apply_tactical_plan, compute_tactical_plan
from utils.parsers import (
    _normalize_time_tw,
    _normalize_trade_date_et,
    _parse_ymd_loose,
    _safe_float,
    _safe_int,
    _to_yyyy_mm_dd,
    _trade_time_tw_to_et_dt,
)
from utils.precision import format_fixed, round_with_precision, state_engine_numeric_precision

def _load_json(path: str) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding='utf-8'))

def _load_runtime_config(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    raw = json.loads(p.read_text(encoding='utf-8'))
    if not isinstance(raw, dict):
        raise TypeError(f'config root must be an object: {path}')
    scoped = raw.get('state_engine')
    if not isinstance(scoped, dict):
        raise KeyError(f"config.json must contain object key 'state_engine': {path}")
    return dict(scoped)

def _runtime_config_value(states: Dict[str, Any], key: str, default: Any=None) -> Any:
    cfg = states.get('config', {}) or {}
    value = cfg.get(key, default)
    return default if value is None else value

def _runtime_config(runtime: Dict[str, Any]) -> Dict[str, Any]:
    cfg = runtime.get('config') or {}
    return cfg if isinstance(cfg, dict) else {}


def _runtime_numeric_precision(runtime: Dict[str, Any]) -> Dict[str, int]:
    return state_engine_numeric_precision(_runtime_config(runtime))

def _runtime_history(runtime: Dict[str, Any]) -> Dict[str, Any]:
    hist = runtime.get('history')
    if not isinstance(hist, dict):
        hist = {}
        runtime['history'] = hist
    return hist

def _save_json(obj: Dict[str, Any], path: str) -> str:
    payload = json.dumps(obj, ensure_ascii=False, indent=2)
    p = Path(path)
    try:
        p.write_text(payload, encoding='utf-8')
        return str(p)
    except PermissionError:
        fallback = p.with_name(f'{p.stem}.new{p.suffix}')
        fallback.write_text(payload, encoding='utf-8')
        print(f'[WARN] Cannot write {p} (permission denied). Wrote fallback: {fallback}')
        return str(fallback)

def _load_trades_payload(path: str) -> Optional[List[Dict[str, Any]]]:
    p = Path(path)
    if not p.exists():
        return None
    obj = json.loads(p.read_text(encoding='utf-8'))
    if isinstance(obj, list):
        out = []
        for t in obj:
            if isinstance(t, dict):
                out.append(dict(t))
        return out
    if isinstance(obj, dict) and isinstance(obj.get('trades'), list):
        out = []
        for t in obj.get('trades') or []:
            if isinstance(t, dict):
                out.append(dict(t))
        return out
    return None

def _load_imported_trades_json(path: str) -> List[Dict[str, Any]]:
    trades = _load_trades_payload(path)
    if trades is None:
        raise ValueError(f"{path}: imported trades JSON must be a list or an object with a 'trades' array")
    return trades

def _trade_import_label(path: str, trades: List[Dict[str, Any]]) -> str:
    for trade in trades:
        if not isinstance(trade, dict):
            continue
        source_file = str(trade.get("source_file") or "").strip()
        if source_file:
            return source_file
        source = str(trade.get("source") or "").strip()
        if source:
            return source
    return Path(path).name

def _save_trades_payload(trades: List[Dict[str, Any]], path: str) -> str:
    payload = json.dumps(trades, ensure_ascii=False, indent=2)
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(payload, encoding='utf-8')
    return str(p)

def _compact_trade_row(trade: Dict[str, Any]) -> Dict[str, Any]:
    keep = [
        'trade_id',
        'trade_date_et',
        'time_tw',
        'ticker',
        'side',
        'shares',
        'cash_amount',
        'price',
        'gross',
        'fee',
        'notes',
        'source',
    ]
    out: Dict[str, Any] = {}
    for k in keep:
        if k in trade:
            v = trade.get(k)
            if v is None:
                continue
            if isinstance(v, str) and not v.strip():
                continue
            out[k] = v
    return out

def _round_selected_numeric_fields(obj: Any, keys: set, ndigits: int=4) -> None:
    if isinstance(obj, dict):
        for k, v in list(obj.items()):
            if k in keys and isinstance(v, (int, float)) and not isinstance(v, bool):
                obj[k] = round(float(v), ndigits)
            else:
                _round_selected_numeric_fields(v, keys, ndigits=ndigits)
    elif isinstance(obj, list):
        for item in obj:
            _round_selected_numeric_fields(item, keys, ndigits=ndigits)
_ET_TZ = 'America/New_York'
_TW_TZ = 'Asia/Taipei'
_OPEN_TIME_ET = time(9, 30)
_DEFAULT_CLOSE_TIME_ET = time(16, 0)

def _default_trading_calendar_2026() -> Dict[str, Any]:
    return {'full_day_closed': [{'date_et': '2026-01-01', 'name': 'New Year’s Day', 'note': 'Market closed'}, {'date_et': '2026-01-19', 'name': 'Martin Luther King, Jr. Day', 'note': 'Market closed'}, {'date_et': '2026-02-16', 'name': 'Presidents Day (Washington’s Birthday)', 'note': 'Market closed'}, {'date_et': '2026-04-03', 'name': 'Good Friday', 'note': 'Market closed'}, {'date_et': '2026-05-25', 'name': 'Memorial Day', 'note': 'Market closed'}, {'date_et': '2026-06-19', 'name': 'Juneteenth', 'note': 'Market closed'}, {'date_et': '2026-07-03', 'name': 'Independence Day (Observed)', 'note': 'Market closed'}, {'date_et': '2026-09-07', 'name': 'Labor Day', 'note': 'Market closed'}, {'date_et': '2026-11-26', 'name': 'Thanksgiving Day', 'note': 'Market closed'}, {'date_et': '2026-12-25', 'name': 'Christmas Day', 'note': 'Market closed'}], 'early_close': [{'date_et': '2026-11-27', 'reason': 'Day After Thanksgiving', 'close_time_et': '13:00', 'note': 'Eligible options are usually available until 13:15 (subject to exchange notice)'}, {'date_et': '2026-12-24', 'reason': 'Christmas Eve', 'close_time_et': '13:00', 'note': 'Eligible options are usually available until 13:15 (subject to exchange notice)'}]}

def _ensure_trading_calendar(runtime: Dict[str, Any]) -> Dict[str, Any]:
    cfg = _runtime_config(runtime)
    cal = cfg.setdefault('trading_calendar', {})
    cal.setdefault('exchange', 'XNYS')
    cal.setdefault('timezone', _ET_TZ)
    years = cal.setdefault('years', {})
    years.setdefault('2026', _default_trading_calendar_2026())
    cal.setdefault('source_refs', [{'title': 'NYSE Group holiday/early close calendar (2024-2026)', 'url': 'https://www.nasdaq.com/press-release/nyse-group-announces-2024-2025-and-2026-holiday-and-early-closings-calendar-2023-11'}, {'title': 'Nasdaq holiday schedule', 'url': 'https://www.nasdaq.com/'}])
    return cal

def _is_weekend_et(d: date) -> bool:
    return d.weekday() >= 5

def _closed_set_for_year_block(year_block: Dict[str, Any]) -> set:
    s = set()
    for item in year_block.get('full_day_closed') or []:
        if isinstance(item, dict) and item.get('date_et'):
            s.add(str(item['date_et']).strip())
    return s

def _next_trading_day_et_from_states(runtime: Dict[str, Any], t_et: str) -> Optional[str]:
    t_et = str(t_et or '').strip()
    if not t_et:
        return None
    try:
        cal = _runtime_config(runtime).get('trading_calendar') or {}
        years = cal.get('years') or {}
        d = date.fromisoformat(_to_yyyy_mm_dd(t_et))
        for _ in range(370):
            d = d + timedelta(days=1)
            if _is_weekend_et(d):
                continue
            y = f'{d.year:04d}'
            year_block = years.get(y)
            if not isinstance(year_block, dict):
                break
            closed = _closed_set_for_year_block(year_block)
            ds = d.isoformat()
            if ds in closed:
                continue
            return ds
    except Exception:
        pass
    try:
        import pandas as pd
        import exchange_calendars as xc
        cal = xc.get_calendar('XNYS')
        ts = pd.Timestamp(_to_yyyy_mm_dd(t_et))
        if not cal.is_session(ts):
            ts = cal.previous_session(ts)
        nxt = cal.next_session(ts)
        return str(nxt.date())
    except Exception:
        pass
    try:
        d = date.fromisoformat(_to_yyyy_mm_dd(t_et))
        for _ in range(10):
            d = d + timedelta(days=1)
            if _is_weekend_et(d):
                continue
            return d.isoformat()
    except Exception:
        return None
    return None

def _prev_trading_day_et_from_states(runtime: Dict[str, Any], t_et: str) -> Optional[str]:
    t_et = str(t_et or '').strip()
    if not t_et:
        return None
    try:
        cal = _runtime_config(runtime).get('trading_calendar') or {}
        years = cal.get('years') or {}
        d = date.fromisoformat(_to_yyyy_mm_dd(t_et))
        for _ in range(370):
            d = d - timedelta(days=1)
            if _is_weekend_et(d):
                continue
            y = f'{d.year:04d}'
            year_block = years.get(y)
            if not isinstance(year_block, dict):
                break
            closed = _closed_set_for_year_block(year_block)
            ds = d.isoformat()
            if ds in closed:
                continue
            return ds
    except Exception:
        pass
    try:
        import pandas as pd
        import exchange_calendars as xc
        cal = xc.get_calendar('XNYS')
        ts = pd.Timestamp(_to_yyyy_mm_dd(t_et))
        if not cal.is_session(ts):
            ts = cal.next_session(ts)
        prev = cal.previous_session(ts)
        return str(prev.date())
    except Exception:
        pass
    try:
        d = date.fromisoformat(_to_yyyy_mm_dd(t_et))
        for _ in range(10):
            d = d - timedelta(days=1)
            if _is_weekend_et(d):
                continue
            return d.isoformat()
    except Exception:
        return None
    return None

def _is_full_day_closed_et(runtime: Dict[str, Any], d: date) -> bool:
    if _is_weekend_et(d):
        return True
    try:
        cal = _runtime_config(runtime).get('trading_calendar') or {}
        years = cal.get('years') or {}
        year_block = years.get(f'{d.year:04d}') or {}
        return d.isoformat() in _closed_set_for_year_block(year_block)
    except Exception:
        return False

def _is_trading_day_et(runtime: Dict[str, Any], d: date) -> bool:
    return not _is_full_day_closed_et(runtime, d)

def _close_time_et_from_states(runtime: Dict[str, Any], d: date) -> time:
    try:
        cal = _runtime_config(runtime).get('trading_calendar') or {}
        years = cal.get('years') or {}
        year_block = years.get(f'{d.year:04d}') or {}
        for item in year_block.get('early_close') or []:
            if not isinstance(item, dict):
                continue
            if str(item.get('date_et') or '').strip() != d.isoformat():
                continue
            raw = str(item.get('close_time_et') or '').strip()
            if not raw:
                continue
            parts = raw.split(':')
            hh = int(parts[0])
            mm = int(parts[1]) if len(parts) > 1 else 0
            return time(hh, mm)
    except Exception:
        pass
    return _DEFAULT_CLOSE_TIME_ET

def _session_class_for_now_et(runtime: Dict[str, Any], now_et: datetime) -> str:
    d = now_et.date()
    if not _is_trading_day_et(runtime, d):
        return 'closed'
    open_dt = datetime.combine(d, _OPEN_TIME_ET, tzinfo=ZoneInfo(_ET_TZ))
    close_dt = datetime.combine(d, _close_time_et_from_states(runtime, d), tzinfo=ZoneInfo(_ET_TZ))
    if now_et < open_dt:
        return 'premarket'
    if now_et < close_dt:
        return 'intraday'
    return 'afterclose'

def _resolve_report_context(states: Dict[str, Any], runtime: Dict[str, Any], mode_label: str, now_et: datetime) -> ReportContext:
    mode_key = _normalize_mode_key(mode_label)
    session = _session_class_for_now_et(runtime, now_et)
    today = now_et.date()
    now_iso = now_et.replace(microsecond=0).isoformat()
    if mode_key == 'premarket':
        if session == 'premarket':
            t_et = _prev_trading_day_et_from_states(runtime, today.isoformat()) or today.isoformat()
            t1 = today.isoformat()
            return ReportContext(mode_label, mode_key, session, now_iso, t_et, t1, t1, t_et, '', 'eod', True, 'today is a trading day before the open; premarket uses the latest completed trading day as t and today as t+1.', '')
        if session == 'afterclose':
            t_et = today.isoformat()
            t1 = _next_trading_day_et_from_states(runtime, t_et) or t_et
            return ReportContext(mode_label, mode_key, session, now_iso, t_et, t1, t1, t_et, '', 'eod', True, 'market has already closed; premarket can reasonably prepare the next trading day.', '')
        if session == 'closed':
            t1 = _next_trading_day_et_from_states(runtime, today.isoformat()) or today.isoformat()
            t_et = _prev_trading_day_et_from_states(runtime, t1) or t1
            return ReportContext(mode_label, mode_key, session, now_iso, t_et, t1, t1, t_et, '', 'eod', True, 'today is not a trading day; premarket maps to the next trading day.', '')
        t_et = _prev_trading_day_et_from_states(runtime, today.isoformat()) or today.isoformat()
        return ReportContext(mode_label, mode_key, session, now_iso, t_et, today.isoformat(), today.isoformat(), t_et, '', 'eod', False, 'premarket is defined before the regular session opens.', 'current ET session is intraday, so premarket semantics are no longer valid.')
    if mode_key == 'intraday':
        next_after_today = _next_trading_day_et_from_states(runtime, today.isoformat()) or today.isoformat()
        if session == 'intraday':
            return ReportContext(mode_label, mode_key, session, now_iso, today.isoformat(), next_after_today, today.isoformat(), today.isoformat(), now_iso, 'intraday', True, 'market is open; intraday uses today as t and the next trading day as t+1.', '')
        if session == 'premarket':
            return ReportContext(mode_label, mode_key, session, now_iso, today.isoformat(), next_after_today, today.isoformat(), today.isoformat(), now_iso, 'intraday', False, 'intraday requires an active regular session.', 'current ET session is premarket; the regular session has not started yet.')
        if session == 'afterclose':
            return ReportContext(mode_label, mode_key, session, now_iso, today.isoformat(), next_after_today, today.isoformat(), today.isoformat(), now_iso, 'intraday', False, 'intraday requires an active regular session.', 'current ET session is afterclose; the regular session has already ended.')
        t_et = _next_trading_day_et_from_states(runtime, today.isoformat()) or today.isoformat()
        t1 = _next_trading_day_et_from_states(runtime, t_et) or t_et
        return ReportContext(mode_label, mode_key, session, now_iso, t_et, t1, t_et, t_et, now_iso, 'intraday', False, 'intraday requires an active trading day.', 'today is not a trading day, so intraday semantics are unavailable.')
    if mode_key == 'afterclose':
        if session == 'afterclose':
            t_et = today.isoformat()
            t1 = _next_trading_day_et_from_states(runtime, t_et) or t_et
            return ReportContext(mode_label, mode_key, session, now_iso, t_et, t1, t_et, t_et, '', 'eod', True, 'market has closed; afterclose uses today as t and the next trading day as t+1.', '')
        if session == 'premarket':
            t_et = _prev_trading_day_et_from_states(runtime, today.isoformat()) or today.isoformat()
            return ReportContext(mode_label, mode_key, session, now_iso, t_et, today.isoformat(), t_et, t_et, '', 'eod', True, 'before the open, the latest completed trading day is the previous trading day, which is valid for afterclose.', '')
        if session == 'closed':
            t_et = _prev_trading_day_et_from_states(runtime, today.isoformat()) or today.isoformat()
            t1 = _next_trading_day_et_from_states(runtime, today.isoformat()) or today.isoformat()
            return ReportContext(mode_label, mode_key, session, now_iso, t_et, t1, t_et, t_et, '', 'eod', True, 'today is not a trading day; afterclose maps to the latest completed trading day.', '')
        t1 = _next_trading_day_et_from_states(runtime, today.isoformat()) or today.isoformat()
        return ReportContext(mode_label, mode_key, session, now_iso, today.isoformat(), t1, today.isoformat(), today.isoformat(), '', 'eod', False, 'afterclose requires a completed session close for t.', "current ET session is intraday, so today's close is not finalized yet.")
    raise ValueError(f'unsupported mode: {mode_label}')

def _get_mode_snapshot(states: Dict[str, Any], mode: Any) -> Dict[str, Any]:
    mode_label = str(mode or '').strip()
    mode_key = _normalize_mode_key(mode_label)
    if not mode_key:
        return {}
    store = states.get('by_mode')
    if not isinstance(store, dict):
        return {}
    snap = store.get(mode_key)
    if not isinstance(snap, dict):
        return {}
    out = dict(snap)
    out.setdefault('mode', mode_label or out.get('mode') or mode_key)
    out.setdefault('mode_key', mode_key)
    return out

def _snapshot_effective_meta(states: Dict[str, Any], mode: Any) -> Dict[str, Any]:
    eff = dict(states.get('meta') or {})
    snap = _get_mode_snapshot(states, mode)
    if snap:
        for k in ('signal_basis', 'execution_basis', 'version_anchor_et', 'version'):
            if k in snap:
                eff[k] = snap.get(k)
        eff['mode'] = snap.get('mode') or str(mode or '').strip()
    return eff

def _iter_mode_candidate_days(states: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    store = states.get('by_mode') or {}
    if not isinstance(store, dict):
        return out
    for snap in store.values():
        if not isinstance(snap, dict):
            continue
        for v in (snap.get('version_anchor_et'), (snap.get('signal_basis') or {}).get('t_et'), (snap.get('execution_basis') or {}).get('t_plus_1_et')):
            if isinstance(v, str) and re.fullmatch('\\d{4}-\\d{2}-\\d{2}', v):
                out.append(v)
    return out

def _migrate_state_schema(states: Dict[str, Any]) -> None:
    states.setdefault('meta', {})
    store = states.get('by_mode')
    if isinstance(store, dict):
        for snap in store.values():
            if isinstance(snap, dict):
                snap.pop('report_context', None)
                snap.pop('broker_context', None)
    states.setdefault('portfolio', {})

def _report_meta_from_mode_dates(mode_label: str, t_et: str, t_plus_1_et: Optional[str]) -> Dict[str, Any]:
    mode_key = _normalize_mode_key(mode_label)
    version_anchor_et = _version_anchor_day_et(mode_label, t_et, t_plus_1_et)
    meta = {
        'mode': str(mode_label or '').strip(),
        'mode_key': mode_key,
        'signal_basis': {'t_et': t_et, 'basis': 'NYSE Intraday' if mode_key == 'intraday' else 'NYSE Close'},
        'execution_basis': {'t_plus_1_et': t_plus_1_et, 'basis': 'NYSE Trading Day'},
    }
    if version_anchor_et:
        meta['version_anchor_et'] = version_anchor_et
    return meta

def _report_meta_from_context(ctx: ReportContext) -> Dict[str, Any]:
    return _report_meta_from_mode_dates(ctx.mode_label, ctx.t_et, ctx.t_plus_1_et)

def _report_meta_from_report_date(runtime: Dict[str, Any], mode_label: str, report_date: str) -> Dict[str, Any]:
    anchor_et = _to_yyyy_mm_dd(report_date)
    mode_key = _normalize_mode_key(mode_label)
    if mode_key == 'premarket':
        t_plus_1_et = anchor_et
        t_et = _prev_trading_day_et_from_states(runtime, anchor_et) or anchor_et
    else:
        t_et = anchor_et
        t_plus_1_et = _next_trading_day_et_from_states(runtime, anchor_et) or anchor_et
    return _report_meta_from_mode_dates(mode_label, t_et, t_plus_1_et)

def _resolve_runtime_report_meta(runtime: Dict[str, Any], mode_label: str, report_date: str='') -> Dict[str, Any]:
    report_date = str(report_date or '').strip()
    if report_date:
        return _report_meta_from_report_date(runtime, mode_label, report_date)
    now_et = datetime.now(ZoneInfo(_ET_TZ))
    return _report_meta_from_context(_resolve_report_context({}, runtime, mode_label, now_et))

def _report_date_from_meta(meta: Dict[str, Any]) -> str:
    report_date = str(meta.get('version_anchor_et') or '').strip()
    if report_date:
        return report_date
    execution_basis = meta.get('execution_basis') or {}
    report_date = str(execution_basis.get('t_plus_1_et') or '').strip()
    if report_date:
        return report_date
    signal_basis = meta.get('signal_basis') or {}
    return str(signal_basis.get('t_et') or '').strip()

def _strip_persisted_report_transients(states: Dict[str, Any]) -> None:
    market = states.get('market')
    if isinstance(market, dict):
        market.pop('signals_inputs', None)
        market.pop('next_close_threshold_inputs', None)
    portfolio = states.get('portfolio')
    if isinstance(portfolio, dict):
        positions = portfolio.get('positions')
        if isinstance(positions, list):
            for pos in positions:
                if isinstance(pos, dict):
                    pos.pop('notes', None)
    states.pop('signals', None)
    states.pop('thresholds', None)
    meta = states.get('meta')
    if isinstance(meta, dict):
        meta.pop('notes', None)
        if not meta:
            states.pop('meta', None)
    states.pop('by_mode', None)

def _build_report_output(
    states: Dict[str, Any],
    schema_path: str,
    report_dir: str,
    report_out: str,
    mode: str,
    config: Optional[Dict[str, Any]] = None,
    trades: Optional[List[Dict[str, Any]]] = None,
    tactical_plan: Optional[Any] = None,
    report_meta: Optional[Dict[str, Any]] = None,
) -> Tuple[str, str]:
    from core.reporting import load_schema as _load_report_schema, render_report as _render_report_markdown

    schema = _load_report_schema(schema_path)
    report_root = build_report_root(states, config=config, trades=trades, tactical_plan=tactical_plan, report_meta=report_meta)
    md = _render_report_markdown(report_root, schema, mode)
    meta = dict(report_meta or {})
    if not meta:
        meta = _snapshot_effective_meta(states, mode)
    report_date = _report_date_from_meta(meta)
    if not report_date:
        report_date = datetime.now().strftime('%Y-%m-%d')
    mode_key = _normalize_mode_key(mode) or 'report'
    if str(report_out or '').strip():
        out_path = str(report_out).strip()
    else:
        out_path = str(Path(report_dir) / f'{report_date}_{mode_key}.md')
    return (md, out_path)

def _normalize_mode_key(mode: Any) -> str:
    return re.sub('[\\s_\\-]+', '', str(mode or '').strip().lower())

def _version_anchor_day_et(mode: Any, t_et: str, t_plus_1_et: Optional[str]) -> Optional[str]:
    m = _normalize_mode_key(mode)
    if m == 'premarket':
        return t_plus_1_et or t_et
    if m in {'intraday', 'afterclose'}:
        return t_et or t_plus_1_et
    return t_plus_1_et or t_et

def _parse_broker_asof(states: Dict[str, Any], broker_asof_et: str, broker_asof_et_time: str, broker_asof_et_datetime: str, mode: str='') -> Tuple[Optional[str], Optional[datetime], str]:
    meta = states.get('meta', {}) or {}
    broker_asof_et = str(broker_asof_et or '').strip()
    broker_asof_et_time = str(broker_asof_et_time or '').strip()
    broker_asof_et_datetime = str(broker_asof_et_datetime or '').strip()
    if (broker_asof_et_time or broker_asof_et_datetime) and (not broker_asof_et):
        raise ValueError('--broker-asof-et is required when --broker-asof-et-time or --broker-asof-et-datetime is provided.')
    if not broker_asof_et:
        portfolio = states.get('portfolio', {}) or {}
        broker = portfolio.get('broker', {}) or {}
        broker_asof_et = str(broker.get('asof_et') or '').strip()
    if not broker_asof_et:
        market = states.get('market', {}) or {}
        broker_asof_et = str(market.get('asof_t_et') or '').strip()
    if broker_asof_et:
        broker_asof_et = _to_yyyy_mm_dd(broker_asof_et)
    mode_key = _normalize_mode_key(mode)
    if mode_key == 'intraday':
        snapshot_kind = 'intraday'
    elif mode_key in {'premarket', 'afterclose'}:
        snapshot_kind = 'eod'
    else:
        snapshot_kind = 'unknown'
    return (broker_asof_et or None, None, snapshot_kind)
    if broker_asof_et:
        d = _to_yyyy_mm_dd(broker_asof_et)
        return (d, None, 'eod')
    return (None, None, 'unknown')

def _market_history_rows_map(runtime: Dict[str, Any]) -> Dict[str, Any]:
    return _runtime_history(runtime)

def _reprice_and_totals(states: Dict[str, Any], runtime: Dict[str, Any]) -> None:
    market = states.setdefault('market', {})
    prices_now = market.setdefault('prices_now', {})
    history = _market_history_rows_map(runtime)
    portfolio = states.setdefault('portfolio', {})
    positions = portfolio.setdefault('positions', [])
    usd_amount_ndigits = int(_runtime_numeric_precision(runtime)["usd_amount"])
    _ensure_cash_buckets(states, usd_amount_ndigits=usd_amount_ndigits)
    cash = portfolio.setdefault('cash', {'usd': 0.0})
    cash_usd = float(cash.get('usd') or 0.0)
    deployable_cash_usd = float(cash.get('deployable_usd') or 0.0)
    reserve_cash_usd = float(cash.get('reserve_usd') or 0.0)
    for p in positions:
        ticker = p.get('ticker')
        if not ticker:
            continue
        override = p.get('price_now_override')
        if override is not None:
            try:
                p['price_now'] = float(override)
            except Exception:
                pass
        elif ticker in prices_now and prices_now[ticker] is not None:
            p['price_now'] = float(prices_now[ticker])
        elif ticker in history and history[ticker].get('rows'):
            p['price_now'] = float(history[ticker]['rows'][-1]['Close'])
        shares = float(p.get('shares') or 0.0)
        cost = float(p.get('cost_usd') or 0.0)
        price = p.get('price_now')
        mv = shares * float(price) if price is not None else None
        pnl = mv - cost if mv is not None else None
        p['market_value_usd'] = mv
        p['unrealized_pnl_usd'] = pnl
        if cost > 0 and pnl is not None:
            p['unrealized_pnl_pct'] = pnl / cost
        else:
            p['unrealized_pnl_pct'] = None

    def agg(bucket_names: set) -> Dict[str, float]:
        cost_sum = 0.0
        mv_sum = 0.0
        pnl_sum = 0.0
        for p in positions:
            if p.get('bucket') not in bucket_names:
                continue
            cost_sum += float(p.get('cost_usd') or 0.0)
            mv = p.get('market_value_usd')
            if mv is not None:
                mv_sum += float(mv)
            pnl = p.get('unrealized_pnl_usd')
            if pnl is not None:
                pnl_sum += float(pnl)
        return {'holdings_cost_usd': cost_sum, 'holdings_mv_usd': mv_sum, 'unrealized_pnl_usd': pnl_sum}
    core_tot = agg({'core'})
    tactical_tot = agg({'tactical', 'tactical_cash_pool'})
    portfolio_tot = agg({'core', 'tactical', 'tactical_cash_pool'})
    totals = portfolio.setdefault('totals', {})
    totals['core'] = core_tot
    totals['tactical'] = {**tactical_tot, 'cash_usd': cash_usd, 'deployable_cash_usd': deployable_cash_usd, 'reserve_cash_usd': reserve_cash_usd, 'total_assets_usd': round_with_precision(tactical_tot['holdings_mv_usd'] + cash_usd, usd_amount_ndigits)}
    totals['portfolio'] = {**portfolio_tot, 'cash_usd': cash_usd, 'deployable_cash_usd': deployable_cash_usd, 'reserve_cash_usd': reserve_cash_usd, 'nav_usd': portfolio_tot['holdings_mv_usd'] + cash_usd}

def _lookup_action_price_usd(states: Dict[str, Any], runtime: Dict[str, Any], ticker: str) -> Optional[float]:
    portfolio = states.get('portfolio', {}) or {}
    positions = portfolio.get('positions', []) or []
    market = states.get('market', {}) or {}
    history = _market_history_rows_map(runtime)
    for p in positions:
        if str(p.get('ticker') or '').upper() != ticker:
            continue
        val = p.get('price_now_override')
        if val is not None:
            try:
                return float(val)
            except Exception:
                break
        val = p.get('price_now')
        if val is not None:
            try:
                pos_price = float(val)
            except Exception:
                pos_price = None
            else:
                market_px = (market.get('prices_now') or {}).get(ticker)
                if market_px is None and (history.get(ticker) or {}).get('rows'):
                    rows = history[ticker].get('rows') or []
                    if rows:
                        market_px = rows[-1].get('Close')
                if market_px is None:
                    return pos_price
        break
    val = (market.get('prices_now') or {}).get(ticker)
    if val is not None:
        try:
            return float(val)
        except Exception:
            pass
    rows = (history.get(ticker) or {}).get('rows') or []
    if rows:
        try:
            return float(rows[-1].get('Close'))
        except Exception:
            pass
    for p in positions:
        if str(p.get('ticker') or '').upper() == ticker:
            try:
                return float(p.get('price_now')) if p.get('price_now') is not None else None
            except Exception:
                return None
    return None

def _current_signal_day_et(states: Dict[str, Any], runtime: Dict[str, Any], mode: Optional[str]=None) -> Optional[str]:
    candidates: List[Any] = []
    if mode:
        snap = _get_mode_snapshot(states, mode)
        if snap:
            candidates.extend([(snap.get('signal_basis') or {}).get('t_et'), snap.get('version_anchor_et')])
    candidates.extend(_iter_mode_candidate_days(states))
    candidates.append((states.get('market') or {}).get('asof_t_et'))
    for cand in candidates:
        s = str(cand or '').strip()
        if s:
            try:
                return _to_yyyy_mm_dd(s)
            except Exception:
                pass
    return None

def _update_signals_and_thresholds(states: Dict[str, Any], runtime: Dict[str, Any], derive_signals_inputs: str, derive_threshold_inputs: str, mode: Optional[str]=None, trades: Optional[List[Dict[str, Any]]]=None) -> None:
    plan = compute_tactical_plan(
        states,
        runtime,
        derive_signals_inputs=derive_signals_inputs,
        derive_threshold_inputs=derive_threshold_inputs,
        mode=mode,
        trades=trades,
    )
    apply_tactical_plan(states, plan)

def _discover_tickers_from_config(states: Dict[str, Any], runtime: Dict[str, Any]) -> List[str]:
    cfg = _runtime_config(runtime)
    buckets = cfg.get('buckets', {}) or {}
    tickers: List[str] = []
    tickers += list((buckets.get('core') or {}).get('tickers', []))
    tickers += list((buckets.get('tactical') or {}).get('tickers', []))
    cash_pool = (buckets.get('tactical') or {}).get('cash_pool_ticker')
    if cash_pool:
        tickers.append(str(cash_pool))
    tickers += list((buckets.get('tactical_cash_pool') or {}).get('tickers', []))
    tickers += list((cfg.get('tactical_indicators') or {}).keys())
    for p in (states.get('portfolio', {}) or {}).get('positions', []) or []:
        t = p.get('ticker')
        if t:
            tickers.append(str(t))
    seen = set()
    tickers = [t for t in tickers if t and (not (t.upper() in seen or seen.add(t.upper())))]
    return [t.upper() for t in tickers]

def _compute_keep_history_rows(states: Dict[str, Any], runtime: Dict[str, Any]) -> int:
    market = states.get('market', {}) or {}
    thr_inp = market.get('next_close_threshold_inputs', {}) or {}
    windows: List[int] = []
    for v in thr_inp.values():
        if isinstance(v, dict) and v.get('window') is not None:
            try:
                windows.append(int(v['window']))
            except Exception:
                pass
    cfg = _runtime_config(runtime)
    for ma_rule in (cfg.get('tactical_indicators') or {}).values():
        w = _parse_indicator_window(ma_rule)
        if w:
            windows.append(w)
    max_w = max(windows) if windows else 100
    return int(max_w) + 10

def _resolve_csv_candidates(runtime: Dict[str, Any], csv_dir: str, ticker: str) -> List[str]:
    cfg = _runtime_config(runtime)
    csv_sources = cfg.get('csv_sources', {}) or {}
    src = None
    if isinstance(csv_sources.get(ticker), str):
        src = csv_sources.get(ticker)
    cands: List[str] = []
    if src:
        src = str(src)
        p = src if os.path.isabs(src) else os.path.join(csv_dir, src)
        cands.append(p)
    cands.append(os.path.join(csv_dir, f'{ticker}.csv'))
    cands.append(os.path.join(csv_dir, f'{ticker}.CSV'))
    seen = set()
    out = []
    for p in cands:
        if p not in seen:
            out.append(p)
            seen.add(p)
    return out

def _import_csvs_into_states(states: Dict[str, Any], runtime: Dict[str, Any], csv_dir: str, tickers: List[str], prices_now_from: str, keep_history_rows: int, persist_market_snapshot: bool=True) -> List[ImportResult]:
    market = states.setdefault('market', {})
    runtime_history = _runtime_history(runtime)
    prices_now = market.setdefault('prices_now', {})
    results: List[ImportResult] = []
    for ticker in tickers:
        candidates = _resolve_csv_candidates(runtime, csv_dir, ticker)
        csv_path = next((p for p in candidates if os.path.exists(p)), '')
        if not csv_path:
            msg = f"[SKIP] {ticker}: CSV not found (tried: {', '.join(candidates)})"
            print(msg)
            results.append(ImportResult(ticker=ticker, status='skipped_missing', csv_path='', message=msg))
            continue
        try:
            rows = _read_ohlcv_csv(csv_path, keep_last_n=keep_history_rows)
            last_date = rows[-1]['Date'] if rows else ''
            last_close = rows[-1]['Close'] if rows else None
            runtime_history[ticker] = {'columns': ['Date', 'Open', 'High', 'Low', 'Close', 'Volume'], 'rows': rows, 'window_trading_days': keep_history_rows, 'source': os.path.basename(csv_path)}
            if persist_market_snapshot and prices_now_from == 'close' and last_close is not None:
                prices_now[ticker] = float(last_close)
            msg = f'[OK] {ticker}: imported {len(rows)} rows (kept last {keep_history_rows}), last={last_date} close={last_close}'
            print(msg)
            results.append(ImportResult(ticker=ticker, status='imported', csv_path=csv_path, rows_kept=len(rows), last_date=last_date, last_close=last_close, message=msg))
        except Exception as e:
            msg = f'[ERR] {ticker}: failed to read {csv_path}: {e}'
            print(msg)
            results.append(ImportResult(ticker=ticker, status='error', csv_path=csv_path, message=msg))
            continue
    imported_dates = [r.last_date for r in results if r.status == 'imported' and r.last_date]
    if persist_market_snapshot and imported_dates:
        market['asof_t_et'] = max(imported_dates)
    return results

def _net_cash_change_from_trades(trades: List[Dict[str, Any]], cutoff_et_dt: Optional[datetime]=None) -> Tuple[float, List[str]]:
    net = 0.0
    warns: List[str] = []
    for t in trades or []:
        if cutoff_et_dt is not None:
            et_dt = _trade_time_tw_to_et_dt(str(t.get('time_tw') or ''))
            if et_dt is None:
                warns.append(f"trade_id={t.get('trade_id')}: cannot parse time_tw='{t.get('time_tw')}', included by default")
            elif et_dt > cutoff_et_dt:
                continue
        amt = t.get('cash_amount')
        if amt is None:
            continue
        try:
            amt_f = float(amt)
        except Exception:
            warns.append(f"trade_id={t.get('trade_id')}: cash_amount not numeric: {amt}")
            continue
        side = str(t.get('side') or '').strip().upper()
        if side.startswith('B'):
            net -= amt_f
        elif side.startswith('S'):
            net += amt_f
        else:
            warns.append(f"trade_id={t.get('trade_id')}: unknown side='{t.get('side')}', ignored in cash ledger")
    return (net, warns)

def _sync_cash_external_flow_summary(states: Dict[str, Any], usd_amount_ndigits: int) -> None:
    portfolio = states.setdefault('portfolio', {})
    cash = portfolio.setdefault('cash', {'usd': 0.0, 'bucket': 'tactical_pool'})
    flows = cash.get('external_flows') or []
    net_external = round_with_precision(cash.get('net_external_cash_flow_usd') or 0.0, usd_amount_ndigits)
    last_flow = None
    if isinstance(flows, list):
        for item in reversed(flows):
            if isinstance(item, dict):
                last_flow = item
                break
    cash['external_cash_flow'] = {'net_usd': net_external, 'flow_count': len(flows) if isinstance(flows, list) else 0, 'last_flow_asof_et': str(last_flow.get('asof_et') or '').strip() if isinstance(last_flow, dict) else '', 'last_flow_kind': str(last_flow.get('kind') or '').strip() if isinstance(last_flow, dict) else '', 'last_flow_amount_usd': round_with_precision(last_flow.get('amount_usd') or 0.0, usd_amount_ndigits) if isinstance(last_flow, dict) and last_flow.get('amount_usd') is not None else None}

def _ensure_cash_buckets(states: Dict[str, Any], usd_amount_ndigits: int) -> None:
    portfolio = states.setdefault('portfolio', {})
    cash = portfolio.setdefault('cash', {'usd': 0.0, 'bucket': 'tactical_pool'})
    cash.setdefault('bucket', 'tactical_pool')

    def _f(v, default=0.0):
        try:
            return float(v)
        except Exception:
            return float(default)
    total = max(0.0, _f(cash.get('usd'), 0.0))
    deployable = cash.get('deployable_usd')
    reserve = cash.get('reserve_usd')
    if deployable is None and reserve is None:
        deployable_f = total
        reserve_f = 0.0
    elif deployable is None:
        reserve_f = max(0.0, _f(reserve, 0.0))
        reserve_f = min(reserve_f, total)
        deployable_f = max(0.0, total - reserve_f)
    elif reserve is None:
        deployable_f = max(0.0, _f(deployable, 0.0))
        deployable_f = min(deployable_f, total)
        reserve_f = max(0.0, total - deployable_f)
    else:
        deployable_f = max(0.0, _f(deployable, 0.0))
        reserve_f = max(0.0, _f(reserve, 0.0))
        total = deployable_f + reserve_f
    cash['deployable_usd'] = round_with_precision(deployable_f, usd_amount_ndigits)
    cash['reserve_usd'] = round_with_precision(reserve_f, usd_amount_ndigits)
    cash['usd'] = round_with_precision(deployable_f + reserve_f, usd_amount_ndigits)

def _set_total_cash_preserve_reserve(states: Dict[str, Any], total_cash_usd: float, usd_amount_ndigits: int) -> None:
    _ensure_cash_buckets(states, usd_amount_ndigits=usd_amount_ndigits)
    cash = states.setdefault('portfolio', {}).setdefault('cash', {'usd': 0.0, 'bucket': 'tactical_pool'})
    total = round_with_precision(max(0.0, float(total_cash_usd or 0.0)), usd_amount_ndigits)
    reserve = round_with_precision(max(0.0, float(cash.get('reserve_usd') or 0.0)), usd_amount_ndigits)
    reserve = min(reserve, total)
    deployable = round_with_precision(total - reserve, usd_amount_ndigits)
    cash['deployable_usd'] = deployable
    cash['reserve_usd'] = reserve
    cash['usd'] = round_with_precision(deployable + reserve, usd_amount_ndigits)

def _apply_cash_transfer_to_reserve(states: Dict[str, Any], amount_usd: float, usd_amount_ndigits: int, asof_et: Optional[str]=None) -> None:
    _ensure_cash_buckets(states, usd_amount_ndigits=usd_amount_ndigits)
    cash = states.setdefault('portfolio', {}).setdefault('cash', {'usd': 0.0, 'bucket': 'tactical_pool'})
    amt = round_with_precision(amount_usd, usd_amount_ndigits)
    if abs(amt) < 1e-12:
        return
    deployable = round_with_precision(cash.get('deployable_usd') or 0.0, usd_amount_ndigits)
    reserve = round_with_precision(cash.get('reserve_usd') or 0.0, usd_amount_ndigits)
    if amt > 0 and amt > deployable + 1e-09:
        raise ValueError(f"cash transfer to reserve is out of range: requested={format_fixed(amt, usd_amount_ndigits)}, deployable_usd={format_fixed(deployable, usd_amount_ndigits)}")
    if amt < 0 and -amt > reserve + 1e-09:
        raise ValueError(f"cash transfer back to deployable is out of range: requested={format_fixed(amt, usd_amount_ndigits)}, reserve_usd={format_fixed(reserve, usd_amount_ndigits)}")
    cash['deployable_usd'] = round_with_precision(deployable - amt, usd_amount_ndigits)
    cash['reserve_usd'] = round_with_precision(reserve + amt, usd_amount_ndigits)
    cash['usd'] = round_with_precision(float(cash['deployable_usd']) + float(cash['reserve_usd']), usd_amount_ndigits)
    transfers = cash.setdefault('internal_transfers', [])
    transfers.append({'amount_usd': amt, 'kind': 'to_reserve' if amt > 0 else 'to_deployable', 'asof_et': str(asof_et or '').strip(), 'ts_utc': datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')})
    print(f"[CASH] applied internal cash transfer: amount_usd={format_fixed(amt, usd_amount_ndigits)}, deployable_usd={format_fixed(cash['deployable_usd'], usd_amount_ndigits)}, reserve_usd={format_fixed(cash['reserve_usd'], usd_amount_ndigits)}")

def _set_initial_investment_usd(states: Dict[str, Any], amount_usd: float, usd_amount_ndigits: int) -> None:
    portfolio = states.setdefault('portfolio', {})
    perf = portfolio.setdefault('performance', {})
    baseline = perf.setdefault('baseline', {})
    amount_rounded = round_with_precision(amount_usd, usd_amount_ndigits)
    perf['initial_investment_usd'] = amount_rounded
    baseline['initial_investment_usd'] = amount_rounded

def _clear_holdings_reconciliation_snapshot(states: Dict[str, Any]) -> None:
    broker = (states.get('portfolio') or {}).get('broker') or {}
    if not isinstance(broker, dict):
        return
    for key in ('investment_total_usd', 'investments_total_usd', 'investment_total_excludes_cash', 'investment_total_kind', 'holdings_mv_usd', 'holdings_cost_usd', 'diff_usd', 'tolerance_usd', 'status', 'source'):
        broker.pop(key, None)
    broker.pop('reconciliation', None)

def _apply_cash_adjustment(states: Dict[str, Any], trades: List[Dict[str, Any]], amount_usd: float, usd_amount_ndigits: int, note: str='', asof_et: Optional[str]=None) -> None:
    portfolio = states.setdefault('portfolio', {})
    _ensure_cash_buckets(states, usd_amount_ndigits=usd_amount_ndigits)
    cash = portfolio.setdefault('cash', {'usd': 0.0, 'bucket': 'tactical_pool'})
    amt = round_with_precision(amount_usd, usd_amount_ndigits)
    if abs(amt) < 1e-12:
        return
    baseline = cash.get('baseline_usd')
    if baseline is not None:
        try:
            baseline = float(baseline)
        except Exception:
            baseline = None
    if baseline is None:
        net_cash_change, _ = _net_cash_change_from_trades(trades, cutoff_et_dt=None)
        baseline = float(cash.get('usd') or 0.0) - net_cash_change
        cash['baseline_source'] = 'inferred_from_existing_cash'
    cash['baseline_usd'] = round_with_precision(float(baseline) + amt, usd_amount_ndigits)
    cash['baseline_source'] = 'manual_cash_adjustment'
    cash['net_external_cash_flow_usd'] = round_with_precision(float(cash.get('net_external_cash_flow_usd') or 0.0) + amt, usd_amount_ndigits)
    flows = cash.setdefault('external_flows', [])
    flows.append({'amount_usd': amt, 'kind': 'deposit' if amt >= 0 else 'withdrawal', 'asof_et': str(asof_et or '').strip(), 'note': str(note or '').strip(), 'ts_utc': datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')})
    _sync_cash_external_flow_summary(states, usd_amount_ndigits=usd_amount_ndigits)
    print(f"[CASH] applied external cash {('deposit' if amt >= 0 else 'withdrawal')}: amount_usd={format_fixed(amt, usd_amount_ndigits)}, baseline_usd={format_fixed(cash['baseline_usd'], usd_amount_ndigits)}")

def _update_portfolio_performance(states: Dict[str, Any], usd_amount_ndigits: int) -> None:
    portfolio = states.setdefault('portfolio', {})
    totals = portfolio.setdefault('totals', {})
    perf = portfolio.setdefault('performance', {})
    cash = portfolio.setdefault('cash', {'usd': 0.0})
    baseline = perf.setdefault('baseline', {})
    returns = perf.setdefault('returns', {})
    current_total_assets = float((totals.get('portfolio') or {}).get('nav_usd') or 0.0)
    initial = baseline.get('initial_investment_usd', perf.get('initial_investment_usd'))
    if initial is not None:
        try:
            initial = float(initial)
        except Exception:
            initial = None
    net_external = float(cash.get('net_external_cash_flow_usd') or 0.0)
    perf['current_total_assets_usd'] = round_with_precision(current_total_assets, usd_amount_ndigits)
    perf['net_external_cash_flow_usd'] = round_with_precision(net_external, usd_amount_ndigits)
    returns['current_total_assets_usd'] = round_with_precision(current_total_assets, usd_amount_ndigits)
    _sync_cash_external_flow_summary(states, usd_amount_ndigits=usd_amount_ndigits)
    baseline['net_external_cash_flow_usd'] = round_with_precision(net_external, usd_amount_ndigits)
    baseline['method'] = 'initial_investment_plus_net_external_cash_flow'
    if initial is None:
        baseline.pop('initial_investment_usd', None)
        baseline.pop('effective_capital_base_usd', None)
        returns.pop('profit_usd', None)
        returns.pop('profit_rate', None)
        return
    effective_base = round_with_precision(initial + net_external, usd_amount_ndigits)
    profit_usd = round_with_precision(current_total_assets - effective_base, usd_amount_ndigits)
    profit_rate = profit_usd / effective_base if abs(effective_base) > 1e-12 else None
    perf['initial_investment_usd'] = round_with_precision(initial, usd_amount_ndigits)
    perf['effective_capital_base_usd'] = effective_base
    perf['profit_usd'] = profit_usd
    perf['profit_rate'] = profit_rate
    baseline['initial_investment_usd'] = round_with_precision(initial, usd_amount_ndigits)
    baseline['effective_capital_base_usd'] = effective_base
    returns['profit_usd'] = profit_usd
    returns['profit_rate'] = profit_rate

def _replace_trades_for_incoming_scope(trades: List[Dict[str, Any]], incoming: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    if not isinstance(trades, list) or not trades:
        return (trades if isinstance(trades, list) else [], 0)
    scope = set()
    for t in incoming:
        d = _normalize_trade_date_et(str(t.get('trade_date_et') or ''))
        tk = str(t.get('ticker') or '').upper()
        if d and tk:
            scope.add((d, tk))
    if not scope:
        return (trades, 0)
    keep = []
    removed = []
    for t in trades:
        if not isinstance(t, dict):
            keep.append(t)
            continue
        d = _normalize_trade_date_et(str(t.get('trade_date_et') or ''))
        tk = str(t.get('ticker') or '').upper()
        if (d, tk) in scope:
            removed.append(t)
        else:
            keep.append(t)
    if removed:
        print(f'[REPLACE] removed {len(removed)} existing trade(s) in scope {len(scope)} (date,ticker) pairs.')
    return (keep if removed else trades, len(removed))

def _sort_key_trade_for_portfolio(t: Dict[str, Any]) -> tuple:
    return (_normalize_trade_date_et(str(t.get('trade_date_et') or '')), _normalize_time_tw(str(t.get('time_tw') or '')), int(t.get('trade_id') or 0))

def _position_bucket_default(states: Dict[str, Any], runtime: Dict[str, Any], ticker: str) -> str:
    ticker = str(ticker or '').upper().strip()
    cfg = _runtime_config(runtime)
    buckets_cfg = cfg.get('buckets', {}) or {}
    core_tickers = {str(x).upper().strip() for x in ((buckets_cfg.get('core') or {}).get('tickers') or []) if str(x).strip()}
    tactical_tickers = {str(x).upper().strip() for x in ((buckets_cfg.get('tactical') or {}).get('tickers') or []) if str(x).strip()}
    tactical_cash_pool_ticker = str((buckets_cfg.get('tactical') or {}).get('cash_pool_ticker') or '').upper().strip()
    if ticker in core_tickers:
        return 'core'
    if tactical_cash_pool_ticker and ticker == tactical_cash_pool_ticker:
        return 'tactical_cash_pool'
    if ticker in tactical_tickers:
        return 'tactical'
    portfolio = states.setdefault('portfolio', {})
    positions = portfolio.setdefault('positions', [])
    if isinstance(positions, list):
        for p in positions:
            if isinstance(p, dict) and str(p.get('ticker') or '').upper().strip() == ticker:
                bucket = str(p.get('bucket') or '').strip()
                if bucket:
                    return bucket
    return 'tactical'

def _get_or_create_position(states: Dict[str, Any], runtime: Dict[str, Any], ticker: str) -> Dict[str, Any]:
    ticker = str(ticker or '').upper().strip()
    portfolio = states.setdefault('portfolio', {})
    positions = portfolio.setdefault('positions', [])
    if not isinstance(positions, list):
        positions = []
        portfolio['positions'] = positions
    for p in positions:
        if isinstance(p, dict) and str(p.get('ticker') or '').upper().strip() == ticker:
            if not str(p.get('bucket') or '').strip():
                p['bucket'] = _position_bucket_default(states, runtime, ticker)
            return p
    p = {'ticker': ticker, 'bucket': _position_bucket_default(states, runtime, ticker), 'shares': 0, 'cost_usd': 0.0}
    positions.append(p)
    return p

def _prune_zero_share_positions(states: Dict[str, Any]) -> None:
    portfolio = states.setdefault('portfolio', {})
    positions = portfolio.get('positions') or []
    if not isinstance(positions, list):
        portfolio['positions'] = []
        return
    kept: List[Dict[str, Any]] = []
    removed = 0
    for pos in positions:
        if not isinstance(pos, dict):
            continue
        try:
            shares = float(pos.get('shares') or 0.0)
        except Exception:
            shares = 0.0
        if abs(shares) <= 1e-12:
            removed += 1
            continue
        kept.append(pos)
    portfolio['positions'] = kept
    if removed > 0:
        print(f'[PORTFOLIO] pruned {removed} zero-share position(s).')

def _trade_buy_total_cost_usd(t: Dict[str, Any]) -> float:
    try:
        cash_amount = t.get('cash_amount')
        if cash_amount is not None:
            return float(cash_amount)
    except Exception:
        pass
    return float(t.get('gross') or 0.0) + float(t.get('fee') or 0.0)

def _fifo_lots_from_position(pos: Dict[str, Any]) -> List[Dict[str, float]]:
    try:
        shares = int(float(pos.get('shares') or 0))
    except Exception:
        shares = 0
    try:
        cost = float(pos.get('cost_usd') or 0.0)
    except Exception:
        cost = 0.0
    if shares <= 0:
        return []
    return [{'shares': float(shares), 'unit_cost_usd': (cost / float(shares)) if shares > 0 else 0.0}]

def _fifo_lots_total_shares(lots: List[Dict[str, float]]) -> float:
    total = 0.0
    for lot in lots:
        try:
            total += float(lot.get('shares') or 0.0)
        except Exception:
            continue
    return total

def _fifo_lots_total_cost(lots: List[Dict[str, float]]) -> float:
    total = 0.0
    for lot in lots:
        try:
            total += float(lot.get('shares') or 0.0) * float(lot.get('unit_cost_usd') or 0.0)
        except Exception:
            continue
    return total

def _fifo_lots_apply_buy(lots: List[Dict[str, float]], shares: int, total_cost_usd: float) -> None:
    if shares <= 0:
        return
    lots.append({'shares': float(shares), 'unit_cost_usd': float(total_cost_usd) / float(shares)})

def _fifo_lots_apply_sell(lots: List[Dict[str, float]], shares: int) -> bool:
    remain = float(max(0, int(shares)))
    while remain > 1e-12 and lots:
        lot = lots[0]
        try:
            lot_shares = float(lot.get('shares') or 0.0)
        except Exception:
            lot_shares = 0.0
        if lot_shares <= 1e-12:
            lots.pop(0)
            continue
        use = min(remain, lot_shares)
        remain -= use
        lot_shares -= use
        if lot_shares <= 1e-12:
            lots.pop(0)
        else:
            lot['shares'] = lot_shares
    return remain <= 1e-12

def _set_position_from_fifo_lots(pos: Dict[str, Any], lots: List[Dict[str, float]], usd_amount_ndigits: int) -> None:
    shares_now = int(round(_fifo_lots_total_shares(lots)))
    cost_now = round_with_precision(_fifo_lots_total_cost(lots), usd_amount_ndigits) if shares_now > 0 else 0.0
    pos['shares'] = shares_now
    pos['cost_usd'] = cost_now

def _apply_incremental_trades_to_portfolio_fifo(states: Dict[str, Any], runtime: Dict[str, Any], trades_delta: List[Dict[str, Any]]) -> None:
    if not trades_delta:
        return
    usd_amount_ndigits = int(_runtime_numeric_precision(runtime)["usd_amount"])
    lots_by_ticker: Dict[str, List[Dict[str, float]]] = {}
    for t in sorted(trades_delta, key=_sort_key_trade_for_portfolio):
        ticker = str(t.get('ticker') or '').upper().strip()
        side = str(t.get('side') or '').upper().strip()
        try:
            shares = int(float(t.get('shares') or 0))
        except Exception:
            shares = 0
        if not ticker or shares <= 0:
            continue
        pos = _get_or_create_position(states, runtime, ticker)
        lots = lots_by_ticker.setdefault(ticker, _fifo_lots_from_position(pos))
        if side.startswith('B'):
            _fifo_lots_apply_buy(lots, shares, _trade_buy_total_cost_usd(t))
        elif side.startswith('S'):
            if _fifo_lots_total_shares(lots) <= 0:
                lots.clear()
                print(f'[PORTFOLIO][WARN] {ticker}: sell trade ignored for cost basis because current shares are zero.')
                continue
            if not _fifo_lots_apply_sell(lots, shares):
                lots.clear()
                print(f'[PORTFOLIO][WARN] {ticker}: sell trade ignored for cost basis because current shares are zero.')
                continue
        _set_position_from_fifo_lots(pos, lots, usd_amount_ndigits)
    _prune_zero_share_positions(states)

def _rebuild_portfolio_positions_from_day1_fifo(states: Dict[str, Any], runtime: Dict[str, Any], trades_all: List[Dict[str, Any]]) -> None:
    portfolio = states.setdefault('portfolio', {})
    positions = portfolio.setdefault('positions', [])
    if not isinstance(positions, list):
        positions = []
        portfolio['positions'] = positions
    pos_by_ticker: Dict[str, Dict[str, Any]] = {}
    tickers = set()
    for p in positions:
        if isinstance(p, dict):
            tk = str(p.get('ticker') or '').upper().strip()
            if tk:
                pos_by_ticker[tk] = p
                tickers.add(tk)
    usd_amount_ndigits = int(_runtime_numeric_precision(runtime)["usd_amount"])
    state_by_ticker: Dict[str, List[Dict[str, float]]] = {}
    if not isinstance(trades_all, list):
        trades_all = []
    for t in sorted([x for x in trades_all if isinstance(x, dict)], key=_sort_key_trade_for_portfolio):
        ticker = str(t.get('ticker') or '').upper().strip()
        side = str(t.get('side') or '').upper().strip()
        try:
            shares = int(float(t.get('shares') or 0))
        except Exception:
            shares = 0
        if not ticker or shares <= 0:
            continue
        tickers.add(ticker)
        lots = state_by_ticker.setdefault(ticker, [])
        if side.startswith('B'):
            _fifo_lots_apply_buy(lots, shares, _trade_buy_total_cost_usd(t))
        elif side.startswith('S'):
            if _fifo_lots_total_shares(lots) <= 0:
                lots.clear()
                print(f'[PORTFOLIO][WARN] {ticker}: replace/day1 rebuild found sell larger than holdings; clamping to zero.')
                continue
            if not _fifo_lots_apply_sell(lots, shares):
                lots.clear()
                print(f'[PORTFOLIO][WARN] {ticker}: replace/day1 rebuild found sell larger than holdings; clamping to zero.')
                continue
    for ticker in sorted(tickers):
        pos = pos_by_ticker.get(ticker)
        if pos is None:
            pos = {'ticker': ticker, 'bucket': _position_bucket_default(states, runtime, ticker)}
            positions.append(pos)
            pos_by_ticker[ticker] = pos
        lots = state_by_ticker.get(ticker, [])
        pos['ticker'] = ticker
        if not str(pos.get('bucket') or '').strip():
            pos['bucket'] = _position_bucket_default(states, runtime, ticker)
        _set_position_from_fifo_lots(pos, lots, usd_amount_ndigits)
    _prune_zero_share_positions(states)
    print('[PORTFOLIO] rebuilt portfolio.positions from day1 trades ledger.')

def _update_tactical_cash_from_trades_and_snapshot(states: Dict[str, Any], trades: List[Dict[str, Any]], tactical_cash_usd: Optional[float], broker_asof_et: Optional[str], usd_amount_ndigits: int, verify_tolerance_usd: float=1.0, cutoff_et_dt: Optional[datetime]=None, snapshot_kind: str='eod') -> None:
    portfolio = states.setdefault('portfolio', {})
    cash = portfolio.setdefault('cash', {'usd': 0.0, 'bucket': 'tactical_pool'})
    net_cash_change, warns = _net_cash_change_from_trades(trades, cutoff_et_dt=cutoff_et_dt)
    for w in warns:
        print(f'[WARN] {w}')
    cash_existing = float(cash.get('usd') or 0.0)
    baseline = cash.get('baseline_usd')
    if baseline is not None:
        try:
            baseline = float(baseline)
        except Exception:
            baseline = None
    if baseline is None:
        baseline = cash_existing - net_cash_change
        cash['baseline_usd'] = baseline
        cash['baseline_source'] = 'inferred_from_existing_cash'
        print(f"[INFO] tactical cash baseline inferred from existing cash.usd: baseline_usd={format_fixed(baseline, usd_amount_ndigits)}")
    cash_from_baseline = baseline + net_cash_change
    if tactical_cash_usd is not None:
        tactical_cash_usd = float(tactical_cash_usd)
        diff = cash_from_baseline - tactical_cash_usd
        status = 'OK' if abs(diff) <= verify_tolerance_usd else 'MISMATCH'
        print(f"[VERIFY] broker_tactical_cash_usd={format_fixed(tactical_cash_usd, usd_amount_ndigits)} | cash_from_baseline={format_fixed(cash_from_baseline, usd_amount_ndigits)} | diff={format_fixed(diff, usd_amount_ndigits)} | tol={format_fixed(verify_tolerance_usd, usd_amount_ndigits)} => {status}")
        baseline_new = tactical_cash_usd - net_cash_change
        _set_total_cash_preserve_reserve(states, tactical_cash_usd, usd_amount_ndigits=usd_amount_ndigits)
        cash['baseline_usd'] = baseline_new
        cash['baseline_source'] = 'inferred_from_broker_tactical_cash'
        cash['derived_from_trades'] = True
        cash['last_reconciled_with_broker_cash'] = {'asof_et': broker_asof_et, 'snapshot_kind': snapshot_kind, 'asof_et_datetime': cutoff_et_dt.isoformat() if cutoff_et_dt is not None else None, 'broker_tactical_cash_usd': tactical_cash_usd, 'cash_from_baseline_usd': cash_from_baseline, 'baseline_usd': baseline_new, 'net_cash_change_usd': net_cash_change, 'diff_usd': diff, 'tolerance_usd': verify_tolerance_usd, 'status': status}
    else:
        _set_total_cash_preserve_reserve(states, cash_from_baseline, usd_amount_ndigits=usd_amount_ndigits)
        cash['derived_from_trades'] = True
        cut = f', cutoff_et={cutoff_et_dt.isoformat()}' if cutoff_et_dt is not None else ''
        print(f"[INFO] tactical cash.usd derived from trades: cash_usd={format_fixed(cash_from_baseline, usd_amount_ndigits)} (baseline_usd={format_fixed(baseline, usd_amount_ndigits)}, net_cash_change={format_fixed(net_cash_change, usd_amount_ndigits)}{cut})")

def _ensure_report_fields(states: Dict[str, Any]) -> List[str]:
    return ensure_report_root_fields(states)

def _close_from_history_asof(runtime: Dict[str, Any], ticker: str, asof_et: Optional[str]) -> Tuple[Optional[str], Optional[float]]:
    hist = (_market_history_rows_map(runtime).get(ticker) or {})
    rows = hist.get('rows') or []
    if not rows:
        return (None, None)
    if not asof_et:
        r = rows[-1]
        return (str(r.get('Date') or ''), _safe_float(r.get('Close')))
    asof_d = _parse_ymd_loose(asof_et)
    if not asof_d:
        r = rows[-1]
        return (str(r.get('Date') or ''), _safe_float(r.get('Close')))
    chosen = None
    for r in rows:
        rd = _parse_ymd_loose(str(r.get('Date') or ''))
        if rd and rd <= asof_d:
            chosen = r
        elif rd and rd > asof_d:
            break
    if chosen is None:
        chosen = rows[0]
    return (str(chosen.get('Date') or ''), _safe_float(chosen.get('Close')))

def _position_price_used(states: Dict[str, Any], runtime: Dict[str, Any], pos: Dict[str, Any], ticker: str) -> Tuple[Optional[float], str]:
    if pos.get('price_now') is not None:
        return (_safe_float(pos.get('price_now')), 'position.price_now')
    pn = ((states.get('market') or {}).get('prices_now') or {}).get(ticker)
    if pn is not None:
        try:
            return (float(pn), 'market.prices_now')
        except Exception:
            pass
    d, c = _close_from_history_asof(runtime, ticker, None)
    if c is not None:
        return (c, f'history.last_close({d})')
    return (None, 'missing')


def _mode_required_operations_requested(args: argparse.Namespace) -> bool:
    return bool(str(args.tickers or '').strip() or bool(getattr(args, 'render_report', False)) or getattr(args, 'broker_investment_total_usd', None) is not None or (getattr(args, 'tactical_cash_usd', None) is not None))

def _standalone_update_allowed_without_mode(args: argparse.Namespace) -> bool:
    return bool(getattr(args, 'imported_trades_json', None) or getattr(args, 'cash_adjust_usd', None) is not None or getattr(args, 'cash_transfer_to_reserve_usd', None) is not None or (getattr(args, 'initial_investment_usd', None) is not None))

def _late_hydrate_new_position_tickers(states: Dict[str, Any], runtime: Dict[str, Any], csv_dir: str, prices_now_from: str, keep_history_rows: int, already_processed: List[str]) -> List[ImportResult]:
    known = {str(t or '').upper() for t in already_processed if str(t or '').strip()}
    late_tickers = [t for t in _discover_tickers_from_config(states, runtime) if t not in known]
    if not late_tickers:
        return []
    print(f"[INFO] late CSV hydration for new tickers from trades/cash updates: {', '.join(late_tickers)}")
    return _import_csvs_into_states(states, runtime, csv_dir=csv_dir, tickers=late_tickers, prices_now_from=prices_now_from, keep_history_rows=keep_history_rows)

def _run_main(args: argparse.Namespace) -> int:
    states_path = args.states
    states = _load_json(states_path)
    config_path = str(getattr(args, 'config', '') or '').strip() or str(Path(states_path).resolve().parent / 'config.json')
    runtime: Dict[str, Any] = {'config': _load_runtime_config(config_path), 'history': {}}
    numeric_precision = state_engine_numeric_precision(_runtime_config(runtime))
    trades_file = str(getattr(args, 'trades_file', '') or (_runtime_config(runtime).get('trades_file') or 'trades.json')).strip() or 'trades.json'
    external_trades = _load_trades_payload(trades_file)
    trades: List[Dict[str, Any]] = external_trades if isinstance(external_trades, list) else []
    _migrate_state_schema(states)
    _ensure_trading_calendar(runtime)
    _ensure_cash_buckets(states, usd_amount_ndigits=int(numeric_precision["usd_amount"]))
    mode_label = str(args.mode or '').strip()
    if not mode_label:
        if args.render_report:
            print('[ABORT] --render-report requires --mode.')
            raise SystemExit(2)
        if _mode_required_operations_requested(args):
            print('[ABORT] This command requires --mode.')
            print('[ABORT] Only imported trades / cash adjustment / reserve transfer / initial investment update may run without --mode.')
            raise SystemExit(2)
        if not _standalone_update_allowed_without_mode(args):
            print('[ABORT] No actionable update requested. Provide --mode for a normal state/report refresh, or use imported-trades/cash update arguments.')
            raise SystemExit(2)
    now_et_raw = str(args.now_et or '').strip()
    if now_et_raw:
        now_et = datetime.fromisoformat(now_et_raw)
        if now_et.tzinfo is None:
            now_et = now_et.replace(tzinfo=ZoneInfo(_ET_TZ))
        else:
            now_et = now_et.astimezone(ZoneInfo(_ET_TZ))
    else:
        now_et = datetime.now(ZoneInfo(_ET_TZ))
    resolved_ctx = None
    report_meta: Optional[Dict[str, Any]] = None
    if mode_label:
        if str(args.broker_asof_et or '').strip() or str(args.broker_asof_et_time or '').strip() or str(args.broker_asof_et_datetime or '').strip():
            print('[WARN] --broker-asof-et / --broker-asof-et-time / --broker-asof-et-datetime are ignored when --mode is used; update_states resolves t / t+1 automatically.')
        resolved_ctx = _resolve_report_context(states, runtime, mode_label, now_et)
        print(f'[INFO] mode={resolved_ctx.mode_label} | session={resolved_ctx.session_class} | now_et={resolved_ctx.now_et_iso} | t={resolved_ctx.t_et} | t+1={resolved_ctx.t_plus_1_et} | report_date={resolved_ctx.report_date}')
        print(f'[INFO] {resolved_ctx.rationale}')
        if not resolved_ctx.reasonable:
            print(f'[ABORT] {resolved_ctx.warning}')
            print('[ABORT] No state update and no report file were generated.')
            raise SystemExit(2)
        report_meta = _report_meta_from_context(resolved_ctx)
    else:
        print('[INFO] running without --mode; only imported-trades/cash/initial-investment updates will be applied.')
    tickers = [t.strip().upper() for t in args.tickers.split(',') if t.strip()] if args.tickers.strip() else _discover_tickers_from_config(states, runtime)
    keep_history_rows = args.keep_history_rows if args.keep_history_rows > 0 else _compute_keep_history_rows(states, runtime)
    out_path = args.out.strip()
    if not out_path:
        p = Path(states_path)
        out_path = str(p.with_name(f'{p.stem}.updated.json'))
    results = _import_csvs_into_states(states, runtime, csv_dir=args.csv_dir, tickers=tickers, prices_now_from=args.prices_now_from, keep_history_rows=keep_history_rows)
    imported = [r for r in results if r.status == 'imported']
    skipped = [r for r in results if r.status == 'skipped_missing']
    errors = [r for r in results if r.status == 'error']
    last_dates = sorted({str(r.last_date) for r in imported if r.last_date})
    csv_asof = last_dates[-1] if last_dates else '-'
    processed_tickers = list(tickers)
    trade_import_runs: List[Dict[str, Any]] = []
    _normalize_trades_inplace(trades, cash_amount_ndigits=int(numeric_precision["trade_cash_amount"]))
    if args.imported_trades_json:
        for import_path in args.imported_trades_json:
            try:
                incoming = _load_imported_trades_json(import_path)
                _normalize_trades_inplace(incoming, cash_amount_ndigits=int(numeric_precision["trade_cash_amount"]))
                import_label = _trade_import_label(import_path, incoming)
                mode = (args.trades_import_mode or 'append').strip().lower()
                if mode not in ('append', 'replace'):
                    mode = 'append'
                replaced_scope_count = 0
                if mode == 'replace':
                    trades, replaced_scope_count = _replace_trades_for_incoming_scope(trades, incoming)
                added, dup = _upsert_trades(
                    trades,
                    incoming,
                    cash_amount_ndigits=int(numeric_precision["trade_cash_amount"]),
                    trade_dedupe_amount_ndigits=int(numeric_precision["trade_dedupe_amount"]),
                )
                if added > 0 or replaced_scope_count > 0:
                    _rebuild_portfolio_positions_from_day1_fifo(states, runtime, trades)
                    portfolio_delta_desc = 'day1_rebuild'
                else:
                    portfolio_delta_desc = '0'
                    print(f'[PORTFOLIO] {mode}: no trade ledger changes; skipped portfolio rebuild.')
                trade_import_runs.append({'file': import_label, 'status': 'ok', 'parsed': len(incoming), 'added': added, 'dup': dup, 'mode': mode})
                print(f'[OK] trades import {import_label}: parsed={len(incoming)}, added={added}, dup={dup}, mode={mode}, portfolio_delta={portfolio_delta_desc}')
            except Exception as e:
                trade_import_runs.append({'file': Path(import_path).name, 'status': 'error', 'error': str(e)})
                print(f'[ERR] trades import failed for {import_path}: {e}')
    if trade_import_runs:
        trade_import_ok = [x for x in trade_import_runs if str(x.get('status') or '') == 'ok']
        trade_import_err = [x for x in trade_import_runs if str(x.get('status') or '') != 'ok']
        trade_import_parsed = sum(int(x.get('parsed') or 0) for x in trade_import_ok)
        trade_import_added = sum(int(x.get('added') or 0) for x in trade_import_ok)
        trade_import_dup = sum(int(x.get('dup') or 0) for x in trade_import_ok)
        trade_import_mode = str(trade_import_ok[-1].get('mode') or '-') if trade_import_ok else '-'
    late_results = _late_hydrate_new_position_tickers(states, runtime, csv_dir=args.csv_dir, prices_now_from=args.prices_now_from, keep_history_rows=keep_history_rows, already_processed=processed_tickers)
    if late_results:
        results.extend(late_results)
        processed_tickers.extend([r.ticker for r in late_results])
    _reprice_and_totals(states, runtime)
    broker_investment_total_usd = args.broker_investment_total_usd
    broker_investment_total_supplied = broker_investment_total_usd is not None
    tactical_cash_usd = args.tactical_cash_usd
    if resolved_ctx is not None:
        broker_asof_et = resolved_ctx.broker_asof_et
        broker_asof_et_dt = None
        snapshot_kind = resolved_ctx.snapshot_kind
        broker_asof_et_datetime = resolved_ctx.broker_asof_et_datetime or None
    else:
        broker_asof_et, broker_asof_et_dt, snapshot_kind = _parse_broker_asof(states, str(args.broker_asof_et or ''), str(args.broker_asof_et_time or ''), str(args.broker_asof_et_datetime or ''), mode='')
        broker_asof_et_datetime = str(args.broker_asof_et_datetime or '').strip() or None
    if args.initial_investment_usd is not None:
        _set_initial_investment_usd(states, float(args.initial_investment_usd), usd_amount_ndigits=int(numeric_precision["usd_amount"]))
    if args.cash_adjust_usd is not None:
        _apply_cash_adjustment(states, trades, amount_usd=float(args.cash_adjust_usd), usd_amount_ndigits=int(numeric_precision["usd_amount"]), note=str(args.cash_adjust_note or ''), asof_et=broker_asof_et if broker_asof_et else None)
    if broker_investment_total_supplied:
        _verify_holdings_with_broker_investment_total(states, broker_investment_total_usd=broker_investment_total_usd, broker_asof_et=broker_asof_et if broker_asof_et else None, broker_investment_total_kind=str(args.broker_investment_total_kind), verify_tolerance_usd=float(args.verify_tolerance_usd))
    else:
        _clear_holdings_reconciliation_snapshot(states)
        print('[INFO] holdings reconciliation skipped: no --broker-investment-total-usd supplied.')
    _update_tactical_cash_from_trades_and_snapshot(states, trades, tactical_cash_usd=tactical_cash_usd, broker_asof_et=broker_asof_et if broker_asof_et else None, usd_amount_ndigits=int(numeric_precision["usd_amount"]), verify_tolerance_usd=float(args.verify_tolerance_usd), cutoff_et_dt=broker_asof_et_dt if snapshot_kind == 'intraday' else None, snapshot_kind=snapshot_kind)
    cash_block = (states.get('portfolio', {}) or {}).get('cash', {}) or {}
    rec = cash_block.get('last_reconciled_with_broker_cash') or {}
    rec_status = str(rec.get('status') or 'N/A') if tactical_cash_usd is not None else 'SKIP'
    if args.cash_transfer_to_reserve_usd is not None:
        try:
            _apply_cash_transfer_to_reserve(states, amount_usd=float(args.cash_transfer_to_reserve_usd), usd_amount_ndigits=int(numeric_precision["usd_amount"]), asof_et=broker_asof_et if broker_asof_et else None)
        except Exception as e:
            print(f'[ABORT] invalid --cash-transfer-to-reserve-usd: {e}')
            print('[ABORT] No state update and no report file were generated.')
            raise SystemExit(2)
    _reprice_and_totals(states, runtime)
    tactical_plan = None
    if mode_label:
        tactical_plan = compute_tactical_plan(
            states,
            runtime,
            derive_signals_inputs=args.derive_signals_inputs,
            derive_threshold_inputs=args.derive_threshold_inputs,
            mode=mode_label,
            trades=trades,
        )
    else:
        print('[INFO] signal/threshold refresh skipped because --mode was not supplied.')
    _update_portfolio_performance(states, usd_amount_ndigits=int(numeric_precision["usd_amount"]))
    mismatches: List[Dict[str, Any]] = []
    broker_block = (states.get('portfolio', {}) or {}).get('broker', {}) or {}
    if broker_investment_total_supplied and str(broker_block.get('status') or '').upper() == 'MISMATCH':
        mismatches.append({'kind': 'broker_investment_total_vs_holdings_cost' if str(broker_block.get('investment_total_kind') or '').lower() == 'cost_basis' else 'broker_investment_total_vs_holdings_mv', 'diff_usd': broker_block.get('diff_usd'), 'tolerance_usd': broker_block.get('tolerance_usd'), 'broker_investment_total_usd': broker_block.get('investment_total_usd'), 'holdings_mv_usd': broker_block.get('holdings_mv_usd'), 'holdings_cost_usd': broker_block.get('holdings_cost_usd'), 'asof_et': broker_asof_et})
    if tactical_cash_usd is not None:
        cash_block = (states.get('portfolio', {}) or {}).get('cash', {}) or {}
        rec = cash_block.get('last_reconciled_with_broker_cash') or {}
        if str(rec.get('status') or '').upper() == 'MISMATCH':
            mismatches.append({'kind': 'broker_tactical_cash_vs_cash_from_baseline', 'diff_usd': rec.get('diff_usd'), 'tolerance_usd': rec.get('tolerance_usd'), 'broker_tactical_cash_usd': rec.get('broker_tactical_cash_usd'), 'cash_from_baseline_usd': rec.get('cash_from_baseline_usd'), 'asof_et': rec.get('asof_et')})
    if mismatches:
        print('[MISMATCH] Broker verification failed beyond tolerance, but continuing.')
        for m in mismatches:
            print('  -', m)
    report_root = build_report_root(states, config=_runtime_config(runtime), trades=trades, tactical_plan=tactical_plan)
    if mode_label:
        warns = _ensure_report_fields(report_root)
        if warns:
            print('[WARN] Some report fields look missing/incomplete:')
            for w in warns:
                print('  -', w)
    _round_selected_numeric_fields(states, keys={
        'fee_rate_pct',
        'ma_sum_prev',
        'threshold_from_ma',
        'threshold',
        'ma_t',
        'profit_rate',
        'holdings_cost_usd',
        'holdings_mv_usd',
        'market_value_usd',
        'nav_usd',
        'total_assets_usd',
        'unrealized_pnl_usd',
        'unrealized_pnl_pct',
    }, ndigits=int(numeric_precision["state_selected_fields"]))
    report_md = None
    report_out_path = None
    if args.render_report:
        report_md, report_out_path = _build_report_output(states, schema_path=str(args.report_schema), report_dir=str(args.report_dir), report_out=str(args.report_out), mode=mode_label, config=_runtime_config(runtime), trades=trades, tactical_plan=tactical_plan, report_meta=report_meta)
    _strip_persisted_report_transients(states)
    trades_to_save: List[Dict[str, Any]] = []
    for t in trades:
        if isinstance(t, dict):
            trades_to_save.append(_compact_trade_row(t))
    trades_written = _save_trades_payload(trades_to_save, trades_file)
    meta = states.get('meta')
    market = states.setdefault('market', {})
    if isinstance(meta, dict) and not meta:
        states.pop('meta', None)
    out_written = _save_json(states, out_path)
    if report_md is not None and report_out_path:
        rp = Path(report_out_path)
        rp.parent.mkdir(parents=True, exist_ok=True)
        rp.write_text(report_md, encoding='utf-8')
        print(f'[OK] wrote {rp}')
    imported_cnt = sum((1 for r in results if r.status == 'imported'))
    skipped_cnt = sum((1 for r in results if r.status == 'skipped_missing'))
    err_cnt = sum((1 for r in results if r.status == 'error'))
    print(f'[DONE] wrote {out_written} | trades={trades_written} ({len(trades_to_save)} rows) | imported={imported_cnt}, skipped={skipped_cnt}, errors={err_cnt} | keep_history_rows={keep_history_rows}')
    return 0
