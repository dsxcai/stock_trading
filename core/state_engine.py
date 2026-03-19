from __future__ import annotations
import argparse
import csv
import json
import os
import re
import shlex
import sys
import traceback
from zoneinfo import ZoneInfo
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import re

from core.reconciliation import _import_trades_from_os_history_xml as _shared_import_trades_from_os_history_xml

def _load_json(path: str) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding='utf-8'))

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

def _parse_indicator_window(ma_rule: str) -> Optional[int]:
    m = re.search('(\\d+)', str(ma_rule))
    return int(m.group(1)) if m else None

def _normalize_ma_rule(spec: Any) -> str:
    if isinstance(spec, str):
        return spec.strip()
    if isinstance(spec, dict):
        ma_type = str(spec.get('ma_type') or 'SMA').strip()
        win = spec.get('window')
        try:
            win_i = int(win)
            return f'{ma_type}{win_i}'
        except Exception:
            return ma_type
    return str(spec)

def _to_yyyy_mm_dd(s: str) -> str:
    s = str(s).strip()
    if not s:
        raise ValueError('Empty date')
    s2 = s.replace('/', '-')
    parts = s2.split('-')
    if len(parts) != 3:
        dt = datetime.fromisoformat(s2)
        return dt.date().isoformat()
    y, m, d = parts
    return f'{int(y):04d}-{int(m):02d}-{int(d):02d}'

def _safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    s = str(x).strip()
    if s == '':
        return None
    return float(s)

def _safe_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    s = str(x).strip()
    if s == '':
        return None
    return int(float(s))

def _fmt_usd(x: Optional[float]) -> str:
    if x is None:
        return ''
    try:
        return f'${float(x):,.2f}'
    except Exception:
        return str(x)

class _TeeStream:

    def __init__(self, *streams):
        self._streams = streams

    def write(self, data: str) -> int:
        for s in self._streams:
            s.write(data)
        return len(data)

    def flush(self) -> None:
        for s in self._streams:
            s.flush()

def _default_log_path(script_name: str='update_states') -> str:
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    pid = os.getpid()
    return str(Path('logs') / f'{script_name}_{ts}_{pid}.log')

def _enable_log_tee(log_path: str) -> Tuple[Path, Any, Any, Any]:
    p = Path(log_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fh = p.open('a', encoding='utf-8')
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    sys.stdout = _TeeStream(old_stdout, fh)
    sys.stderr = _TeeStream(old_stderr, fh)
    return (p, fh, old_stdout, old_stderr)

def _disable_log_tee(fh: Any, old_stdout: Any, old_stderr: Any) -> None:
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        fh.close()

def _log_run_header(script_name: str, args: argparse.Namespace, log_path: Path) -> None:
    argv = [script_name, *sys.argv[1:]]
    print(f'[LOG] file={log_path}')
    print(f"[RUN] started_at={datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')}")
    print(f'[RUN] cwd={os.getcwd()}')
    print(f'[RUN] argv={shlex.join(argv)}')
    print(f'[RUN] args={json.dumps(vars(args), ensure_ascii=False, sort_keys=True)}')
_ET_TZ = 'America/New_York'
_TW_TZ = 'Asia/Taipei'
_OPEN_TIME_ET = time(9, 30)
_DEFAULT_CLOSE_TIME_ET = time(16, 0)

def _default_trading_calendar_2026() -> Dict[str, Any]:
    return {'full_day_closed': [{'date_et': '2026-01-01', 'name': 'New Year’s Day', 'note': 'Market closed'}, {'date_et': '2026-01-19', 'name': 'Martin Luther King, Jr. Day', 'note': 'Market closed'}, {'date_et': '2026-02-16', 'name': 'Presidents Day (Washington’s Birthday)', 'note': 'Market closed'}, {'date_et': '2026-04-03', 'name': 'Good Friday', 'note': 'Market closed'}, {'date_et': '2026-05-25', 'name': 'Memorial Day', 'note': 'Market closed'}, {'date_et': '2026-06-19', 'name': 'Juneteenth', 'note': 'Market closed'}, {'date_et': '2026-07-03', 'name': 'Independence Day (Observed)', 'note': 'Market closed'}, {'date_et': '2026-09-07', 'name': 'Labor Day', 'note': 'Market closed'}, {'date_et': '2026-11-26', 'name': 'Thanksgiving Day', 'note': 'Market closed'}, {'date_et': '2026-12-25', 'name': 'Christmas Day', 'note': 'Market closed'}], 'early_close': [{'date_et': '2026-11-27', 'reason': 'Day After Thanksgiving', 'close_time_et': '13:00', 'note': 'Eligible options are usually available until 13:15 (subject to exchange notice)'}, {'date_et': '2026-12-24', 'reason': 'Christmas Eve', 'close_time_et': '13:00', 'note': 'Eligible options are usually available until 13:15 (subject to exchange notice)'}]}

def _ensure_trading_calendar(states: Dict[str, Any]) -> Dict[str, Any]:
    cfg = states.setdefault('config', {})
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

def _next_trading_day_et_from_states(states: Dict[str, Any], t_et: str) -> Optional[str]:
    t_et = str(t_et or '').strip()
    if not t_et:
        return None
    try:
        cal = (states.get('config') or {}).get('trading_calendar') or {}
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

def _prev_trading_day_et_from_states(states: Dict[str, Any], t_et: str) -> Optional[str]:
    t_et = str(t_et or '').strip()
    if not t_et:
        return None
    try:
        cal = (states.get('config') or {}).get('trading_calendar') or {}
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

def _is_full_day_closed_et(states: Dict[str, Any], d: date) -> bool:
    if _is_weekend_et(d):
        return True
    try:
        cal = (states.get('config') or {}).get('trading_calendar') or {}
        years = cal.get('years') or {}
        year_block = years.get(f'{d.year:04d}') or {}
        return d.isoformat() in _closed_set_for_year_block(year_block)
    except Exception:
        return False

def _is_trading_day_et(states: Dict[str, Any], d: date) -> bool:
    return not _is_full_day_closed_et(states, d)

def _close_time_et_from_states(states: Dict[str, Any], d: date) -> time:
    try:
        cal = (states.get('config') or {}).get('trading_calendar') or {}
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

def _session_class_for_now_et(states: Dict[str, Any], now_et: datetime) -> str:
    d = now_et.date()
    if not _is_trading_day_et(states, d):
        return 'closed'
    open_dt = datetime.combine(d, _OPEN_TIME_ET, tzinfo=ZoneInfo(_ET_TZ))
    close_dt = datetime.combine(d, _close_time_et_from_states(states, d), tzinfo=ZoneInfo(_ET_TZ))
    if now_et < open_dt:
        return 'premarket'
    if now_et < close_dt:
        return 'intraday'
    return 'afterclose'

@dataclass
class ReportContext:
    mode_label: str
    mode_key: str
    session_class: str
    now_et_iso: str
    t_et: str
    t_plus_1_et: str
    report_date: str
    broker_asof_et: str
    broker_asof_et_datetime: str
    snapshot_kind: str
    reasonable: bool
    rationale: str
    warning: str

def _resolve_report_context(states: Dict[str, Any], mode_label: str, now_et: datetime) -> ReportContext:
    mode_key = _normalize_mode_key(mode_label)
    session = _session_class_for_now_et(states, now_et)
    today = now_et.date()
    now_iso = now_et.replace(microsecond=0).isoformat()
    if mode_key == 'premarket':
        if session == 'premarket':
            t_et = _prev_trading_day_et_from_states(states, today.isoformat()) or today.isoformat()
            t1 = today.isoformat()
            return ReportContext(mode_label, mode_key, session, now_iso, t_et, t1, t1, t_et, '', 'eod', True, 'today is a trading day before the open; premarket uses the latest completed trading day as t and today as t+1.', '')
        if session == 'afterclose':
            t_et = today.isoformat()
            t1 = _next_trading_day_et_from_states(states, t_et) or t_et
            return ReportContext(mode_label, mode_key, session, now_iso, t_et, t1, t1, t_et, '', 'eod', True, 'market has already closed; premarket can reasonably prepare the next trading day.', '')
        if session == 'closed':
            t1 = _next_trading_day_et_from_states(states, today.isoformat()) or today.isoformat()
            t_et = _prev_trading_day_et_from_states(states, t1) or t1
            return ReportContext(mode_label, mode_key, session, now_iso, t_et, t1, t1, t_et, '', 'eod', True, 'today is not a trading day; premarket maps to the next trading day.', '')
        t_et = _prev_trading_day_et_from_states(states, today.isoformat()) or today.isoformat()
        return ReportContext(mode_label, mode_key, session, now_iso, t_et, today.isoformat(), today.isoformat(), t_et, '', 'eod', False, 'premarket is defined before the regular session opens.', 'current ET session is intraday, so premarket semantics are no longer valid.')
    if mode_key == 'intraday':
        next_after_today = _next_trading_day_et_from_states(states, today.isoformat()) or today.isoformat()
        if session == 'intraday':
            return ReportContext(mode_label, mode_key, session, now_iso, today.isoformat(), next_after_today, today.isoformat(), today.isoformat(), now_iso, 'intraday', True, 'market is open; intraday uses today as t and the next trading day as t+1.', '')
        if session == 'premarket':
            return ReportContext(mode_label, mode_key, session, now_iso, today.isoformat(), next_after_today, today.isoformat(), today.isoformat(), now_iso, 'intraday', False, 'intraday requires an active regular session.', 'current ET session is premarket; the regular session has not started yet.')
        if session == 'afterclose':
            return ReportContext(mode_label, mode_key, session, now_iso, today.isoformat(), next_after_today, today.isoformat(), today.isoformat(), now_iso, 'intraday', False, 'intraday requires an active regular session.', 'current ET session is afterclose; the regular session has already ended.')
        t_et = _next_trading_day_et_from_states(states, today.isoformat()) or today.isoformat()
        t1 = _next_trading_day_et_from_states(states, t_et) or t_et
        return ReportContext(mode_label, mode_key, session, now_iso, t_et, t1, t_et, t_et, now_iso, 'intraday', False, 'intraday requires an active trading day.', 'today is not a trading day, so intraday semantics are unavailable.')
    if mode_key == 'afterclose':
        if session == 'afterclose':
            t_et = today.isoformat()
            t1 = _next_trading_day_et_from_states(states, t_et) or t_et
            return ReportContext(mode_label, mode_key, session, now_iso, t_et, t1, t_et, t_et, '', 'eod', True, 'market has closed; afterclose uses today as t and the next trading day as t+1.', '')
        if session == 'premarket':
            t_et = _prev_trading_day_et_from_states(states, today.isoformat()) or today.isoformat()
            return ReportContext(mode_label, mode_key, session, now_iso, t_et, today.isoformat(), t_et, t_et, '', 'eod', True, 'before the open, the latest completed trading day is the previous trading day, which is valid for afterclose.', '')
        if session == 'closed':
            t_et = _prev_trading_day_et_from_states(states, today.isoformat()) or today.isoformat()
            t1 = _next_trading_day_et_from_states(states, today.isoformat()) or today.isoformat()
            return ReportContext(mode_label, mode_key, session, now_iso, t_et, t1, t_et, t_et, '', 'eod', True, 'today is not a trading day; afterclose maps to the latest completed trading day.', '')
        t1 = _next_trading_day_et_from_states(states, today.isoformat()) or today.isoformat()
        return ReportContext(mode_label, mode_key, session, now_iso, today.isoformat(), t1, today.isoformat(), today.isoformat(), '', 'eod', False, 'afterclose requires a completed session close for t.', "current ET session is intraday, so today's close is not finalized yet.")
    raise ValueError(f'unsupported mode: {mode_label}')

def _strip_selector_meta(meta: Dict[str, Any]) -> None:
    for k in ('mode', 'active_mode', 'last_run', 'signal_basis', 'execution_basis', 'version_anchor_et', 'version', 'report_context'):
        meta.pop(k, None)

def _mode_snapshot_store(states: Dict[str, Any]) -> Dict[str, Any]:
    store = states.get('by_mode')
    if not isinstance(store, dict):
        store = {}
        states['by_mode'] = store
    return store

def _get_mode_snapshot(states: Dict[str, Any], mode: Any, create: bool=False) -> Dict[str, Any]:
    mode_label = str(mode or '').strip()
    mode_key = _normalize_mode_key(mode_label)
    if not mode_key:
        return {}
    store = _mode_snapshot_store(states)
    snap = store.get(mode_key)
    if not isinstance(snap, dict):
        if not create:
            return {}
        snap = {'mode': mode_label or mode_key, 'mode_key': mode_key}
        store[mode_key] = snap
    else:
        snap.setdefault('mode', mode_label or snap.get('mode') or mode_key)
        snap.setdefault('mode_key', mode_key)
    return snap

def _snapshot_effective_meta(states: Dict[str, Any], mode: Any) -> Dict[str, Any]:
    eff = dict(states.get('meta') or {})
    snap = _get_mode_snapshot(states, mode, create=False)
    if snap:
        for k in ('signal_basis', 'execution_basis', 'version_anchor_et', 'version', 'report_context', 'broker_context'):
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
        for v in (snap.get('version_anchor_et'), (snap.get('signal_basis') or {}).get('t_et'), (snap.get('execution_basis') or {}).get('t_plus_1_et'), (snap.get('broker_context') or {}).get('asof_et')):
            if isinstance(v, str) and re.fullmatch('\\d{4}-\\d{2}-\\d{2}', v):
                out.append(v)
    return out

def _migrate_state_schema(states: Dict[str, Any]) -> None:
    meta = states.setdefault('meta', {})
    states.setdefault('by_mode', {})
    portfolio = states.setdefault('portfolio', {})
    broker = portfolio.setdefault('broker', {})
    broker.setdefault('snapshot', {})
    _strip_selector_meta(meta)

def _apply_report_context_meta(states: Dict[str, Any], ctx: ReportContext) -> None:
    meta = states.setdefault('meta', {})
    _strip_selector_meta(meta)
    meta.setdefault('timezone', {'trading_day': _ET_TZ, 'trade_time_display': _TW_TZ})
    meta['timezone']['trading_day'] = _ET_TZ
    meta['timezone']['trade_time_display'] = _TW_TZ
    version_anchor_et = _version_anchor_day_et(ctx.mode_label, ctx.t_et, ctx.t_plus_1_et)
    snap = _get_mode_snapshot(states, ctx.mode_label, create=True)
    snap['mode'] = ctx.mode_label
    snap['mode_key'] = ctx.mode_key
    snap['signal_basis'] = {'t_et': ctx.t_et, 'basis': 'NYSE Intraday' if ctx.mode_key == 'intraday' else 'NYSE Close'}
    snap['execution_basis'] = {'t_plus_1_et': ctx.t_plus_1_et, 'basis': 'NYSE Trading Day'}
    if version_anchor_et:
        snap['version_anchor_et'] = version_anchor_et
        snap['version'] = _bump_meta_version(snap.get('version'), version_anchor_et)
    snap['report_context'] = {'mode': ctx.mode_label, 'mode_key': ctx.mode_key, 'session_class': ctx.session_class, 'now_et': ctx.now_et_iso, 'report_date': ctx.report_date, 'reasonable': ctx.reasonable, 'rationale': ctx.rationale, 'warning': ctx.warning}
    snap['broker_context'] = {'asof_et': ctx.broker_asof_et, 'asof_et_datetime': ctx.broker_asof_et_datetime, 'snapshot_kind': ctx.snapshot_kind}

def _build_report_output(states: Dict[str, Any], schema_path: str, report_dir: str, report_out: str, mode: str) -> Tuple[str, str]:
    from generate_report import load_schema as _load_report_schema, render_report as _render_report_markdown
    schema = _load_report_schema(schema_path)
    md = _render_report_markdown(states, schema, mode)
    meta = _snapshot_effective_meta(states, mode)
    rc = meta.get('report_context', {}) or {}
    report_date = str(rc.get('report_date') or meta.get('version_anchor_et') or '').strip()
    if not report_date:
        report_date = datetime.now().strftime('%Y-%m-%d')
    mode_key = _normalize_mode_key(mode) or 'report'
    if str(report_out or '').strip():
        out_path = str(report_out).strip()
    else:
        out_path = str(Path(report_dir) / f'{report_date}_{mode_key}.md')
    return (md, out_path)

def _bump_meta_version(old: Any, exec_day: str) -> str:
    exec_day = str(exec_day or '').strip()
    if not exec_day:
        return str(old or '')
    s = str(old or '').strip()
    m = re.fullmatch('v(\\d{4}-\\d{2}-\\d{2})-(\\d{3})', s)
    if not m:
        return f'v{exec_day}-001'
    d0, n0 = (m.group(1), int(m.group(2)))
    return f'v{exec_day}-{n0 + 1:03d}' if d0 == exec_day else f'v{exec_day}-001'

def _normalize_mode_key(mode: Any) -> str:
    return re.sub('[\\s_\\-]+', '', str(mode or '').strip().lower())

def _version_anchor_day_et(mode: Any, t_et: str, t_plus_1_et: Optional[str]) -> Optional[str]:
    m = _normalize_mode_key(mode)
    if m == 'premarket':
        return t_plus_1_et or t_et
    if m in {'intraday', 'afterclose'}:
        return t_et or t_plus_1_et
    return t_plus_1_et or t_et

def _auto_sync_meta(states: Dict[str, Any], mode: str, broker_asof_et: Optional[str]=None) -> None:
    market = states.get('market', {}) or {}
    t_et = str(broker_asof_et or '').strip()
    if t_et:
        try:
            t_et = _to_yyyy_mm_dd(t_et)
        except Exception:
            pass
    if not t_et:
        t_et = str(market.get('asof_t_et') or '').strip()
    if not t_et:
        return
    t_plus_1_et = _next_trading_day_et_from_states(states, t_et)
    version_anchor_et = _version_anchor_day_et(mode, t_et, t_plus_1_et)
    meta = states.setdefault('meta', {})
    _strip_selector_meta(meta)
    meta.setdefault('timezone', {'trading_day': _ET_TZ, 'trade_time_display': _TW_TZ})
    snap = _get_mode_snapshot(states, mode, create=bool(_normalize_mode_key(mode)))
    if not snap:
        return
    sb = snap.setdefault('signal_basis', {})
    sb['t_et'] = t_et
    sb['basis'] = 'NYSE Intraday' if _normalize_mode_key(mode) == 'intraday' else 'NYSE Close'
    eb = snap.setdefault('execution_basis', {})
    if t_plus_1_et:
        eb['t_plus_1_et'] = t_plus_1_et
    eb['basis'] = 'NYSE Trading Day'
    if version_anchor_et:
        snap['version_anchor_et'] = version_anchor_et
        snap['version'] = _bump_meta_version(snap.get('version'), version_anchor_et)

def _normalize_trade_date_et(s: str) -> str:
    try:
        return _to_yyyy_mm_dd(s)
    except Exception:
        return str(s).strip()

def _normalize_time_tw(s: str) -> str:
    raw = str(s or '').strip()
    if not raw:
        return raw
    raw = raw.replace('-', '/')
    if ' ' not in raw and 'T' in raw:
        raw = raw.replace('T', ' ')
    parts = raw.split()
    if len(parts) < 2:
        return raw
    d, t = (parts[0], parts[1])
    try:
        y, m, dd = [int(x) for x in d.replace('/', '-').split('-')]
        d2 = f'{y:04d}/{m:02d}/{dd:02d}'
    except Exception:
        d2 = d
    tt = t
    if '.' in tt:
        tt = tt.split('.')[0]
    seg = tt.split(':')
    if len(seg) == 2:
        hh, mm = seg
        ss = '00'
    elif len(seg) >= 3:
        hh, mm, ss = (seg[0], seg[1], seg[2])
    else:
        return f'{d2} {tt}'
    try:
        hh_i, mm_i, ss_i = (int(hh), int(mm), int(ss))
        t2 = f'{hh_i:02d}:{mm_i:02d}:{ss_i:02d}'
    except Exception:
        t2 = tt
    return f'{d2} {t2}'

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

def _trade_time_tw_to_et_dt(time_tw: str) -> Optional[datetime]:
    s = str(time_tw or '').strip()
    if not s:
        return None
    s = s.replace('T', ' ').replace('/', '-')
    parts = s.split()
    if len(parts) < 2:
        return None
    dpart, tpart = (parts[0], parts[1])
    try:
        y, m, d = [int(x) for x in dpart.split('-')]
    except Exception:
        return None
    seg = tpart.split(':')
    try:
        hh = int(seg[0])
        mm = int(seg[1]) if len(seg) > 1 else 0
        ss = int(seg[2]) if len(seg) > 2 else 0
    except Exception:
        return None
    tw = datetime(y, m, d, hh, mm, ss, tzinfo=ZoneInfo(_TW_TZ))
    return tw.astimezone(ZoneInfo(_ET_TZ))

def _dedupe_by_date_keep_last(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not rows:
        return rows
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        out[r['Date']] = r
    return [out[d] for d in sorted(out.keys())]

def _read_ohlcv_csv(csv_path: str, keep_last_n: Optional[int]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(csv_path, 'r', encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        expected = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        for col in expected:
            if reader.fieldnames is None or col not in reader.fieldnames:
                raise ValueError(f'{csv_path}: missing column {col}. Found: {reader.fieldnames}')
        for r in reader:
            d = _to_yyyy_mm_dd(r['Date'])
            row = {'Date': d, 'Open': float(r['Open']), 'High': float(r['High']), 'Low': float(r['Low']), 'Close': float(r['Close']), 'Volume': int(float(r['Volume'])) if str(r['Volume']).strip() != '' else 0}
            rows.append(row)
    rows.sort(key=lambda x: x['Date'])
    rows = _dedupe_by_date_keep_last(rows)
    if keep_last_n is not None and keep_last_n > 0 and (len(rows) > keep_last_n):
        rows = rows[-keep_last_n:]
    return rows

def _market_history_rows_map(states: Dict[str, Any]) -> Dict[str, Any]:
    market = states.get('market', {}) or {}
    runtime_hist = market.get('_runtime_history')
    if isinstance(runtime_hist, dict):
        return runtime_hist
    legacy_hist = market.get('history_400d')
    if isinstance(legacy_hist, dict):
        return legacy_hist
    return {}

def _derive_signals_inputs_from_history(rows: List[Dict[str, Any]], window: int) -> Dict[str, Any]:
    closes = [float(r['Close']) for r in rows]
    out = {'close_t': None, 'ma_t': None, 'close_t_minus_5': None}
    if len(closes) >= 1:
        out['close_t'] = closes[-1]
    if len(closes) >= 6:
        out['close_t_minus_5'] = closes[-6]
    if window and len(closes) >= window:
        out['ma_t'] = sum(closes[-window:]) / window
    return out

def _derive_threshold_inputs_from_history(rows: List[Dict[str, Any]], window: int) -> Dict[str, Any]:
    closes = [float(r['Close']) for r in rows]
    out = {'window': window, 'sum_n_minus_1': None, 'close_t_minus_5_next': None}
    if window and len(closes) >= window - 1 and (window > 1):
        out['sum_n_minus_1'] = sum(closes[-(window - 1):])
    if len(closes) >= 5:
        out['close_t_minus_5_next'] = closes[-5]
    return out

def _calc_threshold_row(ticker: str, ma_rule: str, window: int, inp: Dict[str, Any]) -> Dict[str, Any]:
    sum_n_minus_1 = inp.get('sum_n_minus_1')
    close_t_minus_5_next = inp.get('close_t_minus_5_next')
    sma_equiv = None
    if window and window > 1 and (sum_n_minus_1 is not None):
        sma_equiv = float(sum_n_minus_1) / float(window - 1)
    p_min = None
    if sma_equiv is not None and close_t_minus_5_next is not None:
        p_min = max(sma_equiv, float(close_t_minus_5_next))
    elif sma_equiv is not None:
        p_min = sma_equiv
    elif close_t_minus_5_next is not None:
        p_min = float(close_t_minus_5_next)
    display = f'{p_min:.2f}+' if p_min is not None else None
    return {'ticker': ticker, 'ma_rule': ma_rule, 'sum_n_minus_1': sum_n_minus_1, 'close_t_minus_5': close_t_minus_5_next, 'close_t_minus_5_next': close_t_minus_5_next, 'sma_equiv_threshold_exclusive': sma_equiv, 'p_min_exclusive': p_min, 'display': display}

def _reprice_and_totals(states: Dict[str, Any]) -> None:
    market = states.setdefault('market', {})
    prices_now = market.setdefault('prices_now', {})
    history = _market_history_rows_map(states)
    portfolio = states.setdefault('portfolio', {})
    positions = portfolio.setdefault('positions', [])
    _ensure_cash_buckets(states)
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
    totals['tactical'] = {**tactical_tot, 'cash_usd': cash_usd, 'deployable_cash_usd': deployable_cash_usd, 'reserve_cash_usd': reserve_cash_usd, 'total_assets_usd': tactical_tot['holdings_mv_usd'] + cash_usd}
    totals['portfolio'] = {**portfolio_tot, 'cash_usd': cash_usd, 'deployable_cash_usd': deployable_cash_usd, 'reserve_cash_usd': reserve_cash_usd, 'nav_usd': portfolio_tot['holdings_mv_usd'] + cash_usd}

def _estimate_tactical_buy_budget_usd(states: Dict[str, Any]) -> float:
    _ensure_cash_buckets(states)
    portfolio = states.setdefault('portfolio', {})
    _ensure_cash_buckets(states)
    cash = portfolio.setdefault('cash', {'usd': 0.0, 'bucket': 'tactical_pool'})
    budget = float(cash.get('deployable_usd') or cash.get('usd') or 0.0)
    return round(max(0.0, float(budget)), 2)

def _lookup_action_price_usd(states: Dict[str, Any], ticker: str) -> Optional[float]:
    portfolio = states.get('portfolio', {}) or {}
    positions = portfolio.get('positions', []) or []
    market = states.get('market', {}) or {}
    history = _market_history_rows_map(states)
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

def _allocate_buy_shares_across_triggered_signals(candidates: List[Dict[str, Any]], investable_cash_usd: float) -> Dict[str, int]:
    eps = 1e-09
    budget = max(0.0, float(investable_cash_usd or 0.0))
    cleaned: List[Dict[str, Any]] = []
    for item in candidates or []:
        ticker = str(item.get('ticker') or '').upper().strip()
        try:
            price = float(item.get('price_usd'))
        except Exception:
            continue
        if not ticker or price <= 0:
            continue
        cleaned.append({'ticker': ticker, 'price_usd': price})
    if not cleaned:
        return {}
    cleaned.sort(key=lambda x: (x['price_usd'], x['ticker']))
    if budget + eps < cleaned[0]['price_usd']:
        return {c['ticker']: 0 for c in cleaned}
    full_one_share_cost = sum((c['price_usd'] for c in cleaned))
    if budget + eps >= full_one_share_cost:
        chosen = list(cleaned)
    else:
        chosen = []
        used = 0.0
        for c in cleaned:
            if used + c['price_usd'] <= budget + eps:
                chosen.append(c)
                used += c['price_usd']
            else:
                break
    shares = {c['ticker']: 0 for c in cleaned}
    if not chosen:
        return shares
    for c in chosen:
        shares[c['ticker']] = 1
    used = sum((c['price_usd'] for c in chosen))
    remaining = budget - used
    m = len(chosen)
    per_ticker_budget = remaining / m if m > 0 else 0.0
    for c in chosen:
        add = int(per_ticker_budget // c['price_usd'])
        if add > 0:
            shares[c['ticker']] += add
    used = sum((shares[c['ticker']] * c['price_usd'] for c in chosen))
    remaining = budget - used
    chosen_desc = sorted(chosen, key=lambda x: (-x['price_usd'], x['ticker']))
    while True:
        target = None
        for c in chosen_desc:
            if c['price_usd'] <= remaining + eps:
                target = c
                break
        if target is None:
            break
        shares[target['ticker']] += 1
        remaining -= target['price_usd']
    return shares

def _current_signal_day_et(states: Dict[str, Any], mode: Optional[str]=None) -> Optional[str]:
    candidates: List[Any] = []
    if mode:
        snap = _get_mode_snapshot(states, mode, create=False)
        if snap:
            candidates.extend([(snap.get('signal_basis') or {}).get('t_et'), (snap.get('broker_context') or {}).get('asof_et'), snap.get('version_anchor_et')])
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

def _latest_open_entry_trade_date_et(states: Dict[str, Any], ticker: str) -> Optional[str]:
    trades = states.get('trades') or []
    rows = []
    tkr = str(ticker or '').upper().strip()
    for tr in trades:
        if str(tr.get('ticker') or '').upper().strip() != tkr:
            continue
        side = str(tr.get('side') or '').upper().strip()
        if side not in {'BUY', 'SELL'}:
            continue
        try:
            d = _to_yyyy_mm_dd(str(tr.get('trade_date_et') or '').strip())
            shares = int(tr.get('shares') or 0)
        except Exception:
            continue
        rows.append((d, str(tr.get('time_tw') or ''), int(tr.get('trade_id') or 0), side, shares))
    if not rows:
        return None
    rows.sort(key=lambda x: (x[0], x[1], x[2]))
    pos = 0
    entry_date = None
    for d, _tw, _id, side, shares in rows:
        delta = shares if side == 'BUY' else -shares
        prev = pos
        pos += delta
        if prev <= 0 and pos > 0:
            entry_date = d
        elif pos <= 0:
            entry_date = None
    return entry_date if pos > 0 else None

def _trading_day_age_from_states(states: Dict[str, Any], start_et: str, end_et: str) -> Optional[int]:
    try:
        start = _to_yyyy_mm_dd(start_et)
        end = _to_yyyy_mm_dd(end_et)
    except Exception:
        return None
    if start == end:
        return 0
    cur = start
    age = 0
    for _ in range(370):
        nxt = _next_trading_day_et_from_states(states, cur)
        if not nxt:
            return None
        age += 1
        cur = nxt
        if cur == end:
            return age
    return None

def _recent_buy_protection_info(states: Dict[str, Any], ticker: str, shares_pre: int, protection_days: int, mode: Optional[str]=None) -> Dict[str, Any]:
    info = {'latest_buy_trade_date_et': None, 'holding_days': None, 'protection_active': False}
    if int(shares_pre or 0) <= 0:
        return info
    signal_day = _current_signal_day_et(states, mode=mode)
    entry_day = _latest_open_entry_trade_date_et(states, ticker)
    info['latest_buy_trade_date_et'] = entry_day
    if not signal_day or not entry_day:
        return info
    age = _trading_day_age_from_states(states, entry_day, signal_day)
    info['holding_days'] = age
    if age is None or int(protection_days or 0) <= 0:
        return info
    info['protection_active'] = age < int(protection_days)
    return info

def _update_signals_and_thresholds(states: Dict[str, Any], derive_signals_inputs: str, derive_threshold_inputs: str, mode: Optional[str]=None) -> None:
    config = states.setdefault('config', {})
    tactical_indicators = config.get('tactical_indicators') or {'GOOG': 'SMA50', 'SMH': 'SMA100', 'NVDA': 'SMA50'}
    market = states.setdefault('market', {})
    history = _market_history_rows_map(states)
    signals_inputs = market.setdefault('signals_inputs', {})
    threshold_inputs = market.setdefault('next_close_threshold_inputs', {})
    for ticker, ind_spec in tactical_indicators.items():
        ma_rule = _normalize_ma_rule(ind_spec)
        window = _parse_indicator_window(ma_rule) or 0
        rows = (history.get(ticker) or {}).get('rows') or []
        if derive_signals_inputs != 'never':
            if derive_signals_inputs == 'force' or (derive_signals_inputs == 'missing' and ticker not in signals_inputs):
                if rows:
                    signals_inputs[ticker] = _derive_signals_inputs_from_history(rows, window)
        if derive_threshold_inputs != 'never':
            if derive_threshold_inputs == 'force' or (derive_threshold_inputs == 'missing' and ticker not in threshold_inputs):
                if rows:
                    threshold_inputs[ticker] = _derive_threshold_inputs_from_history(rows, window)
    portfolio = states.get('portfolio', {}) or {}
    positions = portfolio.get('positions', []) or []

    def tactical_shares(ticker: str) -> int:
        for p in positions:
            if p.get('ticker') == ticker and p.get('bucket') == 'tactical':
                return int(p.get('shares') or 0)
        return 0
    protection_days = 5
    pre_rows: List[Dict[str, Any]] = []
    buy_candidates: List[Dict[str, Any]] = []
    sell_candidates: List[Dict[str, Any]] = []
    for ticker, ind_spec in tactical_indicators.items():
        ma_rule = _normalize_ma_rule(ind_spec)
        inp = signals_inputs.get(ticker) or {}
        close_t = inp.get('close_t')
        ma_t = inp.get('ma_t')
        close_t_minus_5 = inp.get('close_t_minus_5')
        shares_pre = tactical_shares(ticker)
        protection = _recent_buy_protection_info(states, ticker, shares_pre, protection_days, mode=mode)
        recent_buy_protection = bool(protection.get('protection_active'))
        holding_days = protection.get('holding_days')
        latest_buy_trade_date_et = protection.get('latest_buy_trade_date_et')
        ma_ok = bool(close_t is not None and ma_t is not None and (close_t > ma_t))
        close_t_minus_5_ok = bool(close_t_minus_5 is not None and close_t is not None and (close_t > close_t_minus_5))
        buy_signal = bool(ma_ok and close_t_minus_5_ok)
        sell_signal = bool((not buy_signal) and (shares_pre > 0))
        sell_blocked_by_recent_buy = bool(sell_signal and recent_buy_protection)
        if holding_days is None:
            holding_days_display = '-'
        else:
            holding_days_display = str(int(holding_days))
        if ma_ok:
            close_gt_ma_label = 'PASS'
        else:
            close_gt_ma_label = 'FAIL'
        if sell_blocked_by_recent_buy:
            if holding_days is None:
                t5_filter_label = f'SELL_BLOCKED (<={protection_days}d)'
            else:
                t5_filter_label = f'SELL_BLOCKED ({int(holding_days)}d<={protection_days}d)'
        else:
            t5_filter_label = 'PASS' if close_t_minus_5_ok else 'FAIL'
        action_price_usd = _lookup_action_price_usd(states, ticker)
        row = {'ticker': ticker, 'close_t': close_t, 'ma_rule': ma_rule, 'ma_t': ma_t, 'close_t_minus_5': close_t_minus_5, 'buy_signal': buy_signal, 'buy_signal_ma_ok': ma_ok, 'buy_signal_close_t_minus_5_ok': close_t_minus_5_ok, 'sell_signal': sell_signal, 'sell_blocked_by_recent_buy': sell_blocked_by_recent_buy, 'recent_buy_protection_days': protection_days, 'recent_buy_protection_active': recent_buy_protection, 'close_t_minus_5_ignored': False, 'holding_days': holding_days, 'holding_days_display': holding_days_display, 'latest_buy_trade_date_et': latest_buy_trade_date_et, 'close_gt_ma_label': close_gt_ma_label, 't5_filter_label': t5_filter_label, 'tactical_shares_pre': shares_pre, 'action_price_usd': action_price_usd}
        pre_rows.append(row)
        if buy_signal and action_price_usd is not None and (action_price_usd > 0):
            buy_candidates.append({'ticker': ticker, 'price_usd': action_price_usd})
        if sell_signal and (not sell_blocked_by_recent_buy) and action_price_usd is not None and (action_price_usd > 0):
            sell_candidates.append({'ticker': ticker, 'price_usd': float(action_price_usd), 'shares_pre': shares_pre})
    investable_cash_base_usd = _estimate_tactical_buy_budget_usd(states)
    estimated_sell_reclaim_usd = round(sum((float(item['price_usd']) * int(item['shares_pre']) for item in sell_candidates)), 2)
    investable_cash_usd = round(float(investable_cash_base_usd) + float(estimated_sell_reclaim_usd), 2)
    buy_alloc = _allocate_buy_shares_across_triggered_signals(buy_candidates, investable_cash_usd)
    tactical_rows = []
    for row in pre_rows:
        ticker = row['ticker']
        buy_signal = bool(row['buy_signal'])
        sell_signal = bool(row.get('sell_signal'))
        sell_blocked_by_recent_buy = bool(row.get('sell_blocked_by_recent_buy'))
        shares_pre = int(row['tactical_shares_pre'] or 0)
        action_price_usd = row.get('action_price_usd')
        if sell_signal:
            if sell_blocked_by_recent_buy:
                action = 'HOLD'
                action_shares = 0
            else:
                action = 'SELL_ALL'
                action_shares = shares_pre
        elif not buy_signal and shares_pre > 0:
            action = 'SELL_ALL'
            action_shares = shares_pre
        elif buy_signal:
            action_shares = int(buy_alloc.get(ticker) or 0)
            if action_shares > 0 and shares_pre > 0:
                action = 'BUY_MORE'
            elif action_shares > 0 and shares_pre == 0:
                action = 'BUY'
            elif shares_pre > 0:
                action = 'HOLD'
                action_shares = 0
            else:
                action = 'NO_ACTION'
                action_shares = 0
        else:
            action = 'NO_ACTION'
            action_shares = 0
        tactical_rows.append({**row, 'investable_cash_base_usd': investable_cash_base_usd, 'estimated_sell_reclaim_usd': estimated_sell_reclaim_usd, 'investable_cash_usd': investable_cash_usd, 't_plus_1_action': action, 'action_shares': action_shares, 'action_cash_amount_usd': round(float(action_price_usd) * action_shares, 2) if action in {'BUY', 'BUY_MORE'} and action_price_usd is not None else 0.0})
    states.setdefault('signals', {})['tactical'] = tactical_rows
    thr_rows = []
    for ticker, ind_spec in tactical_indicators.items():
        ma_rule = _normalize_ma_rule(ind_spec)
        window = _parse_indicator_window(ma_rule) or 0
        inp = threshold_inputs.get(ticker) or {}
        if not inp and derive_threshold_inputs != 'never' and (history.get(ticker) or {}).get('rows'):
            inp = _derive_threshold_inputs_from_history(history[ticker]['rows'], window)
            threshold_inputs[ticker] = inp
        thr_rows.append(_calc_threshold_row(ticker, ma_rule, window, inp))
    states.setdefault('thresholds', {})['buy_signal_close_price_thresholds'] = thr_rows

@dataclass
class ImportResult:
    ticker: str
    status: str
    csv_path: str
    rows_kept: int = 0
    last_date: str = ''
    last_close: Optional[float] = None
    message: str = ''

def _discover_tickers_from_config(states: Dict[str, Any]) -> List[str]:
    cfg = states.get('config', {}) or {}
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

def _compute_keep_history_rows(states: Dict[str, Any]) -> int:
    market = states.get('market', {}) or {}
    thr_inp = market.get('next_close_threshold_inputs', {}) or {}
    windows: List[int] = []
    for v in thr_inp.values():
        if isinstance(v, dict) and v.get('window') is not None:
            try:
                windows.append(int(v['window']))
            except Exception:
                pass
    cfg = states.get('config', {}) or {}
    for ma_rule in (cfg.get('tactical_indicators') or {}).values():
        w = _parse_indicator_window(ma_rule)
        if w:
            windows.append(w)
    max_w = max(windows) if windows else 100
    return int(max_w) + 10

def _resolve_csv_candidates(states: Dict[str, Any], csv_dir: str, ticker: str) -> List[str]:
    market = states.get('market', {}) or {}
    csv_sources = market.get('csv_sources', {}) or {}
    history = market.get('history_400d', {}) or {}
    src = None
    if isinstance(csv_sources.get(ticker), str):
        src = csv_sources.get(ticker)
    if src is None and isinstance(history.get(ticker), dict):
        src = history[ticker].get('source')
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

def _import_csvs_into_states(states: Dict[str, Any], csv_dir: str, tickers: List[str], prices_now_from: str, keep_history_rows: int) -> List[ImportResult]:
    market = states.setdefault('market', {})
    runtime_history = market.setdefault('_runtime_history', {})
    csv_sources = market.setdefault('csv_sources', {})
    prices_now = market.setdefault('prices_now', {})
    results: List[ImportResult] = []
    for ticker in tickers:
        candidates = _resolve_csv_candidates(states, csv_dir, ticker)
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
            csv_sources[ticker] = os.path.basename(csv_path)
            if prices_now_from == 'close' and last_close is not None:
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
    if imported_dates:
        market['asof_t_et'] = max(imported_dates)
    return results

def _meta_note_day_fallback(states: Dict[str, Any]) -> Optional[str]:
    for v in [*_iter_mode_candidate_days(states), (states.get('market') or {}).get('asof_t_et')]:
        if isinstance(v, str) and re.fullmatch('\\d{4}-\\d{2}-\\d{2}', v):
            return v
    return None

def _meta_note_extract_day(note: Any, fallback_day: Optional[str]=None) -> Optional[str]:
    if isinstance(note, dict):
        for k in ('day', 'date', 'et_date'):
            v = note.get(k)
            if isinstance(v, str) and re.fullmatch('\\d{4}-\\d{2}-\\d{2}', v):
                return v
        for k in ('ts', 'timestamp'):
            v = note.get(k)
            if isinstance(v, str):
                m = re.search('(\\d{4}-\\d{2}-\\d{2})', v)
                if m:
                    return m.group(1)
        text = str(note.get('text') or note.get('message') or '')
    else:
        text = str(note)
    m = re.search('(\\d{4}-\\d{2}-\\d{2})', text)
    if m:
        return m.group(1)
    return fallback_day

def _prune_meta_notes_last_days(states: Dict[str, Any], keep_days: int=3) -> None:
    if keep_days <= 0:
        states.setdefault('meta', {})['notes'] = []
        return
    meta = states.setdefault('meta', {})
    notes = meta.get('notes', [])
    if not isinstance(notes, list):
        notes = [str(notes)] if notes else []
    fallback_day = _meta_note_day_fallback(states)
    dated: List[Tuple[Any, Optional[str]]] = [(n, _meta_note_extract_day(n, fallback_day=fallback_day)) for n in notes]
    unique_days = sorted({d for _, d in dated if d})
    if not unique_days:
        meta['notes'] = notes[-keep_days:]
        return
    keep_day_set = set(unique_days[-keep_days:])
    meta['notes'] = [n for n, d in dated if d in keep_day_set]

def _append_meta_notes(states: Dict[str, Any], results: List[ImportResult], keep_history_rows: int) -> None:
    meta = states.setdefault('meta', {})
    notes = meta.setdefault('notes', [])
    if not isinstance(notes, list):
        notes = [str(notes)]
        meta['notes'] = notes
    note_day = _meta_note_day_fallback(states) or datetime.now(timezone.utc).date().isoformat()

    def _with_day_prefix(msg: str) -> str:
        return f'[{note_day}] {msg}'
    imported = [r for r in results if r.status == 'imported']
    skipped = [r for r in results if r.status == 'skipped_missing']
    errors = [r for r in results if r.status == 'error']
    if imported:
        parts = [f'{r.ticker}: {r.rows_kept} rows(last {keep_history_rows}), last={r.last_date} close={r.last_close:.2f}' for r in imported if r.last_close is not None]
        notes.append(_with_day_prefix('CSV imported successfully: ' + '; '.join(parts)))
    else:
        notes.append(_with_day_prefix('CSV not found.'))
    if skipped:
        notes.append(_with_day_prefix('CSV missing and skipped: ' + '; '.join([r.ticker for r in skipped])))
    if errors:
        notes.append(_with_day_prefix('CSV failed to read: ' + '; '.join([r.ticker for r in errors])))
    _prune_meta_notes_last_days(states, keep_days=3)

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

def _sync_cash_external_flow_summary(states: Dict[str, Any]) -> None:
    portfolio = states.setdefault('portfolio', {})
    cash = portfolio.setdefault('cash', {'usd': 0.0, 'bucket': 'tactical_pool'})
    flows = cash.get('external_flows') or []
    net_external = round(float(cash.get('net_external_cash_flow_usd') or 0.0), 2)
    last_flow = None
    if isinstance(flows, list):
        for item in reversed(flows):
            if isinstance(item, dict):
                last_flow = item
                break
    cash['external_cash_flow'] = {'net_usd': net_external, 'flow_count': len(flows) if isinstance(flows, list) else 0, 'last_flow_asof_et': str(last_flow.get('asof_et') or '').strip() if isinstance(last_flow, dict) else '', 'last_flow_kind': str(last_flow.get('kind') or '').strip() if isinstance(last_flow, dict) else '', 'last_flow_amount_usd': round(float(last_flow.get('amount_usd') or 0.0), 2) if isinstance(last_flow, dict) and last_flow.get('amount_usd') is not None else None}

def _ensure_cash_buckets(states: Dict[str, Any]) -> None:
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
    cash['deployable_usd'] = round(deployable_f, 2)
    cash['reserve_usd'] = round(reserve_f, 2)
    cash['usd'] = round(deployable_f + reserve_f, 2)

def _set_total_cash_preserve_reserve(states: Dict[str, Any], total_cash_usd: float) -> None:
    _ensure_cash_buckets(states)
    cash = states.setdefault('portfolio', {}).setdefault('cash', {'usd': 0.0, 'bucket': 'tactical_pool'})
    total = round(max(0.0, float(total_cash_usd or 0.0)), 2)
    reserve = round(max(0.0, float(cash.get('reserve_usd') or 0.0)), 2)
    reserve = min(reserve, total)
    deployable = round(total - reserve, 2)
    cash['deployable_usd'] = deployable
    cash['reserve_usd'] = reserve
    cash['usd'] = round(deployable + reserve, 2)

def _apply_cash_transfer_to_reserve(states: Dict[str, Any], amount_usd: float, asof_et: Optional[str]=None) -> None:
    _ensure_cash_buckets(states)
    cash = states.setdefault('portfolio', {}).setdefault('cash', {'usd': 0.0, 'bucket': 'tactical_pool'})
    amt = round(float(amount_usd), 2)
    if abs(amt) < 1e-12:
        return
    deployable = round(float(cash.get('deployable_usd') or 0.0), 2)
    reserve = round(float(cash.get('reserve_usd') or 0.0), 2)
    if amt > 0 and amt > deployable + 1e-09:
        raise ValueError(f'cash transfer to reserve is out of range: requested={amt:.2f}, deployable_usd={deployable:.2f}')
    if amt < 0 and -amt > reserve + 1e-09:
        raise ValueError(f'cash transfer back to deployable is out of range: requested={amt:.2f}, reserve_usd={reserve:.2f}')
    cash['deployable_usd'] = round(deployable - amt, 2)
    cash['reserve_usd'] = round(reserve + amt, 2)
    cash['usd'] = round(float(cash['deployable_usd']) + float(cash['reserve_usd']), 2)
    transfers = cash.setdefault('internal_transfers', [])
    transfers.append({'amount_usd': amt, 'kind': 'to_reserve' if amt > 0 else 'to_deployable', 'asof_et': str(asof_et or '').strip(), 'ts_utc': datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')})
    print(f"[CASH] applied internal cash transfer: amount_usd={amt:.2f}, deployable_usd={cash['deployable_usd']:.2f}, reserve_usd={cash['reserve_usd']:.2f}")

def _set_initial_investment_usd(states: Dict[str, Any], amount_usd: float) -> None:
    portfolio = states.setdefault('portfolio', {})
    perf = portfolio.setdefault('performance', {})
    baseline = perf.setdefault('baseline', {})
    amount_rounded = round(float(amount_usd), 2)
    perf['initial_investment_usd'] = amount_rounded
    baseline['initial_investment_usd'] = amount_rounded

def _sync_broker_snapshot_meta(states: Dict[str, Any], mode: str, broker_asof_et: Optional[str], broker_asof_et_datetime: Optional[str], snapshot_kind: str) -> None:
    snap = _get_mode_snapshot(states, mode, create=True)
    bctx = snap.setdefault('broker_context', {})
    if broker_asof_et:
        bctx['asof_et'] = broker_asof_et
    else:
        bctx.pop('asof_et', None)
    if broker_asof_et_datetime:
        bctx['asof_et_datetime'] = broker_asof_et_datetime
    else:
        bctx.pop('asof_et_datetime', None)
    bctx['snapshot_kind'] = snapshot_kind

def _clear_holdings_reconciliation_snapshot(states: Dict[str, Any]) -> None:
    broker = (states.get('portfolio') or {}).get('broker') or {}
    if not isinstance(broker, dict):
        return
    for key in ('investment_total_usd', 'investments_total_usd', 'investment_total_excludes_cash', 'investment_total_kind', 'holdings_mv_usd', 'holdings_cost_usd', 'diff_usd', 'tolerance_usd', 'status', 'source'):
        broker.pop(key, None)
    broker.pop('reconciliation', None)

def _apply_cash_adjustment(states: Dict[str, Any], amount_usd: float, note: str='', asof_et: Optional[str]=None) -> None:
    portfolio = states.setdefault('portfolio', {})
    _ensure_cash_buckets(states)
    cash = portfolio.setdefault('cash', {'usd': 0.0, 'bucket': 'tactical_pool'})
    trades = states.get('trades', []) or []
    amt = round(float(amount_usd), 2)
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
    cash['baseline_usd'] = round(float(baseline) + amt, 2)
    cash['baseline_source'] = 'manual_cash_adjustment'
    cash['net_external_cash_flow_usd'] = round(float(cash.get('net_external_cash_flow_usd') or 0.0) + amt, 2)
    flows = cash.setdefault('external_flows', [])
    flows.append({'amount_usd': amt, 'kind': 'deposit' if amt >= 0 else 'withdrawal', 'asof_et': str(asof_et or '').strip(), 'note': str(note or '').strip(), 'ts_utc': datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')})
    _sync_cash_external_flow_summary(states)
    print(f"[CASH] applied external cash {('deposit' if amt >= 0 else 'withdrawal')}: amount_usd={amt:.2f}, baseline_usd={cash['baseline_usd']:.2f}")

def _update_portfolio_performance(states: Dict[str, Any]) -> None:
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
    perf['current_total_assets_usd'] = round(current_total_assets, 2)
    perf['net_external_cash_flow_usd'] = round(net_external, 2)
    returns['current_total_assets_usd'] = round(current_total_assets, 2)
    _sync_cash_external_flow_summary(states)
    baseline['net_external_cash_flow_usd'] = round(net_external, 2)
    baseline['method'] = 'initial_investment_plus_net_external_cash_flow'
    if initial is None:
        baseline.pop('initial_investment_usd', None)
        baseline.pop('effective_capital_base_usd', None)
        returns.pop('profit_usd', None)
        returns.pop('profit_rate', None)
        return
    effective_base = round(initial + net_external, 2)
    profit_usd = round(current_total_assets - effective_base, 2)
    profit_rate = profit_usd / effective_base if abs(effective_base) > 1e-12 else None
    perf['initial_investment_usd'] = round(initial, 2)
    perf['effective_capital_base_usd'] = effective_base
    perf['profit_usd'] = profit_usd
    perf['profit_rate'] = profit_rate
    baseline['initial_investment_usd'] = round(initial, 2)
    baseline['effective_capital_base_usd'] = effective_base
    returns['profit_usd'] = profit_usd
    returns['profit_rate'] = profit_rate

def _first_token_ticker(product_name: str) -> str:
    s = str(product_name or '').strip()
    m = re.match('^([A-Z0-9.\\-]+)', s)
    if m:
        return m.group(1).upper()
    return s.split()[0].upper() if s.split() else ''

def _num_from_cell(s: str) -> Optional[float]:
    raw = str(s or '').strip().replace(',', '')
    if not raw:
        return None
    raw = re.sub('[A-Za-z\\u4e00-\\u9fff]+$', '', raw).strip()
    if raw == '':
        return None
    try:
        return float(raw)
    except Exception:
        return None

def _import_trades_from_os_history_xml(xml_path: str) -> List[Dict[str, Any]]:
    return _shared_import_trades_from_os_history_xml(xml_path)

def _trade_key(t: Dict[str, Any]) -> str:
    tt = _normalize_time_tw(str(t.get('time_tw') or ''))
    tt_min = tt[:16] if len(tt) >= 16 else tt
    return '|'.join([_normalize_trade_date_et(str(t.get('trade_date_et') or '')), str(t.get('ticker') or '').upper(), str(t.get('side') or '').upper(), tt_min, f"{float(t.get('price') or 0.0):.4f}", str(int(float(t.get('shares') or 0))), f"{float(t.get('cash_amount') or 0.0):.2f}"])

def _normalize_trades_inplace(states: Dict[str, Any]) -> None:
    trades = states.get('trades') or []
    if not isinstance(trades, list):
        return
    for t in trades:
        if not isinstance(t, dict):
            continue
        if 'trade_date_et' in t:
            t['trade_date_et'] = _normalize_trade_date_et(str(t.get('trade_date_et') or ''))
        if 'time_tw' in t:
            t['time_tw'] = _normalize_time_tw(str(t.get('time_tw') or ''))
        if 'ticker' in t and t['ticker']:
            t['ticker'] = str(t['ticker']).upper()
        if 'side' in t and t['side']:
            t['side'] = str(t['side']).upper()

def _upsert_trades(states: Dict[str, Any], incoming: List[Dict[str, Any]]) -> Tuple[int, int]:
    trades = states.setdefault('trades', [])
    if not isinstance(trades, list):
        trades = []
        states['trades'] = trades
    _normalize_trades_inplace(states)
    existing = set()
    max_id = 0
    for t in trades:
        if isinstance(t, dict):
            existing.add(_trade_key(t))
            try:
                max_id = max(max_id, int(t.get('trade_id') or 0))
            except Exception:
                pass
    added = dup = 0
    for t in incoming:
        k = _trade_key(t)
        if k in existing:
            dup += 1
            continue
        max_id += 1
        t['trade_id'] = max_id
        trades.append(t)
        existing.add(k)
        added += 1
    return (added, dup)

def _is_broker_trade(t: Dict[str, Any]) -> bool:
    src = str(t.get('source') or '')
    notes = str(t.get('notes') or '')
    return src.startswith('xml:') or 'OSHistoryDealAll' in notes

def _archive_trades(states: Dict[str, Any], to_archive: List[Dict[str, Any]], reason: str) -> None:
    if not to_archive:
        return
    arch = states.setdefault('trades_archived', [])
    if not isinstance(arch, list):
        arch = []
        states['trades_archived'] = arch
    ts = datetime.now().isoformat(timespec='seconds')
    for t in to_archive:
        rec = dict(t)
        rec['archived_at'] = ts
        rec['archived_reason'] = reason
        arch.append(rec)

def _group_key_trade(t: Dict[str, Any]) -> tuple:
    return (_normalize_trade_date_et(str(t.get('trade_date_et') or '')), str(t.get('ticker') or '').upper(), str(t.get('side') or '').upper())

def _trade_cash_total_for_match(trades: List[Dict[str, Any]], side: str) -> float:
    side_u = (side or '').upper()
    s = 0.0
    for t in trades:
        ca = t.get('cash_amount')
        if ca is not None:
            try:
                s += float(ca)
                continue
            except Exception:
                pass
        g = float(t.get('gross') or 0.0)
        f = float(t.get('fee') or 0.0)
        if side_u.startswith('B'):
            s += g + f
        elif side_u.startswith('S'):
            s += max(g - f, 0.0)
        else:
            s += g
    return s

def _reconcile_manual_aggregates(states: Dict[str, Any], incoming: List[Dict[str, Any]], abs_tol_usd: float=1.0, rel_tol: float=0.003) -> Tuple[int, set]:
    trades = states.get('trades') or []
    if not isinstance(trades, list) or not trades:
        return (0, set())
    g_in = {}
    for t in incoming:
        k = _group_key_trade(t)
        g = g_in.setdefault(k, [])
        g.append(t)
    removed_total = 0
    superseded_groups = set()
    g_ex = {}
    for t in trades:
        if not isinstance(t, dict):
            continue
        k = _group_key_trade(t)
        g_ex.setdefault(k, []).append(t)
    remove_ids = set()
    for k, inc_list in g_in.items():
        ex_list = g_ex.get(k, [])
        if not ex_list:
            continue
        candidates = [t for t in ex_list if not _is_broker_trade(t)]
        if not candidates:
            continue
        side = k[2]
        inc_shares = sum((int(float(t.get('shares') or 0)) for t in inc_list))
        cand_shares = sum((int(float(t.get('shares') or 0)) for t in candidates))
        if inc_shares <= 0 or cand_shares <= 0 or inc_shares != cand_shares:
            continue
        inc_cash = _trade_cash_total_for_match(inc_list, side)
        cand_cash = _trade_cash_total_for_match(candidates, side)
        tol = max(float(abs_tol_usd), float(rel_tol) * abs(inc_cash))
        if abs(cand_cash - inc_cash) <= tol:
            for t in candidates:
                tid = t.get('trade_id')
                remove_ids.add(tid if tid is not None else id(t))
            reason = f'superseded_manual_aggregate_by_broker_xml: group={k[0]}/{k[1]}/{k[2]}, manual_count={len(candidates)}, broker_count={len(inc_list)}, shares={inc_shares}, cash_manual={cand_cash:.2f}, cash_broker={inc_cash:.2f}, tol={tol:.2f}'
            _archive_trades(states, candidates, reason)
            removed_total += len(candidates)
            superseded_groups.add(k)
            print(f'[RECON] {k[1]} {k[2]} {k[0]}: superseded {len(candidates)} manual trade(s) with {len(inc_list)} broker fill(s) (shares={inc_shares}).')
        else:
            print(f'[RECON][WARN] {k[1]} {k[2]} {k[0]}: shares match (manual={cand_shares}, broker={inc_shares}) but cash differs (manual={cand_cash:.2f}, broker={inc_cash:.2f}, tol={tol:.2f}). Keeping manual trades; review.')
    if not remove_ids:
        return (0, superseded_groups)
    new_trades = []
    for t in trades:
        if not isinstance(t, dict):
            new_trades.append(t)
            continue
        tid = t.get('trade_id')
        key = tid if tid is not None else id(t)
        if key in remove_ids:
            continue
        new_trades.append(t)
    states['trades'] = new_trades
    return (removed_total, superseded_groups)

def _replace_trades_for_incoming_scope(states: Dict[str, Any], incoming: List[Dict[str, Any]]) -> int:
    trades = states.get('trades') or []
    if not isinstance(trades, list) or not trades:
        return 0
    scope = set()
    for t in incoming:
        d = _normalize_trade_date_et(str(t.get('trade_date_et') or ''))
        tk = str(t.get('ticker') or '').upper()
        if d and tk:
            scope.add((d, tk))
    if not scope:
        return 0
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
        _archive_trades(states, removed, f'replaced_by_broker_xml: scope_size={len(scope)}')
        states['trades'] = keep
        print(f'[REPLACE] removed {len(removed)} existing trade(s) in scope {len(scope)} (date,ticker) pairs; archived to trades_archived.')
    return len(removed)

def _sort_key_trade_for_portfolio(t: Dict[str, Any]) -> tuple:
    return (_normalize_trade_date_et(str(t.get('trade_date_et') or '')), _normalize_time_tw(str(t.get('time_tw') or '')), int(t.get('trade_id') or 0))

def _position_bucket_default(states: Dict[str, Any], ticker: str) -> str:
    ticker = str(ticker or '').upper().strip()
    portfolio = states.setdefault('portfolio', {})
    positions = portfolio.setdefault('positions', [])
    if isinstance(positions, list):
        for p in positions:
            if isinstance(p, dict) and str(p.get('ticker') or '').upper().strip() == ticker:
                bucket = str(p.get('bucket') or '').strip()
                if bucket:
                    return bucket
    return 'tactical'

def _get_or_create_position(states: Dict[str, Any], ticker: str) -> Dict[str, Any]:
    ticker = str(ticker or '').upper().strip()
    portfolio = states.setdefault('portfolio', {})
    positions = portfolio.setdefault('positions', [])
    if not isinstance(positions, list):
        positions = []
        portfolio['positions'] = positions
    for p in positions:
        if isinstance(p, dict) and str(p.get('ticker') or '').upper().strip() == ticker:
            if not str(p.get('bucket') or '').strip():
                p['bucket'] = _position_bucket_default(states, ticker)
            return p
    p = {'ticker': ticker, 'bucket': _position_bucket_default(states, ticker), 'shares': 0, 'cost_usd': 0.0, 'notes': 'No current position'}
    positions.append(p)
    return p

def _trade_buy_total_cost_usd(t: Dict[str, Any]) -> float:
    try:
        cash_amount = t.get('cash_amount')
        if cash_amount is not None:
            return float(cash_amount)
    except Exception:
        pass
    return float(t.get('gross') or 0.0) + float(t.get('fee') or 0.0)

def _apply_incremental_trades_to_portfolio(states: Dict[str, Any], trades_delta: List[Dict[str, Any]]) -> None:
    if not trades_delta:
        return
    for t in sorted(trades_delta, key=_sort_key_trade_for_portfolio):
        ticker = str(t.get('ticker') or '').upper().strip()
        side = str(t.get('side') or '').upper().strip()
        try:
            shares = int(float(t.get('shares') or 0))
        except Exception:
            shares = 0
        if not ticker or shares <= 0:
            continue
        pos = _get_or_create_position(states, ticker)
        cur_shares = int(float(pos.get('shares') or 0))
        cur_cost = float(pos.get('cost_usd') or 0.0)
        if side.startswith('B'):
            pos['shares'] = cur_shares + shares
            pos['cost_usd'] = round(cur_cost + _trade_buy_total_cost_usd(t), 2)
            if 'No current position' in str(pos.get('notes') or ''):
                pos['notes'] = ''
        elif side.startswith('S'):
            if cur_shares <= 0:
                pos['shares'] = 0
                pos['cost_usd'] = 0.0
                pos['notes'] = 'No current position'
                print(f'[PORTFOLIO][WARN] {ticker}: sell trade ignored for cost basis because current shares are zero.')
                continue
            if shares >= cur_shares:
                pos['shares'] = 0
                pos['cost_usd'] = 0.0
                pos['notes'] = 'No current position'
            else:
                avg_cost = cur_cost / cur_shares if cur_shares > 0 else 0.0
                new_shares = cur_shares - shares
                new_cost = cur_cost - avg_cost * shares
                pos['shares'] = new_shares
                pos['cost_usd'] = round(max(new_cost, 0.0), 2)
                if pos['shares'] > 0 and 'No current position' in str(pos.get('notes') or ''):
                    pos['notes'] = ''

def _rebuild_portfolio_positions_from_day1(states: Dict[str, Any]) -> None:
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
    state_by_ticker: Dict[str, Dict[str, float]] = {}
    trades_all = states.get('trades') or []
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
        st = state_by_ticker.setdefault(ticker, {'shares': 0.0, 'cost': 0.0})
        cur_shares = float(st['shares'])
        cur_cost = float(st['cost'])
        if side.startswith('B'):
            st['shares'] = cur_shares + shares
            st['cost'] = cur_cost + _trade_buy_total_cost_usd(t)
        elif side.startswith('S'):
            if cur_shares <= 0:
                st['shares'] = 0.0
                st['cost'] = 0.0
                print(f'[PORTFOLIO][WARN] {ticker}: replace/day1 rebuild found sell larger than holdings; clamping to zero.')
                continue
            if shares >= cur_shares:
                st['shares'] = 0.0
                st['cost'] = 0.0
            else:
                avg_cost = cur_cost / cur_shares if cur_shares > 0 else 0.0
                st['shares'] = cur_shares - shares
                st['cost'] = max(cur_cost - avg_cost * shares, 0.0)
    for ticker in sorted(tickers):
        pos = pos_by_ticker.get(ticker)
        if pos is None:
            pos = {'ticker': ticker, 'bucket': _position_bucket_default(states, ticker)}
            positions.append(pos)
            pos_by_ticker[ticker] = pos
        st = state_by_ticker.get(ticker, {'shares': 0.0, 'cost': 0.0})
        shares_now = int(round(float(st['shares'])))
        cost_now = round(float(st['cost']), 2) if shares_now > 0 else 0.0
        pos['ticker'] = ticker
        if not str(pos.get('bucket') or '').strip():
            pos['bucket'] = _position_bucket_default(states, ticker)
        pos['shares'] = shares_now
        pos['cost_usd'] = cost_now
        if shares_now <= 0:
            pos['notes'] = 'No current position'
        elif 'No current position' in str(pos.get('notes') or ''):
            pos['notes'] = ''
    print('[PORTFOLIO] replace mode: rebuilt portfolio.positions from day1 trades ledger.')

def _verify_holdings_with_broker_investment_total(states: Dict[str, Any], broker_investment_total_usd: Optional[float], broker_asof_et: Optional[str], broker_investment_total_kind: str='market_value', verify_tolerance_usd: float=1.0) -> None:
    if broker_investment_total_usd is None:
        return
    portfolio = states.setdefault('portfolio', {})
    totals = portfolio.get('totals', {}) or {}
    kind = (broker_investment_total_kind or 'market_value').strip().lower()
    if kind not in ('market_value', 'cost_basis'):
        kind = 'market_value'
    holdings_val = None
    metric_key = 'holdings_mv_usd'
    label = 'holdings_mv_usd'
    if kind == 'cost_basis':
        metric_key = 'holdings_cost_usd'
        label = 'holdings_cost_usd'
    if (totals.get('portfolio') or {}).get(metric_key) is not None:
        holdings_val = float(totals['portfolio'][metric_key])
    else:
        holdings_val = 0.0
        for p in portfolio.get('positions', []) or []:
            if kind == 'cost_basis':
                holdings_val += float(p.get('cost_usd') or 0.0)
            else:
                mv = p.get('market_value_usd')
                if mv is not None:
                    holdings_val += float(mv)
    broker_investment_total_usd = float(broker_investment_total_usd)
    diff = holdings_val - broker_investment_total_usd
    status = 'OK' if abs(diff) <= verify_tolerance_usd else 'MISMATCH'
    print(f'[VERIFY] broker_investment_total_usd(ex-cash)={broker_investment_total_usd:.2f} | {label}={holdings_val:.2f} | diff={diff:.2f} | tol={verify_tolerance_usd:.2f} => {status}')
    broker = portfolio.setdefault('broker', {})
    broker['investment_total_usd'] = broker_investment_total_usd
    broker['investment_total_excludes_cash'] = True
    broker['investment_total_kind'] = kind
    holdings_mv_usd = None
    holdings_cost_usd = None
    try:
        holdings_mv_usd = float((totals.get('portfolio') or {}).get('holdings_mv_usd') or broker.get('holdings_mv_usd') or 0.0)
        broker['holdings_mv_usd'] = holdings_mv_usd
    except Exception:
        pass
    try:
        holdings_cost_usd = float((totals.get('portfolio') or {}).get('holdings_cost_usd') or broker.get('holdings_cost_usd') or 0.0)
        broker['holdings_cost_usd'] = holdings_cost_usd
    except Exception:
        pass
    broker[label] = holdings_val
    broker['diff_usd'] = diff
    broker['tolerance_usd'] = verify_tolerance_usd
    broker['status'] = status
    broker['source'] = 'cli'
    broker['reconciliation'] = {'mode': 'holdings_investment_total_ex_cash', 'source': 'cli', 'input': {'investment_total_usd': broker_investment_total_usd, 'investment_total_kind': kind, 'investment_total_excludes_cash': True, 'asof_et': broker_asof_et}, 'computed': {'holdings_mv_usd': holdings_mv_usd, 'holdings_cost_usd': holdings_cost_usd, 'compared_metric': label, 'compared_value_usd': holdings_val}, 'result': {'diff_usd': diff, 'tolerance_usd': verify_tolerance_usd, 'status': status}}

def _update_tactical_cash_from_trades_and_snapshot(states: Dict[str, Any], tactical_cash_usd: Optional[float], broker_asof_et: Optional[str], verify_tolerance_usd: float=1.0, cutoff_et_dt: Optional[datetime]=None, snapshot_kind: str='eod') -> None:
    portfolio = states.setdefault('portfolio', {})
    cash = portfolio.setdefault('cash', {'usd': 0.0, 'bucket': 'tactical_pool'})
    trades = states.get('trades', []) or []
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
        print(f'[INFO] tactical cash baseline inferred from existing cash.usd: baseline_usd={baseline:.2f}')
    cash_from_baseline = baseline + net_cash_change
    if tactical_cash_usd is not None:
        tactical_cash_usd = float(tactical_cash_usd)
        diff = cash_from_baseline - tactical_cash_usd
        status = 'OK' if abs(diff) <= verify_tolerance_usd else 'MISMATCH'
        print(f'[VERIFY] broker_tactical_cash_usd={tactical_cash_usd:.2f} | cash_from_baseline={cash_from_baseline:.2f} | diff={diff:.2f} | tol={verify_tolerance_usd:.2f} => {status}')
        baseline_new = tactical_cash_usd - net_cash_change
        _set_total_cash_preserve_reserve(states, tactical_cash_usd)
        cash['baseline_usd'] = baseline_new
        cash['baseline_source'] = 'inferred_from_broker_tactical_cash'
        cash['derived_from_trades'] = True
        cash['last_reconciled_with_broker_cash'] = {'asof_et': broker_asof_et, 'snapshot_kind': snapshot_kind, 'asof_et_datetime': cutoff_et_dt.isoformat() if cutoff_et_dt is not None else None, 'broker_tactical_cash_usd': tactical_cash_usd, 'cash_from_baseline_usd': cash_from_baseline, 'baseline_usd': baseline_new, 'net_cash_change_usd': net_cash_change, 'diff_usd': diff, 'tolerance_usd': verify_tolerance_usd, 'status': status}
    else:
        _set_total_cash_preserve_reserve(states, cash_from_baseline)
        cash['derived_from_trades'] = True
        cut = f', cutoff_et={cutoff_et_dt.isoformat()}' if cutoff_et_dt is not None else ''
        print(f'[INFO] tactical cash.usd derived from trades: cash_usd={cash_from_baseline:.2f} (baseline_usd={baseline:.2f}, net_cash_change={net_cash_change:.2f}{cut})')

def _ensure_report_fields(states: Dict[str, Any]) -> List[str]:
    warns: List[str] = []
    for k in ['meta', 'config', 'market', 'portfolio', 'trades', 'signals', 'thresholds']:
        if k not in states:
            warns.append(f'missing root key: {k}')
    market = states.get('market', {}) or {}
    for k in ['prices_now', 'signals_inputs', 'next_close_threshold_inputs']:
        if k not in market:
            warns.append(f'missing market.{k}')
    portfolio = states.get('portfolio', {}) or {}
    if 'positions' not in portfolio:
        warns.append('missing portfolio.positions')
    if 'cash' not in portfolio:
        warns.append('missing portfolio.cash')
    if 'totals' not in portfolio:
        warns.append('missing portfolio.totals')
    signals = states.get('signals', {}) or {}
    if 'tactical' not in signals:
        warns.append('missing signals.tactical')
    thresholds = states.get('thresholds', {}) or {}
    if 'buy_signal_close_price_thresholds' not in thresholds:
        warns.append('missing thresholds.buy_signal_close_price_thresholds')
    return warns

def _parse_ymd_loose(s: str) -> Optional[date]:
    if not s:
        return None
    s = str(s).strip()
    s = s.replace('/', '-')
    s = s.split()[0]
    m = re.match('^(\\d{4})-(\\d{1,2})-(\\d{1,2})$', s)
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except Exception:
        return None

def _close_from_history_asof(states: Dict[str, Any], ticker: str, asof_et: Optional[str]) -> Tuple[Optional[str], Optional[float]]:
    hist = (_market_history_rows_map(states).get(ticker) or {})
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

def _position_price_used(states: Dict[str, Any], pos: Dict[str, Any], ticker: str) -> Tuple[Optional[float], str]:
    if pos.get('price_now') is not None:
        return (_safe_float(pos.get('price_now')), 'position.price_now')
    pn = ((states.get('market') or {}).get('prices_now') or {}).get(ticker)
    if pn is not None:
        try:
            return (float(pn), 'market.prices_now')
        except Exception:
            pass
    d, c = _close_from_history_asof(states, ticker, None)
    if c is not None:
        return (c, f'history.last_close({d})')
    return (None, 'missing')

def _diagnose_holdings_mismatch_md(states: Dict[str, Any], broker_investment_total_usd: Optional[float], broker_asof_et: Optional[str], verify_tolerance_usd: float, level: str='full') -> str:
    portfolio = states.get('portfolio') or {}
    positions = portfolio.get('positions') or []
    lines: List[str] = []
    lines.append('## Diagnosis: Holdings market value mismatch (broker investment total, ex-cash)')
    lines.append('')
    if broker_investment_total_usd is None:
        lines.append('- No `broker_investment_total_usd` provided; cannot diagnose this section.')
        lines.append('')
        return '\n'.join(lines)
    rows: List[Dict[str, Any]] = []
    sum_used = 0.0
    sum_asof = 0.0
    missing_prices: List[str] = []
    stale_prices: List[str] = []
    for p in positions:
        ticker = str(p.get('ticker') or '').upper()
        if not ticker:
            continue
        shares = _safe_float(p.get('shares')) or 0.0
        if abs(shares) < 1e-12:
            continue
        price_used, src = _position_price_used(states, p, ticker)
        hist_last_date, hist_last_close = _close_from_history_asof(states, ticker, None)
        asof_date, asof_close = _close_from_history_asof(states, ticker, broker_asof_et)
        mv_used = None if price_used is None else shares * price_used
        mv_last = None if hist_last_close is None else shares * hist_last_close
        mv_asof = None if asof_close is None else shares * asof_close
        if price_used is None:
            missing_prices.append(ticker)
        elif hist_last_close is not None and abs(price_used - hist_last_close) > 1e-09:
            stale_prices.append(f'{ticker} used={price_used} vs last_close={hist_last_close}({hist_last_date})')
        if mv_used is not None:
            sum_used += mv_used
        if mv_asof is not None:
            sum_asof += mv_asof
        rows.append({'ticker': ticker, 'bucket': p.get('bucket'), 'shares': shares, 'price_used': price_used, 'price_source': src, 'mv_used': mv_used, 'hist_last_date': hist_last_date, 'hist_last_close': hist_last_close, 'mv_last_close': mv_last, 'asof_date': asof_date, 'asof_close': asof_close, 'mv_asof_close': mv_asof})
    diff_used = sum_used - float(broker_investment_total_usd)
    status = 'OK' if abs(diff_used) <= verify_tolerance_usd else 'MISMATCH'
    lines.append(f"- As-of (ET): `{broker_asof_et or 'N/A'}`")
    lines.append(f'- Broker investment total (ex-cash): **{broker_investment_total_usd:.2f}**')
    lines.append(f'- Computed holdings_mv_usd (using price_used): **{sum_used:.2f}**')
    lines.append(f'- Diff: **{diff_used:.2f}** (tol={verify_tolerance_usd:.2f}) => **{status}**')
    lines.append('')
    if broker_asof_et and rows:
        diff_asof = sum_asof - float(broker_investment_total_usd)
        lines.append(f'- Alt valuation using history close as-of `{broker_asof_et}` (per ticker): **{sum_asof:.2f}** | diff vs broker: **{diff_asof:.2f}**')
        lines.append('  - If this alt diff is much smaller than the main diff, the root cause is likely **as-of timing** (broker snapshot uses an earlier close).')
        lines.append('')
    if missing_prices:
        lines.append(f"- Missing prices for tickers with shares: `{', '.join(missing_prices)}`")
        lines.append('  - Likely causes: CSV missing / prices_now not updated / ticker name mismatch.')
        lines.append('')
    if stale_prices and level == 'full':
        lines.append('- Price_used differs from history.last_close for:')
        for s in stale_prices[:20]:
            lines.append(f'  - {s}')
        if len(stale_prices) > 20:
            lines.append(f'  - ... ({len(stale_prices) - 20} more)')
        lines.append('')
    if rows:
        contrib = []
        for r in rows:
            if r['mv_used'] is None or r['mv_asof_close'] is None:
                continue
            contrib.append((r['ticker'], r['mv_used'] - r['mv_asof_close']))
        contrib.sort(key=lambda x: abs(x[1]), reverse=True)
        if broker_asof_et and contrib:
            lines.append('### Top contributors (price_used vs close-as-of)')
            lines.append('| Ticker | ΔMV (used - asof_close) |')
            lines.append('| --- | ---: |')
            for t, dmv in contrib[:10]:
                lines.append(f'| {t} | {_fmt_usd(dmv)} |')
            lines.append('')
            lines.append("If one ticker dominates ΔMV, check that ticker's CSV date/close and the broker's valuation timestamp.")
            lines.append('')
        if level == 'full':
            lines.append('### Per-position breakdown')
            lines.append('| Bucket | Ticker | Shares | Price_used | Source | MV_used | Hist_last_date | Last_close | MV_last | As-of_date | As-of_close | MV_asof |')
            lines.append('| --- | --- | ---: | ---: | --- | ---: | --- | ---: | ---: | --- | ---: | ---: |')
            for r in sorted(rows, key=lambda x: abs(x.get('mv_used') or 0.0), reverse=True):
                lines.append(f"| {r.get('bucket', '')} | {r['ticker']} | {r['shares']:.4f} | {(r['price_used'] if r['price_used'] is not None else '')} | {r['price_source']} | {(_fmt_usd(r['mv_used']) if r['mv_used'] is not None else '')} | {r['hist_last_date'] or ''} | {(r['hist_last_close'] if r['hist_last_close'] is not None else '')} | {(_fmt_usd(r['mv_last_close']) if r['mv_last_close'] is not None else '')} | {r['asof_date'] or ''} | {(r['asof_close'] if r['asof_close'] is not None else '')} | {(_fmt_usd(r['mv_asof_close']) if r['mv_asof_close'] is not None else '')} |")
            lines.append('')
    lines.append('### Next debugging steps (high-signal checks)')
    lines.append("1) Confirm the broker 'as-of' timestamp: close vs real-time price. If broker uses yesterday close, pass `--broker-asof-et YYYY-MM-DD` accordingly.")
    lines.append('2) Check whether any held ticker CSV was missing/failed import. The script prints `[SKIP]`/`[ERROR]` per ticker; missing prices often cascade into MV mismatch.')
    lines.append('3) Validate ticker mapping: broker symbol vs your CSV filename vs states ticker (e.g., BRK.B vs BRK-B).')
    lines.append("4) If mismatch is close to one position's MV, that position is the likely culprit; inspect its shares and price inputs in `states.updated.json`.")
    lines.append('')
    return '\n'.join(lines)

def _diagnose_holdings_cost_basis_mismatch_md(states: Dict[str, Any], broker_investment_total_usd: Optional[float], broker_asof_et: Optional[str], verify_tolerance_usd: float, level: str='full') -> str:
    portfolio = states.get('portfolio') or {}
    positions = portfolio.get('positions') or []
    lines: List[str] = []
    lines.append('## Diagnosis: Holdings cost basis mismatch (broker investment total = cost basis, ex-cash)')
    lines.append('')
    if broker_investment_total_usd is None:
        lines.append('- No `broker_investment_total_usd` provided; cannot diagnose this section.')
        lines.append('')
        return '\n'.join(lines)
    rows = []
    cost_sum = 0.0
    for p in positions:
        ticker = str(p.get('ticker') or '').upper()
        if not ticker:
            continue
        shares = float(p.get('shares') or 0.0)
        if abs(shares) < 1e-12:
            continue
        cost = float(p.get('cost_usd') or 0.0)
        cost_sum += cost
        rows.append({'bucket': p.get('bucket') or '', 'ticker': ticker, 'shares': shares, 'cost_usd': cost, 'notes': p.get('notes') or ''})
    diff = cost_sum - float(broker_investment_total_usd)
    status = 'OK' if abs(diff) <= verify_tolerance_usd else 'MISMATCH'
    lines.append(f"- As-of (ET): `{broker_asof_et or 'N/A'}` (as-of does not affect cost basis, kept for traceability)")
    lines.append(f'- Broker investment total = cost basis (ex-cash): **{broker_investment_total_usd:.2f}**')
    lines.append(f'- Computed holdings_cost_usd (sum of position.cost_usd): **{cost_sum:.2f}**')
    lines.append(f'- Diff: **{diff:.2f}** (tol={verify_tolerance_usd:.2f}) => **{status}**')
    lines.append('')
    if level == 'full':
        lines.append('### Per-position cost basis breakdown')
        lines.append('| Bucket | Ticker | Shares | Cost(USD) | Notes |')
        lines.append('| --- | --- | ---: | ---: | --- |')
        for r in sorted(rows, key=lambda x: abs(x.get('cost_usd') or 0.0), reverse=True):
            lines.append(f"| {r['bucket']} | {r['ticker']} | {r['shares']:.4f} | {_fmt_usd(r['cost_usd'])} | {r['notes']} |")
        lines.append('')
    lines.append('### Next debugging steps (high-signal checks)')
    lines.append('1) If broker investment total excludes some position(s), ensure all held tickers are present in `portfolio.positions` and bucket assignments are correct.')
    lines.append('2) Confirm `cost_usd` is updated correctly when you add/reduce positions (partial sells can change remaining cost basis depending on your method).')
    lines.append("3) If diff is close to a single position's cost, inspect that position's `shares` and `cost_usd` first.")
    lines.append('')
    return '\n'.join(lines)

def _diagnose_cash_mismatch_md(states: Dict[str, Any], tactical_cash_usd: Optional[float], broker_asof_et: Optional[str], verify_tolerance_usd: float, level: str='full', broker_asof_et_dt: Optional[datetime]=None, snapshot_kind: str='eod') -> str:
    lines: List[str] = []
    lines.append('## Diagnosis: Tactical cash mismatch (broker cash snapshot vs trades-derived ledger)')
    lines.append('')
    if tactical_cash_usd is None:
        lines.append('- No `tactical_cash_usd` snapshot provided; cannot diagnose this section.')
        lines.append('')
        return '\n'.join(lines)
    portfolio = states.get('portfolio') or {}
    cash = portfolio.get('cash') or {}
    trades = states.get('trades') or []
    seen = {}
    dup_ids = []
    for t in trades:
        tid = t.get('trade_id')
        if tid is None:
            continue
        if tid in seen:
            dup_ids.append(tid)
        else:
            seen[tid] = 1
    net_total, warns = _net_cash_change_from_trades(trades)
    asof_d = _parse_ymd_loose(broker_asof_et or '')
    net_asof = 0.0
    net_after = 0.0
    use_cutoff = snapshot_kind == 'intraday' and broker_asof_et_dt is not None
    by_date: Dict[str, float] = {}
    fee_sum = 0.0
    gross_sum = 0.0
    cash_amt_sum = 0.0
    missing_cash_amt = 0
    for t in trades:
        side = str(t.get('side') or '').strip().upper()
        td = _parse_ymd_loose(str(t.get('trade_date_et') or ''))
        amt = t.get('cash_amount')
        if amt is None:
            missing_cash_amt += 1
            continue
        try:
            amt_f = float(amt)
        except Exception:
            continue
        if side.startswith('B'):
            delta = -amt_f
        elif side.startswith('S'):
            delta = +amt_f
        else:
            continue
        cash_amt_sum += amt_f
        fee_sum += float(t.get('fee') or 0.0) if t.get('fee') is not None else 0.0
        gross_sum += float(t.get('gross') or 0.0) if t.get('gross') is not None else 0.0
        dkey = str(t.get('trade_date_et') or '')
        by_date[dkey] = by_date.get(dkey, 0.0) + delta
        if use_cutoff:
            et_dt = _trade_time_tw_to_et_dt(str(t.get('time_tw') or ''))
            if et_dt is not None:
                if et_dt <= broker_asof_et_dt:
                    net_asof += delta
                else:
                    net_after += delta
            elif asof_d and td and (td <= asof_d):
                net_asof += delta
        elif asof_d and td:
            if td <= asof_d:
                net_asof += delta
            else:
                net_after += delta
    baseline = _safe_float(cash.get('baseline_usd'))
    cash_existing = _safe_float(cash.get('usd')) or 0.0
    cash_from_baseline = baseline + net_total if baseline is not None else None
    diff = cash_from_baseline - float(tactical_cash_usd) if cash_from_baseline is not None else None
    status = 'OK' if diff is not None and abs(diff) <= verify_tolerance_usd else 'MISMATCH'
    lines.append(f"- As-of (ET): `{broker_asof_et or 'N/A'}`")
    lines.append(f'- Broker tactical cash snapshot: **{float(tactical_cash_usd):.2f}**')
    if baseline is not None:
        lines.append(f'- cash.baseline_usd: **{baseline:.2f}**')
    lines.append(f'- Net cash change from trades (all): **{net_total:.2f}**')
    if cash_from_baseline is not None:
        lines.append(f'- Derived cash (baseline + trades): **{cash_from_baseline:.2f}** | diff vs broker: **{diff:.2f}** (tol={verify_tolerance_usd:.2f}) => **{status}**')
    else:
        lines.append('- Cannot compute cash_from_baseline (baseline missing).')
    lines.append('')
    if use_cutoff:
        lines.append(f'- Snapshot kind: **intraday** | as-of ET datetime: `{broker_asof_et_dt.isoformat()}`')
        lines.append(f'- Net cash change up to as-of (intraday cutoff): **{net_asof:.2f}**')
        lines.append(f'- Net cash change after as-of: **{net_after:.2f}**')
        lines.append('  - If `after as-of` is non-zero, your broker snapshot likely happened before some trades in your ledger.')
        lines.append('')
    elif asof_d:
        lines.append(f'- Snapshot kind: **eod** | as-of ET date: `{broker_asof_et}`')
        lines.append(f'- Net cash change up to as-of `{broker_asof_et}`: **{net_asof:.2f}**')
        lines.append(f'- Net cash change after as-of: **{net_after:.2f}**')
        lines.append('  - If `after as-of` is non-zero, make sure your broker cash snapshot is taken after those trades settle / are reflected.')
        lines.append('')
    if warns and level == 'full':
        lines.append('### Trade parsing warnings')
        for w in warns[:20]:
            lines.append(f'- {w}')
        if len(warns) > 20:
            lines.append(f'- ... ({len(warns) - 20} more)')
        lines.append('')
    if dup_ids:
        lines.append(f"- Duplicate trade_id detected: `{', '.join(map(str, sorted(set(dup_ids))))}`")
        lines.append('  - Duplicate entries will distort the cash ledger. Remove duplicates or ensure unique trade_id.')
        lines.append('')
    if missing_cash_amt > 0:
        lines.append(f'- Trades missing `cash_amount`: **{missing_cash_amt}** (ignored in cash ledger)')
        lines.append('  - If those trades should affect cash, ensure `cash_amount` is present for them (gross+fee also acceptable, but you must define the rule).')
        lines.append('')
    if diff is not None:
        if abs(abs(diff) - abs(fee_sum)) <= max(verify_tolerance_usd, 0.5):
            lines.append(f'- Heuristic hit: |diff| ~= sum(fees) = {fee_sum:.2f}')
            lines.append('  - Possible bug: broker cash snapshot excludes fees or your `cash_amount` already includes fees but trades were recorded differently.')
            lines.append('')
    if level == 'full' and by_date:
        lines.append('### Cash ledger by trade_date_et (delta)')
        lines.append('| Trade Date (ET) | Net cash delta |')
        lines.append('| --- | ---: |')
        for dkey, delta in sorted(by_date.items(), key=lambda x: _parse_ymd_loose(x[0]) or date.min, reverse=True):
            lines.append(f'| {dkey} | {_fmt_usd(delta)} |')
        lines.append('')
    lines.append('### Next debugging steps (high-signal checks)')
    lines.append('1) Check trade date coverage vs broker snapshot time. If broker snapshot is before some trades, filter those trades when reconciling or pass the correct `--broker-asof-et`.')
    lines.append('2) Inspect whether `cash_amount` includes fee; the script assumes `cash_amount` is the true cash movement (gross+fee for BUY, gross-fee for SELL).')
    lines.append('3) Ensure there are no duplicated trades and trade_id is unique.')
    lines.append('4) If you simplified historical trades, verify you did not drop `cash_amount` for those rows (otherwise the ledger will be incomplete).')
    lines.append('')
    return '\n'.join(lines)

def _mode_required_operations_requested(args: argparse.Namespace) -> bool:
    return bool(str(args.tickers or '').strip() or bool(getattr(args, 'render_report', False)) or getattr(args, 'broker_investment_total_usd', None) is not None or (getattr(args, 'tactical_cash_usd', None) is not None))

def _standalone_update_allowed_without_mode(args: argparse.Namespace) -> bool:
    return bool(getattr(args, 'trades_xml', None) or getattr(args, 'cash_adjust_usd', None) is not None or getattr(args, 'cash_transfer_to_reserve_usd', None) is not None or (getattr(args, 'initial_investment_usd', None) is not None))

def _late_hydrate_new_position_tickers(states: Dict[str, Any], csv_dir: str, prices_now_from: str, keep_history_rows: int, already_processed: List[str]) -> List[ImportResult]:
    known = {str(t or '').upper() for t in already_processed if str(t or '').strip()}
    late_tickers = [t for t in _discover_tickers_from_config(states) if t not in known]
    if not late_tickers:
        return []
    print(f"[INFO] late CSV hydration for new tickers from trades/cash updates: {', '.join(late_tickers)}")
    return _import_csvs_into_states(states, csv_dir=csv_dir, tickers=late_tickers, prices_now_from=prices_now_from, keep_history_rows=keep_history_rows)

def _write_mismatch_diagnostics(states: Dict[str, Any], mismatches: List[Dict[str, Any]], broker_investment_total_usd: Optional[float], tactical_cash_usd: Optional[float], broker_asof_et: Optional[str], broker_asof_et_datetime: Optional[str], snapshot_kind: str, verify_tolerance_usd: float, diagnose_out: str, out_states_path: str, level: str='full') -> Optional[str]:
    if not mismatches:
        return None
    out_path = diagnose_out.strip()
    if not out_path:
        base = Path(out_states_path)
        tag = broker_asof_et or datetime.now(timezone.utc).strftime('%Y-%m-%d')
        out_path = str(base.with_name(f'reconcile_{tag}.md'))
    lines: List[str] = []
    lines.append('# Reconciliation / Debug Report')
    lines.append('')
    lines.append(f"- Generated at: `{datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')}`")
    lines.append(f"- As-of (ET): `{broker_asof_et or 'N/A'}`")
    lines.append(f'- Tolerance (USD): `{verify_tolerance_usd:.2f}`')
    lines.append('')
    lines.append('## Mismatches')
    lines.append('')
    lines.append('| Kind | Diff (USD) | Tolerance | As-of (ET) |')
    lines.append('| --- | ---: | ---: | --- |')
    for m in mismatches:
        lines.append(f"| {m.get('kind', '')} | {float(m.get('diff_usd') or 0.0):.2f} | {float(m.get('tolerance_usd') or verify_tolerance_usd):.2f} | {m.get('asof_et') or broker_asof_et or ''} |")
    lines.append('')
    kinds = {m.get('kind') for m in mismatches}
    if 'broker_investment_total_vs_holdings_cost' in kinds:
        lines.append(_diagnose_holdings_cost_basis_mismatch_md(states, broker_investment_total_usd, broker_asof_et, verify_tolerance_usd, level=level))
    elif 'broker_investment_total_vs_holdings_mv' in kinds:
        lines.append(_diagnose_holdings_mismatch_md(states, broker_investment_total_usd, broker_asof_et, verify_tolerance_usd, level=level))
    if 'broker_tactical_cash_vs_cash_from_baseline' in kinds:
        asof_dt = None
        if broker_asof_et_datetime:
            try:
                asof_dt = datetime.fromisoformat(str(broker_asof_et_datetime))
            except Exception:
                asof_dt = None
        lines.append(_diagnose_cash_mismatch_md(states, tactical_cash_usd, broker_asof_et, verify_tolerance_usd, level=level, broker_asof_et_dt=asof_dt, snapshot_kind=str(snapshot_kind or 'eod')))
    Path(out_path).write_text('\n'.join(lines), encoding='utf-8')
    return out_path

def _run_main(args: argparse.Namespace) -> int:
    states_path = args.states
    states = _load_json(states_path)
    trades_file = str(getattr(args, 'trades_file', '') or 'trades.json').strip() or 'trades.json'
    external_trades = _load_trades_payload(trades_file)
    if external_trades is not None:
        states['trades'] = external_trades
    market = states.setdefault('market', {})
    if isinstance(market.get('_runtime_history'), dict):
        market.pop('_runtime_history', None)
    _migrate_state_schema(states)
    _ensure_trading_calendar(states)
    _ensure_cash_buckets(states)
    mode_label = str(args.mode or '').strip()
    if not mode_label:
        if args.render_report:
            print('[ABORT] --render-report requires --mode.')
            raise SystemExit(2)
        if _mode_required_operations_requested(args):
            print('[ABORT] This command requires --mode.')
            print('[ABORT] Only XML import / cash adjustment / reserve transfer / initial investment update may run without --mode.')
            raise SystemExit(2)
        if not _standalone_update_allowed_without_mode(args):
            print('[ABORT] No actionable update requested. Provide --mode for a normal state/report refresh, or use XML/cash update arguments.')
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
    if mode_label:
        if str(args.broker_asof_et or '').strip() or str(args.broker_asof_et_time or '').strip() or str(args.broker_asof_et_datetime or '').strip():
            print('[WARN] --broker-asof-et / --broker-asof-et-time / --broker-asof-et-datetime are ignored when --mode is used; update_states resolves t / t+1 automatically.')
        resolved_ctx = _resolve_report_context(states, mode_label, now_et)
        print(f'[INFO] mode={resolved_ctx.mode_label} | session={resolved_ctx.session_class} | now_et={resolved_ctx.now_et_iso} | t={resolved_ctx.t_et} | t+1={resolved_ctx.t_plus_1_et} | report_date={resolved_ctx.report_date}')
        print(f'[INFO] {resolved_ctx.rationale}')
        if not resolved_ctx.reasonable:
            print(f'[ABORT] {resolved_ctx.warning}')
            print('[ABORT] No state update and no report file were generated.')
            raise SystemExit(2)
        _apply_report_context_meta(states, resolved_ctx)
    else:
        print('[INFO] running without --mode; only XML/cash/initial-investment updates will be applied.')
    tickers = [t.strip().upper() for t in args.tickers.split(',') if t.strip()] if args.tickers.strip() else _discover_tickers_from_config(states)
    keep_history_rows = args.keep_history_rows if args.keep_history_rows > 0 else _compute_keep_history_rows(states)
    out_path = args.out.strip()
    if not out_path:
        p = Path(states_path)
        out_path = str(p.with_name(f'{p.stem}.updated.json'))
    results = _import_csvs_into_states(states, csv_dir=args.csv_dir, tickers=tickers, prices_now_from=args.prices_now_from, keep_history_rows=keep_history_rows)
    processed_tickers = list(tickers)
    _append_meta_notes(states, results, keep_history_rows=keep_history_rows)
    _normalize_trades_inplace(states)
    if args.trades_xml:
        for xp in args.trades_xml:
            try:
                incoming = _import_trades_from_os_history_xml(xp)
                mode = (args.trades_import_mode or 'reconcile').strip().lower()
                if mode not in ('append', 'reconcile', 'replace'):
                    mode = 'reconcile'
                superseded_groups = set()
                if mode == 'replace':
                    _replace_trades_for_incoming_scope(states, incoming)
                elif mode == 'reconcile':
                    superseded_groups = _reconcile_manual_aggregate_trades_against_broker_import(states, incoming, abs_tol_usd=float(args.trade_reconcile_abs_tol_usd), rel_tol=float(args.trade_reconcile_rel_tol))
                existing_before = len(states.get('trades') or [])
                added, dup = _upsert_trades(states, incoming)
                existing_after = len(states.get('trades') or [])
                incoming_added_rows = (states.get('trades') or [])[max(0, existing_after - added):] if added > 0 else []
                if mode == 'reconcile' and superseded_groups:
                    portfolio_delta_rows = [t for t in incoming_added_rows if _group_key_trade(t) not in superseded_groups]
                else:
                    portfolio_delta_rows = incoming_added_rows
                if mode == 'replace':
                    _rebuild_portfolio_positions_from_day1(states)
                    portfolio_delta_desc = 'day1_rebuild'
                else:
                    _apply_incremental_trades_to_portfolio(states, portfolio_delta_rows)
                    portfolio_delta_desc = str(len(portfolio_delta_rows))
                    print(f'[PORTFOLIO] {mode}: incrementally applied {len(portfolio_delta_rows)} new trade(s) to portfolio.positions')
                print(f'[OK] trades xml import {Path(xp).name}: parsed={len(incoming)}, added={added}, dup={dup}, mode={mode}, portfolio_delta={portfolio_delta_desc}')
            except Exception as e:
                print(f'[ERR] trades xml import failed for {xp}: {e}')
    late_results = _late_hydrate_new_position_tickers(states, csv_dir=args.csv_dir, prices_now_from=args.prices_now_from, keep_history_rows=keep_history_rows, already_processed=processed_tickers)
    if late_results:
        results.extend(late_results)
        processed_tickers.extend([r.ticker for r in late_results])
        _append_meta_notes(states, late_results, keep_history_rows=keep_history_rows)
    _reprice_and_totals(states)
    broker_investment_total_usd = args.broker_investment_total_usd
    broker_investment_total_supplied = broker_investment_total_usd is not None
    tactical_cash_usd = args.tactical_cash_usd
    if resolved_ctx is not None:
        broker_asof_et = resolved_ctx.broker_asof_et
        broker_asof_et_dt = None
        snapshot_kind = resolved_ctx.snapshot_kind
        broker_asof_et_datetime = resolved_ctx.broker_asof_et_datetime or None
        _sync_broker_snapshot_meta(states, mode=mode_label, broker_asof_et=broker_asof_et if broker_asof_et else None, broker_asof_et_datetime=broker_asof_et_datetime, snapshot_kind=snapshot_kind)
    else:
        broker_asof_et, broker_asof_et_dt, snapshot_kind = _parse_broker_asof(states, str(args.broker_asof_et or ''), str(args.broker_asof_et_time or ''), str(args.broker_asof_et_datetime or ''), mode='')
        broker_asof_et_datetime = str(args.broker_asof_et_datetime or '').strip() or None
    if args.initial_investment_usd is not None:
        _set_initial_investment_usd(states, float(args.initial_investment_usd))
    if args.cash_adjust_usd is not None:
        _apply_cash_adjustment(states, amount_usd=float(args.cash_adjust_usd), note=str(args.cash_adjust_note or ''), asof_et=broker_asof_et if broker_asof_et else None)
    if broker_investment_total_supplied:
        _verify_holdings_with_broker_investment_total(states, broker_investment_total_usd=broker_investment_total_usd, broker_asof_et=broker_asof_et if broker_asof_et else None, broker_investment_total_kind=str(args.broker_investment_total_kind), verify_tolerance_usd=float(args.verify_tolerance_usd))
    else:
        _clear_holdings_reconciliation_snapshot(states)
        print('[INFO] holdings reconciliation skipped: no --broker-investment-total-usd supplied.')
    _update_tactical_cash_from_trades_and_snapshot(states, tactical_cash_usd=tactical_cash_usd, broker_asof_et=broker_asof_et if broker_asof_et else None, verify_tolerance_usd=float(args.verify_tolerance_usd), cutoff_et_dt=broker_asof_et_dt if snapshot_kind == 'intraday' else None, snapshot_kind=snapshot_kind)
    if args.cash_transfer_to_reserve_usd is not None:
        try:
            _apply_cash_transfer_to_reserve(states, amount_usd=float(args.cash_transfer_to_reserve_usd), asof_et=broker_asof_et if broker_asof_et else None)
        except Exception as e:
            print(f'[ABORT] invalid --cash-transfer-to-reserve-usd: {e}')
            print('[ABORT] No state update and no report file were generated.')
            raise SystemExit(2)
    _reprice_and_totals(states)
    if mode_label:
        _update_signals_and_thresholds(states, args.derive_signals_inputs, args.derive_threshold_inputs, mode=mode_label)
    else:
        print('[INFO] signal/threshold refresh skipped because --mode was not supplied.')
    _update_portfolio_performance(states)
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
        if args.diagnose_mismatch != 'none':
            try:
                diag_path = _write_mismatch_diagnostics(states, mismatches=mismatches, broker_investment_total_usd=broker_investment_total_usd, tactical_cash_usd=tactical_cash_usd, broker_asof_et=broker_asof_et if broker_asof_et else None, broker_asof_et_datetime=broker_asof_et_datetime, snapshot_kind=snapshot_kind, verify_tolerance_usd=float(args.verify_tolerance_usd), diagnose_out=args.diagnose_out, out_states_path=out_path, level=args.diagnose_mismatch)
                if diag_path:
                    print(f'[DIAG] wrote reconciliation report: {diag_path}')
            except Exception as e:
                print(f'[DIAG][ERROR] failed to write reconciliation report: {e}')
        if args.mismatch_policy == 'abort':
            print('[MISMATCH] Broker verification failed beyond tolerance. Output will NOT be written.')
            for m in mismatches:
                print('  -', m)
            print('[HINT] Re-run with --mismatch-policy warn (keep going) or --mismatch-policy force (force override).')
            raise SystemExit(2)
        elif args.mismatch_policy == 'warn':
            print('[MISMATCH] Broker verification failed beyond tolerance, but continuing (warn policy).')
            for m in mismatches:
                print('  -', m)
        else:
            print('[MISMATCH] Broker verification failed beyond tolerance, but continuing (FORCE override).')
            for m in mismatches:
                print('  -', m)
            meta = states.setdefault('meta', {})
            meta.setdefault('force_overrides', []).append({'ts': datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z'), 'policy': 'force', 'mismatches': mismatches})
    _prune_meta_notes_last_days(states, keep_days=3)
    if mode_label:
        warns = _ensure_report_fields(states)
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
        'market_value_usd',
        'unrealized_pnl_usd',
        'unrealized_pnl_pct',
    }, ndigits=4)
    report_md = None
    report_out_path = None
    if args.render_report:
        report_md, report_out_path = _build_report_output(states, schema_path=str(args.report_schema), report_dir=str(args.report_dir), report_out=str(args.report_out), mode=mode_label)
    trades_to_save: List[Dict[str, Any]] = []
    for t in (states.get('trades') or []):
        if isinstance(t, dict):
            trades_to_save.append(_compact_trade_row(t))
    trades_written = _save_trades_payload(trades_to_save, trades_file)
    states['trades'] = []
    states.setdefault('meta', {})['trades_file'] = os.path.basename(trades_file)
    states['meta']['trades_count'] = len(trades_to_save)
    market = states.setdefault('market', {})
    market.pop('history_400d', None)
    market.pop('_runtime_history', None)
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

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--states', default='states.json', help='Input states.json path')
    ap.add_argument('--trades-file', default='trades.json', help='External trades JSON path (default: trades.json).')
    ap.add_argument('--csv-dir', default='./data', help='Directory containing CSV files (default: ./data)')
    ap.add_argument('--tickers', default='', help='Comma-separated tickers (optional override)')
    ap.add_argument('--out', default='', help='Output path (default: <states_stem>.updated.json)')
    ap.add_argument('--keep-history-rows', type=int, default=0, help='Override history rows to keep (0 = auto: max(window)+10)')
    ap.add_argument('--derive-signals-inputs', choices=['missing', 'force', 'never'], default='missing')
    ap.add_argument('--derive-threshold-inputs', choices=['missing', 'force', 'never'], default='missing')
    ap.add_argument('--prices-now-from', choices=['close', 'never'], default='close')
    ap.add_argument('--sync-meta', choices=['auto', 'never'], default='auto', help='Control whether mode metadata is synchronized into by_mode snapshots.')
    ap.add_argument('--mode', default='', help='Mode for report-scoped updates (e.g., Premarket / Intraday / AfterClose). Optional for XML/cash-only updates.')
    ap.add_argument('--now-et', default='', help='Override current ET datetime for report-context resolution, format YYYY-MM-DDTHH:MM[:SS].')
    ap.add_argument('--render-report', action='store_true', help='Render the markdown report in-process after state updates complete successfully.')
    ap.add_argument('--report-schema', default='report_spec.json', help='Schema path used when --render-report is enabled.')
    ap.add_argument('--report-dir', default='report', help='Output directory for generated report when --render-report is enabled and --report-out is not set.')
    ap.add_argument('--report-out', default='', help='Explicit output path for generated report when --render-report is enabled.')
    ap.add_argument('--log-file', default='', help='Write a detailed run log here. Default: logs/update_states_<timestamp>_<pid>.log')
    ap.add_argument('--broker-investment-total-usd', type=float, default=None, help='Broker investment total excluding cash, in USD. Holdings reconciliation runs only when this argument is explicitly provided.')
    ap.add_argument('--broker-investment-total-kind', choices=['market_value', 'cost_basis'], default='cost_basis', help='How to interpret broker investment total: cost_basis=holdings cost excluding cash [default], market_value=holdings market value excluding cash.')
    ap.add_argument('--tactical-cash-usd', type=float, default=None, help='Tactical cash balance excluding holdings, in USD. Used to reconcile or derive the cash baseline from trades.')
    ap.add_argument('--initial-investment-usd', type=float, default=None, help='Persistent initial investment amount in USD. This is written into states.json and retained for current_total_assets / profit / profit_rate calculations.')
    ap.add_argument('--cash-adjust-usd', type=float, default=None, help='External cash adjustment in USD. Positive values add capital / deposits; negative values represent withdrawals / capital outflows. The amount accumulates into the tactical cash baseline.')
    ap.add_argument('--cash-adjust-note', default='', help='Optional note for the external cash adjustment.')
    ap.add_argument('--cash-transfer-to-reserve-usd', type=float, default=None, help='Internal cash bucket transfer: positive values move cash from deployable cash into reserve cash; negative values move reserve cash back into deployable cash. The run aborts without writing output when the requested amount exceeds the available balance.')
    ap.add_argument('--broker-asof-et', default='', help='Broker semantic as-of trade day t (ET), e.g. 2026-03-03. Ignored when --mode is used.')
    ap.add_argument('--broker-asof-et-time', default='', help='Opaque ET time HH:MM[:SS] for your broker/live-price record. Ignored when --mode is used.')
    ap.add_argument('--broker-asof-et-datetime', default='', help="Opaque ET datetime 'YYYY-MM-DDTHH:MM[:SS]' for your live-price record. Ignored when --mode is used.")
    ap.add_argument('--trades-xml', action='append', default=[], help='OSHistoryDealAll.xml path to import trades (repeatable).')
    ap.add_argument('--trades-import-mode', choices=['append', 'reconcile', 'replace'], default='reconcile', help='How to merge imported broker XML trades into existing states.trades. append=only add non-duplicates; reconcile=also supersede matching manual aggregates (recommended); replace=remove/archive existing trades for the same (trade_date_et,ticker) scope before inserting broker trades.')
    ap.add_argument('--trade-reconcile-abs-tol-usd', type=float, default=1.0, help='Absolute USD tolerance when reconciling manual aggregate trades against broker fills.')
    ap.add_argument('--trade-reconcile-rel-tol', type=float, default=0.003, help='Relative tolerance (fraction of broker cash) when reconciling manual aggregate trades.')
    ap.add_argument('--verify-tolerance-usd', type=float, default=1.0, help='Tolerance (USD) for broker verification (holdings mv and cash).')
    ap.add_argument('--mismatch-policy', choices=['abort', 'warn', 'force'], default='warn', help='What to do if broker verification mismatches beyond tolerance. abort=exit non-zero and do NOT write output; warn=write output but keep MISMATCH flags; force=write output and record a force-override note in meta.')
    ap.add_argument('--diagnose-mismatch', choices=['none', 'summary', 'full'], default='full', help='If broker verification mismatches, generate a reconciliation report to help locate the root cause. Only runs when mismatch occurs.')
    ap.add_argument('--diagnose-out', default='', help='Path for reconciliation report (default: reconcile_<asof_et>.md next to the output states file).')
    args = ap.parse_args()
    log_target = str(args.log_file or '').strip() or _default_log_path('update_states')
    log_path, fh, old_stdout, old_stderr = _enable_log_tee(log_target)
    try:
        _log_run_header('update_states.py', args, log_path)
        exit_code = int(_run_main(args) or 0)
        print(f'[EXIT] code={exit_code}')
    except SystemExit as e:
        try:
            exit_code = int(e.code) if e.code is not None else 0
        except Exception:
            exit_code = 1
        print(f'[EXIT] code={exit_code}')
        raise
    except Exception:
        exit_code = 1
        print('[EXCEPTION] uncaught exception follows')
        traceback.print_exc()
        print(f'[EXIT] code={exit_code}')
        raise
    finally:
        print(f'[LOG] complete file={log_path}')
        _disable_log_tee(fh, old_stdout, old_stderr)
if __name__ == '__main__':
    main()
