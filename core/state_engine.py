from __future__ import annotations
import argparse
import csv
import os
import re
from zoneinfo import ZoneInfo
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from core.models import ImportResult
from core.report_bundle import build_report_root, ensure_report_root_fields
from core.report_context import (
    _ensure_trading_calendar,
    _next_trading_day_et_from_states,
    _parse_broker_asof,
    _prev_trading_day_et_from_states,
    _report_date_from_meta,
    _report_meta_from_context,
    _report_meta_from_mode_dates,
    _report_meta_from_report_date,
    _resolve_report_context,
    _resolve_runtime_report_meta,
)
from core.report_output import (
    _build_report_json_output_path,
    _build_report_output,
    _build_report_output_path,
    _render_report_output,
)
from core.report_meta import _effective_report_meta, _migrate_state_schema, _normalize_mode_key
from core.reconciliation import (
    _find_trade_conflicts,
    _first_token_ticker,
    _normalize_trades_inplace,
    _num_from_cell,
    _trade_buy_total_cost_usd,
    _upsert_trades,
    _verify_holdings_with_broker_investment_total,
)
from core.runtime_io import (
    _compact_persistent_states,
    _compact_trade_row,
    _load_json,
    _load_runtime_config,
    _load_trades_payload,
    _market_history_rows_map,
    _round_selected_numeric_fields,
    _runtime_config,
    _runtime_data_config,
    _runtime_history,
    _runtime_numeric_precision,
    _runtime_report_meta,
    _runtime_signal_basis_day,
    _save_json,
    _save_trades_payload,
    _strip_persisted_report_transients,
)
from core.strategy import (
    _dedupe_by_date_keep_last,
    _fmt_usd,
    _normalize_ma_rule,
    _parse_indicator_window,
    _read_ohlcv_csv,
)
from core.tactical_engine import apply_tactical_plan, compute_tactical_plan
from core.trade_imports import (
    _iter_imported_trade_batches,
    _normalize_trade_date_bounds,
    _replace_trades,
    _trade_is_within_trade_date_bounds,
)
from utils.config_access import (
    config_buckets,
    config_csv_sources,
    config_fx_pairs,
    config_tactical_indicators,
    config_trades_file,
    config_trading_calendar,
    discover_state_engine_tickers,
)
from utils.dates import (
    ET_TZ,
    _normalize_time_tw,
    _normalize_trade_date_et,
    _parse_ymd_loose,
    _to_yyyy_mm_dd,
    _trade_time_tw_to_et_dt,
)
from utils.parsers import _safe_float, _safe_int
from utils.precision import format_fixed, round_with_precision, state_engine_numeric_precision
from utils.trading_calendar import is_weekend_et as _is_weekend_et

def _positions_need_trade_hydration(states: Dict[str, Any]) -> bool:
    positions = (((states.get('portfolio') or {}).get('positions')) or [])
    if not isinstance(positions, list):
        return True
    if not positions:
        return True
    for pos in positions:
        if not isinstance(pos, dict):
            return True
        if pos.get('cost_usd') is None:
            return True
        if not str(pos.get('bucket') or '').strip():
            return True
    return False


def _hydrate_positions_from_trade_ledger_if_needed(states: Dict[str, Any], runtime: Dict[str, Any], trades: List[Dict[str, Any]]) -> None:
    if not isinstance(trades, list) or not trades:
        return
    if not _positions_need_trade_hydration(states):
        return
    _rebuild_portfolio_positions_from_day1_fifo(states, runtime, trades)

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
            _, selected_close = _selected_market_close_for_runtime(runtime, str(ticker), history[ticker].get('rows') or [])
            if selected_close is not None:
                p['price_now'] = float(selected_close)
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
                    _, market_px = _selected_market_close_for_runtime(runtime, ticker, (history.get(ticker) or {}).get('rows') or [])
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
        _, selected_close = _selected_market_close_for_runtime(runtime, ticker, rows)
        if selected_close is not None:
            try:
                return float(selected_close)
            except Exception:
                pass
    for p in positions:
        if str(p.get('ticker') or '').upper() == ticker:
            try:
                return float(p.get('price_now')) if p.get('price_now') is not None else None
            except Exception:
                return None
    return None

def _discover_tickers_from_config(states: Dict[str, Any], runtime: Dict[str, Any]) -> List[str]:
    tickers = discover_state_engine_tickers(_runtime_config(runtime))
    for p in (states.get('portfolio', {}) or {}).get('positions', []) or []:
        t = p.get('ticker')
        if t:
            tickers.append(str(t))
    seen = set()
    tickers = [t for t in tickers if t and (not (t.upper() in seen or seen.add(t.upper())))]
    return [t.upper() for t in tickers]

def _fx_tickers_from_config(runtime: Dict[str, Any]) -> set[str]:
    tickers: set[str] = set()
    for fx_cfg in config_fx_pairs(_runtime_config(runtime)).values():
        if not isinstance(fx_cfg, dict):
            continue
        fx_ticker = str(fx_cfg.get('ticker') or '').upper().strip()
        if fx_ticker:
            tickers.add(fx_ticker)
    return tickers

def _history_rows_on_or_before(rows: List[Dict[str, Any]], asof_et: Optional[str]) -> List[Dict[str, Any]]:
    if not asof_et:
        return list(rows or [])
    try:
        asof_d = _parse_ymd_loose(asof_et)
    except Exception:
        asof_d = None
    if asof_d is None:
        return list(rows or [])
    kept: List[Dict[str, Any]] = []
    for row in rows or []:
        row_date = _parse_ymd_loose(str((row or {}).get('Date') or ''))
        if row_date is None:
            continue
        if row_date <= asof_d:
            kept.append(row)
        else:
            break
    return kept

def _selected_market_close_for_runtime(runtime: Dict[str, Any], ticker: str, rows: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[float]]:
    if not rows:
        return (None, None)
    ticker_norm = str(ticker or '').upper().strip()
    if ticker_norm in _fx_tickers_from_config(runtime):
        row = rows[-1]
        return (str(row.get('Date') or ''), _safe_float(row.get('Close')))
    signal_day = _runtime_signal_basis_day(runtime)
    if signal_day:
        filtered = _history_rows_on_or_before(rows, signal_day)
        if filtered:
            row = filtered[-1]
            return (str(row.get('Date') or ''), _safe_float(row.get('Close')))
    row = rows[-1]
    return (str(row.get('Date') or ''), _safe_float(row.get('Close')))

def _rebuild_market_snapshot_from_history(states: Dict[str, Any], runtime: Dict[str, Any], tickers: Optional[List[str]]=None) -> None:
    market = states.setdefault('market', {})
    history = _runtime_history(runtime)
    keep_tickers = tickers if isinstance(tickers, list) and tickers else _discover_tickers_from_config(states, runtime)
    new_prices_now: Dict[str, Optional[float]] = {}
    imported_dates: List[str] = []
    for ticker in keep_tickers:
        ticker_norm = str(ticker or '').upper().strip()
        if not ticker_norm:
            continue
        new_prices_now.setdefault(ticker_norm, None)
        rows = ((history.get(ticker_norm) or {}).get('rows') or [])
        if not rows:
            continue
        selected_date, selected_close = _selected_market_close_for_runtime(runtime, ticker_norm, rows)
        try:
            if selected_close is None:
                continue
            new_prices_now[ticker_norm] = float(selected_close)
        except Exception:
            continue
        if selected_date:
            imported_dates.append(selected_date)
    old_prices_now = market.get('prices_now') or {}
    removed = []
    if isinstance(old_prices_now, dict):
        removed = sorted([
            str(ticker)
            for ticker in old_prices_now.keys()
            if str(ticker or '').upper().strip() not in set(new_prices_now.keys())
        ])
    market['prices_now'] = new_prices_now
    if imported_dates:
        market['asof_t_et'] = max(imported_dates)
    else:
        market.pop('asof_t_et', None)
    if removed:
        print(f"[MARKET] rebuilt market snapshot and removed {len(removed)} stale ticker(s): {', '.join(removed)}")

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
    for ma_rule in config_tactical_indicators(cfg).values():
        w = _parse_indicator_window(ma_rule)
        if w:
            windows.append(w)
    max_w = max(windows) if windows else 100
    return int(max_w) + 10

def _resolve_csv_candidates(runtime: Dict[str, Any], csv_dir: str, ticker: str) -> List[str]:
    cfg = _runtime_config(runtime)
    csv_sources = config_csv_sources(cfg)
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

def _csv_date_bounds(csv_path: str) -> Tuple[Optional[date], Optional[date]]:
    p = Path(str(csv_path or '').strip())
    if not p.exists():
        return (None, None)
    first: Optional[date] = None
    last: Optional[date] = None
    try:
        with p.open('r', encoding='utf-8', newline='') as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                raw = str((row or {}).get('Date') or '').strip()
                if not raw:
                    continue
                try:
                    day = _parse_ymd_loose(raw)
                except Exception:
                    continue
                if first is None:
                    first = day
                last = day
    except Exception:
        return (None, None)
    return (first, last)

def _autocsv_target_end_for_ticker(runtime: Dict[str, Any], ticker: str, now_et: datetime, mode_label: str) -> date:
    _ = runtime
    _ = ticker
    _ = mode_label
    return now_et.date()

def _refresh_csv_history_for_mode_updates(
    states: Dict[str, Any],
    runtime: Dict[str, Any],
    *,
    csv_dir: str,
    tickers: List[str],
    now_et: datetime,
    mode_label: str,
    allow_incomplete_rows: bool = False,
) -> List[str]:
    if not str(mode_label or '').strip():
        print('[AUTOCSV] skipped: no --mode supplied.')
        return []
    active_tickers: List[str] = []
    seen = set()
    for ticker in tickers or []:
        ticker_norm = str(ticker or '').upper().strip()
        if not ticker_norm or ticker_norm in seen:
            continue
        active_tickers.append(ticker_norm)
        seen.add(ticker_norm)
    refresh_specs: List[Tuple[str, Path, date, date, Optional[date], Optional[date]]] = []
    for ticker in active_tickers:
        target_end = _autocsv_target_end_for_ticker(runtime, ticker, now_et, mode_label)
        default_start = target_end - timedelta(days=370)
        candidates = _resolve_csv_candidates(runtime, csv_dir, ticker)
        existing_path = next((candidate for candidate in candidates if os.path.exists(candidate)), '')
        chosen_path = existing_path or (candidates[0] if candidates else os.path.join(csv_dir, f'{ticker}.csv'))
        first_date, last_date = _csv_date_bounds(chosen_path)
        start_date = first_date or default_start
        refresh_specs.append((ticker, Path(chosen_path), start_date, target_end, first_date, last_date))
    if not refresh_specs:
        print('[AUTOCSV] skipped: no active tickers resolved for refresh.')
        return []
    from download_1y import download_history, yf
    if yf is None:
        msg = '[AUTOCSV] yfinance is not installed; automatic CSV refresh skipped.'
        print(f'[WARN] {msg}')
        return []
    refreshed: List[str] = []
    csv_root = Path(csv_dir)
    csv_root.mkdir(parents=True, exist_ok=True)
    for ticker, output_path, start_date, target_end, first_date, last_date in refresh_specs:
        before = last_date.isoformat() if last_date is not None else 'missing'
        try:
            download_history(
                ticker,
                start_date,
                target_end + timedelta(days=1),
                csv_root,
                output_path=output_path,
                allow_incomplete_rows=allow_incomplete_rows,
            )
            refreshed.append(ticker)
            print(f'[AUTOCSV] refreshed {ticker}: previous_last={before}, target_end={target_end.isoformat()}, start={start_date.isoformat()}, path={output_path}')
        except Exception as exc:
            msg = f'[ERR] [AUTOCSV] {ticker}: refresh failed: {exc}'
            if '--allow-incomplete-csv-rows' in str(exc):
                print(msg)
                raise RuntimeError(msg) from exc
            print(f'[WARN] {msg}')
    return refreshed

def _import_csvs_into_states(
    states: Dict[str, Any],
    runtime: Dict[str, Any],
    csv_dir: str,
    tickers: List[str],
    prices_now_from: str,
    keep_history_rows: int,
    persist_market_snapshot: bool = True,
    *,
    allow_incomplete_rows: bool = False,
    bypass_option_hint: str = "--allow-incomplete-csv-rows",
) -> List[ImportResult]:
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
            rows = _read_ohlcv_csv(
                csv_path,
                keep_last_n=keep_history_rows,
                allow_incomplete_rows=allow_incomplete_rows,
                bypass_option_hint=bypass_option_hint,
            )
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
            if '--allow-incomplete-csv-rows' in str(e):
                raise RuntimeError(msg) from e
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

def _sort_key_trade_for_portfolio(t: Dict[str, Any]) -> tuple:
    return (_normalize_trade_date_et(str(t.get('trade_date_et') or '')), _normalize_time_tw(str(t.get('time_tw') or '')), int(t.get('trade_id') or 0))

def _position_bucket_default(states: Dict[str, Any], runtime: Dict[str, Any], ticker: str) -> str:
    ticker = str(ticker or '').upper().strip()
    cfg = _runtime_config(runtime)
    buckets_cfg = config_buckets(cfg)
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

def _mode_required_operations_requested(args: argparse.Namespace) -> bool:
    return bool(str(args.tickers or '').strip() or bool(getattr(args, 'render_report', False)) or getattr(args, 'broker_investment_total_usd', None) is not None or (getattr(args, 'tactical_cash_usd', None) is not None))

def _standalone_update_allowed_without_mode(args: argparse.Namespace) -> bool:
    return bool(getattr(args, 'imported_trade_batches', None) or getattr(args, 'imported_trades_json', None) or getattr(args, 'cash_adjust_usd', None) is not None or getattr(args, 'cash_transfer_to_reserve_usd', None) is not None or getattr(args, 'initial_investment_usd', None) is not None)


def _has_persistent_state_updates_requested(args: argparse.Namespace) -> bool:
    return bool(
        getattr(args, 'imported_trades_json', None)
        or getattr(args, 'cash_adjust_usd', None) is not None
        or getattr(args, 'cash_transfer_to_reserve_usd', None) is not None
        or getattr(args, 'initial_investment_usd', None) is not None
        or getattr(args, 'broker_investment_total_usd', None) is not None
        or getattr(args, 'tactical_cash_usd', None) is not None
    )

def _late_hydrate_new_position_tickers(
    states: Dict[str, Any],
    runtime: Dict[str, Any],
    csv_dir: str,
    prices_now_from: str,
    keep_history_rows: int,
    already_processed: List[str],
    *,
    allow_incomplete_rows: bool = False,
    bypass_option_hint: str = "--allow-incomplete-csv-rows",
) -> List[ImportResult]:
    known = {str(t or '').upper() for t in already_processed if str(t or '').strip()}
    late_tickers = [t for t in _discover_tickers_from_config(states, runtime) if t not in known]
    if not late_tickers:
        return []
    print(f"[INFO] late CSV hydration for new tickers from trades/cash updates: {', '.join(late_tickers)}")
    return _import_csvs_into_states(
        states,
        runtime,
        csv_dir=csv_dir,
        tickers=late_tickers,
        prices_now_from=prices_now_from,
        keep_history_rows=keep_history_rows,
        allow_incomplete_rows=allow_incomplete_rows,
        bypass_option_hint=bypass_option_hint,
    )

def _run_main(args: argparse.Namespace) -> int:
    states_path = args.states
    states = _load_json(states_path)
    config_path = str(getattr(args, 'config', '') or '').strip() or str(Path(states_path).resolve().parent / 'config.json')
    runtime: Dict[str, Any] = {'config': _load_runtime_config(config_path), 'history': {}}
    numeric_precision = state_engine_numeric_precision(_runtime_config(runtime))
    trades_file = str(getattr(args, 'trades_file', '') or config_trades_file(_runtime_config(runtime)) or 'trades.json').strip() or 'trades.json'
    external_trades = _load_trades_payload(trades_file)
    trades: List[Dict[str, Any]] = external_trades if isinstance(external_trades, list) else []
    _migrate_state_schema(states)
    _ensure_trading_calendar(runtime)
    _ensure_cash_buckets(states, usd_amount_ndigits=int(numeric_precision["usd_amount"]))
    _hydrate_positions_from_trade_ledger_if_needed(states, runtime, trades)
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
            now_et = now_et.replace(tzinfo=ZoneInfo(ET_TZ))
        else:
            now_et = now_et.astimezone(ZoneInfo(ET_TZ))
    else:
        now_et = datetime.now(ZoneInfo(ET_TZ))
    resolved_ctx = None
    report_meta: Optional[Dict[str, Any]] = None
    force_mode = bool(getattr(args, 'force_mode', False))
    if mode_label:
        if str(args.broker_asof_et or '').strip() or str(args.broker_asof_et_time or '').strip() or str(args.broker_asof_et_datetime or '').strip():
            print('[WARN] --broker-asof-et / --broker-asof-et-time / --broker-asof-et-datetime are ignored when --mode is used; update_states resolves t / t+1 automatically.')
        resolved_ctx = _resolve_report_context(states, runtime, mode_label, now_et)
        print(f'[INFO] mode={resolved_ctx.mode_label} | session={resolved_ctx.session_class} | now_et={resolved_ctx.now_et_iso} | t={resolved_ctx.t_et} | t+1={resolved_ctx.t_plus_1_et} | report_date={resolved_ctx.report_date}')
        print(f'[INFO] {resolved_ctx.rationale}')
        if not resolved_ctx.reasonable:
            if force_mode:
                print(f'[WARN] forcing mode={resolved_ctx.mode_label} via -f/--force-mode despite ET/session mismatch: {resolved_ctx.warning}')
            else:
                print(f'[ABORT] {resolved_ctx.warning}')
                print(f'[ABORT] Re-run with -f / --force-mode to bypass the ET/session check for mode={resolved_ctx.mode_label}.')
                print('[ABORT] No state update and no report file were generated.')
                raise SystemExit(2)
        report_meta = _report_meta_from_context(resolved_ctx)
        runtime['report_meta'] = dict(report_meta)
    else:
        print('[INFO] running without --mode; only imported-trades/cash/initial-investment updates will be applied.')
    tickers = [t.strip().upper() for t in args.tickers.split(',') if t.strip()] if args.tickers.strip() else _discover_tickers_from_config(states, runtime)
    _refresh_csv_history_for_mode_updates(
        states,
        runtime,
        csv_dir=args.csv_dir,
        tickers=tickers,
        now_et=now_et,
        mode_label=mode_label,
        allow_incomplete_rows=bool(getattr(args, 'allow_incomplete_csv_rows', False)),
    )
    keep_history_rows = args.keep_history_rows if args.keep_history_rows > 0 else _compute_keep_history_rows(states, runtime)
    out_path = args.out.strip()
    if not out_path:
        p = Path(states_path)
        out_path = str(p.with_name(f'{p.stem}.updated.json'))
    results = _import_csvs_into_states(
        states,
        runtime,
        csv_dir=args.csv_dir,
        tickers=tickers,
        prices_now_from=args.prices_now_from,
        keep_history_rows=keep_history_rows,
        allow_incomplete_rows=bool(getattr(args, 'allow_incomplete_csv_rows', False)),
    )
    processed_tickers = list(tickers)
    _normalize_trades_inplace(trades, cash_amount_ndigits=int(numeric_precision["trade_cash_amount"]))
    trade_date_from, trade_date_to = _normalize_trade_date_bounds(
        str(getattr(args, 'trade_date_from', '') or '').strip(),
        str(getattr(args, 'trade_date_to', '') or '').strip(),
    )
    imported_trade_batches = _iter_imported_trade_batches(
        args,
        cash_amount_ndigits=int(numeric_precision["trade_cash_amount"]),
    )
    if imported_trade_batches:
        for import_label, import_path, incoming in imported_trade_batches:
            try:
                if trade_date_from or trade_date_to:
                    incoming = [
                        trade for trade in incoming
                        if isinstance(trade, dict) and _trade_is_within_trade_date_bounds(trade, trade_date_from, trade_date_to)
                    ]
                if trade_date_from or trade_date_to:
                    start = trade_date_from or 'min'
                    end = trade_date_to or 'max'
                    print(f'[FILTER] trades import {import_label}: trade_date_et range {start}..{end} | kept={len(incoming)}')
                mode = (args.trades_import_mode or 'append').strip().lower()
                if mode not in ('append', 'replace'):
                    mode = 'append'
                removed_trade_count = 0
                if mode == 'append':
                    conflicts = _find_trade_conflicts(
                        trades,
                        incoming,
                        cash_amount_ndigits=int(numeric_precision["trade_cash_amount"]),
                        trade_dedupe_amount_ndigits=int(numeric_precision["trade_dedupe_amount"]),
                    )
                    if conflicts:
                        detail = "; ".join(conflicts[:5])
                        if len(conflicts) > 5:
                            detail += f"; ... ({len(conflicts)} conflicts total)"
                        raise ValueError(
                            f"append import conflict detected for {import_label}: {detail}"
                        )
                if mode == 'replace':
                    trades, removed_trade_count = _replace_trades(trades, trade_date_from, trade_date_to)
                added, dup = _upsert_trades(
                    trades,
                    incoming,
                    cash_amount_ndigits=int(numeric_precision["trade_cash_amount"]),
                    trade_dedupe_amount_ndigits=int(numeric_precision["trade_dedupe_amount"]),
                )
                if added > 0 or removed_trade_count > 0:
                    _rebuild_portfolio_positions_from_day1_fifo(states, runtime, trades)
                    portfolio_delta_desc = 'day1_rebuild'
                else:
                    portfolio_delta_desc = '0'
                    print(f'[PORTFOLIO] {mode}: no trade ledger changes; skipped portfolio rebuild.')
                print(f'[OK] trades import {import_label}: parsed={len(incoming)}, added={added}, dup={dup}, mode={mode}, portfolio_delta={portfolio_delta_desc}')
            except Exception as e:
                print(f'[ERR] trades import failed for {import_path}: {e}')
                print('[ABORT] No state update and no report file were generated.')
                raise SystemExit(2)
    late_results = _late_hydrate_new_position_tickers(
        states,
        runtime,
        csv_dir=args.csv_dir,
        prices_now_from=args.prices_now_from,
        keep_history_rows=keep_history_rows,
        already_processed=processed_tickers,
        allow_incomplete_rows=bool(getattr(args, 'allow_incomplete_csv_rows', False)),
    )
    if late_results:
        results.extend(late_results)
        processed_tickers.extend([r.ticker for r in late_results])
    _rebuild_market_snapshot_from_history(states, runtime)
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
    report_root = build_report_root(
        states,
        config=_runtime_config(runtime),
        trades=trades,
        tactical_plan=tactical_plan,
        report_meta=report_meta,
        market_history=_runtime_history(runtime),
    )
    if mode_label:
        warns = ensure_report_root_fields(report_root)
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
    report_json_written = None
    if mode_label:
        report_json_path = _build_report_json_output_path(
            report_dir=str(args.report_dir),
            report_json_out=str(getattr(args, 'report_json_out', '') or ''),
            mode=mode_label,
            report_meta=report_meta,
            states=report_root,
        )
        report_json_written = _save_json(report_root, report_json_path)
        print(f'[OK] wrote {report_json_written}')
    if args.render_report:
        render_source = report_root
        if report_json_written:
            render_source = _load_json(report_json_written)
        report_md, report_out_path = _render_report_output(
            render_source,
            schema_path=str(args.report_schema),
            report_dir=str(args.report_dir),
            report_out=str(args.report_out),
            mode=mode_label,
            report_meta=report_meta,
        )
    trades_to_save: List[Dict[str, Any]] = []
    for t in trades:
        if isinstance(t, dict):
            trades_to_save.append(_compact_trade_row(t))
    trades_written = _save_trades_payload(trades_to_save, trades_file)
    persisted_states = _compact_persistent_states(states)
    explicit_out_requested = bool(str(args.out or '').strip())
    write_primary_state = True
    if mode_label and (not _has_persistent_state_updates_requested(args)) and (not explicit_out_requested):
        write_primary_state = False
        print(f'[INFO] skipped writing primary states file {states_path}: mode-only run writes report snapshots only.')
    out_written = out_path
    if write_primary_state:
        out_written = _save_json(persisted_states, out_path)
    if report_md is not None and report_out_path:
        rp = Path(report_out_path)
        rp.parent.mkdir(parents=True, exist_ok=True)
        rp.write_text(report_md, encoding='utf-8')
        print(f'[OK] wrote {rp}')
    imported_cnt = sum((1 for r in results if r.status == 'imported'))
    skipped_cnt = sum((1 for r in results if r.status == 'skipped_missing'))
    err_cnt = sum((1 for r in results if r.status == 'error'))
    state_label = out_written if write_primary_state else f'{out_written} (not written)'
    print(f'[DONE] wrote {state_label} | trades={trades_written} ({len(trades_to_save)} rows) | imported={imported_cnt}, skipped={skipped_cnt}, errors={err_cnt} | keep_history_rows={keep_history_rows}')
    return 0
