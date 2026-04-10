from __future__ import annotations

from typing import Any, Dict, List, Optional

from core.models import TacticalPlan
from core.report_meta import _normalize_mode_key
from core.reconciliation import _trade_buy_total_cost_usd
from utils.config_access import config_buckets, config_fx_pairs, config_tactical_indicators


def _trade_note_sort_key(trade: Dict[str, Any]) -> tuple[str, str, int]:
    trade_id = trade.get("trade_id")
    try:
        trade_id_int = int(trade_id or 0)
    except Exception:
        trade_id_int = 0
    return (
        str(trade.get("trade_date_et") or "").strip(),
        str(trade.get("time_tw") or "").strip(),
        trade_id_int,
    )


def _open_lots_by_ticker(trades: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    lots_by_ticker: Dict[str, List[Dict[str, Any]]] = {}
    for trade in sorted(trades, key=_trade_note_sort_key):
        ticker = str(trade.get("ticker") or "").upper().strip()
        side = str(trade.get("side") or "").upper().strip()
        note = str(trade.get("notes") or "").strip()
        trade_date_et = str(trade.get("trade_date_et") or "").strip()
        try:
            shares = int(float(trade.get("shares") or 0))
        except Exception:
            shares = 0
        if not ticker or shares <= 0:
            continue
        ticker_lots = lots_by_ticker.setdefault(ticker, [])
        if side.startswith("B"):
            total_cost_usd = _trade_buy_total_cost_usd(trade)
            if total_cost_usd is None:
                continue
            ticker_lots.append(
                {
                    "shares": shares,
                    "note": note,
                    "trade_date_et": trade_date_et,
                    "unit_cost_usd": float(total_cost_usd) / float(shares),
                }
            )
            continue
        if side.startswith("S"):
            remaining = shares
            while remaining > 0 and ticker_lots:
                lot = ticker_lots[0]
                lot_shares = int(lot.get("shares") or 0)
                used = min(remaining, lot_shares)
                remaining -= used
                lot_shares -= used
                if lot_shares <= 0:
                    ticker_lots.pop(0)
                else:
                    lot["shares"] = lot_shares
    return lots_by_ticker


def _open_lot_notes_by_ticker(trades: List[Dict[str, Any]]) -> Dict[str, str]:
    notes_by_ticker: Dict[str, str] = {}
    for ticker, lots in _open_lots_by_ticker(trades).items():
        note_shares: Dict[str, int] = {}
        ordered_notes: List[str] = []
        for lot in lots:
            note = str(lot.get("note") or "").strip()
            try:
                shares = int(lot.get("shares") or 0)
            except Exception:
                shares = 0
            if not note or shares <= 0:
                continue
            if note not in note_shares:
                ordered_notes.append(note)
                note_shares[note] = 0
            note_shares[note] += shares
        notes_by_ticker[ticker] = " | ".join(f"{note} x{note_shares[note]}" for note in ordered_notes)
    return notes_by_ticker


def _usd_twd_fx_ticker(config: Optional[Dict[str, Any]]) -> str:
    if not isinstance(config, dict):
        return ""
    usd_twd_cfg = (config_fx_pairs(config).get("usd_twd") or {})
    if not isinstance(usd_twd_cfg, dict):
        return ""
    return str(usd_twd_cfg.get("ticker") or "").upper().strip()


def _tactical_cash_pool_ticker(config: Optional[Dict[str, Any]]) -> str:
    if not isinstance(config, dict):
        return ""
    tactical_bucket = config_buckets(config).get("tactical") or {}
    if not isinstance(tactical_bucket, dict):
        return ""
    return str(tactical_bucket.get("cash_pool_ticker") or "").upper().strip()


def _row_has_any_value(row: Dict[str, Any], keys: tuple[str, ...]) -> bool:
    for key in keys:
        if row.get(key) is not None:
            return True
    return False


def _filter_report_signal_rows(rows: List[Dict[str, Any]], config: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            shares_pre = float(row.get("tactical_shares_pre") or 0.0)
        except Exception:
            shares_pre = 0.0
        if shares_pre > 0 or _row_has_any_value(row, ("close_t", "ma_t", "close_t_minus_5")):
            filtered.append(dict(row))
    return filtered


def _filter_report_threshold_rows(rows: List[Dict[str, Any]], config: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cash_pool_ticker = _tactical_cash_pool_ticker(config)
    filtered: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").upper().strip()
        if cash_pool_ticker and ticker == cash_pool_ticker:
            continue
        if _row_has_any_value(row, ("ma_sum_prev", "close_t_minus_5_next", "threshold_from_ma", "threshold")):
            filtered.append(dict(row))
    return filtered


def _cash_event_sort_key(event: Dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(event.get("event_date_et") or "").strip(),
        str(event.get("ts_utc") or "").strip(),
        str(event.get("event_id") or "").strip(),
    )


def _cash_event_to_activity_row(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    kind = str(event.get("kind") or "").strip().lower()
    if kind not in {"deposit", "withdrawal"}:
        return None
    event_id = str(event.get("event_id") or "").strip()
    event_date_et = str(event.get("event_date_et") or "").strip()
    if not event_id or not event_date_et:
        return None
    try:
        amount_usd = float(event.get("amount_usd") or 0.0)
    except Exception:
        amount_usd = 0.0
    return {
        "trade_id": event_id,
        "trade_date_et": event_date_et,
        "ticker": "CASH",
        "side": "DEPOSIT" if kind == "deposit" else "WITHDRAWAL",
        "time_tw": None,
        "cash_amount": amount_usd,
        "cash_basis": "External Cash Flow",
        "notes": str(event.get("note") or "").strip(),
        "source": str(event.get("source") or "").strip(),
        "activity_type": "cash_event",
    }


def _build_report_activities(trades: List[Dict[str, Any]], cash_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    activities: List[Dict[str, Any]] = []
    for item in sorted(trades, key=_trade_note_sort_key):
        if isinstance(item, dict):
            row = dict(item)
            row.setdefault("activity_type", "trade")
            activities.append(row)
    for item in sorted(cash_events, key=_cash_event_sort_key):
        if not isinstance(item, dict):
            continue
        projected = _cash_event_to_activity_row(item)
        if projected is not None:
            activities.append(projected)
    activities.sort(key=_trade_note_sort_key)
    return activities


def _signal_basis_day(report_meta: Optional[Dict[str, Any]]) -> str:
    if not isinstance(report_meta, dict):
        return ""
    signal_basis = report_meta.get("signal_basis") or {}
    return str(signal_basis.get("t_et") or "").strip()


def _latest_row_date(rows: List[Dict[str, Any]]) -> str:
    for row in reversed(rows):
        row_date = str((row or {}).get("Date") or "").strip()
        if row_date:
            return row_date
    return ""


def _report_active_tickers(states: Dict[str, Any], config: Optional[Dict[str, Any]]) -> List[str]:
    tickers: List[str] = []
    seen: set[str] = set()
    positions = ((states.get("portfolio") or {}).get("positions")) or []
    for position in positions:
        if not isinstance(position, dict):
            continue
        ticker = str(position.get("ticker") or "").upper().strip()
        try:
            shares = float(position.get("shares") or 0.0)
        except Exception:
            shares = 0.0
        if ticker and shares > 0 and ticker not in seen:
            seen.add(ticker)
            tickers.append(ticker)
    tactical_indicators = config_tactical_indicators(config) if isinstance(config, dict) else {}
    for ticker in tactical_indicators:
        ticker_norm = str(ticker or "").upper().strip()
        if ticker_norm and ticker_norm not in seen:
            seen.add(ticker_norm)
            tickers.append(ticker_norm)
    return tickers


def _estimated_price_notes(
    states: Dict[str, Any],
    config: Optional[Dict[str, Any]],
    report_meta: Optional[Dict[str, Any]],
    market_history: Optional[Dict[str, Any]],
) -> List[str]:
    notes: List[str] = []
    if not isinstance(report_meta, dict) or not isinstance(market_history, dict):
        return notes
    mode_key = _normalize_mode_key(report_meta.get("mode_key") or report_meta.get("mode"))
    signal_day = _signal_basis_day(report_meta)
    fx_ticker = _usd_twd_fx_ticker(config)
    if mode_key == "intraday" and signal_day:
        same_day_tickers: List[str] = []
        for ticker in _report_active_tickers(states, config):
            if ticker == fx_ticker:
                continue
            rows = list(((market_history.get(ticker) or {}).get("rows")) or [])
            if _latest_row_date(rows) == signal_day:
                same_day_tickers.append(ticker)
        if same_day_tickers:
            notes.append(
                "Estimated Price: Intraday current positions and signal trigger use same-day CSV prices "
                f"when available ({', '.join(same_day_tickers)})."
            )
    if mode_key == "premarket" and signal_day and fx_ticker:
        fx_rows = list(((market_history.get(fx_ticker) or {}).get("rows")) or [])
        latest_fx_date = _latest_row_date(fx_rows)
        if latest_fx_date and latest_fx_date > signal_day:
            positions = ((states.get("portfolio") or {}).get("positions")) or []
            has_open_position = False
            for position in positions:
                if not isinstance(position, dict):
                    continue
                try:
                    shares = float(position.get("shares") or 0.0)
                except Exception:
                    shares = 0.0
                if shares > 0:
                    has_open_position = True
                    break
            if has_open_position:
                notes.append(
                    "Estimated Price: Premarket Unrealized PnL (TWD) uses the latest "
                    f"{fx_ticker} CSV quote from {latest_fx_date}."
                )
    return notes


def _latest_close(rows: List[Dict[str, Any]]) -> Optional[float]:
    for row in reversed(rows):
        try:
            close = row.get("Close")
            if close is not None:
                return float(close)
        except Exception:
            continue
    return None


def _close_on_or_before(rows: List[Dict[str, Any]], target_date: str) -> Optional[float]:
    target = str(target_date or "").strip()
    if not target:
        return None
    candidate: Optional[float] = None
    for row in rows:
        row_date = str((row or {}).get("Date") or "").strip()
        if not row_date:
            continue
        if row_date > target:
            break
        try:
            close = row.get("Close")
            if close is not None:
                candidate = float(close)
        except Exception:
            continue
    return candidate


def _position_twd_metrics(
    position: Dict[str, Any],
    lots: List[Dict[str, Any]],
    fx_rows: List[Dict[str, Any]],
) -> Optional[Dict[str, float]]:
    current_fx = _latest_close(fx_rows)
    if current_fx is None:
        return None
    try:
        shares = float(position.get("shares") or 0.0)
    except Exception:
        shares = 0.0
    if shares <= 0:
        return None
    market_value_usd = position.get("market_value_usd")
    if market_value_usd is None:
        try:
            market_value_usd = shares * float(position.get("price_now") or 0.0)
        except Exception:
            market_value_usd = None
    try:
        if market_value_usd is None:
            return None
        market_value_twd = float(market_value_usd) * current_fx
    except Exception:
        return None
    cost_twd = 0.0
    for lot in lots:
        try:
            lot_shares = float(lot.get("shares") or 0.0)
            unit_cost_usd = float(lot.get("unit_cost_usd") or 0.0)
        except Exception:
            return None
        if lot_shares <= 0 or unit_cost_usd < 0:
            continue
        trade_fx = _close_on_or_before(fx_rows, str(lot.get("trade_date_et") or ""))
        if trade_fx is None:
            return None
        cost_twd += lot_shares * unit_cost_usd * trade_fx
    if cost_twd <= 0:
        return None
    unrealized_pnl_twd = market_value_twd - cost_twd
    return {
        "holdings_cost_twd": cost_twd,
        "holdings_mv_twd": market_value_twd,
        "unrealized_pnl_twd": unrealized_pnl_twd,
        "unrealized_pnl_twd_pct": unrealized_pnl_twd / cost_twd,
    }


def build_report_root(
    states: Dict[str, Any],
    *,
    config: Optional[Dict[str, Any]] = None,
    trades: Optional[List[Dict[str, Any]]] = None,
    cash_events: Optional[List[Dict[str, Any]]] = None,
    tactical_plan: Optional[TacticalPlan] = None,
    report_meta: Optional[Dict[str, Any]] = None,
    market_history: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build the transient render root used by report generation.

    Persisted state stays report-agnostic. Report-only inputs such as config and
    external trades are injected here instead of being written back to states.json.
    """
    root = dict(states)
    market = dict(root.get("market") or {})
    portfolio = dict(root.get("portfolio") or {})
    signals = dict(root.get("signals") or {})
    thresholds = dict(root.get("thresholds") or {})
    if tactical_plan is not None:
        market["signals_inputs"] = dict(tactical_plan.signals_inputs)
        market["next_close_threshold_inputs"] = dict(tactical_plan.threshold_inputs)
        signals["tactical"] = _filter_report_signal_rows(list(tactical_plan.tactical_rows), config)
        thresholds["buy_signal_close_price_thresholds"] = _filter_report_threshold_rows(list(tactical_plan.threshold_rows), config)
    if market:
        root["market"] = market
    if signals:
        root["signals"] = signals
    if thresholds:
        root["thresholds"] = thresholds
    if isinstance(config, dict):
        root["config"] = dict(config)
    report_trades: List[Dict[str, Any]] = []
    if isinstance(trades, list):
        for item in trades:
            if isinstance(item, dict):
                report_trades.append(dict(item))
        root["trades"] = report_trades
    report_cash_events: List[Dict[str, Any]] = []
    if isinstance(cash_events, list):
        for item in cash_events:
            if isinstance(item, dict):
                report_cash_events.append(dict(item))
        root["cash_events"] = report_cash_events
    root["activities"] = _build_report_activities(report_trades, report_cash_events)
    if portfolio:
        totals = portfolio.get("totals")
        if isinstance(totals, dict):
            portfolio["totals"] = {key: (dict(value) if isinstance(value, dict) else value) for key, value in totals.items()}
        positions = portfolio.get("positions")
        if isinstance(positions, list):
            notes_by_ticker = _open_lot_notes_by_ticker(report_trades)
            open_lots_by_ticker = _open_lots_by_ticker(report_trades)
            fx_ticker = _usd_twd_fx_ticker(config)
            fx_rows = []
            if fx_ticker and isinstance(market_history, dict):
                fx_rows = list(((market_history.get(fx_ticker) or {}).get("rows")) or [])
            twd_totals = {
                "core": {"holdings_cost_twd": 0.0, "holdings_mv_twd": 0.0, "unrealized_pnl_twd": 0.0, "count": 0, "complete": True},
                "tactical": {"holdings_cost_twd": 0.0, "holdings_mv_twd": 0.0, "unrealized_pnl_twd": 0.0, "count": 0, "complete": True},
                "portfolio": {"holdings_cost_twd": 0.0, "holdings_mv_twd": 0.0, "unrealized_pnl_twd": 0.0, "count": 0, "complete": True},
            }
            report_positions: List[Dict[str, Any]] = []
            for item in positions:
                if not isinstance(item, dict):
                    continue
                position = dict(item)
                ticker = str(position.get("ticker") or "").upper().strip()
                position["notes"] = notes_by_ticker.get(ticker, "")
                if fx_rows:
                    metrics = _position_twd_metrics(
                        position,
                        open_lots_by_ticker.get(ticker, []),
                        fx_rows,
                    )
                    if metrics is not None:
                        position["unrealized_pnl_twd"] = metrics["unrealized_pnl_twd"]
                        position["unrealized_pnl_twd_pct"] = metrics["unrealized_pnl_twd_pct"]
                        bucket = str(position.get("bucket") or "").strip()
                        bucket_targets = ["portfolio"]
                        if bucket == "core":
                            bucket_targets.insert(0, "core")
                        elif bucket in {"tactical", "tactical_cash_pool"}:
                            bucket_targets.insert(0, "tactical")
                        for bucket_key in bucket_targets:
                            agg = twd_totals.get(bucket_key)
                            if not isinstance(agg, dict):
                                continue
                            agg["holdings_cost_twd"] += metrics["holdings_cost_twd"]
                            agg["holdings_mv_twd"] += metrics["holdings_mv_twd"]
                            agg["unrealized_pnl_twd"] += metrics["unrealized_pnl_twd"]
                            agg["count"] += 1
                    else:
                        bucket = str(position.get("bucket") or "").strip()
                        bucket_targets = ["portfolio"]
                        if bucket == "core":
                            bucket_targets.insert(0, "core")
                        elif bucket in {"tactical", "tactical_cash_pool"}:
                            bucket_targets.insert(0, "tactical")
                        for bucket_key in bucket_targets:
                            agg = twd_totals.get(bucket_key)
                            if isinstance(agg, dict):
                                agg["complete"] = False
                report_positions.append(position)
            portfolio["positions"] = report_positions
            totals = portfolio.get("totals")
            if isinstance(totals, dict) and fx_rows:
                for bucket_key, agg in twd_totals.items():
                    bucket_totals = totals.get(bucket_key)
                    if not isinstance(bucket_totals, dict):
                        continue
                    if int(agg.get("count") or 0) <= 0 or not bool(agg.get("complete")):
                        continue
                    holdings_cost_twd = float(agg["holdings_cost_twd"])
                    unrealized_pnl_twd = float(agg["unrealized_pnl_twd"])
                    bucket_totals["holdings_cost_twd"] = holdings_cost_twd
                    bucket_totals["holdings_mv_twd"] = float(agg["holdings_mv_twd"])
                    bucket_totals["unrealized_pnl_twd"] = unrealized_pnl_twd
                    bucket_totals["unrealized_pnl_twd_pct"] = (unrealized_pnl_twd / holdings_cost_twd) if holdings_cost_twd > 0 else None
        root["portfolio"] = portfolio
    if isinstance(report_meta, dict):
        meta = dict(report_meta)
        price_notes = [str(item).strip() for item in (meta.get("price_notes") or []) if str(item).strip()]
        for note in _estimated_price_notes(root, config, meta, market_history):
            if note not in price_notes:
                price_notes.append(note)
        if price_notes:
            meta["price_notes"] = price_notes
        root["_report_meta"] = meta
    return root


def ensure_report_root_fields(report_root: Dict[str, Any]) -> List[str]:
    """Check whether the assembled report root looks complete enough to render."""
    warns: List[str] = []
    for key in ["config", "market", "portfolio", "signals", "thresholds"]:
        if key not in report_root:
            warns.append(f"missing root key: {key}")
    market = report_root.get("market", {}) or {}
    for key in ["prices_now", "signals_inputs", "next_close_threshold_inputs"]:
        if key not in market:
            warns.append(f"missing market.{key}")
    portfolio = report_root.get("portfolio", {}) or {}
    if "positions" not in portfolio:
        warns.append("missing portfolio.positions")
    if "cash" not in portfolio:
        warns.append("missing portfolio.cash")
    if "totals" not in portfolio:
        warns.append("missing portfolio.totals")
    signals = report_root.get("signals", {}) or {}
    if "tactical" not in signals:
        warns.append("missing signals.tactical")
    thresholds = report_root.get("thresholds", {}) or {}
    if "buy_signal_close_price_thresholds" not in thresholds:
        warns.append("missing thresholds.buy_signal_close_price_thresholds")
    return warns
