from __future__ import annotations

from typing import Any, Dict, List, Optional

from core.models import TacticalPlan


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


def _open_lot_notes_by_ticker(trades: List[Dict[str, Any]]) -> Dict[str, str]:
    lots_by_ticker: Dict[str, List[Dict[str, Any]]] = {}
    for trade in sorted(trades, key=_trade_note_sort_key):
        ticker = str(trade.get("ticker") or "").upper().strip()
        side = str(trade.get("side") or "").upper().strip()
        note = str(trade.get("notes") or "").strip()
        try:
            shares = int(float(trade.get("shares") or 0))
        except Exception:
            shares = 0
        if not ticker or shares <= 0:
            continue
        ticker_lots = lots_by_ticker.setdefault(ticker, [])
        if side.startswith("B"):
            ticker_lots.append({"shares": shares, "note": note})
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
    notes_by_ticker: Dict[str, str] = {}
    for ticker, lots in lots_by_ticker.items():
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


def build_report_root(
    states: Dict[str, Any],
    *,
    config: Optional[Dict[str, Any]] = None,
    trades: Optional[List[Dict[str, Any]]] = None,
    tactical_plan: Optional[TacticalPlan] = None,
    report_meta: Optional[Dict[str, Any]] = None,
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
        signals["tactical"] = list(tactical_plan.tactical_rows)
        thresholds["buy_signal_close_price_thresholds"] = list(tactical_plan.threshold_rows)
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
    if portfolio:
        positions = portfolio.get("positions")
        if isinstance(positions, list):
            notes_by_ticker = _open_lot_notes_by_ticker(report_trades)
            report_positions: List[Dict[str, Any]] = []
            for item in positions:
                if not isinstance(item, dict):
                    continue
                position = dict(item)
                ticker = str(position.get("ticker") or "").upper().strip()
                position["notes"] = notes_by_ticker.get(ticker, "")
                report_positions.append(position)
            portfolio["positions"] = report_positions
        root["portfolio"] = portfolio
    if isinstance(report_meta, dict):
        root["_report_meta"] = dict(report_meta)
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
