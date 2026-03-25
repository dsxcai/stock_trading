from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from utils.parsers import _normalize_time_tw, _normalize_trade_date_et


def _first_token_ticker(product_name: str) -> str:
    """Infer the ticker from the leading token of a broker product string."""
    token = str(product_name or "").strip().split()[0] if str(product_name or "").strip() else ""
    return token.upper()


def _num_from_cell(value: str) -> Optional[float]:
    """Parse a numeric broker cell with commas, blanks, or accounting negatives."""
    raw = str(value or "").strip()
    if not raw:
        return None
    normalized = raw.replace(",", "")
    normalized = re.sub(r"[A-Za-z\u4e00-\u9fff]+$", "", normalized).strip()
    if not normalized:
        return None
    if normalized.startswith("(") and normalized.endswith(")"):
        normalized = f"-{normalized[1:-1]}"
    return float(normalized)


def _trade_key(trade: Dict[str, Any], amount_ndigits: int) -> str:
    """Build a stable deduplication key for a normalized trade."""
    return "|".join(
        [
            str(trade.get("trade_date_et") or ""),
            str(trade.get("time_tw") or ""),
            str(trade.get("ticker") or "").upper(),
            str(trade.get("side") or "").upper(),
            str(int(float(trade.get("shares") or 0))),
            f"{float(trade.get('gross') or 0.0):.{int(amount_ndigits)}f}",
            f"{float(trade.get('fee') or 0.0):.{int(amount_ndigits)}f}",
        ]
    )


def _trade_buy_total_cost_usd(trade: Dict[str, Any]) -> Optional[float]:
    """Return the all-in buy cost using the normalized trade cash fields when present."""
    for key in ("cash_amount", "amount"):
        value = trade.get(key)
        try:
            if value is not None:
                return float(value)
        except Exception:
            continue
    try:
        return float(trade.get("gross") or 0.0) + float(trade.get("fee") or 0.0)
    except Exception:
        return None


def _normalize_trades_inplace(trades: List[Dict[str, Any]], cash_amount_ndigits: int) -> None:
    """Normalize existing trades in-place for stable downstream reconciliation."""
    if not isinstance(trades, list):
        return
    for trade in trades:
        if not isinstance(trade, dict):
            continue
        if trade.get("trade_date_et") is not None:
            trade["trade_date_et"] = _normalize_trade_date_et(str(trade.get("trade_date_et") or ""))
        if trade.get("time_tw") is not None:
            trade["time_tw"] = _normalize_time_tw(str(trade.get("time_tw") or ""))
        if trade.get("ticker") is not None:
            trade["ticker"] = str(trade.get("ticker") or "").upper()
        if trade.get("side") is not None:
            trade["side"] = str(trade.get("side") or "").upper()
        if trade.get("shares") is not None:
            trade["shares"] = int(float(trade.get("shares") or 0))
        for field in ["gross", "fee", "net", "price"]:
            if field in trade and trade.get(field) not in (None, ""):
                trade[field] = float(trade[field])
        if "cash_amount" in trade and trade.get("cash_amount") not in (None, ""):
            trade["cash_amount"] = round(float(trade["cash_amount"]), int(cash_amount_ndigits))


def _upsert_trades(
    trades: List[Dict[str, Any]],
    incoming: List[Dict[str, Any]],
    *,
    cash_amount_ndigits: int,
    trade_dedupe_amount_ndigits: int,
) -> Tuple[int, int]:
    """Append only new trades according to the normalized trade key."""
    if not isinstance(trades, list):
        raise TypeError("trades must be a list")

    _normalize_trades_inplace(trades, cash_amount_ndigits=cash_amount_ndigits)
    _normalize_trades_inplace(incoming, cash_amount_ndigits=cash_amount_ndigits)
    existing_keys = {_trade_key(trade, amount_ndigits=trade_dedupe_amount_ndigits) for trade in trades if isinstance(trade, dict)}
    max_id = 0
    for trade in trades:
        if not isinstance(trade, dict):
            continue
        try:
            max_id = max(max_id, int(trade.get("trade_id") or 0))
        except Exception:
            continue
    added = 0
    duplicates = 0
    for trade in incoming:
        key = _trade_key(trade, amount_ndigits=trade_dedupe_amount_ndigits)
        if key in existing_keys:
            duplicates += 1
            continue
        max_id += 1
        row = dict(trade)
        row["trade_id"] = max_id
        trades.append(row)
        existing_keys.add(key)
        added += 1
    return added, duplicates


def _group_key_trade(trade: Dict[str, Any]) -> tuple[str, str, str]:
    """Group trades by day, ticker, and side."""
    return (
        str(trade.get("trade_date_et") or ""),
        str(trade.get("ticker") or "").upper(),
        str(trade.get("side") or "").upper(),
    )


def _verify_holdings_with_broker_investment_total(
    states: Dict[str, Any],
    broker_investment_total_usd: Optional[float],
    broker_asof_et: Optional[str],
    broker_investment_total_kind: str = "market_value",
    verify_tolerance_usd: float = 1.0,
) -> None:
    """Verify broker investment totals against portfolio holdings totals."""
    if broker_investment_total_usd is None:
        return

    portfolio = states.setdefault("portfolio", {})
    totals = portfolio.get("totals", {}) or {}
    kind = str(broker_investment_total_kind or "market_value").strip().lower()
    if kind not in {"market_value", "cost_basis"}:
        kind = "market_value"

    metric_key = "holdings_cost_usd" if kind == "cost_basis" else "holdings_mv_usd"
    label = metric_key
    holdings_value: float

    portfolio_totals = totals.get("portfolio") or {}
    if portfolio_totals.get(metric_key) is not None:
        holdings_value = float(portfolio_totals[metric_key])
    else:
        holdings_value = 0.0
        for position in portfolio.get("positions", []) or []:
            if kind == "cost_basis":
                holdings_value += float(position.get("cost_usd") or 0.0)
            else:
                holdings_value += float(position.get("market_value_usd") or 0.0)

    broker_total = float(broker_investment_total_usd)
    diff = holdings_value - broker_total
    status = "OK" if abs(diff) <= verify_tolerance_usd else "MISMATCH"

    broker = portfolio.setdefault("broker", {})
    broker.update(
        {
            "investment_total_usd": broker_total,
            "investment_total_excludes_cash": True,
            "investment_total_kind": kind,
            label: holdings_value,
            "diff_usd": diff,
            "tolerance_usd": float(verify_tolerance_usd),
            "status": status,
            "source": "cli",
            "reconciliation": {
                "mode": "holdings_investment_total_ex_cash",
                "source": "cli",
                "input": {
                    "investment_total_usd": broker_total,
                    "investment_total_kind": kind,
                    "investment_total_excludes_cash": True,
                    "asof_et": broker_asof_et,
                },
                "computed": {
                    "compared_metric": label,
                    "compared_value_usd": holdings_value,
                    "holdings_mv_usd": float((portfolio_totals.get("holdings_mv_usd") or broker.get("holdings_mv_usd") or 0.0)),
                    "holdings_cost_usd": float((portfolio_totals.get("holdings_cost_usd") or broker.get("holdings_cost_usd") or 0.0)),
                },
                "result": {
                    "diff_usd": diff,
                    "tolerance_usd": float(verify_tolerance_usd),
                    "status": status,
                },
            },
        }
    )
