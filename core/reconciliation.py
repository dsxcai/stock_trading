from __future__ import annotations

import hashlib
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from utils.parsers import _normalize_time_tw, _normalize_trade_date_et


_OS_HISTORY_XML_FIELD_ALIASES: Dict[str, Tuple[str, ...]] = {
    "trade_date": ("trade_date", "date", "成交日期"),
    "time_tw": ("trade_time", "time", "成交時間"),
    "side": ("bs", "side", "買賣"),
    "product_name": ("product", "symbol", "商品名稱"),
    "ticker": ("ticker",),
    "shares": ("qty", "quantity", "成交股數"),
    "gross": ("gross", "amount", "成交金額"),
    "fee": ("fee", "手續費"),
    "net": ("net", "淨額"),
    "price": ("price", "成交均價"),
}


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
    if normalized.startswith("(") and normalized.endswith(")"):
        normalized = f"-{normalized[1:-1]}"
    return float(normalized)


def _first_present_attribute(attributes: Dict[str, str], aliases: Tuple[str, ...], default: str = "") -> str:
    for alias in aliases:
        value = str(attributes.get(alias) or "").strip()
        if value:
            return value
    return default


def _normalize_broker_side(raw_side: str) -> str:
    side = str(raw_side or "").strip()
    side_upper = side.upper()
    if side_upper.startswith("B") or "買" in side:
        return "BUY"
    if side_upper.startswith("S") or "賣" in side:
        return "SELL"
    return side_upper


def _normalize_os_history_attributes(attributes: Dict[str, str]) -> Dict[str, str]:
    normalized = {
        field: _first_present_attribute(attributes, aliases)
        for field, aliases in _OS_HISTORY_XML_FIELD_ALIASES.items()
    }
    normalized["ticker"] = str(normalized.get("ticker") or _first_token_ticker(normalized.get("product_name", ""))).upper()
    return normalized


def _import_trades_from_os_history_xml(xml_path: str) -> List[Dict[str, Any]]:
    """Import normalized trade records from a broker OS history XML export."""
    if not Path(xml_path).exists():
        raise FileNotFoundError(xml_path)

    root = ET.parse(xml_path).getroot()
    rows: List[Dict[str, Any]] = []

    for item in root.iter():
        tag = item.tag.lower()
        if not tag.endswith("row"):
            continue
        attributes = {str(key).strip().lower(): str(value).strip() for key, value in item.attrib.items()}
        if not attributes:
            continue

        normalized = _normalize_os_history_attributes(attributes)
        trade_date = normalized["trade_date"]
        time_tw = normalized["time_tw"]
        side = _normalize_broker_side(normalized["side"])
        ticker = normalized["ticker"]
        shares = normalized["shares"]
        gross = normalized["gross"]
        fee = normalized["fee"] or "0"
        net = normalized["net"] or gross
        price = normalized["price"]

        if not trade_date or not ticker or not side:
            continue

        trade_date_et = _normalize_trade_date_et(trade_date)
        time_tw_normalized = _normalize_time_tw(time_tw)
        shares_value = int(float(shares)) if str(shares).strip() else 0
        gross_value = float(_num_from_cell(gross) or 0.0)
        fee_value = float(_num_from_cell(fee) or 0.0)
        net_value = float(_num_from_cell(net) or gross_value)
        price_value = _num_from_cell(price)
        cash_amount_value = gross_value + fee_value if side.startswith("B") else max(gross_value - fee_value, 0.0)
        fee_rate_pct = fee_value / gross_value if gross_value else None

        digest_source = "|".join([trade_date_et, time_tw_normalized, ticker, side, str(shares_value), f"{gross_value:.6f}", f"{fee_value:.6f}"])
        trade_id = hashlib.md5(digest_source.encode("utf-8")).hexdigest()[:16]

        rows.append({
            "trade_id": trade_id,
            "trade_date_et": trade_date_et,
            "ticker": ticker,
            "side": side,
            "shares": shares_value,
            "cash_amount": cash_amount_value,
            "cash_basis": "Total",
            "gross": gross_value,
            "fee": fee_value,
            "fee_rate_pct": fee_rate_pct,
            "net": net_value,
            "price": price_value,
            "time_tw": time_tw_normalized,
            "notes": f"Imported from OSHistoryDealAll ({ticker})",
            "source": f"xml:{Path(xml_path).name}",
        })

    rows.sort(key=lambda row: (row.get("trade_date_et", ""), row.get("time_tw", ""), row.get("ticker", ""), row.get("trade_id", "")))
    return rows


def _trade_key(trade: Dict[str, Any]) -> str:
    """Build a stable deduplication key for a normalized trade."""
    return "|".join(
        [
            str(trade.get("trade_date_et") or ""),
            str(trade.get("time_tw") or ""),
            str(trade.get("ticker") or "").upper(),
            str(trade.get("side") or "").upper(),
            str(int(float(trade.get("shares") or 0))),
            f"{float(trade.get('gross') or 0.0):.6f}",
            f"{float(trade.get('fee') or 0.0):.6f}",
        ]
    )


def _normalize_trades_inplace(states: Dict[str, Any]) -> None:
    """Normalize existing trades in-place for stable downstream reconciliation."""
    trades = states.get("trades") or []
    if not isinstance(trades, list):
        states["trades"] = []
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


def _upsert_trades(states: Dict[str, Any], incoming: List[Dict[str, Any]]) -> Tuple[int, int]:
    """Append only new trades according to the normalized trade key."""
    trades = states.setdefault("trades", [])
    if not isinstance(trades, list):
        raise TypeError("states['trades'] must be a list")

    existing_keys = {_trade_key(trade) for trade in trades if isinstance(trade, dict)}
    added = 0
    duplicates = 0
    for trade in incoming:
        key = _trade_key(trade)
        if key in existing_keys:
            duplicates += 1
            continue
        trades.append(dict(trade))
        existing_keys.add(key)
        added += 1
    return added, duplicates


def _is_broker_trade(trade: Dict[str, Any]) -> bool:
    """Return True when a trade originated from broker XML."""
    source = str(trade.get("source") or "").strip().lower()
    source_type = str(trade.get("source_type") or "").strip().lower()
    notes = str(trade.get("notes") or "")
    return source == "broker_xml" or source_type == "broker_xml" or source.startswith("xml:") or "OSHistoryDealAll" in notes


def _archive_trades(states: Dict[str, Any], to_archive: List[Dict[str, Any]], reason: str) -> None:
    """Move superseded trades into states.trades_archived with a reason."""
    archived = states.setdefault("trades_archived", [])
    if not isinstance(archived, list):
        raise TypeError("states['trades_archived'] must be a list")
    for trade in to_archive:
        record = dict(trade)
        record["archive_reason"] = reason
        archived.append(record)


def _group_key_trade(trade: Dict[str, Any]) -> tuple[str, str, str]:
    """Group trades by day, ticker, and side."""
    return (
        str(trade.get("trade_date_et") or ""),
        str(trade.get("ticker") or "").upper(),
        str(trade.get("side") or "").upper(),
    )


def _trade_cash_total_for_match(trades: List[Dict[str, Any]], side: str) -> float:
    """Compute the cash total used to reconcile manual and broker trades."""
    total = 0.0
    side_upper = str(side or "").upper()
    for trade in trades:
        gross = float(trade.get("gross") or 0.0)
        fee = float(trade.get("fee") or 0.0)
        if side_upper.startswith("B"):
            total += gross + fee
        elif side_upper.startswith("S"):
            total += max(gross - fee, 0.0)
        else:
            total += gross
    return total


def _reconcile_manual_aggregates(
    states: Dict[str, Any],
    incoming: List[Dict[str, Any]],
    abs_tol_usd: float = 1.0,
    rel_tol: float = 0.003,
) -> Tuple[int, set[tuple[str, str, str]]]:
    """Supersede matching manual aggregate trades when broker fills are available."""
    trades = states.get("trades") or []
    if not isinstance(trades, list) or not trades:
        return 0, set()

    incoming_groups: Dict[tuple[str, str, str], List[Dict[str, Any]]] = {}
    for trade in incoming:
        incoming_groups.setdefault(_group_key_trade(trade), []).append(trade)

    existing_groups: Dict[tuple[str, str, str], List[Dict[str, Any]]] = {}
    for trade in trades:
        if isinstance(trade, dict):
            existing_groups.setdefault(_group_key_trade(trade), []).append(trade)

    remove_ids: set[Any] = set()
    superseded_groups: set[tuple[str, str, str]] = set()
    removed_total = 0

    for group_key, incoming_list in incoming_groups.items():
        existing_list = existing_groups.get(group_key, [])
        manual_candidates = [trade for trade in existing_list if not _is_broker_trade(trade)]
        if not manual_candidates:
            continue

        incoming_shares = sum(int(float(trade.get("shares") or 0)) for trade in incoming_list)
        manual_shares = sum(int(float(trade.get("shares") or 0)) for trade in manual_candidates)
        if incoming_shares <= 0 or manual_shares != incoming_shares:
            continue

        side = group_key[2]
        incoming_cash = _trade_cash_total_for_match(incoming_list, side)
        manual_cash = _trade_cash_total_for_match(manual_candidates, side)
        tolerance = max(float(abs_tol_usd), float(rel_tol) * abs(incoming_cash))
        if abs(manual_cash - incoming_cash) > tolerance:
            continue

        for trade in manual_candidates:
            trade_id = trade.get("trade_id")
            remove_ids.add(trade_id if trade_id is not None else id(trade))
        reason = (
            f"superseded_manual_aggregate_by_broker_xml: group={group_key[0]}/{group_key[1]}/{group_key[2]}, "
            f"manual_count={len(manual_candidates)}, broker_count={len(incoming_list)}, shares={incoming_shares}, "
            f"cash_manual={manual_cash:.2f}, cash_broker={incoming_cash:.2f}, tol={tolerance:.2f}"
        )
        _archive_trades(states, manual_candidates, reason)
        removed_total += len(manual_candidates)
        superseded_groups.add(group_key)

    if remove_ids:
        filtered = []
        for trade in trades:
            if not isinstance(trade, dict):
                filtered.append(trade)
                continue
            trade_id = trade.get("trade_id")
            key = trade_id if trade_id is not None else id(trade)
            if key in remove_ids:
                continue
            filtered.append(trade)
        states["trades"] = filtered

    return removed_total, superseded_groups


def _reconcile_manual_aggregate_trades_against_broker_import(
    states: Dict[str, Any],
    incoming: List[Dict[str, Any]],
    abs_tol_usd: float = 1.0,
    rel_tol: float = 0.003,
) -> set[tuple[str, str, str]]:
    """Remove superseded manual aggregate trades after broker XML import."""
    _, superseded_groups = _reconcile_manual_aggregates(states, incoming, abs_tol_usd=abs_tol_usd, rel_tol=rel_tol)
    return superseded_groups


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
