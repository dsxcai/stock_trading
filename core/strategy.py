from __future__ import annotations

import csv
import math
import re
from typing import Any, Dict, List, Optional

from core.models import OHLCVRow, SignalInputs, ThresholdInputs
from utils.parsers import _to_yyyy_mm_dd
from utils.precision import format_currency, format_fixed

_REQUIRED_PRICE_COLUMNS = ("Open", "High", "Low", "Close")


def _parse_indicator_window(ma_rule: str) -> Optional[int]:
    """Extract the numeric window size from a moving-average spec."""
    match = re.search(r"(\d+)", str(ma_rule))
    return int(match.group(1)) if match else None


def _normalize_ma_rule(spec: Any) -> str:
    """Return a stable display string for a moving-average rule."""
    if isinstance(spec, str):
        return spec.strip()
    if isinstance(spec, dict):
        ma_type = str(spec.get("ma_type") or "SMA").strip()
        window = spec.get("window")
        try:
            return f"{ma_type}{int(window)}"
        except Exception:
            return ma_type
    return str(spec)


def _fmt_usd(value: Optional[float], usd_amount_ndigits: int) -> str:
    """Format a USD amount for diagnostics."""
    if value is None:
        return ""
    try:
        return format_currency(value, usd_amount_ndigits)
    except Exception:
        return str(value)


def _dedupe_by_date_keep_last(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep the last row for each trading date after ascending sort order."""
    if not rows:
        return []
    deduped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        deduped[str(row["Date"])] = row
    return [deduped[key] for key in sorted(deduped.keys())]


def _read_ohlcv_csv(
    csv_path: str,
    keep_last_n: Optional[int],
    *,
    allow_incomplete_rows: bool = False,
    bypass_option_hint: str = "--allow-incomplete-csv-rows",
) -> List[Dict[str, Any]]:
    """Read OHLCV data from a canonical Date/Open/High/Low/Close/Volume CSV."""
    rows: List[Dict[str, Any]] = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required_columns = ["Date", "Open", "High", "Low", "Close", "Volume"]
        for column in required_columns:
            if reader.fieldnames is None or column not in reader.fieldnames:
                raise ValueError(f"{csv_path}: missing column {column}. Found: {reader.fieldnames}")
        for line_no, raw_row in enumerate(reader, start=2):
            price_values = []
            try:
                for column in _REQUIRED_PRICE_COLUMNS:
                    raw_value = str(raw_row[column]).strip()
                    if not raw_value:
                        raise ValueError("missing required price")
                    parsed = float(raw_value)
                    if math.isnan(parsed):
                        raise ValueError("nan required price")
                    price_values.append(parsed)
            except Exception as exc:
                if allow_incomplete_rows:
                    continue
                date_text = str(raw_row.get("Date") or "").strip() or "unknown"
                raise ValueError(
                    f"{csv_path}: row {line_no} (Date={date_text}) has incomplete OHLC data. "
                    f"Re-run with {bypass_option_hint} to bypass and skip incomplete rows."
                ) from exc
            row = OHLCVRow(
                date=_to_yyyy_mm_dd(raw_row["Date"]),
                open=price_values[0],
                high=price_values[1],
                low=price_values[2],
                close=price_values[3],
                volume=int(float(raw_row["Volume"])) if str(raw_row["Volume"]).strip() else 0,
            )
            rows.append(row.as_dict())
    rows.sort(key=lambda item: item["Date"])
    rows = _dedupe_by_date_keep_last(rows)
    if keep_last_n is not None and keep_last_n > 0 and len(rows) > keep_last_n:
        rows = rows[-keep_last_n:]
    return rows


def _derive_signals_inputs_from_history(rows: List[Dict[str, Any]], window: int) -> Dict[str, Any]:
    """Derive close, moving average, and t-5 close inputs from recent history."""
    closes = [float(row["Close"]) for row in rows]
    result = SignalInputs()
    if len(closes) >= 1:
        result.close_t = closes[-1]
    if window > 0 and len(closes) >= window:
        result.ma_t = sum(closes[-window:]) / float(window)
    if len(closes) >= 6:
        result.close_t_minus_5 = closes[-6]
    return result.as_dict()


def _derive_threshold_inputs_from_history(rows: List[Dict[str, Any]], window: int) -> Dict[str, Any]:
    """Derive threshold inputs for the next-close price threshold table."""
    closes = [float(row["Close"]) for row in rows]
    result = ThresholdInputs()
    if len(closes) >= 1:
        result.close_t = closes[-1]
    if window > 1 and len(closes) >= window - 1:
        result.ma_sum_previous = sum(closes[-(window - 1):])
    if len(closes) >= 5:
        result.close_t_minus_5_next = closes[-5]

    payload = result.as_dict()
    payload["window"] = window
    return payload


def _calc_threshold_row(ticker: str, ma_rule: str, window: int, inputs: Dict[str, Any], display_price_ndigits: int) -> Dict[str, Any]:
    """Compute a threshold row for the next-close report section."""
    close_t = inputs.get("close_t")
    ma_sum_prev = inputs.get("ma_sum_prev")
    close_t_minus_5_next = inputs.get("close_t_minus_5_next")

    threshold_from_ma = None
    threshold_from_t_minus_5 = None
    threshold_final = None

    if window > 1 and ma_sum_prev is not None:
        threshold_from_ma = float(ma_sum_prev) / float(window - 1)
    if close_t_minus_5_next is not None:
        threshold_from_t_minus_5 = float(close_t_minus_5_next)

    candidates = [value for value in [threshold_from_ma, threshold_from_t_minus_5] if value is not None]
    if candidates:
        threshold_final = max(candidates)

    display = f"{format_fixed(threshold_final, display_price_ndigits)}+" if threshold_final is not None else None
    normalized_ma_rule = _normalize_ma_rule(ma_rule)
    return {
        "ticker": ticker,
        "ma_rule": normalized_ma_rule,
        "window": window,
        "close_t": close_t,
        "ma_sum_prev": ma_sum_prev,
        "close_t_minus_5_next": close_t_minus_5_next,
        "threshold_from_ma": threshold_from_ma,
        "threshold_from_t_minus_5": threshold_from_t_minus_5,
        "threshold": threshold_final,
        "display": display,
    }


def _estimate_tactical_buy_budget_usd(states: Dict[str, Any]) -> float:
    """Estimate deployable tactical cash from the portfolio state."""
    cash = ((states.get("portfolio") or {}).get("cash") or {})
    deployable = cash.get("deployable_usd")
    if deployable is not None:
        return max(float(deployable), 0.0)
    return max(float(cash.get("usd") or 0.0), 0.0)


def _lookup_action_price_usd(states: Dict[str, Any], ticker: str) -> Optional[float]:
    """Resolve the action price used by tactical allocation."""
    market = states.get("market") or {}
    prices_now = market.get("prices_now") or {}
    if ticker in prices_now and prices_now[ticker] is not None:
        try:
            return float(prices_now[ticker])
        except Exception:
            return None
    history_map = market.get("_runtime_history")
    if not isinstance(history_map, dict):
        history_map = market.get("history_400d") or {}
    history = ((history_map.get(ticker) or {}).get("rows")) or []
    if history:
        try:
            return float(history[-1].get("Close"))
        except Exception:
            return None
    return None


def _allocate_buy_shares_across_triggered_signals(
    candidates: List[Dict[str, Any]],
    investable_cash_usd: float,
) -> Dict[str, int]:
    """Allocate BUY shares across triggered tactical signals."""
    epsilon = 1e-9
    budget = max(0.0, float(investable_cash_usd or 0.0))

    cleaned: List[Dict[str, Any]] = []
    for item in candidates or []:
        ticker = str(item.get("ticker") or "").upper().strip()
        try:
            price = float(item.get("price_usd"))
        except Exception:
            continue
        if not ticker or price <= 0:
            continue
        cleaned.append({"ticker": ticker, "price_usd": price})

    if not cleaned:
        return {}

    cleaned.sort(key=lambda item: (item["price_usd"], item["ticker"]))
    if budget + epsilon < cleaned[0]["price_usd"]:
        return {item["ticker"]: 0 for item in cleaned}

    full_one_share_cost = sum(item["price_usd"] for item in cleaned)
    if budget + epsilon >= full_one_share_cost:
        chosen = list(cleaned)
    else:
        chosen = []
        used = 0.0
        for item in cleaned:
            if used + item["price_usd"] <= budget + epsilon:
                chosen.append(item)
                used += item["price_usd"]
            else:
                break

    shares = {item["ticker"]: 0 for item in cleaned}
    if not chosen:
        return shares

    for item in chosen:
        shares[item["ticker"]] = 1

    used = sum(item["price_usd"] for item in chosen)
    remaining = budget - used
    per_ticker_budget = (remaining / len(chosen)) if chosen else 0.0

    for item in chosen:
        additional = int(per_ticker_budget // item["price_usd"])
        if additional > 0:
            shares[item["ticker"]] += additional

    used = sum(shares[item["ticker"]] * item["price_usd"] for item in chosen)
    remaining = budget - used

    chosen_desc = sorted(chosen, key=lambda item: (-item["price_usd"], item["ticker"]))
    while True:
        target = None
        for item in chosen_desc:
            if item["price_usd"] <= remaining + epsilon:
                target = item
                break
        if target is None:
            break
        shares[target["ticker"]] += 1
        remaining -= target["price_usd"]

    return shares
