# Copyright (c) 2026 Sheng-Hsin Tsai
# SPDX-License-Identifier: MIT

from __future__ import annotations

from typing import Any, Dict, List, Optional

from core.models import TacticalPlan
from core.strategy import (
    _allocate_buy_shares_across_triggered_signals,
    _calc_threshold_row,
    _derive_signals_inputs_from_history,
    _derive_threshold_inputs_from_history,
    _estimate_tactical_buy_budget_usd,
    _normalize_ma_rule,
    _parse_indicator_window,
)
from utils.config_access import config_buy_fee_rate, config_sell_fee_rate, config_tactical_indicators
from utils.precision import round_with_precision, state_engine_numeric_precision


def _buy_sizing_price_usd(action_price_usd: Any, fee_rate: Any) -> Optional[float]:
    """Convert a quoted buy price into the effective per-share cash cost for sizing."""
    try:
        price = float(action_price_usd)
    except Exception:
        return None
    if price <= 0:
        return None
    try:
        applied_fee_rate = max(float(fee_rate or 0.0), 0.0)
    except Exception:
        applied_fee_rate = 0.0
    return price * (1.0 + applied_fee_rate)


def _sell_reclaim_price_usd(action_price_usd: Any, fee_rate: Any) -> Optional[float]:
    """Convert a quoted sell price into the effective per-share cash reclaim after fees."""
    try:
        price = float(action_price_usd)
    except Exception:
        return None
    if price <= 0:
        return None
    try:
        applied_fee_rate = max(float(fee_rate or 0.0), 0.0)
    except Exception:
        applied_fee_rate = 0.0
    return price * max(1.0 - applied_fee_rate, 0.0)


def compute_tactical_plan(
    states: Dict[str, Any],
    runtime: Dict[str, Any],
    *,
    derive_signals_inputs: str,
    derive_threshold_inputs: str,
    mode: Optional[str] = None,
    trades: Optional[List[Dict[str, Any]]] = None,
) -> TacticalPlan:
    """Compute tactical signals and actions without mutating persisted state."""
    from core import state_engine as runtime_support

    config = runtime_support._runtime_config(runtime)
    numeric_precision = state_engine_numeric_precision(config)
    buy_fee_rate = max(float(config_buy_fee_rate(config) or 0.0), 0.0)
    sell_fee_rate = max(float(config_sell_fee_rate(config) or 0.0), 0.0)
    tactical_indicators = config_tactical_indicators(config) or {
        "GOOG": "SMA50",
        "SMH": "SMA100",
        "NVDA": "SMA50",
    }
    market = states.get("market", {}) or {}
    history = runtime_support._market_history_rows_map(runtime)
    signal_day_et = runtime_support._runtime_signal_basis_day(runtime)
    fx_tickers = runtime_support._fx_tickers_from_config(runtime)
    signals_inputs = dict(market.get("signals_inputs") or {})
    threshold_inputs = dict(market.get("next_close_threshold_inputs") or {})
    trades = trades if isinstance(trades, list) else []

    for ticker, ind_spec in tactical_indicators.items():
        ma_rule = _normalize_ma_rule(ind_spec)
        window = _parse_indicator_window(ma_rule) or 0
        rows = (history.get(ticker) or {}).get("rows") or []
        if signal_day_et and ticker not in fx_tickers:
            rows = runtime_support._history_rows_on_or_before(rows, signal_day_et)
        if derive_signals_inputs != "never":
            if derive_signals_inputs == "force" or (
                derive_signals_inputs == "missing" and ticker not in signals_inputs
            ):
                if rows:
                    signals_inputs[ticker] = _derive_signals_inputs_from_history(rows, window)
        if derive_threshold_inputs != "never":
            if derive_threshold_inputs == "force" or (
                derive_threshold_inputs == "missing" and ticker not in threshold_inputs
            ):
                if rows:
                    threshold_inputs[ticker] = _derive_threshold_inputs_from_history(rows, window)

    portfolio = states.get("portfolio", {}) or {}
    positions = portfolio.get("positions", []) or []

    def tactical_shares(ticker: str) -> int:
        for position in positions:
            if position.get("ticker") == ticker and position.get("bucket") in {"tactical", "tactical_cash_pool"}:
                return int(position.get("shares") or 0)
        return 0

    pre_rows: List[Dict[str, Any]] = []
    buy_candidates: List[Dict[str, Any]] = []
    sell_candidates: List[Dict[str, Any]] = []
    buy_candidate_seen: set[str] = set()

    for ticker, ind_spec in tactical_indicators.items():
        ma_rule = _normalize_ma_rule(ind_spec)
        inp = signals_inputs.get(ticker) or {}
        close_t = inp.get("close_t")
        ma_t = inp.get("ma_t")
        close_t_minus_5 = inp.get("close_t_minus_5")
        shares_pre = tactical_shares(ticker)
        ma_ok = bool(close_t is not None and ma_t is not None and (close_t > ma_t))
        close_t_minus_5_ok = bool(
            close_t_minus_5 is not None and close_t is not None and (close_t > close_t_minus_5)
        )
        buy_signal = bool(ma_ok and close_t_minus_5_ok)
        action_price_usd = runtime_support._lookup_action_price_usd(states, runtime, ticker)
        buy_sizing_price_usd = _buy_sizing_price_usd(action_price_usd, buy_fee_rate)
        sell_reclaim_price_usd = _sell_reclaim_price_usd(action_price_usd, sell_fee_rate)
        sell_signal = bool((not buy_signal) and (shares_pre > 0))
        row = {
            "ticker": ticker,
            "close_t": close_t,
            "ma_rule": ma_rule,
            "ma_t": ma_t,
            "close_t_minus_5": close_t_minus_5,
            "a_gt_b": ma_ok,
            "a_gt_c": close_t_minus_5_ok,
            "buy_signal": buy_signal,
            "buy_signal_ma_ok": ma_ok,
            "buy_signal_close_t_minus_5_ok": close_t_minus_5_ok,
            "sell_signal": sell_signal,
            "close_t_minus_5_ignored": False,
            "close_gt_ma_label": "TRUE" if ma_ok else "FALSE",
            "tactical_shares_pre": shares_pre,
            "action_price_usd": action_price_usd,
        }
        pre_rows.append(row)
        if buy_signal and buy_sizing_price_usd is not None and ticker not in buy_candidate_seen:
            buy_candidates.append({"ticker": ticker, "price_usd": buy_sizing_price_usd})
            buy_candidate_seen.add(ticker)
        if sell_signal and sell_reclaim_price_usd is not None and sell_reclaim_price_usd > 0:
            sell_candidates.append(
                {"ticker": ticker, "price_usd": float(sell_reclaim_price_usd), "shares_pre": shares_pre}
            )

    investable_cash_base_usd = _estimate_tactical_buy_budget_usd(states)
    estimated_sell_reclaim_usd = round_with_precision(
        sum(float(item["price_usd"]) * int(item["shares_pre"]) for item in sell_candidates),
        int(numeric_precision["usd_amount"]),
    )
    investable_cash_usd = round_with_precision(float(investable_cash_base_usd) + float(estimated_sell_reclaim_usd), int(numeric_precision["usd_amount"]))
    buy_alloc = _allocate_buy_shares_across_triggered_signals(buy_candidates, investable_cash_usd)

    tactical_rows: List[Dict[str, Any]] = []
    buy_alloc_assigned: Dict[str, bool] = {}
    for row in pre_rows:
        ticker = row["ticker"]
        buy_signal = bool(row["buy_signal"])
        sell_signal = bool(row.get("sell_signal"))
        shares_pre = int(row["tactical_shares_pre"] or 0)
        action_price_usd = row.get("action_price_usd")
        buy_sizing_price_usd = _buy_sizing_price_usd(action_price_usd, buy_fee_rate)
        if sell_signal:
            action = "SELL_ALL"
            action_shares = shares_pre
        elif buy_signal:
            if buy_alloc_assigned.get(ticker):
                action_shares = 0
            else:
                action_shares = int(buy_alloc.get(ticker) or 0)
                if action_shares > 0:
                    buy_alloc_assigned[ticker] = True
            if action_shares > 0 and shares_pre > 0:
                action = "BUY_MORE"
            elif action_shares > 0 and shares_pre == 0:
                action = "BUY"
            elif shares_pre > 0:
                action = "HOLD"
                action_shares = 0
            else:
                action = "BUY"
                action_shares = 0
        else:
            action = "NO_ACTION"
            action_shares = 0
        tactical_rows.append(
            {
                **row,
                "investable_cash_base_usd": investable_cash_base_usd,
                "estimated_sell_reclaim_usd": estimated_sell_reclaim_usd,
                "investable_cash_usd": investable_cash_usd,
                "t_plus_1_action": action,
                "action_shares": action_shares,
                "action_cash_amount_usd": round_with_precision(float(buy_sizing_price_usd) * action_shares, int(numeric_precision["usd_amount"]))
                if action in {"BUY", "BUY_MORE"} and buy_sizing_price_usd is not None
                else 0.0,
            }
        )

    threshold_rows: List[Dict[str, Any]] = []
    for ticker, ind_spec in tactical_indicators.items():
        ma_rule = _normalize_ma_rule(ind_spec)
        window = _parse_indicator_window(ma_rule) or 0
        inp = threshold_inputs.get(ticker) or {}
        if not inp and derive_threshold_inputs != "never" and (history.get(ticker) or {}).get("rows"):
            inp = _derive_threshold_inputs_from_history(history[ticker]["rows"], window)
            threshold_inputs[ticker] = inp
        threshold_rows.append(_calc_threshold_row(ticker, ma_rule, window, inp, display_price_ndigits=int(numeric_precision["display_price"])))

    return TacticalPlan(
        signals_inputs=signals_inputs,
        threshold_inputs=threshold_inputs,
        tactical_rows=tactical_rows,
        threshold_rows=threshold_rows,
    )


def apply_tactical_plan(states: Dict[str, Any], plan: TacticalPlan) -> None:
    """Persist a computed tactical plan back into the mutable state snapshot."""
    market = states.setdefault("market", {})
    market["signals_inputs"] = plan.signals_inputs
    market["next_close_threshold_inputs"] = plan.threshold_inputs
    states.setdefault("signals", {})["tactical"] = plan.tactical_rows
    states.setdefault("thresholds", {})["buy_signal_close_price_thresholds"] = plan.threshold_rows
