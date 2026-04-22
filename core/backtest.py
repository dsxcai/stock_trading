# Copyright (c) 2026 Sheng-Hsin Tsai
# SPDX-License-Identifier: MIT

from __future__ import annotations

from bisect import bisect_left, bisect_right
import copy
import csv
import json
from datetime import date
from pathlib import Path
import builtins
from typing import Any, Dict, List, Optional, Tuple

from core import state_engine as live_runtime
from core.models import BacktestCostModel
from core.strategy import _allocate_buy_shares_across_triggered_signals, _read_ohlcv_csv
from core.tactical_engine import compute_tactical_plan
from utils.config_access import (
    config_csv_sources,
    config_tactical_indicators,
    load_json_object,
    load_state_engine_config,
)
from utils.precision import (
    format_currency,
    format_percent_from_ratio,
    normalize_numeric_precision,
    round_with_precision,
    state_engine_numeric_precision,
)


def _backtest_cfg(raw_config: Dict[str, Any]) -> Dict[str, Any]:
    cfg = raw_config.get("backtest")
    if not isinstance(cfg, dict):
        raise KeyError("backtest_config.json must contain object key 'backtest'")
    return cfg


def _nested_config_value(container: Dict[str, Any], *path: str) -> Any:
    current: Any = container
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _resolve_backtest_runtime_config(raw_config: Dict[str, Any], runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    resolved = copy.deepcopy(runtime_config)
    backtest_cfg = _backtest_cfg(raw_config)
    tactical_cfg = backtest_cfg.get("tactical") or {}
    cost_cfg = backtest_cfg.get("costs") or {}
    if not isinstance(tactical_cfg, dict):
        raise KeyError("config.backtest.tactical must be an object")
    if not isinstance(cost_cfg, dict):
        raise KeyError("config.backtest.costs must be an object")

    if cost_cfg.get("fee_rate") is None:
        raise KeyError("config.backtest.costs.fee_rate must be configured")
    resolved.setdefault("execution", {})["fee_rate"] = float(cost_cfg.get("fee_rate") or 0.0)

    indicators_override = tactical_cfg.get("indicators")
    if indicators_override is not None:
        if not isinstance(indicators_override, dict) or not indicators_override:
            raise KeyError("config.backtest.tactical.indicators must be an object when configured")
        resolved.setdefault("strategy", {}).setdefault("tactical", {})["indicators"] = copy.deepcopy(indicators_override)
    resolved_indicators = config_tactical_indicators(resolved)
    if not resolved_indicators:
        raise KeyError("state_engine.strategy.tactical.indicators must be configured")

    tickers_override = tactical_cfg.get("tickers")
    if isinstance(tickers_override, list) and tickers_override:
        resolved_tickers = [str(ticker).upper() for ticker in tickers_override]
    else:
        resolved_tickers = sorted(str(ticker).upper() for ticker in resolved_indicators.keys())
    resolved.setdefault("portfolio", {}).setdefault("buckets", {}).setdefault("tactical", {})["tickers"] = resolved_tickers

    return resolved


def _load_backtest_config(config_path: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    raw = load_json_object(config_path)
    runtime_config_ref = str(raw.get("runtime_config") or "").strip()
    if not runtime_config_ref:
        raise KeyError("backtest_config.json must contain string key 'runtime_config'")
    runtime_config_path = Path(runtime_config_ref)
    if not runtime_config_path.is_absolute():
        runtime_config_path = Path(config_path).resolve().parent / runtime_config_path
    runtime_config = _resolve_backtest_runtime_config(raw, load_state_engine_config(str(runtime_config_path)))
    return raw, runtime_config


def _precision_digits(precision: Dict[str, int], key: str) -> int:
    return int(precision[key])


def _round_precision(value: Any, precision: Dict[str, int], key: str) -> float:
    return round_with_precision(value, _precision_digits(precision, key))


def _bucket_value_snapshot(states: Dict[str, Any], precision: Dict[str, int]) -> Dict[str, float]:
    portfolio = states.get("portfolio", {}) or {}
    totals = portfolio.get("totals", {}) or {}
    core = (totals.get("core") or {}).get("holdings_mv_usd")
    tactical = (totals.get("tactical") or {}).get("total_assets_usd")
    portfolio_nav = (totals.get("portfolio") or {}).get("nav_usd")
    return {
        "core": _round_precision(core or 0.0, precision, "backtest_amount"),
        "tactical": _round_precision(tactical or 0.0, precision, "backtest_amount"),
        "portfolio": _round_precision(portfolio_nav or 0.0, precision, "backtest_amount"),
    }


def _profit_rate(start_value: float, end_value: float, precision: Dict[str, int]) -> Optional[float]:
    start = float(start_value or 0.0)
    if abs(start) <= 1e-12:
        return None
    return _round_precision((float(end_value) - start) / start, precision, "backtest_rate")


def _bucket_return_summary(
    starting_values: Dict[str, float],
    ending_values: Dict[str, float],
    precision: Dict[str, int],
) -> Dict[str, Dict[str, Optional[float]]]:
    out: Dict[str, Dict[str, Optional[float]]] = {}
    for bucket in ("core", "tactical", "portfolio"):
        start_value = _round_precision(starting_values.get(bucket) or 0.0, precision, "backtest_amount")
        end_value = _round_precision(ending_values.get(bucket) or 0.0, precision, "backtest_amount")
        profit = _round_precision(end_value - start_value, precision, "backtest_amount")
        out[bucket] = {
            "start_value_usd": start_value,
            "end_value_usd": end_value,
            "profit_usd": profit,
            "profit_rate": _profit_rate(start_value, end_value, precision),
        }
    return out


def _comparison_summary(
    strategy_buckets: Dict[str, Dict[str, Optional[float]]],
    benchmark_buckets: Dict[str, Dict[str, Optional[float]]],
    precision: Dict[str, int],
) -> Dict[str, Dict[str, Optional[float]]]:
    out: Dict[str, Dict[str, Optional[float]]] = {}
    for bucket in ("core", "tactical", "portfolio"):
        strategy = strategy_buckets.get(bucket) or {}
        benchmark = benchmark_buckets.get(bucket) or {}
        strategy_profit_rate = strategy.get("profit_rate")
        benchmark_profit_rate = benchmark.get("profit_rate")
        excess_profit_rate = None
        if strategy_profit_rate is not None and benchmark_profit_rate is not None:
            excess_profit_rate = _round_precision(float(strategy_profit_rate) - float(benchmark_profit_rate), precision, "backtest_rate")
        out[bucket] = {
            "strategy_profit_rate": strategy_profit_rate,
            "buy_and_hold_profit_rate": benchmark_profit_rate,
            "excess_profit_rate": excess_profit_rate,
            "strategy_profit_usd": strategy.get("profit_usd"),
            "buy_and_hold_profit_usd": benchmark.get("profit_usd"),
            "excess_profit_usd": _round_precision(float(strategy.get("profit_usd") or 0.0) - float(benchmark.get("profit_usd") or 0.0), precision, "backtest_amount"),
        }
    return out


def _resolve_csv_path(runtime_config: Dict[str, Any], csv_dir: str, ticker: str) -> Path:
    csv_sources = config_csv_sources(runtime_config)
    source = str(csv_sources.get(ticker) or f"{ticker}.csv")
    path = Path(source)
    if not path.is_absolute():
        path = Path(csv_dir) / source
    return path


def _load_history_map(
    runtime_config: Dict[str, Any],
    csv_dir: str,
    tickers: List[str],
    *,
    allow_incomplete_rows: bool = False,
    bypass_option_hint: str = "--allow-incomplete-csv-rows",
) -> Dict[str, Dict[str, Any]]:
    history_map: Dict[str, Dict[str, Any]] = {}
    for ticker in tickers:
        csv_path = _resolve_csv_path(runtime_config, csv_dir, ticker)
        rows = _read_ohlcv_csv(
            str(csv_path),
            keep_last_n=None,
            allow_incomplete_rows=allow_incomplete_rows,
            bypass_option_hint=bypass_option_hint,
        )
        history_map[ticker] = {
            "columns": ["Date", "Open", "High", "Low", "Close", "Volume"],
            "rows": rows,
            "source": csv_path.name,
        }
    return history_map


def _common_trading_dates(history_map: Dict[str, Dict[str, Any]]) -> List[str]:
    common: Optional[set[str]] = None
    for payload in history_map.values():
        dates = {str(row.get("Date")) for row in (payload.get("rows") or []) if row.get("Date")}
        if common is None:
            common = dates
        else:
            common &= dates
    return sorted(common or [])


def _warmup_bars(runtime_config: Dict[str, Any]) -> int:
    return max(
        6,
        max(
            (live_runtime._parse_indicator_window(rule) or 0)
            for rule in config_tactical_indicators(runtime_config).values()
        ),
    )


def _normalize_date_arg(value: Optional[str], *, label: str) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise ValueError(f"{label} must be YYYY-MM-DD") from exc


def _select_backtest_dates(
    trading_dates: List[str],
    runtime_config: Dict[str, Any],
    raw_config: Dict[str, Any],
    lookback_trading_days: Optional[int] = None,
    start_date_et: Optional[str] = None,
    end_date_et: Optional[str] = None,
    warmup_bars: Optional[int] = None,
) -> List[str]:
    if not trading_dates:
        return []

    if warmup_bars is None:
        warmup_bars = _warmup_bars(runtime_config)
    start_date_et = _normalize_date_arg(start_date_et, label="start_date")
    end_date_et = _normalize_date_arg(end_date_et, label="end_date")
    if start_date_et and end_date_et and start_date_et > end_date_et:
        raise ValueError("start_date must be on or before end_date")
    first_common_date = trading_dates[0]
    last_common_date = trading_dates[-1]
    if start_date_et and start_date_et < first_common_date:
        raise ValueError("start_date is earlier than the first common trading day")
    if end_date_et and end_date_et > last_common_date:
        raise ValueError("end_date is later than the last common trading day")

    end_idx = len(trading_dates) - 1
    if end_date_et:
        end_idx = bisect_right(trading_dates, end_date_et) - 1
        if end_idx < 0:
            raise ValueError("end_date is earlier than the first common trading day")
    bounded_dates = trading_dates[: end_idx + 1]
    if len(bounded_dates) <= warmup_bars:
        raise ValueError("not enough common trading days after applying end_date")

    if start_date_et:
        report_start_idx = bisect_left(bounded_dates, start_date_et)
        if report_start_idx >= len(bounded_dates):
            raise ValueError("start_date is later than the last selected trading day")
        warmup_start_idx = max(0, report_start_idx - (warmup_bars - 1))
        if report_start_idx - warmup_start_idx < warmup_bars - 1:
            raise ValueError("not enough warmup history before the requested start_date")
        return bounded_dates[warmup_start_idx:]

    if lookback_trading_days is None:
        lookback_trading_days = int(_nested_config_value(_backtest_cfg(raw_config), "lookback_trading_days") or 252)
    if lookback_trading_days <= 0:
        lookback_trading_days = 252
    keep = min(len(bounded_dates), lookback_trading_days + warmup_bars)
    return bounded_dates[-keep:]


def _slice_history_map(history_map: Dict[str, Dict[str, Any]], asof_et: str) -> Dict[str, Dict[str, Any]]:
    sliced: Dict[str, Dict[str, Any]] = {}
    for ticker, payload in history_map.items():
        rows = [dict(row) for row in (payload.get("rows") or []) if str(row.get("Date") or "") <= asof_et]
        sliced[ticker] = {
            "columns": list(payload.get("columns") or []),
            "rows": rows,
            "source": payload.get("source"),
        }
    return sliced


def _row_map_for_date(history_map: Dict[str, Dict[str, Any]], trade_date_et: str) -> Dict[str, Dict[str, Any]]:
    row_map: Dict[str, Dict[str, Any]] = {}
    for ticker, payload in history_map.items():
        for row in payload.get("rows") or []:
            if str(row.get("Date") or "") == trade_date_et:
                row_map[ticker] = dict(row)
                break
    return row_map


def _close_price_map(history_map: Dict[str, Dict[str, Any]], trade_date_et: str) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for ticker, row in _row_map_for_date(history_map, trade_date_et).items():
        try:
            out[ticker] = float(row.get("Close"))
        except Exception:
            continue
    return out


def _reprice_state_for_date(
    states: Dict[str, Any],
    runtime: Dict[str, Any],
    history_map: Dict[str, Dict[str, Any]],
    trade_date_et: str,
    precision: Dict[str, int],
) -> Dict[str, float]:
    states.setdefault("market", {})["asof_t_et"] = trade_date_et
    states.setdefault("market", {})["prices_now"] = _close_price_map(history_map, trade_date_et)
    runtime["history"] = _slice_history_map(history_map, trade_date_et)
    live_runtime._reprice_and_totals(states, runtime)
    return _bucket_value_snapshot(states, precision)


def _curve_row_from_state(
    states: Dict[str, Any],
    trade_date_et: str,
    *,
    trade_count: int,
    precision: Dict[str, int],
) -> Dict[str, Any]:
    portfolio = states.get("portfolio", {}) or {}
    cash = portfolio.get("cash", {}) or {}
    totals = portfolio.get("totals", {}) or {}
    bucket_values = _bucket_value_snapshot(states, precision)
    return {
        "date_et": trade_date_et,
        "nav_usd": _round_precision(bucket_values["portfolio"], precision, "backtest_amount"),
        "cash_usd": _round_precision(cash.get("usd") or 0.0, precision, "backtest_amount"),
        "holdings_mv_usd": _round_precision((totals.get("portfolio") or {}).get("holdings_mv_usd") or 0.0, precision, "backtest_amount"),
        "core_value_usd": _round_precision(bucket_values["core"], precision, "backtest_amount"),
        "tactical_value_usd": _round_precision(bucket_values["tactical"], precision, "backtest_amount"),
        "portfolio_value_usd": _round_precision(bucket_values["portfolio"], precision, "backtest_amount"),
        "trade_count": int(trade_count),
    }


def _mid_price(row: Dict[str, Any]) -> float:
    return (float(row.get("Open")) + float(row.get("Close"))) / 2.0


def _apply_slippage(mid_price: float, side: str, cost_model: BacktestCostModel, include_costs: bool) -> float:
    if not include_costs:
        return float(mid_price)
    bps = max(float(cost_model.slippage_bps or 0.0), 0.0) / 10000.0
    if str(side).upper().startswith("B"):
        return float(mid_price) * (1.0 + bps)
    return float(mid_price) * (1.0 - bps)


def _trade_fee(gross: float, cost_model: BacktestCostModel, include_costs: bool, precision: Dict[str, int]) -> float:
    if not include_costs:
        return 0.0
    fee = float(gross) * max(float(cost_model.fee_rate or 0.0), 0.0)
    fee += max(float(cost_model.commission_per_trade or 0.0), 0.0)
    return _round_precision(fee, precision, "backtest_amount")


def _buy_cash_amount(shares: int, unit_price: float, cost_model: BacktestCostModel, include_costs: bool, precision: Dict[str, int]) -> float:
    gross = _round_precision(float(unit_price) * int(shares), precision, "backtest_amount")
    fee = _trade_fee(gross, cost_model, include_costs, precision)
    return _round_precision(gross + fee, precision, "backtest_amount")


def _sell_cash_amount(shares: int, unit_price: float, cost_model: BacktestCostModel, include_costs: bool, precision: Dict[str, int]) -> float:
    gross = _round_precision(float(unit_price) * int(shares), precision, "backtest_amount")
    fee = _trade_fee(gross, cost_model, include_costs, precision)
    return _round_precision(max(gross - fee, 0.0), precision, "backtest_amount")


def _max_affordable_buy_shares(
    desired_shares: int,
    unit_price: float,
    available_cash_usd: float,
    cost_model: BacktestCostModel,
    include_costs: bool,
) -> int:
    if desired_shares <= 0 or unit_price <= 0 or available_cash_usd <= 0:
        return 0
    if not include_costs:
        return min(desired_shares, int(available_cash_usd // unit_price))
    commission = max(float(cost_model.commission_per_trade or 0.0), 0.0)
    fee_rate = max(float(cost_model.fee_rate or 0.0), 0.0)
    if available_cash_usd <= commission:
        return 0
    per_share = float(unit_price) * (1.0 + fee_rate)
    if per_share <= 0:
        return 0
    affordable = int((available_cash_usd - commission) // per_share)
    return max(0, min(int(desired_shares), affordable))


def _trade_time_tw_for_execution(exec_date_et: str, buy_order: bool) -> str:
    time_part = "22:30:01" if buy_order else "22:30:00"
    return str(exec_date_et).replace("-", "/") + f" {time_part}"


def _make_trade_row(
    *,
    trade_id: Any,
    signal_date_et: str,
    exec_date_et: str,
    ticker: str,
    side: str,
    shares: int,
    unit_price: float,
    cost_model: BacktestCostModel,
    include_costs: bool,
    precision: Dict[str, int],
    notes: str = "",
) -> Dict[str, Any]:
    gross = _round_precision(float(unit_price) * int(shares), precision, "backtest_amount")
    fee = _trade_fee(gross, cost_model, include_costs, precision)
    if str(side).upper().startswith("B"):
        cash_amount = _round_precision(gross + fee, precision, "backtest_amount")
    else:
        cash_amount = _round_precision(max(gross - fee, 0.0), precision, "backtest_amount")
    return {
        "trade_id": trade_id,
        "trade_date_et": exec_date_et,
        "time_tw": _trade_time_tw_for_execution(exec_date_et, buy_order=str(side).upper().startswith("B")),
        "ticker": str(ticker).upper(),
        "side": str(side).upper(),
        "shares": int(shares),
        "price": _round_precision(unit_price, precision, "backtest_price"),
        "gross": gross,
        "fee": fee,
        "cash_amount": cash_amount,
        "notes": notes or f"signal_date_et={signal_date_et}",
        "source": "backtest",
    }


def _execute_plan_rows(
    *,
    plan_rows: List[Dict[str, Any]],
    signal_date_et: str,
    exec_date_et: str,
    exec_row_map: Dict[str, Dict[str, Any]],
    current_deployable_cash_usd: float,
    cost_model: BacktestCostModel,
    include_costs: bool,
    next_trade_id: int,
    precision: Dict[str, int],
) -> Tuple[List[Dict[str, Any]], float, int]:
    trades: List[Dict[str, Any]] = []
    deployable_cash_usd = _round_precision(current_deployable_cash_usd, precision, "backtest_amount")

    for row in plan_rows:
        if str(row.get("t_plus_1_action") or "") != "SELL_ALL":
            continue
        ticker = str(row.get("ticker") or "").upper()
        exec_row = exec_row_map.get(ticker)
        if not exec_row:
            continue
        shares = int(row.get("action_shares") or 0)
        if shares <= 0:
            continue
        unit_price = _apply_slippage(_mid_price(exec_row), "SELL", cost_model, include_costs)
        trade = _make_trade_row(
            trade_id=next_trade_id,
            signal_date_et=signal_date_et,
            exec_date_et=exec_date_et,
            ticker=ticker,
            side="SELL",
            shares=shares,
            unit_price=unit_price,
            cost_model=cost_model,
            include_costs=include_costs,
            precision=precision,
        )
        trades.append(trade)
        next_trade_id += 1
        deployable_cash_usd = _round_precision(deployable_cash_usd + float(trade["cash_amount"]), precision, "backtest_amount")

    for row in plan_rows:
        action = str(row.get("t_plus_1_action") or "")
        if action not in {"BUY", "BUY_MORE"}:
            continue
        ticker = str(row.get("ticker") or "").upper()
        exec_row = exec_row_map.get(ticker)
        if not exec_row:
            continue
        desired_shares = int(row.get("action_shares") or 0)
        if desired_shares <= 0:
            continue
        unit_price = _apply_slippage(_mid_price(exec_row), "BUY", cost_model, include_costs)
        shares = _max_affordable_buy_shares(
            desired_shares,
            unit_price,
            deployable_cash_usd,
            cost_model,
            include_costs,
        )
        if shares <= 0:
            continue
        trade = _make_trade_row(
            trade_id=next_trade_id,
            signal_date_et=signal_date_et,
            exec_date_et=exec_date_et,
            ticker=ticker,
            side="BUY",
            shares=shares,
            unit_price=unit_price,
            cost_model=cost_model,
            include_costs=include_costs,
            precision=precision,
        )
        trades.append(trade)
        next_trade_id += 1
        deployable_cash_usd = _round_precision(deployable_cash_usd - float(trade["cash_amount"]), precision, "backtest_amount")

    return trades, deployable_cash_usd, next_trade_id


def _max_drawdown_pct(nav_values: List[float], precision: Dict[str, int]) -> float:
    peak = None
    max_drawdown = 0.0
    for nav in nav_values:
        value = float(nav)
        peak = value if peak is None else max(peak, value)
        if peak > 0:
            drawdown = (value - peak) / peak
            max_drawdown = min(max_drawdown, drawdown)
    return _round_precision(max_drawdown, precision, "backtest_rate")


def _make_backtest_seed_state(starting_cash: float, precision: Dict[str, int]) -> Dict[str, Any]:
    return {
        "market": {
            "prices_now": {},
        },
        "portfolio": {
            "positions": [],
            "cash": {
                "usd": _round_precision(starting_cash, precision, "backtest_amount"),
                "deployable_usd": _round_precision(starting_cash, precision, "backtest_amount"),
                "reserve_usd": 0.0,
                "bucket": "tactical_pool",
                "net_external_cash_flow_usd": 0.0,
            },
        },
    }


def _build_initial_state(raw_config: Dict[str, Any], precision: Dict[str, int], starting_cash_override: Optional[float] = None) -> Dict[str, Any]:
    starting_cash_raw = starting_cash_override
    if starting_cash_raw is None:
        starting_cash_raw = _nested_config_value(_backtest_cfg(raw_config), "tactical", "starting_cash")
    starting_cash = _round_precision(starting_cash_raw or 0.0, precision, "backtest_amount")
    if starting_cash <= 0:
        raise ValueError("backtest.tactical.starting_cash must be positive")
    return _make_backtest_seed_state(starting_cash, precision)


def _benchmark_note() -> str:
    return "buy_and_hold deploys the starting tactical cash across tactical tickers on the first executable day and never sells"


def _backtest_cost_model(raw_config: Dict[str, Any]) -> BacktestCostModel:
    cost_cfg = _nested_config_value(_backtest_cfg(raw_config), "costs") or {}
    if not isinstance(cost_cfg, dict):
        raise KeyError("config.backtest.costs must be an object")
    if cost_cfg.get("fee_rate") is None:
        raise KeyError("config.backtest.costs.fee_rate must be configured")
    if cost_cfg.get("commission_per_trade") is None:
        raise KeyError("config.backtest.costs.commission_per_trade must be configured")
    if cost_cfg.get("slippage_bps") is None:
        raise KeyError("config.backtest.costs.slippage_bps must be configured")
    return BacktestCostModel(
        fee_rate=float(cost_cfg.get("fee_rate") or 0.0),
        commission_per_trade=float(cost_cfg.get("commission_per_trade") or 0.0),
        slippage_bps=float(cost_cfg.get("slippage_bps") or 0.0),
    )


def _seed_buy_and_hold_trades(
    states: Dict[str, Any],
    exec_date_et: str,
    exec_row_map: Dict[str, Dict[str, Any]],
    tactical_tickers: List[str],
    cost_model: BacktestCostModel,
    include_costs: bool,
    precision: Dict[str, int],
) -> Tuple[List[Dict[str, Any]], float]:
    portfolio = states.setdefault("portfolio", {})
    cash_block = portfolio.setdefault(
        "cash",
        {"usd": 0.0, "deployable_usd": 0.0, "reserve_usd": 0.0, "bucket": "tactical_pool"},
    )
    deployable_cash = _round_precision(cash_block.get("deployable_usd") or 0.0, precision, "backtest_amount")
    if deployable_cash <= 0:
        return [], deployable_cash

    candidates: List[Dict[str, Any]] = []
    unit_prices: Dict[str, float] = {}
    fee_rate_multiplier = 1.0 + max(float(cost_model.fee_rate or 0.0), 0.0) if include_costs else 1.0
    commission_reserve = 0.0
    for ticker in tactical_tickers:
        exec_row = exec_row_map.get(ticker)
        if not exec_row:
            continue
        unit_price = _apply_slippage(_mid_price(exec_row), "BUY", cost_model, include_costs)
        unit_prices[ticker] = unit_price
        candidates.append({"ticker": ticker, "price_usd": unit_price * fee_rate_multiplier})
        if include_costs:
            commission_reserve += max(float(cost_model.commission_per_trade or 0.0), 0.0)
    if not candidates:
        return [], deployable_cash

    budget_for_allocation = max(deployable_cash - commission_reserve, 0.0)
    alloc = _allocate_buy_shares_across_triggered_signals(candidates, budget_for_allocation)
    trades: List[Dict[str, Any]] = []
    next_trade_id = 1
    for ticker in sorted(unit_prices.keys()):
        desired_shares = int(alloc.get(ticker) or 0)
        if desired_shares <= 0:
            continue
        shares = _max_affordable_buy_shares(
            desired_shares,
            unit_prices[ticker],
            deployable_cash,
            cost_model,
            include_costs,
        )
        if shares <= 0:
            continue
        trade = _make_trade_row(
            trade_id=next_trade_id,
            signal_date_et="buy_and_hold_seed",
            exec_date_et=exec_date_et,
            ticker=ticker,
            side="BUY",
            shares=shares,
            unit_price=unit_prices[ticker],
            cost_model=cost_model,
            include_costs=include_costs,
            precision=precision,
        )
        trades.append(trade)
        next_trade_id += 1
        deployable_cash = _round_precision(deployable_cash - float(trade["cash_amount"]), precision, "backtest_amount")
    return trades, deployable_cash


def _simulate_buy_and_hold_path(
    seed_states: Dict[str, Any],
    runtime_config: Dict[str, Any],
    history_map: Dict[str, Dict[str, Any]],
    trading_dates: List[str],
    tactical_tickers: List[str],
    cost_model: BacktestCostModel,
    precision: Dict[str, int],
    *,
    include_costs: bool,
) -> Dict[str, Any]:
    states = copy.deepcopy(seed_states)
    runtime = {"config": copy.deepcopy(runtime_config), "history": {}}
    live_runtime._ensure_trading_calendar(runtime)
    live_runtime._ensure_cash_buckets(states, usd_amount_ndigits=int(precision["usd_amount"]))
    warmup_bars = _warmup_bars(runtime_config)
    start_date_et = trading_dates[warmup_bars - 1]
    first_exec_date_et = trading_dates[warmup_bars] if len(trading_dates) > warmup_bars else start_date_et

    starting_values = _reprice_state_for_date(states, runtime, history_map, start_date_et, precision)
    cash_block = ((states.get("portfolio") or {}).get("cash") or {})
    starting_cash = _round_precision(cash_block.get("usd") or 0.0, precision, "backtest_amount")
    curve: List[Dict[str, Any]] = [_curve_row_from_state(states, start_date_et, trade_count=0, precision=precision)]
    trades: List[Dict[str, Any]] = []

    if tactical_tickers and len(trading_dates) > warmup_bars:
        benchmark_trades, deployable_cash = _seed_buy_and_hold_trades(
            states,
            first_exec_date_et,
            _row_map_for_date(history_map, first_exec_date_et),
            tactical_tickers,
            cost_model,
            include_costs,
            precision,
        )
        if benchmark_trades:
            trades.extend(benchmark_trades)
            live_runtime._apply_incremental_trades_to_portfolio_fifo(states, runtime, benchmark_trades)
        cash_block = states.setdefault("portfolio", {}).setdefault("cash", {"usd": 0.0})
        reserve_cash = _round_precision(cash_block.get("reserve_usd") or 0.0, precision, "backtest_amount")
        cash_block["deployable_usd"] = _round_precision(deployable_cash, precision, "backtest_amount")
        cash_block["reserve_usd"] = reserve_cash
        cash_block["usd"] = _round_precision(deployable_cash + reserve_cash, precision, "backtest_amount")

    for idx in range(warmup_bars, len(trading_dates)):
        trade_date_et = trading_dates[idx]
        _reprice_state_for_date(states, runtime, history_map, trade_date_et, precision)
        trade_count = len(trades) if trade_date_et == first_exec_date_et else 0
        curve.append(_curve_row_from_state(states, trade_date_et, trade_count=trade_count, precision=precision))

    ending_values = _bucket_value_snapshot(states, precision)
    ending_nav = float(ending_values["portfolio"])
    starting_nav = float(starting_values["portfolio"])
    summary = {
        "start_date_et": start_date_et,
        "end_date_et": curve[-1]["date_et"],
        "signal_days": len(trading_dates) - warmup_bars,
        "starting_cash_usd": starting_cash,
        "starting_nav_usd": _round_precision(starting_nav, precision, "backtest_amount"),
        "ending_nav_usd": _round_precision(ending_nav, precision, "backtest_amount"),
        "profit_usd": _round_precision(ending_nav - starting_nav, precision, "backtest_amount"),
        "profit_rate": _profit_rate(starting_nav, ending_nav, precision),
        "max_drawdown_pct": _max_drawdown_pct([float(row["nav_usd"]) for row in curve], precision),
        "trade_count": len(trades),
        "buy_count": sum(1 for trade in trades if str(trade.get("side") or "").upper() == "BUY"),
        "sell_count": 0,
        "costs_included": bool(include_costs),
        "fee_rate": _round_precision(cost_model.fee_rate or 0.0, precision, "backtest_cost_param"),
        "commission_per_trade": _round_precision(cost_model.commission_per_trade or 0.0, precision, "backtest_cost_param"),
        "slippage_bps": _round_precision(cost_model.slippage_bps or 0.0, precision, "backtest_cost_param"),
    }
    return {
        "summary": summary,
        "equity_curve": curve,
        "trades": trades,
        "bucket_returns": _bucket_return_summary(starting_values, ending_values, precision),
    }


def _simulate_path(
    raw_config: Dict[str, Any],
    runtime_config: Dict[str, Any],
    history_map: Dict[str, Dict[str, Any]],
    trading_dates: List[str],
    precision: Dict[str, int],
    *,
    starting_cash_override: Optional[float],
    include_costs: bool,
) -> Dict[str, Any]:
    if len(trading_dates) < 7:
        raise ValueError("not enough common trading days to simulate the tactical rules")

    tactical_tickers = sorted(str(ticker).upper() for ticker in config_tactical_indicators(runtime_config).keys())
    warmup_bars = _warmup_bars(runtime_config)
    if len(trading_dates) <= warmup_bars:
        raise ValueError("not enough common trading days after warmup to run the backtest")

    cost_model = _backtest_cost_model(raw_config)
    seed_states = _build_initial_state(raw_config, precision, starting_cash_override=starting_cash_override)
    states = copy.deepcopy(seed_states)
    runtime = {"config": copy.deepcopy(runtime_config), "history": {}}
    live_runtime._ensure_trading_calendar(runtime)
    live_runtime._ensure_cash_buckets(states, usd_amount_ndigits=int(precision["usd_amount"]))
    start_date_et = trading_dates[warmup_bars - 1]
    starting_values = _reprice_state_for_date(states, runtime, history_map, start_date_et, precision)
    cash_block = states.setdefault("portfolio", {}).setdefault("cash", {"usd": 0.0})
    deployable_cash_usd = _round_precision(cash_block.get("deployable_usd") or 0.0, precision, "backtest_amount")
    reserve_cash_usd = _round_precision(cash_block.get("reserve_usd") or 0.0, precision, "backtest_amount")
    starting_cash = _round_precision(cash_block.get("usd") or 0.0, precision, "backtest_amount")
    live_runtime._set_initial_investment_usd(states, float(starting_values["portfolio"]), usd_amount_ndigits=int(precision["usd_amount"]))

    next_trade_id = 1
    trades: List[Dict[str, Any]] = []
    equity_curve: List[Dict[str, Any]] = [_curve_row_from_state(states, start_date_et, trade_count=0, precision=precision)]

    for signal_idx in range(warmup_bars - 1, len(trading_dates) - 1):
        signal_date_et = trading_dates[signal_idx]
        exec_date_et = trading_dates[signal_idx + 1]
        signal_history = _slice_history_map(history_map, signal_date_et)
        exec_row_map = _row_map_for_date(history_map, exec_date_et)
        states["market"]["asof_t_et"] = signal_date_et
        states["market"]["prices_now"] = {
            ticker: _mid_price(exec_row_map[ticker])
            for ticker in tactical_tickers
            if ticker in exec_row_map
        }
        runtime["history"] = signal_history
        plan = compute_tactical_plan(
            states,
            runtime,
            derive_signals_inputs="force",
            derive_threshold_inputs="force",
            mode=None,
            trades=trades,
        )
        daily_trades, cash_usd, next_trade_id = _execute_plan_rows(
            plan_rows=plan.tactical_rows,
            signal_date_et=signal_date_et,
            exec_date_et=exec_date_et,
            exec_row_map=exec_row_map,
            current_deployable_cash_usd=deployable_cash_usd,
            cost_model=cost_model,
            include_costs=include_costs,
            next_trade_id=next_trade_id,
            precision=precision,
        )
        deployable_cash_usd = cash_usd
        if daily_trades:
            trades.extend(daily_trades)
            live_runtime._apply_incremental_trades_to_portfolio_fifo(states, runtime, daily_trades)
        cash_block = states.setdefault("portfolio", {}).setdefault("cash", {"usd": 0.0})
        cash_block["deployable_usd"] = _round_precision(deployable_cash_usd, precision, "backtest_amount")
        cash_block["reserve_usd"] = _round_precision(reserve_cash_usd, precision, "backtest_amount")
        cash_block["usd"] = _round_precision(deployable_cash_usd + reserve_cash_usd, precision, "backtest_amount")
        _reprice_state_for_date(states, runtime, history_map, exec_date_et, precision)
        equity_curve.append(_curve_row_from_state(states, exec_date_et, trade_count=len(daily_trades), precision=precision))

    ending_values = _bucket_value_snapshot(states, precision)
    ending_nav = float(ending_values["portfolio"])
    starting_nav = float(starting_values["portfolio"])
    summary = {
        "start_date_et": equity_curve[0]["date_et"],
        "end_date_et": equity_curve[-1]["date_et"],
        "signal_days": len(trading_dates) - warmup_bars,
        "starting_cash_usd": _round_precision(starting_cash, precision, "backtest_amount"),
        "starting_nav_usd": _round_precision(starting_nav, precision, "backtest_amount"),
        "ending_nav_usd": _round_precision(ending_nav, precision, "backtest_amount"),
        "profit_usd": _round_precision(ending_nav - starting_nav, precision, "backtest_amount"),
        "profit_rate": _profit_rate(starting_nav, ending_nav, precision),
        "max_drawdown_pct": _max_drawdown_pct([float(row["nav_usd"]) for row in equity_curve], precision),
        "trade_count": len(trades),
        "buy_count": sum(1 for trade in trades if str(trade.get("side") or "").upper() == "BUY"),
        "sell_count": sum(1 for trade in trades if str(trade.get("side") or "").upper() == "SELL"),
        "costs_included": bool(include_costs),
        "fee_rate": _round_precision(cost_model.fee_rate or 0.0, precision, "backtest_cost_param"),
        "commission_per_trade": _round_precision(cost_model.commission_per_trade or 0.0, precision, "backtest_cost_param"),
        "slippage_bps": _round_precision(cost_model.slippage_bps or 0.0, precision, "backtest_cost_param"),
    }
    benchmark = _simulate_buy_and_hold_path(
        seed_states,
        runtime_config,
        history_map,
        trading_dates,
        tactical_tickers,
        cost_model,
        precision,
        include_costs=include_costs,
    )
    bucket_returns = _bucket_return_summary(starting_values, ending_values, precision)
    return {
        "summary": summary,
        "equity_curve": equity_curve,
        "trades": trades,
        "bucket_returns": bucket_returns,
        "benchmark": benchmark,
        "comparison": _comparison_summary(bucket_returns, benchmark.get("bucket_returns") or {}, precision),
    }


def _normalize_backtest_strategy(strategy: Optional[str]) -> str:
    text = str(strategy or "").strip().lower().replace("_", "-")
    if not text:
        return "tactical"
    if text not in {"tactical", "mean-reversion"}:
        raise ValueError("strategy must be 'tactical' or 'mean-reversion'")
    return text


def _resolve_mean_reversion_params(
    raw_config: Dict[str, Any],
    runtime_config: Dict[str, Any],
    *,
    tickers_override: Optional[List[str]],
    entry_drawdown_pct: Optional[float],
    take_profit_pct: Optional[float],
    stop_loss_pct: Optional[float],
    starting_cash_per_ticker: Optional[float],
) -> Dict[str, Any]:
    cfg = _nested_config_value(_backtest_cfg(raw_config), "mean_reversion") or {}
    tickers = [str(ticker).upper() for ticker in (tickers_override or cfg.get("tickers") or []) if str(ticker or "").strip()]
    if not tickers:
        tactical_cfg = _nested_config_value(_backtest_cfg(raw_config), "tactical") or {}
        tickers = [str(ticker).upper() for ticker in (tactical_cfg.get("tickers") or []) if str(ticker or "").strip()]
    if not tickers:
        tickers = sorted(str(ticker).upper() for ticker in config_tactical_indicators(runtime_config).keys())
    if not tickers:
        raise ValueError("mean-reversion backtest requires tickers from backtest.mean_reversion.tickers, backtest.tactical.tickers, or state_engine.strategy.tactical.indicators")

    def _pct_value(name: str, explicit: Optional[float], default_value: float) -> float:
        value = explicit
        if value is None:
            raw_value = cfg.get(name)
            value = float(raw_value) if raw_value is not None else default_value
        value = float(value)
        if value <= 0 or value >= 1:
            raise ValueError(f"backtest.mean_reversion.{name} must be between 0 and 1")
        return value

    starting_cash_value = starting_cash_per_ticker
    if starting_cash_value is None:
        raw_value = cfg.get("starting_cash_per_ticker")
        if raw_value is None:
            raw_value = _nested_config_value(_backtest_cfg(raw_config), "starting_cash")
        starting_cash_value = float(raw_value or 0.0)
    starting_cash_value = float(starting_cash_value)
    if starting_cash_value <= 0:
        raise ValueError("backtest.mean_reversion.starting_cash_per_ticker must be positive")

    return {
        "tickers": tickers,
        "entry_drawdown_pct": _pct_value("entry_drawdown_pct", entry_drawdown_pct, 0.02),
        "take_profit_pct": _pct_value("take_profit_pct", take_profit_pct, 0.02),
        "stop_loss_pct": _pct_value("stop_loss_pct", stop_loss_pct, 0.07),
        "starting_cash_per_ticker": starting_cash_value,
        "execution_basis": "signal_on_close_t_execute_on_mid_t_plus_1",
        "anchor_reset": "trade_execution_day_close",
        "strategy": "mean-reversion",
    }


def _mean_reversion_curve_row(
    *,
    ticker: str,
    trade_date_et: str,
    cash_usd: float,
    shares: int,
    close_price: float,
    trade_count: int,
    precision: Dict[str, int],
) -> Dict[str, Any]:
    holdings_mv = _round_precision(float(close_price) * int(shares), precision, "backtest_amount")
    nav = _round_precision(float(cash_usd) + holdings_mv, precision, "backtest_amount")
    return {
        "date_et": trade_date_et,
        "ticker": ticker,
        "nav_usd": nav,
        "cash_usd": _round_precision(cash_usd, precision, "backtest_amount"),
        "holdings_mv_usd": holdings_mv,
        "shares": int(shares),
        "close_price_usd": _round_precision(close_price, precision, "backtest_price"),
        "trade_count": int(trade_count),
    }


def _mean_reversion_realized_round_trip(
    sell_cash_amount: float,
    last_buy_cash_amount: Optional[float],
    precision: Dict[str, int],
) -> Optional[float]:
    if last_buy_cash_amount is None:
        return None
    return _round_precision(float(sell_cash_amount) - float(last_buy_cash_amount), precision, "backtest_amount")


def _simulate_mean_reversion_ticker_path(
    *,
    ticker: str,
    rows: List[Dict[str, Any]],
    trading_dates: List[str],
    params: Dict[str, Any],
    cost_model: BacktestCostModel,
    precision: Dict[str, int],
    include_costs: bool,
) -> Dict[str, Any]:
    row_map = {str(row.get("Date")): dict(row) for row in rows}
    if len(trading_dates) < 2:
        raise ValueError(f"not enough trading days to simulate mean-reversion for {ticker}")

    starting_cash = _round_precision(params["starting_cash_per_ticker"], precision, "backtest_amount")
    cash_usd = starting_cash
    shares = 0
    trades: List[Dict[str, Any]] = []
    next_trade_id = 1
    anchor_date_et = trading_dates[0]
    anchor_close = float(row_map[anchor_date_et]["Close"])
    entry_price: Optional[float] = None
    last_buy_cash_amount: Optional[float] = None
    take_profit_sell_count = 0
    stop_loss_sell_count = 0
    winning_round_trips = 0
    completed_round_trips = 0

    curve = [
        _mean_reversion_curve_row(
            ticker=ticker,
            trade_date_et=anchor_date_et,
            cash_usd=cash_usd,
            shares=shares,
            close_price=float(row_map[anchor_date_et]["Close"]),
            trade_count=0,
            precision=precision,
        )
    ]

    for idx in range(len(trading_dates) - 1):
        signal_date_et = trading_dates[idx]
        exec_date_et = trading_dates[idx + 1]
        signal_close = float(row_map[signal_date_et]["Close"])
        exec_row = row_map[exec_date_et]
        trade_count = 0

        if shares <= 0:
            anchor_return = (signal_close / anchor_close) - 1.0 if anchor_close > 0 else None
            if anchor_return is not None and anchor_return <= -float(params["entry_drawdown_pct"]):
                unit_price = _apply_slippage(_mid_price(exec_row), "BUY", cost_model, include_costs)
                buyable_shares = _max_affordable_buy_shares(
                    desired_shares=10**9,
                    unit_price=unit_price,
                    available_cash_usd=cash_usd,
                    cost_model=cost_model,
                    include_costs=include_costs,
                )
                if buyable_shares > 0:
                    trade = _make_trade_row(
                        trade_id=f"{ticker}-{next_trade_id}",
                        signal_date_et=signal_date_et,
                        exec_date_et=exec_date_et,
                        ticker=ticker,
                        side="BUY",
                        shares=buyable_shares,
                        unit_price=unit_price,
                        cost_model=cost_model,
                        include_costs=include_costs,
                        precision=precision,
                        notes=(
                            f"signal_date_et={signal_date_et},reason=ENTRY_DROP,"
                            f"anchor_return={_round_precision(anchor_return, precision, 'backtest_rate')}"
                        ),
                    )
                    trades.append(trade)
                    next_trade_id += 1
                    trade_count = 1
                    cash_usd = _round_precision(cash_usd - float(trade["cash_amount"]), precision, "backtest_amount")
                    shares = int(buyable_shares)
                    entry_price = float(unit_price)
                    last_buy_cash_amount = float(trade["cash_amount"])
                    anchor_date_et = exec_date_et
                    anchor_close = float(exec_row["Close"])
        else:
            anchor_return = (signal_close / anchor_close) - 1.0 if anchor_close > 0 else None
            entry_return = (signal_close / float(entry_price)) - 1.0 if entry_price else None
            exit_reason = ""
            if entry_return is not None and entry_return <= -float(params["stop_loss_pct"]):
                exit_reason = "STOP_LOSS"
                stop_loss_sell_count += 1
            elif anchor_return is not None and anchor_return >= float(params["take_profit_pct"]):
                exit_reason = "TAKE_PROFIT"
                take_profit_sell_count += 1

            if exit_reason:
                unit_price = _apply_slippage(_mid_price(exec_row), "SELL", cost_model, include_costs)
                trade = _make_trade_row(
                    trade_id=f"{ticker}-{next_trade_id}",
                    signal_date_et=signal_date_et,
                    exec_date_et=exec_date_et,
                    ticker=ticker,
                    side="SELL",
                    shares=shares,
                    unit_price=unit_price,
                    cost_model=cost_model,
                    include_costs=include_costs,
                    precision=precision,
                    notes=(
                        f"signal_date_et={signal_date_et},reason={exit_reason},"
                        f"anchor_return={_round_precision(anchor_return, precision, 'backtest_rate') if anchor_return is not None else ''},"
                        f"entry_return={_round_precision(entry_return, precision, 'backtest_rate') if entry_return is not None else ''}"
                    ),
                )
                trades.append(trade)
                next_trade_id += 1
                trade_count = 1
                cash_usd = _round_precision(cash_usd + float(trade["cash_amount"]), precision, "backtest_amount")
                realized = _mean_reversion_realized_round_trip(float(trade["cash_amount"]), last_buy_cash_amount, precision)
                completed_round_trips += 1
                if realized is not None and realized > 0:
                    winning_round_trips += 1
                shares = 0
                entry_price = None
                last_buy_cash_amount = None
                anchor_date_et = exec_date_et
                anchor_close = float(exec_row["Close"])

        curve.append(
            _mean_reversion_curve_row(
                ticker=ticker,
                trade_date_et=exec_date_et,
                cash_usd=cash_usd,
                shares=shares,
                close_price=float(exec_row["Close"]),
                trade_count=trade_count,
                precision=precision,
            )
        )

    ending_nav = float(curve[-1]["nav_usd"])
    summary = {
        "ticker": ticker,
        "start_date_et": curve[0]["date_et"],
        "end_date_et": curve[-1]["date_et"],
        "starting_cash_usd": starting_cash,
        "starting_nav_usd": starting_cash,
        "ending_nav_usd": _round_precision(ending_nav, precision, "backtest_amount"),
        "profit_usd": _round_precision(ending_nav - starting_cash, precision, "backtest_amount"),
        "profit_rate": _profit_rate(starting_cash, ending_nav, precision),
        "max_drawdown_pct": _max_drawdown_pct([float(row["nav_usd"]) for row in curve], precision),
        "trade_count": len(trades),
        "buy_count": sum(1 for trade in trades if str(trade.get("side") or "").upper() == "BUY"),
        "sell_count": sum(1 for trade in trades if str(trade.get("side") or "").upper() == "SELL"),
        "take_profit_sell_count": int(take_profit_sell_count),
        "stop_loss_sell_count": int(stop_loss_sell_count),
        "completed_round_trips": int(completed_round_trips),
        "win_rate": (
            _round_precision(winning_round_trips / completed_round_trips, precision, "backtest_rate")
            if completed_round_trips > 0
            else None
        ),
        "ending_cash_usd": _round_precision(cash_usd, precision, "backtest_amount"),
        "ending_shares": int(shares),
        "ending_close_price_usd": _round_precision(float(row_map[trading_dates[-1]]["Close"]), precision, "backtest_price"),
        "costs_included": bool(include_costs),
    }
    return {
        "summary": summary,
        "equity_curve": curve,
        "trades": trades,
    }


def _aggregate_mean_reversion_path(
    *,
    path_results: List[Dict[str, Any]],
    trading_dates: List[str],
    params: Dict[str, Any],
    cost_model: BacktestCostModel,
    precision: Dict[str, int],
    include_costs: bool,
) -> Dict[str, Any]:
    ticker_results = sorted(path_results, key=lambda item: str(((item.get("summary") or {}).get("ticker") or "")).upper())
    combined_curve: List[Dict[str, Any]] = []
    for idx, trade_date_et in enumerate(trading_dates):
        combined_curve.append(
            {
                "date_et": trade_date_et,
                "nav_usd": _round_precision(
                    sum(float(result["equity_curve"][idx]["nav_usd"]) for result in ticker_results),
                    precision,
                    "backtest_amount",
                ),
                "cash_usd": _round_precision(
                    sum(float(result["equity_curve"][idx]["cash_usd"]) for result in ticker_results),
                    precision,
                    "backtest_amount",
                ),
                "holdings_mv_usd": _round_precision(
                    sum(float(result["equity_curve"][idx]["holdings_mv_usd"]) for result in ticker_results),
                    precision,
                    "backtest_amount",
                ),
                "trade_count": int(sum(int(result["equity_curve"][idx]["trade_count"]) for result in ticker_results)),
            }
        )

    starting_nav = float(combined_curve[0]["nav_usd"])
    ending_nav = float(combined_curve[-1]["nav_usd"])
    per_ticker = [result["summary"] for result in ticker_results]
    round_trip_count = sum(int(item.get("completed_round_trips") or 0) for item in per_ticker)
    trades = sorted(
        [trade for result in ticker_results for trade in (result.get("trades") or [])],
        key=lambda item: (str(item.get("trade_date_et") or ""), str(item.get("ticker") or ""), str(item.get("trade_id") or "")),
    )
    summary = {
        "start_date_et": combined_curve[0]["date_et"],
        "end_date_et": combined_curve[-1]["date_et"],
        "signal_days": max(0, len(trading_dates) - 1),
        "ticker_count": len(per_ticker),
        "starting_cash_per_ticker_usd": _round_precision(params["starting_cash_per_ticker"], precision, "backtest_amount"),
        "starting_nav_usd": _round_precision(starting_nav, precision, "backtest_amount"),
        "ending_nav_usd": _round_precision(ending_nav, precision, "backtest_amount"),
        "profit_usd": _round_precision(ending_nav - starting_nav, precision, "backtest_amount"),
        "profit_rate": _profit_rate(starting_nav, ending_nav, precision),
        "max_drawdown_pct": _max_drawdown_pct([float(row["nav_usd"]) for row in combined_curve], precision),
        "trade_count": int(sum(int(item.get("trade_count") or 0) for item in per_ticker)),
        "buy_count": int(sum(int(item.get("buy_count") or 0) for item in per_ticker)),
        "sell_count": int(sum(int(item.get("sell_count") or 0) for item in per_ticker)),
        "take_profit_sell_count": int(sum(int(item.get("take_profit_sell_count") or 0) for item in per_ticker)),
        "stop_loss_sell_count": int(sum(int(item.get("stop_loss_sell_count") or 0) for item in per_ticker)),
        "completed_round_trips": int(round_trip_count),
        "win_rate": (
            _round_precision(
                sum((float(item.get("win_rate") or 0.0) * int(item.get("completed_round_trips") or 0)) for item in per_ticker)
                / round_trip_count,
                precision,
                "backtest_rate",
            )
            if round_trip_count > 0
            else None
        ),
        "costs_included": bool(include_costs),
        "fee_rate": _round_precision(cost_model.fee_rate or 0.0, precision, "backtest_cost_param"),
        "commission_per_trade": _round_precision(cost_model.commission_per_trade or 0.0, precision, "backtest_cost_param"),
        "slippage_bps": _round_precision(cost_model.slippage_bps or 0.0, precision, "backtest_cost_param"),
    }
    return {
        "summary": summary,
        "per_ticker": per_ticker,
        "equity_curve": combined_curve,
        "trades": trades,
    }


def _simulate_mean_reversion_path(
    raw_config: Dict[str, Any],
    runtime_config: Dict[str, Any],
    history_map: Dict[str, Dict[str, Any]],
    trading_dates: List[str],
    precision: Dict[str, int],
    params: Dict[str, Any],
    *,
    include_costs: bool,
) -> Dict[str, Any]:
    if len(trading_dates) < 2:
        raise ValueError("not enough common trading days to simulate the mean-reversion rules")

    cost_model = _backtest_cost_model(raw_config)
    ticker_results = []
    for ticker in params["tickers"]:
        payload = history_map.get(ticker) or {}
        rows = payload.get("rows") or []
        ticker_results.append(
            _simulate_mean_reversion_ticker_path(
                ticker=ticker,
                rows=rows,
                trading_dates=trading_dates,
                params=params,
                cost_model=cost_model,
                precision=precision,
                include_costs=include_costs,
            )
        )
    return _aggregate_mean_reversion_path(
        path_results=ticker_results,
        trading_dates=trading_dates,
        params=params,
        cost_model=cost_model,
        precision=precision,
        include_costs=include_costs,
    )


def run_backtest(
    *,
    config_path: str,
    csv_dir: str,
    lookback_trading_days: Optional[int] = None,
    start_date_et: Optional[str] = None,
    end_date_et: Optional[str] = None,
    starting_cash: Optional[float] = None,
    allow_incomplete_rows: bool = False,
    strategy: Optional[str] = None,
    mean_reversion_entry_drawdown_pct: Optional[float] = None,
    mean_reversion_take_profit_pct: Optional[float] = None,
    mean_reversion_stop_loss_pct: Optional[float] = None,
    mean_reversion_starting_cash_per_ticker: Optional[float] = None,
    mean_reversion_tickers: Optional[List[str]] = None,
) -> Dict[str, Any]:
    raw_config, runtime_config = _load_backtest_config(config_path)
    precision = state_engine_numeric_precision(runtime_config)
    strategy_key = _normalize_backtest_strategy(
        strategy
        or _nested_config_value(_backtest_cfg(raw_config), "default_strategy")
    )
    tactical_indicators = config_tactical_indicators(runtime_config)
    tactical_tickers = sorted(str(ticker).upper() for ticker in tactical_indicators.keys())
    params = None
    if strategy_key == "tactical":
        if not tactical_tickers:
            raise ValueError("state_engine.strategy.tactical.indicators is empty")
        tickers = tactical_tickers
        warmup_bars = None
    else:
        params = _resolve_mean_reversion_params(
            raw_config,
            runtime_config,
            tickers_override=mean_reversion_tickers,
            entry_drawdown_pct=mean_reversion_entry_drawdown_pct,
            take_profit_pct=mean_reversion_take_profit_pct,
            stop_loss_pct=mean_reversion_stop_loss_pct,
            starting_cash_per_ticker=(
                starting_cash if starting_cash is not None else mean_reversion_starting_cash_per_ticker
            ),
        )
        tickers = list(params["tickers"])
        warmup_bars = 1
    history_map = _load_history_map(runtime_config, csv_dir, tickers, allow_incomplete_rows=allow_incomplete_rows)
    trading_dates = _select_backtest_dates(
        _common_trading_dates(history_map),
        runtime_config,
        raw_config,
        lookback_trading_days=lookback_trading_days,
        start_date_et=start_date_et,
        end_date_et=end_date_et,
        warmup_bars=warmup_bars,
    )
    original_print = getattr(live_runtime, "print", builtins.print)
    try:
        live_runtime.print = lambda *parts, **kwargs: None
        if strategy_key == "tactical":
            gross = _simulate_path(
                raw_config,
                runtime_config,
                history_map,
                trading_dates,
                precision,
                starting_cash_override=starting_cash,
                include_costs=False,
            )
            net = _simulate_path(
                raw_config,
                runtime_config,
                history_map,
                trading_dates,
                precision,
                starting_cash_override=starting_cash,
                include_costs=True,
            )
        else:
            gross = _simulate_mean_reversion_path(
                raw_config,
                runtime_config,
                history_map,
                trading_dates,
                precision,
                params or {},
                include_costs=False,
            )
            net = _simulate_mean_reversion_path(
                raw_config,
                runtime_config,
                history_map,
                trading_dates,
                precision,
                params or {},
                include_costs=True,
            )
    finally:
        live_runtime.print = original_print
    if strategy_key == "tactical":
        combined_curve: List[Dict[str, Any]] = []
        gross_benchmark_curve = ((gross.get("benchmark") or {}).get("equity_curve") or [])
        net_benchmark_curve = ((net.get("benchmark") or {}).get("equity_curve") or [])
        for gross_row, gross_benchmark_row, net_row, net_benchmark_row in zip(
            gross["equity_curve"],
            gross_benchmark_curve,
            net["equity_curve"],
            net_benchmark_curve,
        ):
            combined_curve.append(
                {
                    "date_et": gross_row["date_et"],
                    "gross_nav_usd": gross_row["nav_usd"],
                    "gross_cash_usd": gross_row["cash_usd"],
                    "gross_holdings_mv_usd": gross_row["holdings_mv_usd"],
                    "gross_core_value_usd": gross_row["core_value_usd"],
                    "gross_tactical_value_usd": gross_row["tactical_value_usd"],
                    "gross_trade_count": gross_row["trade_count"],
                    "gross_buy_and_hold_nav_usd": gross_benchmark_row.get("nav_usd"),
                    "net_nav_usd": net_row["nav_usd"],
                    "net_cash_usd": net_row["cash_usd"],
                    "net_holdings_mv_usd": net_row["holdings_mv_usd"],
                    "net_core_value_usd": net_row["core_value_usd"],
                    "net_tactical_value_usd": net_row["tactical_value_usd"],
                    "net_trade_count": net_row["trade_count"],
                    "net_buy_and_hold_nav_usd": net_benchmark_row.get("nav_usd"),
                }
            )
        return {
            "strategy": strategy_key,
            "config_path": str(config_path),
            "csv_dir": str(csv_dir),
            "tickers": tickers,
            "benchmark_method": _benchmark_note(),
            "numeric_precision": precision,
            "gross": gross,
            "net": net,
            "equity_curve": combined_curve,
        }

    combined_curve = []
    for gross_row, net_row in zip(gross["equity_curve"], net["equity_curve"]):
        combined_curve.append(
            {
                "date_et": gross_row["date_et"],
                "gross_nav_usd": gross_row["nav_usd"],
                "gross_cash_usd": gross_row["cash_usd"],
                "gross_holdings_mv_usd": gross_row["holdings_mv_usd"],
                "gross_trade_count": gross_row["trade_count"],
                "net_nav_usd": net_row["nav_usd"],
                "net_cash_usd": net_row["cash_usd"],
                "net_holdings_mv_usd": net_row["holdings_mv_usd"],
                "net_trade_count": net_row["trade_count"],
            }
        )
    return {
        "strategy": strategy_key,
        "config_path": str(config_path),
        "csv_dir": str(csv_dir),
        "tickers": tickers,
        "strategy_params": params,
        "numeric_precision": precision,
        "gross": gross,
        "net": net,
        "equity_curve": combined_curve,
    }


def _summary_payload_for_path(path_result: Dict[str, Any]) -> Dict[str, Any]:
    payload = {
        "strategy": path_result.get("summary"),
    }
    if "per_ticker" in path_result:
        payload["per_ticker"] = path_result.get("per_ticker")
    if "bucket_returns" in path_result:
        payload["strategy_bucket_returns"] = path_result.get("bucket_returns")
    if "benchmark" in path_result:
        payload["buy_and_hold"] = ((path_result.get("benchmark") or {}).get("summary") or {})
        payload["buy_and_hold_bucket_returns"] = ((path_result.get("benchmark") or {}).get("bucket_returns") or {})
    if "comparison" in path_result:
        payload["comparison"] = path_result.get("comparison")
    return payload


def _usd_text(value: Optional[float], usd_amount_ndigits: int) -> str:
    if value is None:
        return "-"
    return format_currency(value, usd_amount_ndigits)


def _pct_text(value: Optional[float], display_pct_ndigits: int) -> str:
    if value is None:
        return "-"
    return format_percent_from_ratio(value, display_pct_ndigits)


def render_backtest_report(result: Dict[str, Any]) -> str:
    precision = normalize_numeric_precision(result.get("numeric_precision") or {})
    usd_amount_ndigits = int(precision["usd_amount"])
    display_pct_ndigits = int(precision["display_pct"])
    strategy = _normalize_backtest_strategy(result.get("strategy"))
    net = result.get("net") or {}
    gross = result.get("gross") or {}
    net_summary = net.get("summary") or {}
    gross_summary = gross.get("summary") or {}
    if strategy == "mean-reversion":
        params = result.get("strategy_params") or {}
        lines = [
            "# Mean Reversion Backtest Report",
            "",
            "## Parameters",
            f"- Period: `{net_summary.get('start_date_et')}` -> `{net_summary.get('end_date_et')}`",
            f"- Tickers: `{', '.join(result.get('tickers') or [])}`",
            f"- Entry drawdown: `{_pct_text(-float(params.get('entry_drawdown_pct') or 0.0), display_pct_ndigits)}` from `day0` close",
            f"- Take profit: `{_pct_text(float(params.get('take_profit_pct') or 0.0), display_pct_ndigits)}` from reset `day0` close",
            f"- Stop loss: `{_pct_text(-float(params.get('stop_loss_pct') or 0.0), display_pct_ndigits)}` from entry price",
            "- Execution: signal on `Close(T)`, fill on `(Open(T+1)+Close(T+1))/2`, reset `day0` to `T+1` close after each fill",
            "",
            "## Summary (Net)",
            "| Metric | Value |",
            "| --- | ---: |",
            f"| Total Return | {_pct_text(net_summary.get('profit_rate'), display_pct_ndigits)} |",
            f"| Total Profit | {_usd_text(net_summary.get('profit_usd'), usd_amount_ndigits)} |",
            f"| Max Drawdown | {_pct_text(net_summary.get('max_drawdown_pct'), display_pct_ndigits)} |",
            f"| Trade Count | {int(net_summary.get('trade_count') or 0)} |",
            f"| Stop-Loss Sells | {int(net_summary.get('stop_loss_sell_count') or 0)} |",
            f"| Take-Profit Sells | {int(net_summary.get('take_profit_sell_count') or 0)} |",
            f"| Win Rate | {_pct_text(net_summary.get('win_rate'), display_pct_ndigits)} |",
            "",
            "## Per Ticker (Net)",
            "| Ticker | Start NAV | End NAV | Return | Max Drawdown | Trades | TP Sells | SL Sells | Win Rate | Ending Shares |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
        for item in net.get("per_ticker") or []:
            lines.append(
                "| "
                f"{item.get('ticker')} | {_usd_text(item.get('starting_nav_usd'), usd_amount_ndigits)} | {_usd_text(item.get('ending_nav_usd'), usd_amount_ndigits)} | "
                f"{_pct_text(item.get('profit_rate'), display_pct_ndigits)} | {_pct_text(item.get('max_drawdown_pct'), display_pct_ndigits)} | "
                f"{int(item.get('trade_count') or 0)} | {int(item.get('take_profit_sell_count') or 0)} | "
                f"{int(item.get('stop_loss_sell_count') or 0)} | {_pct_text(item.get('win_rate'), display_pct_ndigits)} | "
                f"{int(item.get('ending_shares') or 0)} |"
            )
        lines.extend(
            [
                "",
                "## Gross vs Net Comparison",
                "| Path | Starting NAV | Ending NAV | Return | Max Drawdown | Trade Count |",
                "| --- | ---: | ---: | ---: | ---: | ---: |",
                f"| Gross | {_usd_text(gross_summary.get('starting_nav_usd'), usd_amount_ndigits)} | {_usd_text(gross_summary.get('ending_nav_usd'), usd_amount_ndigits)} | {_pct_text(gross_summary.get('profit_rate'), display_pct_ndigits)} | {_pct_text(gross_summary.get('max_drawdown_pct'), display_pct_ndigits)} | {int(gross_summary.get('trade_count') or 0)} |",
                f"| Net | {_usd_text(net_summary.get('starting_nav_usd'), usd_amount_ndigits)} | {_usd_text(net_summary.get('ending_nav_usd'), usd_amount_ndigits)} | {_pct_text(net_summary.get('profit_rate'), display_pct_ndigits)} | {_pct_text(net_summary.get('max_drawdown_pct'), display_pct_ndigits)} | {int(net_summary.get('trade_count') or 0)} |",
            ]
        )
        return "\n".join(lines) + "\n"

    net_buckets = net.get("bucket_returns") or {}
    net_benchmark_buckets = ((net.get("benchmark") or {}).get("bucket_returns") or {})
    net_comparison = net.get("comparison") or {}
    lines = [
        "# Tactical Backtest Report",
        "",
        "## Parameters",
        f"- Period: `{net_summary.get('start_date_et')}` -> `{net_summary.get('end_date_et')}`",
        f"- Tickers: `{', '.join(result.get('tickers') or [])}`",
        f"- Benchmark: {result.get('benchmark_method')}",
        "",
        "## Summary (Net)",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Total Return | {_pct_text(net_summary.get('profit_rate'), display_pct_ndigits)} |",
        f"| Buy-and-Hold Return | {_pct_text(((net.get('benchmark') or {}).get('summary') or {}).get('profit_rate'), display_pct_ndigits)} |",
        f"| Excess Return | {_pct_text(((net_comparison.get('portfolio') or {}).get('excess_profit_rate')), display_pct_ndigits)} |",
        f"| Total Profit | {_usd_text(net_summary.get('profit_usd'), usd_amount_ndigits)} |",
        f"| Buy-and-Hold Profit | {_usd_text(((net_comparison.get('portfolio') or {}).get('buy_and_hold_profit_usd')), usd_amount_ndigits)} |",
        f"| Max Drawdown | {_pct_text(net_summary.get('max_drawdown_pct'), display_pct_ndigits)} |",
        f"| Trade Count | {int(net_summary.get('trade_count') or 0)} |",
        "",
        "## Bucket Returns (Net)",
        "| Bucket | Strategy Start | Strategy End | Strategy Return | Buy-and-Hold End | Buy-and-Hold Return | Excess Return |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for bucket, label in (("tactical", "Tactical"), ("portfolio", "Total")):
        strategy_bucket = net_buckets.get(bucket) or {}
        benchmark_bucket = net_benchmark_buckets.get(bucket) or {}
        comparison_bucket = net_comparison.get(bucket) or {}
        lines.append(
            "| "
            f"{label} | {_usd_text(strategy_bucket.get('start_value_usd'), usd_amount_ndigits)} | {_usd_text(strategy_bucket.get('end_value_usd'), usd_amount_ndigits)} | "
            f"{_pct_text(strategy_bucket.get('profit_rate'), display_pct_ndigits)} | {_usd_text(benchmark_bucket.get('end_value_usd'), usd_amount_ndigits)} | "
            f"{_pct_text(benchmark_bucket.get('profit_rate'), display_pct_ndigits)} | {_pct_text(comparison_bucket.get('excess_profit_rate'), display_pct_ndigits)} |"
        )
    lines.extend(
        [
            "",
            "## Gross vs Net Comparison",
            "| Path | Starting NAV | Ending NAV | Return | Max Drawdown | Trade Count |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
            f"| Gross | {_usd_text(gross_summary.get('starting_nav_usd'), usd_amount_ndigits)} | {_usd_text(gross_summary.get('ending_nav_usd'), usd_amount_ndigits)} | {_pct_text(gross_summary.get('profit_rate'), display_pct_ndigits)} | {_pct_text(gross_summary.get('max_drawdown_pct'), display_pct_ndigits)} | {int(gross_summary.get('trade_count') or 0)} |",
            f"| Net | {_usd_text(net_summary.get('starting_nav_usd'), usd_amount_ndigits)} | {_usd_text(net_summary.get('ending_nav_usd'), usd_amount_ndigits)} | {_pct_text(net_summary.get('profit_rate'), display_pct_ndigits)} | {_pct_text(net_summary.get('max_drawdown_pct'), display_pct_ndigits)} | {int(net_summary.get('trade_count') or 0)} |",
        ]
    )
    return "\n".join(lines) + "\n"


def write_backtest_outputs(result: Dict[str, Any], out_dir: str) -> Dict[str, str]:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    strategy = _normalize_backtest_strategy(result.get("strategy"))

    summary_path = out_path / "summary.json"
    summary_payload = {
        "strategy": strategy,
        "config_path": result.get("config_path"),
        "csv_dir": result.get("csv_dir"),
        "benchmark_method": result.get("benchmark_method"),
        "strategy_params": result.get("strategy_params"),
        "tickers": result.get("tickers"),
        "gross": _summary_payload_for_path(result.get("gross") or {}),
        "net": _summary_payload_for_path(result.get("net") or {}),
    }
    summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    curve_path = out_path / "equity_curve.csv"
    curve_rows = result.get("equity_curve") or []
    with curve_path.open("w", encoding="utf-8", newline="") as handle:
        if strategy == "mean-reversion":
            fieldnames = [
                "date_et",
                "gross_nav_usd",
                "gross_cash_usd",
                "gross_holdings_mv_usd",
                "gross_trade_count",
                "net_nav_usd",
                "net_cash_usd",
                "net_holdings_mv_usd",
                "net_trade_count",
            ]
        else:
            fieldnames = [
                "date_et",
                "gross_nav_usd",
                "gross_cash_usd",
                "gross_holdings_mv_usd",
                "gross_core_value_usd",
                "gross_tactical_value_usd",
                "gross_trade_count",
                "gross_buy_and_hold_nav_usd",
                "net_nav_usd",
                "net_cash_usd",
                "net_holdings_mv_usd",
                "net_core_value_usd",
                "net_tactical_value_usd",
                "net_trade_count",
                "net_buy_and_hold_nav_usd",
            ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in curve_rows:
            writer.writerow(row)

    gross_trades_path = out_path / "gross_trades.json"
    gross_trades_path.write_text(
        json.dumps((result.get("gross") or {}).get("trades") or [], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    net_trades_path = out_path / "net_trades.json"
    net_trades_path.write_text(
        json.dumps((result.get("net") or {}).get("trades") or [], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    report_path = out_path / "report.md"
    report_path.write_text(render_backtest_report(result), encoding="utf-8")
    return {
        "summary": str(summary_path),
        "equity_curve": str(curve_path),
        "gross_trades": str(gross_trades_path),
        "net_trades": str(net_trades_path),
        "report": str(report_path),
    }
