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
from utils.precision import (
    format_currency,
    format_percent_from_ratio,
    normalize_numeric_precision,
    round_with_precision,
    state_engine_numeric_precision,
)


def _load_backtest_config(config_path: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    raw = json.loads(Path(config_path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TypeError(f"config root must be an object: {config_path}")
    runtime_config = live_runtime._load_runtime_config(config_path)
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
    csv_sources = runtime_config.get("csv_sources", {}) or {}
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
            for rule in (runtime_config.get("tactical_indicators") or {}).values()
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
) -> List[str]:
    if not trading_dates:
        return []

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
        lookback_trading_days = int(raw_config.get("backtest_lookback_trading_days") or 252)
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
    trade_id: int,
    signal_date_et: str,
    exec_date_et: str,
    ticker: str,
    side: str,
    shares: int,
    unit_price: float,
    cost_model: BacktestCostModel,
    include_costs: bool,
    precision: Dict[str, int],
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
        "notes": f"signal_date_et={signal_date_et}",
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
        starting_cash_raw = raw_config.get("backtest_starting_cash")
    starting_cash = _round_precision(starting_cash_raw or 0.0, precision, "backtest_amount")
    if starting_cash <= 0:
        raise ValueError("backtest_starting_cash must be positive")
    return _make_backtest_seed_state(starting_cash, precision)


def _benchmark_note() -> str:
    return "buy_and_hold deploys the starting tactical cash across tactical tickers on the first executable day and never sells"


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

    tactical_tickers = sorted(str(ticker).upper() for ticker in (runtime_config.get("tactical_indicators") or {}).keys())
    warmup_bars = _warmup_bars(runtime_config)
    if len(trading_dates) <= warmup_bars:
        raise ValueError("not enough common trading days after warmup to run the backtest")

    cost_model = BacktestCostModel(
        fee_rate=float(runtime_config.get("fee_rate") or 0.0),
        commission_per_trade=float(raw_config.get("commission_per_trade") or 0.0),
        slippage_bps=float(raw_config.get("slippage_bps") or 0.0),
    )
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


def run_backtest(
    *,
    config_path: str,
    csv_dir: str,
    lookback_trading_days: Optional[int] = None,
    start_date_et: Optional[str] = None,
    end_date_et: Optional[str] = None,
    starting_cash: Optional[float] = None,
    allow_incomplete_rows: bool = False,
) -> Dict[str, Any]:
    raw_config, runtime_config = _load_backtest_config(config_path)
    precision = state_engine_numeric_precision(runtime_config)
    tactical_indicators = runtime_config.get("tactical_indicators") or {}
    tactical_tickers = sorted(str(ticker).upper() for ticker in tactical_indicators.keys())
    if not tactical_tickers:
        raise ValueError("state_engine.tactical_indicators is empty")
    tickers = tactical_tickers
    history_map = _load_history_map(runtime_config, csv_dir, tickers, allow_incomplete_rows=allow_incomplete_rows)
    trading_dates = _select_backtest_dates(
        _common_trading_dates(history_map),
        runtime_config,
        raw_config,
        lookback_trading_days=lookback_trading_days,
        start_date_et=start_date_et,
        end_date_et=end_date_et,
    )
    original_print = getattr(live_runtime, "print", builtins.print)
    try:
        live_runtime.print = lambda *parts, **kwargs: None
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
    finally:
        live_runtime.print = original_print
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
        "config_path": str(config_path),
        "csv_dir": str(csv_dir),
        "tickers": tickers,
        "benchmark_method": _benchmark_note(),
        "numeric_precision": precision,
        "gross": gross,
        "net": net,
        "equity_curve": combined_curve,
    }


def _summary_payload_for_path(path_result: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "strategy": path_result.get("summary"),
        "strategy_bucket_returns": path_result.get("bucket_returns"),
        "buy_and_hold": ((path_result.get("benchmark") or {}).get("summary") or {}),
        "buy_and_hold_bucket_returns": ((path_result.get("benchmark") or {}).get("bucket_returns") or {}),
        "comparison": path_result.get("comparison"),
    }


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
    net = result.get("net") or {}
    gross = result.get("gross") or {}
    net_summary = net.get("summary") or {}
    gross_summary = gross.get("summary") or {}
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

    summary_path = out_path / "summary.json"
    summary_payload = {
        "config_path": result.get("config_path"),
        "csv_dir": result.get("csv_dir"),
        "benchmark_method": result.get("benchmark_method"),
        "tickers": result.get("tickers"),
        "gross": _summary_payload_for_path(result.get("gross") or {}),
        "net": _summary_payload_for_path(result.get("net") or {}),
    }
    summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    curve_path = out_path / "equity_curve.csv"
    curve_rows = result.get("equity_curve") or []
    with curve_path.open("w", encoding="utf-8", newline="") as handle:
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
