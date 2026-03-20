from __future__ import annotations

from typing import Any, Dict, List, Optional

from core.models import TacticalPlan


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
    if isinstance(trades, list):
        report_trades: List[Dict[str, Any]] = []
        for item in trades:
            if isinstance(item, dict):
                report_trades.append(dict(item))
        root["trades"] = report_trades
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
