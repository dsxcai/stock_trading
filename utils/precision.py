# Copyright (c) 2026 Sheng-Hsin Tsai
# SPDX-License-Identifier: MIT

from __future__ import annotations

from typing import Any, Dict

from utils.config_access import config_numeric_precision, load_state_engine_config

_NUMERIC_PRECISION_KEYS = (
    "usd_amount",
    "display_price",
    "display_pct",
    "trade_cash_amount",
    "trade_dedupe_amount",
    "state_selected_fields",
    "backtest_amount",
    "backtest_price",
    "backtest_rate",
    "backtest_cost_param",
)


def _require_non_negative_int(container: Dict[str, Any], key: str) -> int:
    try:
        parsed = int(container[key])
    except KeyError as exc:
        raise KeyError(f"state_engine.reporting.numeric_precision must define '{key}'") from exc
    except Exception as exc:
        raise ValueError(f"state_engine.reporting.numeric_precision.{key} must be a non-negative integer") from exc
    if parsed < 0:
        raise ValueError(f"state_engine.reporting.numeric_precision.{key} must be a non-negative integer")
    return parsed


def normalize_numeric_precision(precision: Dict[str, Any]) -> Dict[str, int]:
    if not isinstance(precision, dict):
        raise KeyError("state_engine.reporting.numeric_precision must be configured in config.json")
    return {key: _require_non_negative_int(precision, key) for key in _NUMERIC_PRECISION_KEYS}


def state_engine_numeric_precision(state_engine_config: Dict[str, Any]) -> Dict[str, int]:
    precision = config_numeric_precision(state_engine_config)
    if not isinstance(precision, dict):
        raise KeyError("state_engine.reporting.numeric_precision must be configured in config.json")
    return normalize_numeric_precision(precision)


def load_state_engine_numeric_precision(config_path: str) -> Dict[str, int]:
    state_engine = load_state_engine_config(config_path)
    return state_engine_numeric_precision(state_engine)


def round_with_precision(value: Any, ndigits: int) -> float:
    return round(float(value), int(ndigits))


def format_fixed(value: Any, ndigits: int) -> str:
    return f"{float(value):.{int(ndigits)}f}"


def format_currency(value: Any, ndigits: int) -> str:
    return f"${float(value):,.{int(ndigits)}f}"


def format_percent_from_ratio(value: Any, ndigits: int) -> str:
    return f"{float(value) * 100:.{int(ndigits)}f}%"
