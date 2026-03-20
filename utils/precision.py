from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

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
        raise KeyError(f"state_engine.numeric_precision must define '{key}'") from exc
    except Exception as exc:
        raise ValueError(f"state_engine.numeric_precision.{key} must be a non-negative integer") from exc
    if parsed < 0:
        raise ValueError(f"state_engine.numeric_precision.{key} must be a non-negative integer")
    return parsed


def normalize_numeric_precision(precision: Dict[str, Any]) -> Dict[str, int]:
    if not isinstance(precision, dict):
        raise KeyError("state_engine.numeric_precision must be configured in config.json")
    return {key: _require_non_negative_int(precision, key) for key in _NUMERIC_PRECISION_KEYS}


def state_engine_numeric_precision(state_engine_config: Dict[str, Any]) -> Dict[str, int]:
    precision = state_engine_config.get("numeric_precision") if isinstance(state_engine_config, dict) else None
    if not isinstance(precision, dict):
        raise KeyError("state_engine.numeric_precision must be configured in config.json")
    return normalize_numeric_precision(precision)


def load_state_engine_numeric_precision(config_path: str) -> Dict[str, int]:
    raw = json.loads(Path(config_path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TypeError(f"config root must be an object: {config_path}")
    state_engine = raw.get("state_engine")
    if not isinstance(state_engine, dict):
        raise KeyError(f"config.json must contain object key 'state_engine': {config_path}")
    return state_engine_numeric_precision(state_engine)


def round_with_precision(value: Any, ndigits: int) -> float:
    return round(float(value), int(ndigits))


def format_fixed(value: Any, ndigits: int) -> str:
    return f"{float(value):.{int(ndigits)}f}"


def format_currency(value: Any, ndigits: int) -> str:
    return f"${float(value):,.{int(ndigits)}f}"


def format_percent_from_ratio(value: Any, ndigits: int) -> str:
    return f"{float(value) * 100:.{int(ndigits)}f}%"
