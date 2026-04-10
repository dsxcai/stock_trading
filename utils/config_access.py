from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List


def load_json_object(path: str) -> Dict[str, Any]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TypeError(f"config root must be an object: {path}")
    return raw


def load_state_engine_config(path: str) -> Dict[str, Any]:
    """Load the current state_engine config schema with no legacy fallback."""
    raw = load_json_object(path)
    state_engine = raw.get("state_engine")
    if not isinstance(state_engine, dict):
        raise KeyError(f"config.json must contain object key 'state_engine': {path}")
    return copy.deepcopy(state_engine)


def config_meta(config: Dict[str, Any]) -> Dict[str, Any]:
    value = config.get("meta") if isinstance(config, dict) else None
    return value if isinstance(value, dict) else {}


def config_execution(config: Dict[str, Any]) -> Dict[str, Any]:
    value = config.get("execution") if isinstance(config, dict) else None
    return value if isinstance(value, dict) else {}


def config_portfolio(config: Dict[str, Any]) -> Dict[str, Any]:
    value = config.get("portfolio") if isinstance(config, dict) else None
    return value if isinstance(value, dict) else {}


def config_strategy(config: Dict[str, Any]) -> Dict[str, Any]:
    value = config.get("strategy") if isinstance(config, dict) else None
    return value if isinstance(value, dict) else {}


def config_data(config: Dict[str, Any]) -> Dict[str, Any]:
    value = config.get("data") if isinstance(config, dict) else None
    return value if isinstance(value, dict) else {}


def config_reporting(config: Dict[str, Any]) -> Dict[str, Any]:
    value = config.get("reporting") if isinstance(config, dict) else None
    return value if isinstance(value, dict) else {}


def config_doc(config: Dict[str, Any]) -> str:
    return str(config_meta(config).get("doc") or "").strip()


def config_trades_file(config: Dict[str, Any]) -> str:
    return str(config_meta(config).get("trades_file") or "").strip()


def config_cash_events_file(config: Dict[str, Any]) -> str:
    return str(config_meta(config).get("cash_events_file") or "").strip()


def config_fee_rate(config: Dict[str, Any]) -> Any:
    return config_execution(config).get("fee_rate")


def config_numeric_precision(config: Dict[str, Any]) -> Dict[str, Any]:
    value = config_reporting(config).get("numeric_precision")
    return value if isinstance(value, dict) else {}


def config_trade_render_policy(config: Dict[str, Any]) -> Dict[str, Any]:
    value = config_reporting(config).get("trade_render_policy")
    return value if isinstance(value, dict) else {}


def config_buckets(config: Dict[str, Any]) -> Dict[str, Any]:
    value = config_portfolio(config).get("buckets")
    return value if isinstance(value, dict) else {}


def config_tactical_indicators(config: Dict[str, Any]) -> Dict[str, Any]:
    tactical = config_strategy(config).get("tactical")
    if not isinstance(tactical, dict):
        return {}
    indicators = tactical.get("indicators")
    return indicators if isinstance(indicators, dict) else {}


def config_fx_pairs(config: Dict[str, Any]) -> Dict[str, Any]:
    value = config_data(config).get("fx_pairs")
    return value if isinstance(value, dict) else {}


def config_csv_sources(config: Dict[str, Any]) -> Dict[str, Any]:
    value = config_data(config).get("csv_sources")
    return value if isinstance(value, dict) else {}


def config_trading_calendar(config: Dict[str, Any]) -> Dict[str, Any]:
    value = config_data(config).get("trading_calendar")
    return value if isinstance(value, dict) else {}


def _dedupe_tickers(values: Iterable[Any]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for value in values:
        ticker = str(value or "").upper().strip()
        if ticker and ticker not in seen:
            seen.add(ticker)
            out.append(ticker)
    return out


def discover_state_engine_tickers(config: Dict[str, Any]) -> List[str]:
    buckets = config_buckets(config)
    tactical_bucket = buckets.get("tactical") if isinstance(buckets.get("tactical"), dict) else {}
    tickers: List[Any] = []
    tickers.extend(((buckets.get("core") or {}).get("tickers") or []))
    tickers.extend((tactical_bucket.get("tickers") or []))
    tickers.append(tactical_bucket.get("cash_pool_ticker"))
    tickers.extend((((buckets.get("tactical_cash_pool") or {}).get("tickers")) or []))
    tickers.extend(config_tactical_indicators(config).keys())
    for fx_cfg in config_fx_pairs(config).values():
        if isinstance(fx_cfg, dict):
            tickers.append(fx_cfg.get("ticker"))
    return _dedupe_tickers(tickers)
