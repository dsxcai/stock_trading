# Copyright (c) 2026 Sheng-Hsin Tsai
# SPDX-License-Identifier: MIT

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from utils.config_access import load_state_engine_config
from utils.dates import _to_yyyy_mm_dd
from utils.precision import state_engine_numeric_precision


def _load_json(path: str) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _load_runtime_config(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    return load_state_engine_config(path)


def _runtime_config(runtime: Dict[str, Any]) -> Dict[str, Any]:
    cfg = runtime.get("config") or {}
    return cfg if isinstance(cfg, dict) else {}


def _runtime_numeric_precision(runtime: Dict[str, Any]) -> Dict[str, int]:
    return state_engine_numeric_precision(_runtime_config(runtime))


def _runtime_data_config(runtime: Dict[str, Any]) -> Dict[str, Any]:
    cfg = _runtime_config(runtime)
    data = cfg.get("data")
    if not isinstance(data, dict):
        data = {}
        cfg["data"] = data
    return data


def _runtime_history(runtime: Dict[str, Any]) -> Dict[str, Any]:
    hist = runtime.get("history")
    if not isinstance(hist, dict):
        hist = {}
        runtime["history"] = hist
    return hist


def _market_history_rows_map(runtime: Dict[str, Any]) -> Dict[str, Any]:
    return _runtime_history(runtime)


def _runtime_report_meta(runtime: Dict[str, Any]) -> Dict[str, Any]:
    meta = runtime.get("report_meta")
    return dict(meta) if isinstance(meta, dict) else {}


def _runtime_signal_basis_day(runtime: Dict[str, Any]) -> Optional[str]:
    signal_basis = (_runtime_report_meta(runtime).get("signal_basis") or {})
    signal_day = str(signal_basis.get("t_et") or "").strip()
    if not signal_day:
        return None
    try:
        return _to_yyyy_mm_dd(signal_day)
    except Exception:
        return None


def _save_json(obj: Dict[str, Any], path: str) -> str:
    payload = json.dumps(obj, ensure_ascii=False, indent=2)
    p = Path(path)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(payload, encoding="utf-8")
        return str(p)
    except PermissionError:
        fallback = p.with_name(f"{p.stem}.new{p.suffix}")
        fallback.parent.mkdir(parents=True, exist_ok=True)
        fallback.write_text(payload, encoding="utf-8")
        print(f"[WARN] Cannot write {p} (permission denied). Wrote fallback: {fallback}")
        return str(fallback)


def _load_trades_payload(path: str) -> Optional[List[Dict[str, Any]]]:
    p = Path(path)
    if not p.exists():
        return None
    obj = json.loads(p.read_text(encoding="utf-8"))
    if isinstance(obj, list):
        return [dict(trade) for trade in obj if isinstance(trade, dict)]
    if isinstance(obj, dict) and isinstance(obj.get("trades"), list):
        return [dict(trade) for trade in (obj.get("trades") or []) if isinstance(trade, dict)]
    return None


def _load_cash_events_payload(path: str) -> Optional[List[Dict[str, Any]]]:
    p = Path(path)
    if not p.exists():
        return None
    obj = json.loads(p.read_text(encoding="utf-8"))
    if isinstance(obj, list):
        return [dict(event) for event in obj if isinstance(event, dict)]
    if isinstance(obj, dict) and isinstance(obj.get("cash_events"), list):
        return [dict(event) for event in (obj.get("cash_events") or []) if isinstance(event, dict)]
    return None


def _save_trades_payload(trades: List[Dict[str, Any]], path: str) -> str:
    payload = json.dumps(trades, ensure_ascii=False, indent=2)
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(payload, encoding="utf-8")
    return str(p)


def _save_cash_events_payload(cash_events: List[Dict[str, Any]], path: str) -> str:
    payload = json.dumps(cash_events, ensure_ascii=False, indent=2)
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(payload, encoding="utf-8")
    return str(p)


def _compact_trade_row(trade: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key in ("trade_id", "trade_date_et", "time_tw", "ticker", "side", "shares", "cash_amount", "price", "gross", "fee", "notes", "source"):
        if key not in trade:
            continue
        value = trade.get(key)
        if value is None or (isinstance(value, str) and not value.strip()):
            continue
        out[key] = value
    return out


def _compact_cash_event_row(cash_event: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key in ("event_id", "event_date_et", "kind", "amount_usd", "cash_effect_usd", "bucket_from", "bucket_to", "note", "source", "ts_utc"):
        if key not in cash_event:
            continue
        value = cash_event.get(key)
        if value is None or (isinstance(value, str) and not value.strip()):
            continue
        out[key] = value
    return out


def _round_selected_numeric_fields(obj: Any, keys: set, ndigits: int = 4) -> None:
    if isinstance(obj, dict):
        for key, value in list(obj.items()):
            if key in keys and isinstance(value, (int, float)) and not isinstance(value, bool):
                obj[key] = round(float(value), ndigits)
            else:
                _round_selected_numeric_fields(value, keys, ndigits=ndigits)
    elif isinstance(obj, list):
        for item in obj:
            _round_selected_numeric_fields(item, keys, ndigits=ndigits)


def _strip_persisted_report_transients(states: Dict[str, Any]) -> None:
    market = states.get("market")
    if isinstance(market, dict):
        market.pop("signals_inputs", None)
        market.pop("next_close_threshold_inputs", None)
    portfolio = states.get("portfolio")
    if isinstance(portfolio, dict) and isinstance(portfolio.get("positions"), list):
        for pos in portfolio.get("positions") or []:
            if isinstance(pos, dict):
                pos.pop("notes", None)
    states.pop("signals", None)
    states.pop("thresholds", None)
    meta = states.get("meta")
    if isinstance(meta, dict):
        meta.pop("notes", None)
        if not meta:
            states.pop("meta", None)
    states.pop("by_mode", None)


def _compact_persistent_states(states: Dict[str, Any]) -> Dict[str, Any]:
    compacted = copy.deepcopy(states if isinstance(states, dict) else {})
    if not isinstance(compacted, dict):
        return {"portfolio": {"positions": [], "cash": {"usd": 0.0}}}

    _strip_persisted_report_transients(compacted)
    compacted.pop("_report_meta", None)
    compacted.pop("config", None)
    compacted.pop("market", None)

    portfolio = compacted.setdefault("portfolio", {})
    if not isinstance(portfolio, dict):
        portfolio = {}
        compacted["portfolio"] = portfolio

    positions: List[Dict[str, Any]] = []
    for pos in (portfolio.get("positions") or []) if isinstance(portfolio.get("positions"), list) else []:
        if not isinstance(pos, dict):
            continue
        ticker = str(pos.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        try:
            shares = int(round(float(pos.get("shares") or 0.0)))
        except Exception:
            shares = 0
        if shares > 0:
            positions.append({"ticker": ticker, "shares": shares})
    portfolio["positions"] = positions

    cash = portfolio.get("cash")
    if not isinstance(cash, dict):
        cash = {"usd": 0.0}
        portfolio["cash"] = cash
    try:
        cash["usd"] = float(cash.get("usd") or 0.0)
    except Exception:
        cash["usd"] = 0.0
    for key in ("external_flows", "internal_transfers", "external_cash_flow", "net_external_cash_flow_usd"):
        cash.pop(key, None)

    portfolio.pop("totals", None)
    performance = portfolio.get("performance")
    if isinstance(performance, dict):
        for key in ("current_total_assets_usd", "net_external_cash_flow_usd", "effective_capital_base_usd", "profit_usd", "profit_rate", "returns"):
            performance.pop(key, None)
        if not performance:
            portfolio.pop("performance", None)
    return compacted
