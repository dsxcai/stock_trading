from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, List, Tuple

from core.reconciliation import _normalize_trades_inplace
from core.runtime_io import _load_trades_payload
from utils.dates import _normalize_trade_date_et, _parse_ymd_loose


def _load_imported_trades_json(path: str) -> List[Dict[str, Any]]:
    trades = _load_trades_payload(path)
    if trades is None:
        raise ValueError(f"{path}: imported trades JSON must be a list or an object with a 'trades' array")
    return trades


def _trade_import_label(path: str, trades: List[Dict[str, Any]]) -> str:
    for key in ("source_file", "source"):
        for trade in trades:
            value = str((trade or {}).get(key) or "").strip() if isinstance(trade, dict) else ""
            if value:
                return value
    return Path(path).name


def _iter_imported_trade_batches(args: argparse.Namespace, cash_amount_ndigits: int) -> List[Tuple[str, str, List[Dict[str, Any]]]]:
    batches: List[Tuple[str, str, List[Dict[str, Any]]]] = []
    for payload in getattr(args, "imported_trade_batches", None) or []:
        if not isinstance(payload, dict):
            continue
        import_path = str(payload.get("import_path") or payload.get("label") or "<in-memory-import>").strip() or "<in-memory-import>"
        incoming = [dict(trade) for trade in (payload.get("trades") or []) if isinstance(trade, dict)]
        _normalize_trades_inplace(incoming, cash_amount_ndigits=cash_amount_ndigits)
        batches.append((_trade_import_label(import_path, incoming), import_path, incoming))
    for import_path in getattr(args, "imported_trades_json", None) or []:
        incoming = _load_imported_trades_json(import_path)
        _normalize_trades_inplace(incoming, cash_amount_ndigits=cash_amount_ndigits)
        batches.append((_trade_import_label(import_path, incoming), import_path, incoming))
    return batches


def _normalize_trade_date_bounds(trade_date_from: str, trade_date_to: str) -> Tuple[str, str]:
    start = _parse_ymd_loose(trade_date_from)
    end = _parse_ymd_loose(trade_date_to)
    if str(trade_date_from or "").strip() and start is None:
        raise ValueError(f"invalid trade date from: {trade_date_from}")
    if str(trade_date_to or "").strip() and end is None:
        raise ValueError(f"invalid trade date to: {trade_date_to}")
    if start is not None and end is not None and start > end:
        raise ValueError(f"trade date from {start.isoformat()} is after trade date to {end.isoformat()}")
    return start.isoformat() if start is not None else "", end.isoformat() if end is not None else ""


def _trade_is_within_trade_date_bounds(trade: Dict[str, Any], trade_date_from: str, trade_date_to: str) -> bool:
    trade_date_et = _normalize_trade_date_et(str(trade.get("trade_date_et") or ""))
    return bool(trade_date_et) and not (trade_date_from and trade_date_et < trade_date_from) and not (trade_date_to and trade_date_et > trade_date_to)


def _replace_trades(trades: List[Dict[str, Any]], trade_date_from: str = "", trade_date_to: str = "") -> Tuple[List[Dict[str, Any]], int]:
    if not isinstance(trades, list) or not trades:
        return (trades if isinstance(trades, list) else [], 0)
    if not (trade_date_from or trade_date_to):
        print(f"[REPLACE] removed {len(trades)} existing trade(s) from the full trade ledger.")
        return [], len(trades)
    keep = [trade for trade in trades if not isinstance(trade, dict) or not _trade_is_within_trade_date_bounds(trade, trade_date_from, trade_date_to)]
    removed = len(trades) - len(keep)
    if removed:
        start = trade_date_from or "min"
        end = trade_date_to or "max"
        print(f"[REPLACE] removed {removed} existing trade(s) in trade_date_et range {start}..{end}.")
    return (keep if removed else trades, removed)
