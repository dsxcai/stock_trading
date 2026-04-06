from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tempfile
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from utils.parsers import _normalize_time_tw, _normalize_trade_date_et
from utils.precision import load_state_engine_numeric_precision

_CAPITAL_XLS_REQUIRED_HEADERS: Tuple[str, ...] = (
    "商品名稱",
    "交易日",
    "買賣別",
    "成交單價",
    "成交股數/單位數",
    "成交價金",
    "成交時間",
    "原幣手續費",
    "原幣淨收付",
)


def _first_token_ticker(product_name: str) -> str:
    token = str(product_name or "").strip().split()[0] if str(product_name or "").strip() else ""
    return token.upper()


def _num_from_cell(value: str) -> Optional[float]:
    raw = str(value or "").strip()
    if not raw:
        return None
    normalized = raw.replace(",", "")
    normalized = re.sub(r"[A-Za-z\u4e00-\u9fff]+$", "", normalized).strip()
    if not normalized:
        return None
    if normalized.startswith("(") and normalized.endswith(")"):
        normalized = f"-{normalized[1:-1]}"
    return float(normalized)


def _normalize_capital_side(raw_side: str) -> str:
    side = str(raw_side or "").strip()
    if side == "買入":
        return "BUY"
    if side == "賣出":
        return "SELL"
    raise ValueError(f"unsupported Capital XLS side: {side}")


class _CapitalXLSParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: List[List[str]] = []
        self._current_row: Optional[List[str]] = None
        self._current_cell: Optional[List[str]] = None

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        del attrs
        tag_name = str(tag or "").strip().lower()
        if tag_name == "tr":
            self._current_row = []
            return
        if tag_name in {"td", "th"} and self._current_row is not None and self._current_cell is None:
            self._current_cell = []

    def handle_data(self, data: str) -> None:
        if self._current_cell is not None:
            self._current_cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag_name = str(tag or "").strip().lower()
        if tag_name in {"td", "th"} and self._current_cell is not None and self._current_row is not None:
            self._current_row.append("".join(self._current_cell).strip())
            self._current_cell = None
            return
        if tag_name == "tr" and self._current_row is not None:
            if self._current_cell is not None:
                self._current_row.append("".join(self._current_cell).strip())
                self._current_cell = None
            if self._current_row:
                self.rows.append(self._current_row)
            self._current_row = None


def _parse_capital_xls_table(xls_path: str) -> Tuple[Dict[str, int], List[List[str]]]:
    parser = _CapitalXLSParser()
    parser.feed(Path(xls_path).read_text(encoding="utf-8"))
    parser.close()

    rows = parser.rows
    if not rows:
        return ({}, [])

    headers = [str(cell or "").strip() for cell in rows[0]]
    columns = {header: idx for idx, header in enumerate(headers) if header}
    missing = [header for header in _CAPITAL_XLS_REQUIRED_HEADERS if header not in columns]
    if missing:
        raise ValueError(f"{xls_path}: missing required Capital XLS columns: {', '.join(missing)}")
    return (columns, rows[1:])


def _cell(row: List[str], columns: Dict[str, int], header: str) -> str:
    idx = columns[header]
    return str(row[idx] if idx < len(row) else "").strip()


def _build_trade_from_capital_xls_row(xls_path: str, row: List[str], columns: Dict[str, int], cash_amount_ndigits: int) -> Optional[Dict[str, Any]]:
    if not row or all(not str(value or "").strip() for value in row):
        return None

    product_name = _cell(row, columns, "商品名稱")
    trade_date_et = _normalize_trade_date_et(_cell(row, columns, "交易日"))
    side = _normalize_capital_side(_cell(row, columns, "買賣別"))
    price_value = _num_from_cell(_cell(row, columns, "成交單價"))
    shares_value = int(round(_num_from_cell(_cell(row, columns, "成交股數/單位數")) or 0.0))
    gross_value = float(_num_from_cell(_cell(row, columns, "成交價金")) or 0.0)
    time_tw_normalized = _normalize_time_tw(_cell(row, columns, "成交時間"))
    fee_value = float(_num_from_cell(_cell(row, columns, "原幣手續費")) or 0.0)
    net_value = float(_num_from_cell(_cell(row, columns, "原幣淨收付")) or gross_value)
    ticker = _first_token_ticker(product_name).upper()

    if not product_name or not trade_date_et or not time_tw_normalized or not ticker or price_value is None:
        return None

    cash_amount_value = gross_value + fee_value if side.startswith("B") else max(gross_value - fee_value, 0.0)
    cash_amount_value = round(cash_amount_value, int(cash_amount_ndigits))
    fee_rate_pct = fee_value / gross_value if gross_value else None

    return {
        "trade_date_et": trade_date_et,
        "ticker": ticker,
        "side": side,
        "shares": shares_value,
        "cash_amount": cash_amount_value,
        "cash_basis": "Total",
        "gross": gross_value,
        "fee": fee_value,
        "fee_rate_pct": fee_rate_pct,
        "net": net_value,
        "price": price_value,
        "time_tw": time_tw_normalized,
        "notes": f"Imported from Capital XLS ({product_name})",
        "source": f"capital_xls:{Path(xls_path).name}",
        "source_file": Path(xls_path).name,
        "source_type": "capital_xls",
        "product_name": product_name,
    }


def parse_capital_xls_trades(xls_path: str, cash_amount_ndigits: int) -> List[Dict[str, Any]]:
    if not Path(xls_path).exists():
        raise FileNotFoundError(xls_path)

    columns, table_rows = _parse_capital_xls_table(xls_path)
    trades: List[Dict[str, Any]] = []
    for row in table_rows:
        trade = _build_trade_from_capital_xls_row(xls_path, row, columns, cash_amount_ndigits=cash_amount_ndigits)
        if trade is not None:
            trades.append(trade)

    trades.sort(key=lambda trade: (trade.get("trade_date_et", ""), trade.get("time_tw", ""), trade.get("ticker", ""), trade.get("side", "")))
    return trades


def _write_temp_import_json(trades: List[Dict[str, Any]]) -> Path:
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", prefix="capital_xls_", delete=False) as fh:
        json.dump(trades, fh, ensure_ascii=False, indent=2)
        return Path(fh.name)


def _build_update_states_command(
    import_json_path: str,
    passthrough_args: List[str],
    *,
    trade_date_from: str = "",
    trade_date_to: str = "",
) -> List[str]:
    repo_root = Path(__file__).resolve().parents[1]
    command = [sys.executable, str(repo_root / "update_states.py"), "--imported-trades-json", import_json_path]
    if str(trade_date_from or "").strip():
        command.extend(["--trade-date-from", str(trade_date_from).strip()])
    if str(trade_date_to or "").strip():
        command.extend(["--trade-date-to", str(trade_date_to).strip()])
    command.extend(passthrough_args)
    return command


def _config_path_from_passthrough_args(passthrough_args: List[str]) -> str:
    for index, arg in enumerate(passthrough_args):
        raw = str(arg or "").strip()
        if raw == "--config" and index + 1 < len(passthrough_args):
            candidate = str(passthrough_args[index + 1] or "").strip()
            if candidate:
                return candidate
        if raw.startswith("--config="):
            candidate = raw.split("=", 1)[1].strip()
            if candidate:
                return candidate
    return "config.json"


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("capital_xls_path", help="Capital Securities OSHistoryDealAll.xls export path")
    parser.add_argument("--trade-date-from", default="", help="Optional ET trade-date lower bound (YYYY-MM-DD)")
    parser.add_argument("--trade-date-to", default="", help="Optional ET trade-date upper bound (YYYY-MM-DD)")
    args, passthrough_args = parser.parse_known_args(argv)

    config_path = _config_path_from_passthrough_args(passthrough_args)
    numeric_precision = load_state_engine_numeric_precision(config_path)
    trades = parse_capital_xls_trades(
        args.capital_xls_path,
        cash_amount_ndigits=int(numeric_precision["trade_cash_amount"]),
    )
    temp_json_path = _write_temp_import_json(trades)
    try:
        proc = subprocess.run(
            _build_update_states_command(
                str(temp_json_path),
                passthrough_args,
                trade_date_from=args.trade_date_from,
                trade_date_to=args.trade_date_to,
            )
        )
        return int(proc.returncode or 0)
    finally:
        try:
            temp_json_path.unlink()
        except OSError:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
