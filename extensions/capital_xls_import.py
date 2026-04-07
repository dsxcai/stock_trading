from __future__ import annotations

import argparse
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import update_states as update_states_cli
from core.reconciliation import _first_token_ticker, _num_from_cell
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
        elif tag_name in {"td", "th"} and self._current_row is not None and self._current_cell is None:
            self._current_cell = []

    def handle_data(self, data: str) -> None:
        if self._current_cell is not None:
            self._current_cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag_name = str(tag or "").strip().lower()
        if tag_name in {"td", "th"} and self._current_cell is not None and self._current_row is not None:
            self._current_row.append("".join(self._current_cell).strip())
            self._current_cell = None
        elif tag_name == "tr" and self._current_row is not None:
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
    if not parser.rows:
        return ({}, [])
    headers = [str(cell or "").strip() for cell in parser.rows[0]]
    columns = {header: idx for idx, header in enumerate(headers) if header}
    missing = [header for header in _CAPITAL_XLS_REQUIRED_HEADERS if header not in columns]
    if missing:
        raise ValueError(f"{xls_path}: missing required Capital XLS columns: {', '.join(missing)}")
    return columns, parser.rows[1:]


def _cell(row: List[str], columns: Dict[str, int], header: str) -> str:
    idx = columns[header]
    return str(row[idx] if idx < len(row) else "").strip()


def _build_trade_from_capital_xls_row(xls_path: str, row: List[str], columns: Dict[str, int], cash_amount_ndigits: int) -> Optional[Dict[str, Any]]:
    if not row or all(not str(value or "").strip() for value in row):
        return None

    product_name = _cell(row, columns, "商品名稱")
    trade_date_et = _normalize_trade_date_et(_cell(row, columns, "交易日"))
    time_tw_normalized = _normalize_time_tw(_cell(row, columns, "成交時間"))
    price_value = _num_from_cell(_cell(row, columns, "成交單價"))
    gross_value = float(_num_from_cell(_cell(row, columns, "成交價金")) or 0.0)
    fee_value = float(_num_from_cell(_cell(row, columns, "原幣手續費")) or 0.0)
    ticker = _first_token_ticker(product_name).upper()
    if not product_name or not trade_date_et or not time_tw_normalized or not ticker or price_value is None:
        return None

    side = _normalize_capital_side(_cell(row, columns, "買賣別"))
    shares_value = int(round(_num_from_cell(_cell(row, columns, "成交股數/單位數")) or 0.0))
    net_value = float(_num_from_cell(_cell(row, columns, "原幣淨收付")) or gross_value)
    cash_amount_value = round(gross_value + fee_value if side == "BUY" else max(gross_value - fee_value, 0.0), int(cash_amount_ndigits))
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
    trades = [
        trade
        for row in table_rows
        for trade in [_build_trade_from_capital_xls_row(xls_path, row, columns, cash_amount_ndigits=cash_amount_ndigits)]
        if trade is not None
    ]
    trades.sort(key=lambda trade: (trade.get("trade_date_et", ""), trade.get("time_tw", ""), trade.get("ticker", ""), trade.get("side", "")))
    return trades


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("capital_xls_path", help="Capital Securities OSHistoryDealAll.xls export path")
    parser.add_argument("--trade-date-from", default="", help="Optional ET trade-date lower bound (YYYY-MM-DD)")
    parser.add_argument("--trade-date-to", default="", help="Optional ET trade-date upper bound (YYYY-MM-DD)")
    return parser


def _command_argv(args: argparse.Namespace, passthrough_args: List[str]) -> List[str]:
    argv = [str(args.capital_xls_path)]
    if str(args.trade_date_from or "").strip():
        argv.extend(["--trade-date-from", str(args.trade_date_from).strip()])
    if str(args.trade_date_to or "").strip():
        argv.extend(["--trade-date-to", str(args.trade_date_to).strip()])
    argv.extend(passthrough_args)
    return argv


def main(argv: Optional[List[str]] = None) -> int:
    args, passthrough_args = build_parser().parse_known_args(argv)
    update_args = update_states_cli.parse_args(passthrough_args)
    numeric_precision = load_state_engine_numeric_precision(update_args.config)
    update_args.imported_trade_batches = [
        {
            "import_path": str(Path(args.capital_xls_path)),
            "trades": parse_capital_xls_trades(
                args.capital_xls_path,
                cash_amount_ndigits=int(numeric_precision["trade_cash_amount"]),
            ),
        }
    ]
    update_args.trade_date_from = str(args.trade_date_from or "").strip()
    update_args.trade_date_to = str(args.trade_date_to or "").strip()
    return update_states_cli.run_args(
        update_args,
        argv=_command_argv(args, passthrough_args),
        script_name="extensions.capital_xls_import",
        log_name="capital_xls_import",
    )


if __name__ == "__main__":
    raise SystemExit(main())
