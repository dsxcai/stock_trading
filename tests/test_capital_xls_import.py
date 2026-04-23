# Copyright (c) 2026 Sheng-Hsin Tsai
# SPDX-License-Identifier: MIT

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from extensions.capital_xls_import import parse_capital_xls_trades

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES_DIR = REPO_ROOT / "tests" / "fixtures"
_CAPITAL_HEADERS = [
    "商品名稱",
    "交易日",
    "買賣別",
    "成交單價",
    "成交股數/單位數",
    "成交價金",
    "專戶別",
    "庫存別",
    "參考匯率",
    "成交時間",
    "預計入扣帳日期",
    "交易類別",
    "前手息",
    "原幣手續費",
    "原幣淨收付",
    "台幣淨收付(預估)",
    "商品幣別",
    "交割幣別",
    "委託來源",
    "",
]


def _trade_cash_amount_ndigits(config_path: Path | None = None) -> int:
    return 4


def _minimal_config(trade_cash_amount_ndigits: int, state_selected_fields_ndigits: int = 4) -> dict:
    return {
        "state_engine": {
            "meta": {
                "trades_file": "trades.json",
            },
            "reporting": {
                "numeric_precision": {
                    "usd_amount": 2,
                    "display_price": 2,
                    "display_pct": 2,
                    "trade_cash_amount": int(trade_cash_amount_ndigits),
                    "trade_dedupe_amount": 6,
                    "state_selected_fields": int(state_selected_fields_ndigits),
                    "backtest_amount": 4,
                    "backtest_price": 4,
                    "backtest_rate": 6,
                    "backtest_cost_param": 6,
                },
            },
        }
    }


def _capital_row(
    product_name: str,
    trade_date: str,
    side: str,
    price: str,
    shares: str,
    gross: str,
    time_tw: str,
    fee: str,
    net: str,
) -> list[str]:
    return [
        product_name,
        trade_date,
        side,
        price,
        shares,
        gross,
        "外幣",
        "一般/定股",
        "0",
        time_tw,
        "2026/03/24",
        "",
        "0",
        fee,
        net,
        "0",
        "USD",
        "USD",
        "新網上發",
        "",
    ]


def _write_capital_xls(path: Path, rows: list[list[str]]) -> None:
    parts = ["<table><tr>"]
    parts.extend(f"<td>{cell}</td>" for cell in _CAPITAL_HEADERS)
    parts.append("</tr>")
    for row in rows:
        parts.append("<tr>")
        parts.extend(f"<td>{cell}</td>" for cell in row)
        parts.append("</tr>")
    parts.append("</table>")
    path.write_text("".join(parts), encoding="utf-8")


def _trade(trade_date_et: str, time_tw: str, ticker: str, side: str, shares: int, gross: float, fee: float) -> dict:
    cash_amount = gross + fee if side == "BUY" else max(gross - fee, 0.0)
    return {
        "trade_id": 99,
        "trade_date_et": trade_date_et,
        "time_tw": time_tw,
        "ticker": ticker,
        "side": side,
        "shares": shares,
        "gross": gross,
        "fee": fee,
        "cash_amount": cash_amount,
        "cash_basis": "Total",
        "notes": f"seed {ticker}",
        "source": "seed:test",
    }


def _capital_import_command(xls_path: Path) -> list[str]:
    return [
        sys.executable, "-m", "extensions.capital_xls_import", str(xls_path),
        "--config", str(FIXTURES_DIR / "test_config.json"),
    ]


class CapitalXLSImportTests(unittest.TestCase):
    def test_parse_capital_xls_trades(self) -> None:
        ndigits = _trade_cash_amount_ndigits()
        with tempfile.TemporaryDirectory() as tmp:
            xls_path = Path(tmp) / "OSHistoryDealAll.xls"
            _write_capital_xls(
                xls_path,
                [
                    _capital_row("NVDA 輝達", "2026/03/20", "賣出", "177.2000", "33股", "5,847.6000", "2026/03/20 21:31:18", "11.70", "5,835.90"),
                    _capital_row("SMH VanEck半導體ETF", "2026/03/20", "買入", "393.9320", "14股", "5,515.0500", "2026/03/20 21:34:37", "11.03", "5,526.08"),
                ],
            )

            trades = parse_capital_xls_trades(str(xls_path), cash_amount_ndigits=ndigits)

            self.assertEqual(len(trades), 2)

            sell_trade = trades[0]
            self.assertEqual(sell_trade["trade_date_et"], "2026-03-20")
            self.assertEqual(sell_trade["time_tw"], "2026/03/20 21:31:18")
            self.assertEqual(sell_trade["ticker"], "NVDA")
            self.assertEqual(sell_trade["side"], "SELL")
            self.assertEqual(sell_trade["shares"], 33)
            self.assertAlmostEqual(float(sell_trade["cash_amount"]), 5835.90, places=4)
            self.assertEqual(sell_trade["source"], "capital_xls:OSHistoryDealAll.xls")

            buy_trade = trades[1]
            self.assertEqual(buy_trade["ticker"], "SMH")
            self.assertEqual(buy_trade["side"], "BUY")
            self.assertEqual(buy_trade["shares"], 14)
            self.assertAlmostEqual(float(buy_trade["gross"]), 5515.05, places=4)
            self.assertAlmostEqual(float(buy_trade["fee"]), 11.03, places=4)
            self.assertAlmostEqual(float(buy_trade["net"]), 5526.08, places=4)

    def test_parse_capital_xls_rounds_cash_amount_to_4dp(self) -> None:
        ndigits = _trade_cash_amount_ndigits()
        with tempfile.TemporaryDirectory() as tmp:
            xls_path = Path(tmp) / "OSHistoryDealAll.xls"
            _write_capital_xls(
                xls_path,
                [
                    _capital_row("NVDA 輝達", "2026/03/20", "賣出", "177.2000", "33股", "5,847.60009", "2026/03/20 21:31:18", "11.70003", "5,835.90006"),
                    _capital_row("SMH VanEck半導體ETF", "2026/03/20", "買入", "393.9320", "14股", "5,515.05006", "2026/03/20 21:34:37", "11.03463", "5,526.08469"),
                ],
            )

            trades = parse_capital_xls_trades(str(xls_path), cash_amount_ndigits=ndigits)
            by_ticker = {trade["ticker"]: trade for trade in trades}

            self.assertAlmostEqual(float(by_ticker["NVDA"]["cash_amount"]), round(5847.60009 - 11.70003, ndigits), places=ndigits)
            self.assertAlmostEqual(float(by_ticker["SMH"]["cash_amount"]), round(5515.05006 + 11.03463, ndigits), places=ndigits)

    def test_parse_capital_xls_requires_headers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            xls_path = Path(tmp) / "OSHistoryDealAll.xls"
            xls_path.write_text(
                (
                    "<table>"
                    "<tr>"
                    "<td>商品名稱</td><td>交易日</td><td>買賣別</td><td>成交單價</td>"
                    "<td>成交股數/單位數</td><td>成交價金</td><td>成交時間</td><td>原幣手續費</td>"
                    "</tr>"
                    "</table>"
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "missing required Capital XLS columns"):
                parse_capital_xls_trades(str(xls_path), cash_amount_ndigits=_trade_cash_amount_ndigits())

    def test_parse_capital_xls_rejects_unsupported_side(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            xls_path = Path(tmp) / "OSHistoryDealAll.xls"
            _write_capital_xls(
                xls_path,
                [
                    _capital_row("NVDA 輝達", "2026/03/20", "現股當沖", "177.2000", "33股", "5,847.6000", "2026/03/20 21:31:18", "11.70", "5,835.90"),
                ],
            )

            with self.assertRaisesRegex(ValueError, "unsupported Capital XLS side"):
                parse_capital_xls_trades(str(xls_path), cash_amount_ndigits=_trade_cash_amount_ndigits())

    def test_capital_xls_wrapper_defaults_to_append_and_dedupes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            states_path = tmpdir / "states.json"
            trades_path = tmpdir / "trades.json"
            xls_path = tmpdir / "OSHistoryDealAll-0320.xls"

            states_path.write_text("{}\n", encoding="utf-8")
            trades_path.write_text("[]\n", encoding="utf-8")
            _write_capital_xls(
                xls_path,
                [
                    _capital_row("NVDA 輝達", "2026/03/20", "賣出", "177.2000", "33股", "5,847.6000", "2026/03/20 21:31:18", "11.70", "5,835.90"),
                    _capital_row("SMH VanEck半導體ETF", "2026/03/20", "買入", "393.9320", "14股", "5,515.0500", "2026/03/20 21:34:37", "11.03", "5,526.08"),
                ],
            )

            first = subprocess.run(
                _capital_import_command(xls_path)
                + [
                    "--states",
                    str(states_path),
                    "--out",
                    str(states_path),
                    "--trades-file",
                    str(trades_path),
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=True,
            )
            self.assertIn("mode=append", first.stdout)
            self.assertIn("added=2, dup=0", first.stdout)
            self.assertEqual(len(json.loads(trades_path.read_text(encoding="utf-8"))), 2)

            second = subprocess.run(
                _capital_import_command(xls_path)
                + [
                    "--states",
                    str(states_path),
                    "--out",
                    str(states_path),
                    "--trades-file",
                    str(trades_path),
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=True,
            )
            self.assertIn("mode=append", second.stdout)
            self.assertIn("added=0, dup=2", second.stdout)
            self.assertEqual(len(json.loads(trades_path.read_text(encoding="utf-8"))), 2)

    def test_capital_xls_wrapper_append_conflict_aborts_without_changing_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            states_path = tmpdir / "states.json"
            trades_path = tmpdir / "trades.json"
            xls_path = tmpdir / "OSHistoryDealAll-conflict.xls"

            states_path.write_text("{}\n", encoding="utf-8")
            seeded = [
                _trade("2026-03-20", "2026/03/20 21:31:18", "NVDA", "SELL", 30, 5300.0, 10.6),
            ]
            trades_path.write_text(json.dumps(seeded, ensure_ascii=False, indent=2), encoding="utf-8")
            _write_capital_xls(
                xls_path,
                [
                    _capital_row("NVDA 輝達", "2026/03/20", "賣出", "177.2000", "33股", "5,847.6000", "2026/03/20 21:31:18", "11.70", "5,835.90"),
                ],
            )

            result = subprocess.run(
                _capital_import_command(xls_path)
                + [
                    "--states",
                    str(states_path),
                    "--out",
                    str(states_path),
                    "--trades-file",
                    str(trades_path),
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("[ERR] trades import failed", result.stdout)
            self.assertIn("append import conflict detected", result.stdout)

            trades = json.loads(trades_path.read_text(encoding="utf-8"))
            self.assertEqual(trades, seeded)

    def test_capital_xls_wrapper_rounds_cash_amount_to_4dp_before_persist(self) -> None:
        ndigits = _trade_cash_amount_ndigits()
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            states_path = tmpdir / "states.json"
            trades_path = tmpdir / "trades.json"
            xls_path = tmpdir / "OSHistoryDealAll-rounding.xls"

            states_path.write_text("{}\n", encoding="utf-8")
            trades_path.write_text("[]\n", encoding="utf-8")
            _write_capital_xls(
                xls_path,
                [
                    _capital_row("NVDA 輝達", "2026/03/20", "賣出", "177.2000", "33股", "5,847.60009", "2026/03/20 21:31:18", "11.70003", "5,835.90006"),
                    _capital_row("SMH VanEck半導體ETF", "2026/03/20", "買入", "393.9320", "14股", "5,515.05006", "2026/03/20 21:34:37", "11.03463", "5,526.08469"),
                ],
            )

            result = subprocess.run(
                _capital_import_command(xls_path)
                + [
                    "--states",
                    str(states_path),
                    "--out",
                    str(states_path),
                    "--trades-file",
                    str(trades_path),
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=True,
            )

            self.assertIn("mode=append", result.stdout)
            trades = json.loads(trades_path.read_text(encoding="utf-8"))
            by_ticker = {trade["ticker"]: trade for trade in trades}
            self.assertAlmostEqual(float(by_ticker["NVDA"]["cash_amount"]), round(5847.60009 - 11.70003, ndigits), places=ndigits)
            self.assertAlmostEqual(float(by_ticker["SMH"]["cash_amount"]), round(5515.05006 + 11.03463, ndigits), places=ndigits)

    def test_capital_xls_wrapper_uses_configured_cash_amount_precision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            states_path = tmpdir / "states.json"
            trades_path = tmpdir / "trades.json"
            xls_path = tmpdir / "OSHistoryDealAll-rounding.xls"
            config_path = tmpdir / "config.json"

            states_path.write_text("{}\n", encoding="utf-8")
            trades_path.write_text("[]\n", encoding="utf-8")
            config_path.write_text(json.dumps(_minimal_config(trade_cash_amount_ndigits=2), ensure_ascii=False, indent=2), encoding="utf-8")
            _write_capital_xls(
                xls_path,
                [
                    _capital_row("NVDA 輝達", "2026/03/20", "賣出", "177.2000", "33股", "5,847.60009", "2026/03/20 21:31:18", "11.70003", "5,835.90006"),
                ],
            )

            result = subprocess.run(
                _capital_import_command(xls_path)
                + [
                    "--states",
                    str(states_path),
                    "--out",
                    str(states_path),
                    "--trades-file",
                    str(trades_path),
                    "--config",
                    str(config_path),
                    "--csv-dir",
                    str(tmpdir),
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=True,
            )

            self.assertIn("mode=append", result.stdout)
            trades = json.loads(trades_path.read_text(encoding="utf-8"))
            self.assertAlmostEqual(float(trades[0]["cash_amount"]), 5835.90, places=2)

    def test_capital_xls_wrapper_replace_replaces_full_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            states_path = tmpdir / "states.json"
            trades_path = tmpdir / "trades.json"
            xls_path = tmpdir / "OSHistoryDealAll-until-0320.xls"

            states_path.write_text("{}\n", encoding="utf-8")
            seeded = [
                _trade("2026-03-19", "2026/03/19 22:00:00", "GOOG", "BUY", 1, 300.00001, 0.60005),
                _trade("2026-03-20", "2026/03/20 21:31:18", "NVDA", "SELL", 30, 5300.0, 10.6),
                _trade("2026-03-20", "2026/03/20 21:34:37", "SMH", "BUY", 10, 3900.0, 7.8),
            ]
            trades_path.write_text(json.dumps(seeded, ensure_ascii=False, indent=2), encoding="utf-8")
            _write_capital_xls(
                xls_path,
                [
                    _capital_row("NVDA 輝達", "2026/03/20", "賣出", "177.2000", "33股", "5,847.6000", "2026/03/20 21:31:18", "11.70", "5,835.90"),
                    _capital_row("SMH VanEck半導體ETF", "2026/03/20", "買入", "393.9320", "14股", "5,515.0500", "2026/03/20 21:34:37", "11.03", "5,526.08"),
                    _capital_row("ARKQ ARKQUS", "2026/03/20", "買入", "118.0000", "2股", "236.0000", "2026/03/20 21:36:32", "0.47", "236.47"),
                ],
            )

            result = subprocess.run(
                _capital_import_command(xls_path)
                + [
                    "--states",
                    str(states_path),
                    "--out",
                    str(states_path),
                    "--trades-file",
                    str(trades_path),
                    "--trades-import-mode",
                    "replace",
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=True,
            )

            self.assertIn("mode=replace", result.stdout)
            self.assertIn("added=3, dup=0", result.stdout)
            self.assertIn("[REPLACE] removed 3 existing trade(s) from the full trade ledger.", result.stdout)

            trades = json.loads(trades_path.read_text(encoding="utf-8"))
            self.assertEqual(len(trades), 3)
            by_ticker = {trade["ticker"]: trade for trade in trades}
            self.assertEqual(by_ticker["NVDA"]["shares"], 33)
            self.assertEqual(by_ticker["SMH"]["shares"], 14)
            self.assertEqual(by_ticker["ARKQ"]["shares"], 2)
            self.assertNotIn("GOOG", by_ticker)

    def test_capital_xls_wrapper_replace_can_limit_scope_by_trade_date_range(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            states_path = tmpdir / "states.json"
            trades_path = tmpdir / "trades.json"
            xls_path = tmpdir / "OSHistoryDealAll-range.xls"

            states_path.write_text("{}\n", encoding="utf-8")
            seeded = [
                _trade("2026-03-19", "2026/03/19 22:00:00", "GOOG", "BUY", 1, 300.00001, 0.60005),
                _trade("2026-03-20", "2026/03/20 21:31:18", "NVDA", "SELL", 30, 5300.0, 10.6),
                _trade("2026-03-21", "2026/03/21 21:34:37", "SMH", "BUY", 10, 3900.0, 7.8),
            ]
            trades_path.write_text(json.dumps(seeded, ensure_ascii=False, indent=2), encoding="utf-8")
            _write_capital_xls(
                xls_path,
                [
                    _capital_row("NVDA 輝達", "2026/03/20", "賣出", "177.2000", "33股", "5,847.6000", "2026/03/20 21:31:18", "11.70", "5,835.90"),
                    _capital_row("SMH VanEck半導體ETF", "2026/03/21", "買入", "393.9320", "14股", "5,515.0500", "2026/03/21 21:34:37", "11.03", "5,526.08"),
                ],
            )

            result = subprocess.run(
                _capital_import_command(xls_path)
                + [
                    "--states",
                    str(states_path),
                    "--out",
                    str(states_path),
                    "--trades-file",
                    str(trades_path),
                    "--trades-import-mode",
                    "replace",
                    "--trade-date-from",
                    "2026-03-20",
                    "--trade-date-to",
                    "2026-03-20",
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=True,
            )

            self.assertIn("[FILTER] trades import OSHistoryDealAll-range.xls: trade_date_et range 2026-03-20..2026-03-20 | kept=1", result.stdout)
            self.assertIn("mode=replace", result.stdout)
            self.assertIn("added=1, dup=0", result.stdout)
            self.assertIn("[REPLACE] removed 1 existing trade(s)", result.stdout)

            trades = json.loads(trades_path.read_text(encoding="utf-8"))
            self.assertEqual(len(trades), 3)
            by_ticker = {trade["ticker"]: trade for trade in trades}
            self.assertEqual(by_ticker["GOOG"]["shares"], 1)
            self.assertEqual(by_ticker["NVDA"]["shares"], 33)
            self.assertEqual(by_ticker["SMH"]["shares"], 10)

    def test_capital_xls_wrapper_replace_with_empty_filtered_range_deletes_range_trades(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            states_path = tmpdir / "states.json"
            trades_path = tmpdir / "trades.json"
            xls_path = tmpdir / "OSHistoryDealAll-empty-range.xls"

            states_path.write_text("{}\n", encoding="utf-8")
            seeded = [
                _trade("2026-03-19", "2026/03/19 22:00:00", "GOOG", "BUY", 1, 300.00001, 0.60005),
                _trade("2026-03-20", "2026/03/20 21:31:18", "NVDA", "SELL", 30, 5300.0, 10.6),
                _trade("2026-03-20", "2026/03/20 21:34:37", "SMH", "BUY", 10, 3900.0, 7.8),
            ]
            trades_path.write_text(json.dumps(seeded, ensure_ascii=False, indent=2), encoding="utf-8")
            _write_capital_xls(
                xls_path,
                [
                    _capital_row("ARKQ ARKQUS", "2026/03/21", "買入", "118.0000", "2股", "236.0000", "2026/03/21 21:36:32", "0.47", "236.47"),
                ],
            )

            result = subprocess.run(
                _capital_import_command(xls_path)
                + [
                    "--states",
                    str(states_path),
                    "--out",
                    str(states_path),
                    "--trades-file",
                    str(trades_path),
                    "--trades-import-mode",
                    "replace",
                    "--trade-date-from",
                    "2026-03-20",
                    "--trade-date-to",
                    "2026-03-20",
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=True,
            )

            self.assertIn("[FILTER] trades import OSHistoryDealAll-empty-range.xls: trade_date_et range 2026-03-20..2026-03-20 | kept=0", result.stdout)
            self.assertIn("[REPLACE] removed 2 existing trade(s) in trade_date_et range 2026-03-20..2026-03-20.", result.stdout)
            self.assertIn("added=0, dup=0, mode=replace", result.stdout)

            trades = json.loads(trades_path.read_text(encoding="utf-8"))
            self.assertEqual(len(trades), 1)
            self.assertEqual(trades[0]["ticker"], "GOOG")


if __name__ == "__main__":
    unittest.main()
