# Copyright (c) 2026 Sheng-Hsin Tsai
# SPDX-License-Identifier: MIT

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from utils.precision import load_state_engine_numeric_precision

REPO_ROOT = Path(__file__).resolve().parents[1]


def _trade_cash_amount_ndigits(config_path: Path | None = None) -> int:
    resolved = config_path if config_path is not None else (REPO_ROOT / "config.json")
    return int(load_state_engine_numeric_precision(str(resolved))["trade_cash_amount"])


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


def _trade(
    trade_date_et: str,
    time_tw: str,
    ticker: str,
    shares: int,
    gross: float,
    fee: float,
    side: str = "BUY",
) -> dict:
    normalized_side = str(side or "BUY").upper()
    return {
        "trade_date_et": trade_date_et,
        "time_tw": time_tw,
        "ticker": ticker,
        "side": normalized_side,
        "shares": shares,
        "gross": gross,
        "fee": fee,
        "cash_amount": (gross + fee) if normalized_side == "BUY" else max(gross - fee, 0.0),
        "cash_basis": "Total",
        "notes": f"test import {ticker}",
        "source": "test_import:fixture",
    }


class ImportedTradesJsonTests(unittest.TestCase):
    maxDiff = None

    def _run_update_raw(
        self,
        states_path: Path,
        trades_path: Path,
        imported_path: Path,
        *extra_args: str,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                "update_states.py",
                "--states",
                str(states_path),
                "--out",
                str(states_path),
                "--trades-file",
                str(trades_path),
                "--imported-trades-json",
                str(imported_path),
                *extra_args,
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=check,
        )

    def _run_update(self, states_path: Path, trades_path: Path, imported_path: Path, *extra_args: str) -> subprocess.CompletedProcess[str]:
        return self._run_update_raw(states_path, trades_path, imported_path, *extra_args, check=True)

    def test_imported_trades_json_defaults_to_append_and_dedupes(self) -> None:
        batch_a = [
            _trade("2026-03-20", "2026/03/20 21:31:18", "NVDA", 10, 1772.0, 3.54),
            _trade("2026-03-20", "2026/03/20 21:34:37", "SMH", 5, 1969.66, 3.94),
        ]
        batch_b = [
            batch_a[0],
            batch_a[1],
            _trade("2026-03-20", "2026/03/20 21:36:32", "ARKQ", 2, 236.0, 0.47),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            states_path = tmpdir / "states.json"
            trades_path = tmpdir / "trades.json"
            import_a_path = tmpdir / "import_a.json"
            import_b_path = tmpdir / "import_b.json"

            states_path.write_text("{}\n", encoding="utf-8")
            trades_path.write_text("[]\n", encoding="utf-8")
            import_a_path.write_text(json.dumps(batch_a, ensure_ascii=False, indent=2), encoding="utf-8")
            import_b_path.write_text(json.dumps(batch_b, ensure_ascii=False, indent=2), encoding="utf-8")

            first = self._run_update(states_path, trades_path, import_a_path)
            self.assertIn("mode=append", first.stdout)
            self.assertIn("added=2, dup=0", first.stdout)
            first_trades = json.loads(trades_path.read_text(encoding="utf-8"))
            self.assertEqual(len(first_trades), 2)
            self.assertAlmostEqual(float(first_trades[0]["cash_amount"]), 1775.54, places=4)
            self.assertAlmostEqual(float(first_trades[1]["cash_amount"]), 1973.6, places=4)

            second = self._run_update(states_path, trades_path, import_b_path)
            self.assertIn("mode=append", second.stdout)
            self.assertIn("added=1, dup=2", second.stdout)
            second_trades = json.loads(trades_path.read_text(encoding="utf-8"))
            self.assertEqual(len(second_trades), 3)
            self.assertAlmostEqual(float(second_trades[2]["cash_amount"]), 236.47, places=4)

    def test_imported_trades_json_append_conflict_aborts_without_changing_ledger(self) -> None:
        existing = [
            _trade("2026-03-20", "2026/03/20 21:31:18", "NVDA", 10, 1772.0, 3.54),
        ]
        incoming = [
            _trade("2026-03-20", "2026/03/20 21:31:18", "NVDA", 12, 2126.4, 4.25),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            states_path = tmpdir / "states.json"
            trades_path = tmpdir / "trades.json"
            import_path = tmpdir / "import_conflict.json"

            states_path.write_text("{}\n", encoding="utf-8")
            trades_path.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
            import_path.write_text(json.dumps(incoming, ensure_ascii=False, indent=2), encoding="utf-8")

            result = self._run_update_raw(states_path, trades_path, import_path, check=False)

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("[ERR] trades import failed", result.stdout)
            self.assertIn("append import conflict detected", result.stdout)
            self.assertIn("[ABORT] No state update and no report file were generated.", result.stdout)

            trades = json.loads(trades_path.read_text(encoding="utf-8"))
            self.assertEqual(trades, existing)

    def test_imported_trades_json_rounds_cash_amount_to_4dp_before_persist(self) -> None:
        ndigits = _trade_cash_amount_ndigits()
        incoming = [
            _trade("2026-03-20", "2026/03/20 21:31:18", "NVDA", 10, 1772.00004, 3.54005),
            _trade("2026-03-20", "2026/03/20 21:34:37", "SMH", 5, 1969.66004, 3.94005),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            states_path = tmpdir / "states.json"
            trades_path = tmpdir / "trades.json"
            import_path = tmpdir / "import_rounding.json"

            states_path.write_text("{}\n", encoding="utf-8")
            trades_path.write_text("[]\n", encoding="utf-8")
            import_path.write_text(json.dumps(incoming, ensure_ascii=False, indent=2), encoding="utf-8")

            result = self._run_update(states_path, trades_path, import_path)
            self.assertIn("mode=append", result.stdout)

            trades = json.loads(trades_path.read_text(encoding="utf-8"))
            by_ticker = {trade["ticker"]: trade for trade in trades}
            self.assertAlmostEqual(float(by_ticker["NVDA"]["cash_amount"]), round(1772.00004 + 3.54005, ndigits), places=ndigits)
            self.assertAlmostEqual(float(by_ticker["SMH"]["cash_amount"]), round(1969.66004 + 3.94005, ndigits), places=ndigits)

    def test_imported_trades_json_uses_configured_cash_amount_precision(self) -> None:
        incoming = [
            _trade("2026-03-20", "2026/03/20 21:31:18", "NVDA", 10, 1772.004, 3.545),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            states_path = tmpdir / "states.json"
            trades_path = tmpdir / "trades.json"
            import_path = tmpdir / "import_rounding.json"
            config_path = tmpdir / "config.json"

            states_path.write_text("{}\n", encoding="utf-8")
            trades_path.write_text("[]\n", encoding="utf-8")
            import_path.write_text(json.dumps(incoming, ensure_ascii=False, indent=2), encoding="utf-8")
            config_path.write_text(json.dumps(_minimal_config(trade_cash_amount_ndigits=2), ensure_ascii=False, indent=2), encoding="utf-8")

            result = self._run_update(states_path, trades_path, import_path, "--config", str(config_path), "--csv-dir", str(tmpdir))
            self.assertIn("mode=append", result.stdout)

            trades = json.loads(trades_path.read_text(encoding="utf-8"))
            self.assertAlmostEqual(float(trades[0]["cash_amount"]), 1775.55, places=2)

    def test_imported_trades_json_rebuilds_fifo_cost_basis_and_valuation_from_full_ledger(self) -> None:
        existing = [
            _trade("2026-03-18", "2026/03/18 21:30:00", "AAA", 1, 100.0, 1.0),
            _trade("2026-03-18", "2026/03/18 21:31:00", "AAA", 1, 200.0, 1.0),
            _trade("2026-03-18", "2026/03/18 21:32:00", "AAA", 1, 150.0, 1.0, side="SELL"),
        ]
        incoming = [
            _trade("2026-03-20", "2026/03/20 21:33:00", "AAA", 1, 300.0, 1.0),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            states_path = tmpdir / "states.json"
            trades_path = tmpdir / "trades.json"
            import_path = tmpdir / "import_fifo.json"

            states_path.write_text(
                json.dumps(
                    {
                        "portfolio": {
                            "positions": [
                                {
                                    "ticker": "AAA",
                                    "bucket": "tactical",
                                    "shares": 0,
                                    "cost_usd": 0.0,
                                    "price_now_override": 250.0,
                                }
                            ]
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            trades_path.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
            import_path.write_text(json.dumps(incoming, ensure_ascii=False, indent=2), encoding="utf-8")

            result = self._run_update(states_path, trades_path, import_path, "--csv-dir", str(tmpdir))
            self.assertIn("portfolio_delta=day1_rebuild", result.stdout)

            states = json.loads(states_path.read_text(encoding="utf-8"))
            positions = states.get("portfolio", {}).get("positions", [])
            self.assertEqual(len(positions), 1)
            position = positions[0]
            self.assertEqual(position, {"ticker": "AAA", "shares": 2})
            self.assertNotIn("totals", states.get("portfolio", {}))

    def test_imported_trades_json_replace_replaces_full_ledger(self) -> None:
        existing = [
            _trade("2026-03-19", "2026/03/19 22:00:00", "GOOG", 1, 300.00001, 0.60005),
            _trade("2026-03-20", "2026/03/20 21:31:18", "NVDA", 10, 1772.0, 3.54),
            _trade("2026-03-20", "2026/03/20 21:34:37", "SMH", 5, 1969.66, 3.94),
        ]
        incoming = [
            _trade("2026-03-20", "2026/03/20 21:31:18", "NVDA", 12, 2126.40004, 4.25005),
            _trade("2026-03-20", "2026/03/20 21:34:37", "SMH", 6, 2363.59004, 4.73005),
            _trade("2026-03-20", "2026/03/20 21:36:32", "ARKQ", 2, 236.0, 0.47),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            states_path = tmpdir / "states.json"
            trades_path = tmpdir / "trades.json"
            import_path = tmpdir / "import_replace.json"

            states_path.write_text("{}\n", encoding="utf-8")
            trades_path.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
            import_path.write_text(json.dumps(incoming, ensure_ascii=False, indent=2), encoding="utf-8")

            third = self._run_update(states_path, trades_path, import_path, "--trades-import-mode", "replace")
            self.assertIn("mode=replace", third.stdout)
            self.assertIn("added=3, dup=0", third.stdout)
            self.assertIn("[REPLACE] removed 3 existing trade(s) from the full trade ledger.", third.stdout)

            trades = json.loads(trades_path.read_text(encoding="utf-8"))
            self.assertEqual(len(trades), 3)
            by_ticker = {trade["ticker"]: trade for trade in trades}
            self.assertEqual(by_ticker["NVDA"]["shares"], 12)
            self.assertEqual(by_ticker["SMH"]["shares"], 6)
            self.assertEqual(by_ticker["ARKQ"]["shares"], 2)
            self.assertNotIn("GOOG", by_ticker)

    def test_imported_trades_json_replace_with_trade_date_range_replaces_full_range(self) -> None:
        existing = [
            _trade("2026-03-19", "2026/03/19 22:00:00", "GOOG", 1, 300.00001, 0.60005),
            _trade("2026-03-20", "2026/03/20 21:31:18", "NVDA", 10, 1772.0, 3.54),
            _trade("2026-03-20", "2026/03/20 21:34:37", "SMH", 5, 1969.66, 3.94),
            _trade("2026-03-21", "2026/03/21 21:36:32", "ARKQ", 2, 236.0, 0.47),
        ]
        incoming = [
            _trade("2026-03-20", "2026/03/20 21:31:18", "NVDA", 12, 2126.40004, 4.25005),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            states_path = tmpdir / "states.json"
            trades_path = tmpdir / "trades.json"
            import_path = tmpdir / "import_replace_range.json"

            states_path.write_text("{}\n", encoding="utf-8")
            trades_path.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
            import_path.write_text(json.dumps(incoming, ensure_ascii=False, indent=2), encoding="utf-8")

            result = self._run_update(
                states_path,
                trades_path,
                import_path,
                "--trades-import-mode",
                "replace",
                "--trade-date-from",
                "2026-03-20",
                "--trade-date-to",
                "2026-03-20",
            )
            self.assertIn("mode=replace", result.stdout)
            self.assertIn("[REPLACE] removed 2 existing trade(s) in trade_date_et range 2026-03-20..2026-03-20.", result.stdout)

            trades = json.loads(trades_path.read_text(encoding="utf-8"))
            self.assertEqual(len(trades), 3)
            by_ticker = {trade["ticker"]: trade for trade in trades}
            self.assertEqual(by_ticker["GOOG"]["shares"], 1)
            self.assertEqual(by_ticker["NVDA"]["shares"], 12)
            self.assertEqual(by_ticker["ARKQ"]["shares"], 2)
            self.assertNotIn("SMH", by_ticker)
            self.assertAlmostEqual(float(by_ticker["GOOG"]["cash_amount"]), 300.6001, places=4)
            self.assertAlmostEqual(float(by_ticker["NVDA"]["cash_amount"]), 2130.6501, places=4)


if __name__ == "__main__":
    unittest.main()
