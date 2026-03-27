from __future__ import annotations

from datetime import date, timedelta
import json
import tempfile
import unittest
from pathlib import Path

from core.backtest import run_backtest, write_backtest_outputs


def _numeric_precision_overrides(**overrides: int) -> dict:
    base = {
        "usd_amount": 2,
        "display_price": 2,
        "display_pct": 2,
        "trade_cash_amount": 4,
        "trade_dedupe_amount": 6,
        "state_selected_fields": 4,
        "backtest_amount": 4,
        "backtest_price": 4,
        "backtest_rate": 6,
        "backtest_cost_param": 6,
    }
    base.update({key: int(value) for key, value in overrides.items()})
    return base


def _backtest_config(
    *,
    commission_per_trade: float,
    slippage_bps: float,
    backtest_starting_cash: float,
    fee_rate: float,
    csv_sources: dict,
    tactical_indicators: dict,
    numeric_precision: dict | None = None,
    backtest_strategy: str | None = None,
    mean_reversion_backtest: dict | None = None,
    live_fee_rate: float | None = None,
) -> dict:
    payload = {
        "backtest": {
            "default_strategy": backtest_strategy or "tactical",
            "starting_cash": backtest_starting_cash,
            "costs": {
                "fee_rate": fee_rate,
                "commission_per_trade": commission_per_trade,
                "slippage_bps": slippage_bps,
            },
            "tactical": {
                "starting_cash": backtest_starting_cash,
                "tickers": list(tactical_indicators.keys()),
                "indicators": tactical_indicators,
            },
        },
        "state_engine": {
            "fee_rate": fee_rate if live_fee_rate is None else live_fee_rate,
            "csv_sources": csv_sources,
            "buckets": {"tactical": {"tickers": list(tactical_indicators.keys())}},
            "tactical_indicators": tactical_indicators,
            "numeric_precision": numeric_precision or _numeric_precision_overrides(),
        },
    }
    if mean_reversion_backtest is not None:
        payload["backtest"]["mean_reversion"] = mean_reversion_backtest
    return payload


class BacktestTests(unittest.TestCase):
    def test_run_backtest_rejects_end_date_after_last_common_trading_day(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            csv_dir = root / "data"
            csv_dir.mkdir()
            (csv_dir / "AAA.csv").write_text(
                "Date,Open,High,Low,Close,Volume\n"
                "2026-01-01,10,10,10,10,100\n"
                "2026-01-02,10,10,10,10,100\n"
                "2026-01-05,10,10,10,10,100\n"
                "2026-01-06,10,10,10,10,100\n"
                "2026-01-07,10,10,10,10,100\n"
                "2026-01-08,12,12,12,12,100\n"
                "2026-01-09,14,16,13,16,100\n"
                "2026-01-12,17,18,7,8,100\n"
                "2026-01-13,9,11,8,11,100\n",
                encoding="utf-8",
            )
            config_path = root / "config.json"
            config_path.write_text(
                json.dumps(
                    _backtest_config(
                        commission_per_trade=0.0,
                        slippage_bps=0.0,
                        backtest_starting_cash=100.0,
                        fee_rate=0.0,
                        csv_sources={"AAA": "AAA.csv"},
                        tactical_indicators={"AAA": {"ma_type": "SMA", "window": 2}},
                    ),
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "end_date is later than the last common trading day"):
                run_backtest(
                    config_path=str(config_path),
                    csv_dir=str(csv_dir),
                    start_date_et="2026-01-08",
                    end_date_et="2026-01-31",
                )

    def test_run_backtest_rejects_start_date_without_enough_warmup_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            csv_dir = root / "data"
            csv_dir.mkdir()
            rows = ["Date,Open,High,Low,Close,Volume"]
            start = date(2026, 1, 1)
            for idx in range(120):
                day = (start + timedelta(days=idx)).isoformat()
                rows.append(f"{day},10,10,10,10,100")
            (csv_dir / "AAA.csv").write_text("\n".join(rows) + "\n", encoding="utf-8")
            config_path = root / "config.json"
            config_path.write_text(
                json.dumps(
                    _backtest_config(
                        commission_per_trade=0.0,
                        slippage_bps=0.0,
                        backtest_starting_cash=100.0,
                        fee_rate=0.0,
                        csv_sources={"AAA": "AAA.csv"},
                        tactical_indicators={"AAA": {"ma_type": "SMA", "window": 100}},
                    ),
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "not enough warmup history before the requested start_date"):
                run_backtest(
                    config_path=str(config_path),
                    csv_dir=str(csv_dir),
                    start_date_et="2026-02-15",
                    end_date_et="2026-04-20",
                )

    def test_run_backtest_uses_t_plus_1_mid_price_and_fees(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            csv_dir = root / "data"
            csv_dir.mkdir()
            (csv_dir / "AAA.csv").write_text(
                "Date,Open,High,Low,Close,Volume\n"
                "2026-01-01,10,10,10,10,100\n"
                "2026-01-02,10,10,10,10,100\n"
                "2026-01-05,10,10,10,10,100\n"
                "2026-01-06,10,10,10,10,100\n"
                "2026-01-07,10,10,10,10,100\n"
                "2026-01-08,12,12,12,12,100\n"
                "2026-01-09,14,16,13,16,100\n"
                "2026-01-12,17,18,7,8,100\n"
                "2026-01-13,9,11,8,11,100\n",
                encoding="utf-8",
            )
            config_path = root / "config.json"
            config_path.write_text(
                json.dumps(
                    _backtest_config(
                        commission_per_trade=1.0,
                        slippage_bps=0.0,
                        backtest_starting_cash=100.0,
                        fee_rate=0.01,
                        live_fee_rate=0.0,
                        csv_sources={"AAA": "AAA.csv"},
                        tactical_indicators={"AAA": {"ma_type": "SMA", "window": 2}},
                    ),
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            result = run_backtest(config_path=str(config_path), csv_dir=str(csv_dir))

            gross_summary = result["gross"]["summary"]
            net_summary = result["net"]["summary"]
            net_trades = result["net"]["trades"]

            self.assertEqual(gross_summary["trade_count"], 2)
            self.assertEqual(net_summary["trade_count"], 2)
            self.assertAlmostEqual(float(gross_summary["ending_nav_usd"]), 70.0, places=4)
            self.assertAlmostEqual(float(net_summary["ending_nav_usd"]), 66.5, places=4)
            self.assertAlmostEqual(float(net_summary["profit_rate"]), -0.335, places=6)

            self.assertEqual(net_trades[0]["side"], "BUY")
            self.assertAlmostEqual(float(net_trades[0]["price"]), 15.0, places=4)
            self.assertAlmostEqual(float(net_trades[0]["fee"]), 1.9, places=4)
            self.assertAlmostEqual(float(net_trades[0]["cash_amount"]), 91.9, places=4)

            self.assertEqual(net_trades[1]["side"], "SELL")
            self.assertAlmostEqual(float(net_trades[1]["price"]), 10.0, places=4)
            self.assertAlmostEqual(float(net_trades[1]["fee"]), 1.6, places=4)
            self.assertAlmostEqual(float(net_trades[1]["cash_amount"]), 58.4, places=4)

    def test_run_backtest_supports_custom_dates_and_starting_cash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            csv_dir = root / "data"
            csv_dir.mkdir()
            (csv_dir / "AAA.csv").write_text(
                "Date,Open,High,Low,Close,Volume\n"
                "2026-01-01,10,10,10,10,100\n"
                "2026-01-02,10,10,10,10,100\n"
                "2026-01-05,10,10,10,10,100\n"
                "2026-01-06,10,10,10,10,100\n"
                "2026-01-07,10,10,10,10,100\n"
                "2026-01-08,12,12,12,12,100\n"
                "2026-01-09,14,16,13,16,100\n"
                "2026-01-12,17,18,7,8,100\n"
                "2026-01-13,9,11,8,11,100\n",
                encoding="utf-8",
            )
            config_path = root / "config.json"
            config_path.write_text(
                json.dumps(
                    _backtest_config(
                        commission_per_trade=0.0,
                        slippage_bps=0.0,
                        backtest_starting_cash=100.0,
                        fee_rate=0.0,
                        csv_sources={"AAA": "AAA.csv"},
                        tactical_indicators={"AAA": {"ma_type": "SMA", "window": 2}},
                    ),
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            result = run_backtest(
                config_path=str(config_path),
                csv_dir=str(csv_dir),
                start_date_et="2026-01-08",
                end_date_et="2026-01-12",
                starting_cash=200.0,
            )

            net_summary = result["net"]["summary"]
            self.assertEqual(net_summary["start_date_et"], "2026-01-08")
            self.assertEqual(net_summary["end_date_et"], "2026-01-12")
            self.assertAlmostEqual(float(net_summary["starting_cash_usd"]), 200.0, places=4)
            self.assertAlmostEqual(float(net_summary["starting_nav_usd"]), 200.0, places=4)

    def test_write_backtest_outputs_emits_markdown_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            csv_dir = root / "data"
            csv_dir.mkdir()
            (csv_dir / "AAA.csv").write_text(
                "Date,Open,High,Low,Close,Volume\n"
                "2026-01-01,10,10,10,10,100\n"
                "2026-01-02,10,10,10,10,100\n"
                "2026-01-05,10,10,10,10,100\n"
                "2026-01-06,10,10,10,10,100\n"
                "2026-01-07,10,10,10,10,100\n"
                "2026-01-08,12,12,12,12,100\n"
                "2026-01-09,14,16,13,16,100\n"
                "2026-01-12,17,18,7,8,100\n"
                "2026-01-13,9,11,8,11,100\n",
                encoding="utf-8",
            )
            config_path = root / "config.json"
            config_path.write_text(
                json.dumps(
                    _backtest_config(
                        commission_per_trade=0.0,
                        slippage_bps=0.0,
                        backtest_starting_cash=100.0,
                        fee_rate=0.0,
                        csv_sources={"AAA": "AAA.csv"},
                        tactical_indicators={"AAA": {"ma_type": "SMA", "window": 2}},
                    ),
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            out_dir = root / "out"

            result = run_backtest(config_path=str(config_path), csv_dir=str(csv_dir))
            written = write_backtest_outputs(result, str(out_dir))
            summary_payload = json.loads(Path(written["summary"]).read_text(encoding="utf-8"))
            report_text = Path(written["report"]).read_text(encoding="utf-8")

            self.assertIn("## Summary (Net)", report_text)
            self.assertIn("| Total Return | -30.00% |", report_text)
            self.assertIn("| Tactical | $100.00 | $70.00 | -30.00% | $76.00 | -24.00% | -6.00% |", report_text)
            self.assertIn("| Total | $100.00 | $70.00 | -30.00% | $76.00 | -24.00% | -6.00% |", report_text)
            self.assertNotIn("| Core |", report_text)
            self.assertNotIn("scope", summary_payload)

    def test_write_backtest_outputs_uses_configured_display_precision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            csv_dir = root / "data"
            csv_dir.mkdir()
            (csv_dir / "AAA.csv").write_text(
                "Date,Open,High,Low,Close,Volume\n"
                "2026-01-01,10,10,10,10,100\n"
                "2026-01-02,10,10,10,10,100\n"
                "2026-01-05,10,10,10,10,100\n"
                "2026-01-06,10,10,10,10,100\n"
                "2026-01-07,10,10,10,10,100\n"
                "2026-01-08,12,12,12,12,100\n"
                "2026-01-09,14,16,13,16,100\n"
                "2026-01-12,17,18,7,8,100\n"
                "2026-01-13,9,11,8,11,100\n",
                encoding="utf-8",
            )
            config_path = root / "config.json"
            config_path.write_text(
                json.dumps(
                    _backtest_config(
                        commission_per_trade=0.0,
                        slippage_bps=0.0,
                        backtest_starting_cash=100.0,
                        fee_rate=0.0,
                        csv_sources={"AAA": "AAA.csv"},
                        tactical_indicators={"AAA": {"ma_type": "SMA", "window": 2}},
                        numeric_precision=_numeric_precision_overrides(usd_amount=1, display_pct=1),
                    ),
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            out_dir = root / "out"

            result = run_backtest(config_path=str(config_path), csv_dir=str(csv_dir))
            written = write_backtest_outputs(result, str(out_dir))
            report_text = Path(written["report"]).read_text(encoding="utf-8")

            self.assertIn("| Total Return | -30.0% |", report_text)
            self.assertIn("| Tactical | $100.0 | $70.0 | -30.0% | $76.0 | -24.0% | -6.0% |", report_text)

    def test_run_backtest_mean_reversion_uses_t_plus_1_mid_price_and_take_profit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            csv_dir = root / "data"
            csv_dir.mkdir()
            (csv_dir / "AAA.csv").write_text(
                "Date,Open,High,Low,Close,Volume\n"
                "2026-01-01,100,100,100,100,100\n"
                "2026-01-02,98,98,96,97,100\n"
                "2026-01-05,96,99,95,98,100\n"
                "2026-01-06,100,102,99,101,100\n"
                "2026-01-07,102,104,101,104,100\n",
                encoding="utf-8",
            )
            config_path = root / "config.json"
            config_path.write_text(
                json.dumps(
                    _backtest_config(
                        commission_per_trade=0.0,
                        slippage_bps=0.0,
                        backtest_starting_cash=100.0,
                        fee_rate=0.0,
                        csv_sources={"AAA": "AAA.csv"},
                        tactical_indicators={"AAA": {"ma_type": "SMA", "window": 2}},
                        backtest_strategy="mean-reversion",
                        mean_reversion_backtest={
                            "tickers": ["AAA"],
                            "entry_drawdown_pct": 0.02,
                            "take_profit_pct": 0.02,
                            "stop_loss_pct": 0.07,
                            "starting_cash_per_ticker": 100.0,
                        },
                    ),
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            result = run_backtest(config_path=str(config_path), csv_dir=str(csv_dir), strategy="mean-reversion")

            self.assertEqual(result["strategy"], "mean-reversion")
            net_summary = result["net"]["summary"]
            net_trades = result["net"]["trades"]
            net_per_ticker = result["net"]["per_ticker"]

            self.assertEqual(net_summary["trade_count"], 2)
            self.assertEqual(net_summary["take_profit_sell_count"], 1)
            self.assertEqual(net_summary["stop_loss_sell_count"], 0)
            self.assertAlmostEqual(float(net_summary["ending_nav_usd"]), 106.0, places=4)
            self.assertAlmostEqual(float(net_summary["profit_rate"]), 0.06, places=6)
            self.assertEqual(len(net_per_ticker), 1)
            self.assertEqual(net_per_ticker[0]["ticker"], "AAA")
            self.assertAlmostEqual(float(net_per_ticker[0]["ending_nav_usd"]), 106.0, places=4)
            self.assertAlmostEqual(float(net_per_ticker[0]["win_rate"]), 1.0, places=6)

            self.assertEqual(net_trades[0]["side"], "BUY")
            self.assertAlmostEqual(float(net_trades[0]["price"]), 97.0, places=4)
            self.assertEqual(net_trades[1]["side"], "SELL")
            self.assertAlmostEqual(float(net_trades[1]["price"]), 103.0, places=4)

    def test_run_backtest_mean_reversion_stop_loss_uses_entry_price(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            csv_dir = root / "data"
            csv_dir.mkdir()
            (csv_dir / "AAA.csv").write_text(
                "Date,Open,High,Low,Close,Volume\n"
                "2026-01-01,100,100,100,100,100\n"
                "2026-01-02,98,98,96,97,100\n"
                "2026-01-05,96,99,95,98,100\n"
                "2026-01-06,89,90,88,89,100\n"
                "2026-01-07,90,90,90,90,100\n",
                encoding="utf-8",
            )
            config_path = root / "config.json"
            config_path.write_text(
                json.dumps(
                    _backtest_config(
                        commission_per_trade=0.0,
                        slippage_bps=0.0,
                        backtest_starting_cash=100.0,
                        fee_rate=0.0,
                        csv_sources={"AAA": "AAA.csv"},
                        tactical_indicators={"AAA": {"ma_type": "SMA", "window": 2}},
                        backtest_strategy="mean-reversion",
                        mean_reversion_backtest={
                            "tickers": ["AAA"],
                            "entry_drawdown_pct": 0.02,
                            "take_profit_pct": 0.02,
                            "stop_loss_pct": 0.07,
                            "starting_cash_per_ticker": 100.0,
                        },
                    ),
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            out_dir = root / "out"

            result = run_backtest(config_path=str(config_path), csv_dir=str(csv_dir), strategy="mean-reversion")
            written = write_backtest_outputs(result, str(out_dir))
            summary_payload = json.loads(Path(written["summary"]).read_text(encoding="utf-8"))
            report_text = Path(written["report"]).read_text(encoding="utf-8")

            net_summary = result["net"]["summary"]
            self.assertEqual(net_summary["take_profit_sell_count"], 0)
            self.assertEqual(net_summary["stop_loss_sell_count"], 1)
            self.assertAlmostEqual(float(net_summary["ending_nav_usd"]), 93.0, places=4)
            self.assertEqual(summary_payload["strategy"], "mean-reversion")
            self.assertIn("per_ticker", summary_payload["net"])
            self.assertIn("## Per Ticker (Net)", report_text)
            self.assertIn("| AAA | $100.00 | $93.00 | -7.00% |", report_text)


if __name__ == "__main__":
    unittest.main()
