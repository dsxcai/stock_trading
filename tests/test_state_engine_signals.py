from __future__ import annotations

import argparse
from contextlib import ExitStack
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest import mock

from core import state_engine
from core.models import ReportContext, TacticalPlan
from core.tactical_engine import compute_tactical_plan


def _numeric_precision() -> dict:
    return {
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


def _signal_config(tactical_indicators: dict, *, fee_rate: float | None = None) -> dict:
    config = {
        "tactical_indicators": dict(tactical_indicators),
        "numeric_precision": _numeric_precision(),
    }
    if fee_rate is not None:
        config["fee_rate"] = float(fee_rate)
    return config


class StateEngineSignalTests(unittest.TestCase):
    @staticmethod
    def _run_signal_update(states: dict) -> None:
        runtime = {"config": dict(states.get("config") or {}), "history": {}}
        state_engine._ensure_trading_calendar(runtime)
        plan = compute_tactical_plan(
            states,
            runtime,
            derive_signals_inputs="never",
            derive_threshold_inputs="never",
            mode="Premarket",
            trades=list(states.get("trades") or []),
        )
        state_engine.apply_tactical_plan(states, plan)

    def test_sell_signal_with_recent_buy_executes_sell_all(self) -> None:
        states = {
            "config": _signal_config({"AAA": "SMA50"}),
            "market": {
                "asof_t_et": "2026-03-17",
                "prices_now": {"AAA": 90.0},
                "signals_inputs": {
                    "AAA": {"close_t": 90.0, "ma_t": 100.0, "close_t_minus_5": 80.0}
                },
                "next_close_threshold_inputs": {},
            },
            "portfolio": {
                "positions": [{"ticker": "AAA", "bucket": "tactical", "shares": 10, "cost_usd": 1000.0}],
            },
            "trades": [
                {
                    "trade_id": 1,
                    "trade_date_et": "2026-03-16",
                    "time_tw": "2026/03/16 23:00:00",
                    "ticker": "AAA",
                    "side": "BUY",
                    "shares": 10,
                    "cash_amount": 900.0,
                }
            ],
        }
        self._run_signal_update(states)

        row = (states.get("signals", {}).get("tactical") or [])[0]
        self.assertTrue(bool(row.get("sell_signal")))
        self.assertFalse(bool(row.get("buy_signal")))
        self.assertEqual(row.get("t_plus_1_action"), "SELL_ALL")
        self.assertEqual(int(row.get("action_shares") or 0), 10)

    def test_buy_budget_includes_estimated_sell_reclaim(self) -> None:
        states = {
            "config": _signal_config({"AAA": "SMA50", "BBB": "SMA50"}, fee_rate=0.002),
            "market": {
                "asof_t_et": "2026-03-17",
                "prices_now": {"AAA": 100.0, "BBB": 50.0},
                "signals_inputs": {
                    "AAA": {"close_t": 90.0, "ma_t": 100.0, "close_t_minus_5": 95.0},
                    "BBB": {"close_t": 120.0, "ma_t": 100.0, "close_t_minus_5": 110.0},
                },
                "next_close_threshold_inputs": {},
            },
            "portfolio": {
                "positions": [{"ticker": "AAA", "bucket": "tactical", "shares": 10, "cost_usd": 1000.0}],
                "cash": {"usd": 0.0, "deployable_usd": 0.0, "reserve_usd": 0.0},
            },
            "trades": [
                {
                    "trade_id": 1,
                    "trade_date_et": "2026-02-20",
                    "time_tw": "2026/02/20 23:00:00",
                    "ticker": "AAA",
                    "side": "BUY",
                    "shares": 10,
                    "cash_amount": 1000.0,
                }
            ],
        }
        self._run_signal_update(states)

        rows = (states.get("signals", {}).get("tactical") or [])
        by_ticker = {str(r.get("ticker")): r for r in rows}
        sell_row = by_ticker["AAA"]
        buy_row = by_ticker["BBB"]

        self.assertEqual(sell_row.get("t_plus_1_action"), "SELL_ALL")
        self.assertEqual(int(sell_row.get("action_shares") or 0), 10)
        self.assertEqual(float(buy_row.get("investable_cash_base_usd") or 0.0), 0.0)
        self.assertEqual(float(buy_row.get("estimated_sell_reclaim_usd") or 0.0), 1000.0)
        self.assertEqual(float(buy_row.get("investable_cash_usd") or 0.0), 1000.0)
        self.assertEqual(buy_row.get("t_plus_1_action"), "BUY")
        self.assertEqual(int(buy_row.get("action_shares") or 0), 19)
        self.assertEqual(float(buy_row.get("action_cash_amount_usd") or 0.0), 951.9)

    def test_buy_more_share_sizing_uses_fee_adjusted_close(self) -> None:
        states = {
            "config": _signal_config({"AAA": "SMA50"}, fee_rate=0.002),
            "market": {
                "asof_t_et": "2026-03-17",
                "prices_now": {"AAA": 50.0},
                "signals_inputs": {
                    "AAA": {"close_t": 120.0, "ma_t": 100.0, "close_t_minus_5": 110.0}
                },
                "next_close_threshold_inputs": {},
            },
            "portfolio": {
                "positions": [{"ticker": "AAA", "bucket": "tactical", "shares": 10, "cost_usd": 500.0}],
                "cash": {"usd": 1000.0, "deployable_usd": 1000.0, "reserve_usd": 0.0},
            },
            "trades": [
                {
                    "trade_id": 1,
                    "trade_date_et": "2026-03-10",
                    "time_tw": "2026/03/10 23:00:00",
                    "ticker": "AAA",
                    "side": "BUY",
                    "shares": 10,
                    "cash_amount": 500.0,
                }
            ],
        }
        self._run_signal_update(states)

        row = (states.get("signals", {}).get("tactical") or [])[0]
        self.assertEqual(row.get("t_plus_1_action"), "BUY_MORE")
        self.assertEqual(int(row.get("action_shares") or 0), 19)
        self.assertEqual(float(row.get("action_cash_amount_usd") or 0.0), 951.9)

    def test_recent_buy_timing_does_not_turn_failed_buy_into_buy(self) -> None:
        states = {
            "config": _signal_config({"SMH": "SMA100"}),
            "market": {
                "asof_t_et": "2026-03-17",
                "prices_now": {"SMH": 393.67},
                "signals_inputs": {
                    "SMH": {"close_t": 393.67, "ma_t": 377.7339, "close_t_minus_5": 401.03}
                },
                "next_close_threshold_inputs": {},
            },
            "portfolio": {
                "positions": [{"ticker": "SMH", "bucket": "tactical", "shares": 15, "cost_usd": 5968.56}],
            },
            "trades": [
                {
                    "trade_id": 1,
                    "trade_date_et": "2026-03-16",
                    "time_tw": "2026/03/16 23:15:33",
                    "ticker": "SMH",
                    "side": "BUY",
                    "shares": 15,
                    "cash_amount": 5968.56,
                }
            ],
        }
        self._run_signal_update(states)

        row = (states.get("signals", {}).get("tactical") or [])[0]
        self.assertFalse(bool(row.get("buy_signal")))
        self.assertTrue(bool(row.get("sell_signal")))
        self.assertEqual(row.get("t_plus_1_action"), "SELL_ALL")
        self.assertEqual(int(row.get("action_shares") or 0), 15)

    def test_multiple_open_buys_render_as_single_sell_all_row(self) -> None:
        states = {
            "config": _signal_config({"AAA": "SMA50"}),
            "market": {
                "asof_t_et": "2026-03-17",
                "prices_now": {"AAA": 90.0},
                "signals_inputs": {
                    "AAA": {"close_t": 90.0, "ma_t": 100.0, "close_t_minus_5": 80.0}
                },
                "next_close_threshold_inputs": {},
            },
            "portfolio": {
                "positions": [{"ticker": "AAA", "bucket": "tactical", "shares": 15, "cost_usd": 1500.0}],
            },
            "trades": [
                {
                    "trade_id": 1,
                    "trade_date_et": "2026-03-03",
                    "time_tw": "2026/03/03 23:00:00",
                    "ticker": "AAA",
                    "side": "BUY",
                    "shares": 10,
                    "cash_amount": 1000.0,
                },
                {
                    "trade_id": 2,
                    "trade_date_et": "2026-03-16",
                    "time_tw": "2026/03/16 23:00:00",
                    "ticker": "AAA",
                    "side": "BUY",
                    "shares": 5,
                    "cash_amount": 500.0,
                },
            ],
        }
        self._run_signal_update(states)

        rows = [r for r in (states.get("signals", {}).get("tactical") or []) if str(r.get("ticker")) == "AAA"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(int(rows[0].get("tactical_shares_pre") or 0), 15)
        self.assertEqual(str(rows[0].get("t_plus_1_action")), "SELL_ALL")
        self.assertEqual(int(rows[0].get("action_shares") or 0), 15)

    def test_sell_all_is_consistent_across_asof_dates(self) -> None:
        states = {
            "config": _signal_config({"AAA": "SMA50"}),
            "market": {
                "asof_t_et": "2026-03-17",
                "prices_now": {"AAA": 100.0},
                "signals_inputs": {
                    "AAA": {"close_t": 90.0, "ma_t": 100.0, "close_t_minus_5": 95.0}
                },
                "next_close_threshold_inputs": {},
            },
            "portfolio": {
                "positions": [{"ticker": "AAA", "bucket": "tactical", "shares": 15, "cost_usd": 1500.0}],
            },
            "trades": [
                {
                    "trade_id": 1,
                    "trade_date_et": "2026-03-10",
                    "time_tw": "2026/03/10 23:00:00",
                    "ticker": "AAA",
                    "side": "BUY",
                    "shares": 10,
                    "cash_amount": 1000.0,
                },
                {
                    "trade_id": 2,
                    "trade_date_et": "2026-03-14",
                    "time_tw": "2026/03/14 23:00:00",
                    "ticker": "AAA",
                    "side": "BUY",
                    "shares": 5,
                    "cash_amount": 500.0,
                },
            ],
        }
        self._run_signal_update(states)
        rows_day1 = [r for r in (states.get("signals", {}).get("tactical") or []) if str(r.get("ticker")) == "AAA"]
        self.assertEqual(len(rows_day1), 1)
        self.assertEqual(str(rows_day1[0].get("t_plus_1_action")), "SELL_ALL")
        self.assertEqual(int(rows_day1[0].get("action_shares") or 0), 15)

        states["market"]["asof_t_et"] = "2026-03-24"
        self._run_signal_update(states)
        rows_day2 = [r for r in (states.get("signals", {}).get("tactical") or []) if str(r.get("ticker")) == "AAA"]
        self.assertEqual(len(rows_day2), 1)
        self.assertEqual(int(rows_day2[0].get("tactical_shares_pre") or 0), 15)
        self.assertEqual(str(rows_day2[0].get("t_plus_1_action")), "SELL_ALL")
        self.assertEqual(int(rows_day2[0].get("action_shares") or 0), 15)

    def test_sell_with_recent_buy_still_executes_sell_all(self) -> None:
        states = {
            "config": _signal_config({"AAA": "SMA50"}),
            "market": {
                "asof_t_et": "2026-03-17",
                "prices_now": {"AAA": 90.0},
                "signals_inputs": {
                    "AAA": {"close_t": 90.0, "ma_t": 100.0, "close_t_minus_5": 95.0}
                },
                "next_close_threshold_inputs": {},
            },
            "portfolio": {
                "positions": [{"ticker": "AAA", "bucket": "tactical", "shares": 10, "cost_usd": 1000.0}],
            },
            "trades": [
                {
                    "trade_id": 1,
                    "trade_date_et": "2026-03-10",
                    "time_tw": "2026/03/10 23:00:00",
                    "ticker": "AAA",
                    "side": "BUY",
                    "shares": 10,
                    "cash_amount": 1000.0,
                }
            ],
        }
        self._run_signal_update(states)
        row = [r for r in (states.get("signals", {}).get("tactical") or []) if str(r.get("ticker")) == "AAA"][0]
        self.assertTrue(bool(row.get("sell_signal")))
        self.assertEqual(str(row.get("t_plus_1_action")), "SELL_ALL")
        self.assertEqual(int(row.get("action_shares") or 0), 10)

    def test_selected_market_close_uses_mode_specific_history_day(self) -> None:
        rows = [
            {"Date": "2026-03-24", "Close": 100.0},
            {"Date": "2026-03-25", "Close": 110.0},
        ]
        intraday_runtime = {
            "config": {"numeric_precision": _numeric_precision(), "fx_pairs": {"usd_twd": {"ticker": "TWD=X"}}},
            "report_meta": {
                "mode": "Intraday",
                "mode_key": "intraday",
                "signal_basis": {"t_et": "2026-03-25", "basis": "NYSE Intraday"},
            },
        }
        premarket_runtime = {
            "config": {"numeric_precision": _numeric_precision(), "fx_pairs": {"usd_twd": {"ticker": "TWD=X"}}},
            "report_meta": {
                "mode": "Premarket",
                "mode_key": "premarket",
                "signal_basis": {"t_et": "2026-03-24", "basis": "NYSE Close"},
            },
        }

        self.assertEqual(
            state_engine._selected_market_close_for_runtime(intraday_runtime, "AAA", rows),
            ("2026-03-25", 110.0),
        )
        self.assertEqual(
            state_engine._selected_market_close_for_runtime(premarket_runtime, "AAA", rows),
            ("2026-03-24", 100.0),
        )
        self.assertEqual(
            state_engine._selected_market_close_for_runtime(premarket_runtime, "TWD=X", rows),
            ("2026-03-25", 110.0),
        )

    def test_compute_tactical_plan_filters_history_by_signal_basis_day(self) -> None:
        states = {
            "market": {"signals_inputs": {}, "next_close_threshold_inputs": {}},
            "portfolio": {"positions": [], "cash": {"usd": 0.0, "deployable_usd": 0.0, "reserve_usd": 0.0}},
        }
        history = {
            "AAA": {
                "rows": [
                    {"Date": "2026-03-18", "Close": 95.0},
                    {"Date": "2026-03-19", "Close": 96.0},
                    {"Date": "2026-03-20", "Close": 97.0},
                    {"Date": "2026-03-21", "Close": 98.0},
                    {"Date": "2026-03-24", "Close": 99.0},
                    {"Date": "2026-03-25", "Close": 110.0},
                ]
            }
        }
        intraday_runtime = {
            "config": _signal_config({"AAA": "SMA2"}),
            "history": history,
            "report_meta": {
                "mode": "Intraday",
                "mode_key": "intraday",
                "signal_basis": {"t_et": "2026-03-25", "basis": "NYSE Intraday"},
            },
        }
        premarket_runtime = {
            "config": _signal_config({"AAA": "SMA2"}),
            "history": history,
            "report_meta": {
                "mode": "Premarket",
                "mode_key": "premarket",
                "signal_basis": {"t_et": "2026-03-24", "basis": "NYSE Close"},
            },
        }

        intraday_plan = compute_tactical_plan(
            states,
            intraday_runtime,
            derive_signals_inputs="force",
            derive_threshold_inputs="never",
            mode="Intraday",
            trades=[],
        )
        premarket_plan = compute_tactical_plan(
            states,
            premarket_runtime,
            derive_signals_inputs="force",
            derive_threshold_inputs="never",
            mode="Premarket",
            trades=[],
        )

        self.assertEqual(intraday_plan.signals_inputs["AAA"]["close_t"], 110.0)
        self.assertEqual(intraday_plan.signals_inputs["AAA"]["close_t_minus_5"], 95.0)
        self.assertEqual(premarket_plan.signals_inputs["AAA"]["close_t"], 99.0)
        self.assertEqual(premarket_plan.signals_inputs["AAA"]["close_t_minus_5"], None)


class StateEngineCsvRefreshTests(unittest.TestCase):
    @staticmethod
    def _runtime_for_refresh() -> dict:
        runtime = {
            "config": {
                "numeric_precision": _numeric_precision(),
                "buckets": {
                    "core": {"tickers": ["BBB"]},
                    "tactical": {"tickers": ["CCC"], "cash_pool_ticker": "DDD"},
                },
                "fx_pairs": {"usd_twd": {"ticker": "TWD=X"}},
            },
            "history": {},
        }
        state_engine._ensure_trading_calendar(runtime)
        return runtime

    @staticmethod
    def _write_csv(path: Path, *dates: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = ["Date,Open,High,Low,Close,Volume"]
        for idx, day in enumerate(dates, start=1):
            lines.append(f"{day},1,1,1,{float(idx):.4f},0")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def test_refresh_csv_history_for_mode_updates_downloads_all_active_tickers(self) -> None:
        runtime = self._runtime_for_refresh()
        states = {"portfolio": {"positions": [{"ticker": "AAA", "shares": 1, "cost_usd": 100.0}]}}
        now_et = datetime.fromisoformat("2026-03-24T08:00:00-04:00")

        with tempfile.TemporaryDirectory() as tmp:
            csv_dir = Path(tmp)
            self._write_csv(csv_dir / "AAA.csv", "2026-03-20")
            self._write_csv(csv_dir / "BBB.csv", "2026-03-20", "2026-03-23")
            self._write_csv(csv_dir / "DDD.csv", "2026-03-21")
            with mock.patch("download_1y.yf", object()), mock.patch("download_1y.download_history") as mocked_download:
                refreshed = state_engine._refresh_csv_history_for_mode_updates(
                    states,
                    runtime,
                    csv_dir=str(csv_dir),
                    tickers=["AAA", "BBB", "CCC", "DDD", "TWD=X"],
                    now_et=now_et,
                    mode_label="Premarket",
                )

        self.assertEqual(set(refreshed), {"AAA", "BBB", "CCC", "DDD", "TWD=X"})
        requested = [call.args[0] for call in mocked_download.call_args_list]
        self.assertEqual(set(requested), {"AAA", "BBB", "CCC", "DDD", "TWD=X"})
        end_dates = {call.args[0]: call.args[2] for call in mocked_download.call_args_list}
        self.assertEqual(end_dates["AAA"], date(2026, 3, 25))
        self.assertEqual(end_dates["BBB"], date(2026, 3, 25))
        self.assertEqual(end_dates["CCC"], date(2026, 3, 25))
        self.assertEqual(end_dates["DDD"], date(2026, 3, 25))
        self.assertEqual(end_dates["TWD=X"], date(2026, 3, 25))

    def test_refresh_csv_history_for_mode_updates_allows_same_day_fx_rows(self) -> None:
        runtime = self._runtime_for_refresh()
        now_et = datetime.fromisoformat("2026-03-25T08:00:00-04:00")

        with tempfile.TemporaryDirectory() as tmp:
            csv_dir = Path(tmp)
            self._write_csv(csv_dir / "TWD=X.csv", "2026-03-24")
            self._write_csv(csv_dir / "AAA.csv", "2026-03-24")
            with mock.patch("download_1y.yf", object()), mock.patch("download_1y.download_history") as mocked_download:
                state_engine._refresh_csv_history_for_mode_updates(
                    {},
                    runtime,
                    csv_dir=str(csv_dir),
                    tickers=["AAA", "TWD=X"],
                    now_et=now_et,
                    mode_label="Premarket",
                )

        end_dates = {call.args[0]: call.args[2] for call in mocked_download.call_args_list}
        self.assertEqual(end_dates["AAA"], date(2026, 3, 26))
        self.assertEqual(end_dates["TWD=X"], date(2026, 3, 26))

    def test_refresh_csv_history_for_intraday_updates_uses_same_day_equity_rows(self) -> None:
        runtime = self._runtime_for_refresh()
        now_et = datetime.fromisoformat("2026-03-25T11:30:00-04:00")

        with tempfile.TemporaryDirectory() as tmp:
            csv_dir = Path(tmp)
            self._write_csv(csv_dir / "AAA.csv", "2026-03-24")
            self._write_csv(csv_dir / "TWD=X.csv", "2026-03-24")
            with mock.patch("download_1y.yf", object()), mock.patch("download_1y.download_history") as mocked_download:
                state_engine._refresh_csv_history_for_mode_updates(
                    {},
                    runtime,
                    csv_dir=str(csv_dir),
                    tickers=["AAA", "TWD=X"],
                    now_et=now_et,
                    mode_label="Intraday",
                )

        end_dates = {call.args[0]: call.args[2] for call in mocked_download.call_args_list}
        self.assertEqual(end_dates["AAA"], date(2026, 3, 26))
        self.assertEqual(end_dates["TWD=X"], date(2026, 3, 26))

    def test_refresh_csv_history_for_mode_updates_redownloads_target_end_rows(self) -> None:
        runtime = self._runtime_for_refresh()
        now_et = datetime.fromisoformat("2026-03-24T08:00:00-04:00")

        with tempfile.TemporaryDirectory() as tmp:
            csv_dir = Path(tmp)
            self._write_csv(csv_dir / "AAA.csv", "2026-03-20", "2026-03-23")
            with mock.patch("download_1y.yf", object()), mock.patch("download_1y.download_history") as mocked_download:
                refreshed = state_engine._refresh_csv_history_for_mode_updates(
                    {},
                    runtime,
                    csv_dir=str(csv_dir),
                    tickers=["AAA"],
                    now_et=now_et,
                    mode_label="Premarket",
                )

        self.assertEqual(refreshed, ["AAA"])
        mocked_download.assert_called_once()

    def test_refresh_csv_history_for_mode_updates_errors_on_incomplete_download_rows(self) -> None:
        runtime = self._runtime_for_refresh()
        now_et = datetime.fromisoformat("2026-03-24T08:00:00-04:00")

        with tempfile.TemporaryDirectory() as tmp:
            csv_dir = Path(tmp)
            self._write_csv(csv_dir / "AAA.csv", "2026-03-20", "2026-03-23")
            with mock.patch("download_1y.yf", object()), mock.patch(
                "download_1y.download_history",
                side_effect=ValueError(
                    "AAA: downloaded history contains incomplete OHLC data for Date=2026-03-24. "
                    "Re-run with --allow-incomplete-csv-rows to bypass and skip incomplete rows."
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, r"--allow-incomplete-csv-rows"):
                    state_engine._refresh_csv_history_for_mode_updates(
                        {},
                        runtime,
                        csv_dir=str(csv_dir),
                        tickers=["AAA"],
                        now_et=now_et,
                        mode_label="Premarket",
                    )

    def test_import_csvs_into_states_errors_on_incomplete_local_rows(self) -> None:
        runtime = {"config": {"numeric_precision": _numeric_precision()}, "history": {}}

        with tempfile.TemporaryDirectory() as tmp:
            csv_dir = Path(tmp)
            self._write_csv(csv_dir / "AAA.csv", "2026-03-24")
            (csv_dir / "AAA.csv").write_text(
                "Date,Open,High,Low,Close,Volume\n"
                "2026-03-24,1,1,1,1,0\n"
                "2026-03-25,1,1,1,,0\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, r"--allow-incomplete-csv-rows"):
                state_engine._import_csvs_into_states(
                    {},
                    runtime,
                    csv_dir=str(csv_dir),
                    tickers=["AAA"],
                    prices_now_from="close",
                    keep_history_rows=0,
                )

    def test_refresh_csv_history_for_mode_updates_skips_without_mode(self) -> None:
        runtime = self._runtime_for_refresh()
        now_et = datetime.fromisoformat("2026-03-24T08:00:00-04:00")
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch("download_1y.yf", object()), mock.patch("download_1y.download_history") as mocked_download:
                refreshed = state_engine._refresh_csv_history_for_mode_updates(
                    {},
                    runtime,
                    csv_dir=tmp,
                    tickers=["AAA"],
                    now_et=now_et,
                    mode_label="",
                )

        self.assertEqual(refreshed, [])
        mocked_download.assert_not_called()


class StateEngineModeGateTests(unittest.TestCase):
    @staticmethod
    def _mode_args(*, force_mode: bool) -> argparse.Namespace:
        return argparse.Namespace(
            states="states.json",
            config="config.json",
            trades_file="trades.json",
            csv_dir="data",
            allow_incomplete_csv_rows=False,
            tickers="",
            out="out_states.json",
            keep_history_rows=0,
            derive_signals_inputs="missing",
            derive_threshold_inputs="missing",
            prices_now_from="close",
            mode="Intraday",
            now_et="2026-03-18T08:00:00-04:00",
            force_mode=force_mode,
            render_report=False,
            report_schema="report_spec.json",
            report_dir="report",
            report_json_out="",
            report_out="",
            log_file="",
            broker_investment_total_usd=None,
            broker_investment_total_kind="cost_basis",
            tactical_cash_usd=None,
            initial_investment_usd=None,
            cash_adjust_usd=None,
            cash_adjust_note="",
            cash_transfer_to_reserve_usd=None,
            broker_asof_et="",
            broker_asof_et_time="",
            broker_asof_et_datetime="",
            imported_trades_json=[],
            trades_import_mode="append",
            verify_tolerance_usd=1.0,
        )

    @staticmethod
    def _unreasonable_intraday_context() -> ReportContext:
        return ReportContext(
            mode_label="Intraday",
            mode_key="intraday",
            session_class="premarket",
            now_et_iso="2026-03-18T08:00:00-04:00",
            t_et="2026-03-18",
            t_plus_1_et="2026-03-19",
            report_date="2026-03-18",
            broker_asof_et="2026-03-18",
            broker_asof_et_datetime="2026-03-18T08:00:00-04:00",
            snapshot_kind="intraday",
            reasonable=False,
            rationale="intraday requires an active regular session.",
            warning="current ET session is premarket; the regular session has not started yet.",
        )

    def test_run_main_mode_abort_mentions_force_mode_bypass(self) -> None:
        messages: list[str] = []
        had_print = hasattr(state_engine, "print")
        original_print = getattr(state_engine, "print", None)
        state_engine.print = lambda *parts, **kwargs: messages.append(" ".join(str(part) for part in parts))
        try:
            with mock.patch.object(state_engine, "_load_json", return_value={}), mock.patch.object(
                state_engine,
                "_load_runtime_config",
                return_value={"numeric_precision": _numeric_precision()},
            ), mock.patch.object(state_engine, "_load_trades_payload", return_value=[]), mock.patch.object(
                state_engine, "_migrate_state_schema"
            ), mock.patch.object(state_engine, "_ensure_trading_calendar"), mock.patch.object(
                state_engine, "_ensure_cash_buckets"
            ), mock.patch.object(
                state_engine,
                "_resolve_report_context",
                return_value=self._unreasonable_intraday_context(),
            ):
                with self.assertRaises(SystemExit) as exc:
                    state_engine._run_main(self._mode_args(force_mode=False))
        finally:
            if had_print:
                state_engine.print = original_print
            else:
                delattr(state_engine, "print")

        self.assertEqual(exc.exception.code, 2)
        joined = "\n".join(messages)
        self.assertIn("current ET session is premarket", joined)
        self.assertIn("-f / --force-mode", joined)

    def test_run_main_force_mode_bypasses_session_gate(self) -> None:
        messages: list[str] = []
        had_print = hasattr(state_engine, "print")
        original_print = getattr(state_engine, "print", None)
        state_engine.print = lambda *parts, **kwargs: messages.append(" ".join(str(part) for part in parts))
        try:
            with mock.patch.object(state_engine, "_load_json", return_value={}), mock.patch.object(
                state_engine,
                "_load_runtime_config",
                return_value={"numeric_precision": _numeric_precision()},
            ), mock.patch.object(state_engine, "_load_trades_payload", return_value=[]), mock.patch.object(
                state_engine, "_migrate_state_schema"
            ), mock.patch.object(state_engine, "_ensure_trading_calendar"), mock.patch.object(
                state_engine, "_ensure_cash_buckets"
            ), mock.patch.object(
                state_engine,
                "_resolve_report_context",
                return_value=self._unreasonable_intraday_context(),
            ), mock.patch.object(state_engine, "_discover_tickers_from_config", return_value=[]), mock.patch.object(
                state_engine,
                "_refresh_csv_history_for_mode_updates",
                side_effect=RuntimeError("refresh reached"),
            ):
                with self.assertRaisesRegex(RuntimeError, "refresh reached"):
                    state_engine._run_main(self._mode_args(force_mode=True))
        finally:
            if had_print:
                state_engine.print = original_print
            else:
                delattr(state_engine, "print")

        joined = "\n".join(messages)
        self.assertIn("forcing mode=Intraday via -f/--force-mode", joined)

    @staticmethod
    def _reasonable_intraday_context() -> ReportContext:
        return ReportContext(
            mode_label="Intraday",
            mode_key="intraday",
            session_class="intraday",
            now_et_iso="2026-03-18T10:30:00-04:00",
            t_et="2026-03-18",
            t_plus_1_et="2026-03-19",
            report_date="2026-03-18",
            broker_asof_et="2026-03-18",
            broker_asof_et_datetime="2026-03-18T10:30:00-04:00",
            snapshot_kind="intraday",
            reasonable=True,
            rationale="market is open; intraday uses today as t and the next trading day as t+1.",
            warning="",
        )

    def test_compact_persistent_states_keeps_persistent_cash_and_performance_basis(self) -> None:
        compacted = state_engine._compact_persistent_states(
            {
                "market": {"prices_now": {"AAA": 10.0}},
                "portfolio": {
                    "positions": [
                        {"ticker": "AAA", "bucket": "core", "shares": 3, "cost_usd": 30.0},
                        {"ticker": "BBB", "bucket": "tactical", "shares": 0, "cost_usd": 0.0},
                    ],
                    "cash": {
                        "usd": 11.5,
                        "deployable_usd": 9.5,
                        "reserve_usd": 2.0,
                        "baseline_usd": 100.0,
                        "net_external_cash_flow_usd": -8.0,
                        "external_flows": [{"amount_usd": -8.0, "kind": "withdrawal"}],
                    },
                    "totals": {"portfolio": {"nav_usd": 41.5}},
                    "performance": {
                        "initial_investment_usd": 120.0,
                        "current_total_assets_usd": 41.5,
                        "net_external_cash_flow_usd": -8.0,
                        "effective_capital_base_usd": 112.0,
                        "profit_usd": -70.5,
                        "profit_rate": -0.6295,
                        "baseline": {
                            "initial_investment_usd": 120.0,
                            "net_external_cash_flow_usd": -8.0,
                            "method": "initial_investment_plus_net_external_cash_flow",
                        },
                        "returns": {"profit_usd": -70.5},
                    },
                },
            }
        )

        self.assertEqual(
            compacted,
            {
                "portfolio": {
                    "positions": [{"ticker": "AAA", "shares": 3}],
                    "cash": {
                        "usd": 11.5,
                        "deployable_usd": 9.5,
                        "reserve_usd": 2.0,
                        "baseline_usd": 100.0,
                        "net_external_cash_flow_usd": -8.0,
                        "external_flows": [{"amount_usd": -8.0, "kind": "withdrawal"}],
                    },
                    "performance": {
                        "initial_investment_usd": 120.0,
                        "baseline": {
                            "initial_investment_usd": 120.0,
                            "net_external_cash_flow_usd": -8.0,
                            "method": "initial_investment_plus_net_external_cash_flow",
                        },
                    },
                }
            },
        )

    def test_run_main_mode_only_writes_report_json_and_skips_primary_state_without_explicit_out(self) -> None:
        args = self._mode_args(force_mode=False)
        args.out = ""
        args.render_report = True
        args.now_et = "2026-03-18T10:30:00-04:00"
        saved_paths: list[str] = []

        with ExitStack() as stack:
            stack.enter_context(mock.patch.object(state_engine, "_load_json", return_value={"portfolio": {"cash": {"usd": 1.0}, "positions": []}}))
            stack.enter_context(mock.patch.object(state_engine, "_load_runtime_config", return_value={"numeric_precision": _numeric_precision()}))
            stack.enter_context(mock.patch.object(state_engine, "_load_trades_payload", return_value=[]))
            stack.enter_context(mock.patch.object(state_engine, "_migrate_state_schema"))
            stack.enter_context(mock.patch.object(state_engine, "_ensure_trading_calendar"))
            stack.enter_context(mock.patch.object(state_engine, "_ensure_cash_buckets"))
            stack.enter_context(mock.patch.object(state_engine, "_resolve_report_context", return_value=self._reasonable_intraday_context()))
            stack.enter_context(mock.patch.object(state_engine, "_discover_tickers_from_config", return_value=[]))
            stack.enter_context(mock.patch.object(state_engine, "_refresh_csv_history_for_mode_updates", return_value=[]))
            stack.enter_context(mock.patch.object(state_engine, "_compute_keep_history_rows", return_value=1))
            stack.enter_context(mock.patch.object(state_engine, "_import_csvs_into_states", return_value=[]))
            stack.enter_context(mock.patch.object(state_engine, "_late_hydrate_new_position_tickers", return_value=[]))
            stack.enter_context(mock.patch.object(state_engine, "_rebuild_market_snapshot_from_history"))
            stack.enter_context(mock.patch.object(state_engine, "_reprice_and_totals"))
            stack.enter_context(mock.patch.object(state_engine, "compute_tactical_plan", return_value=TacticalPlan()))
            stack.enter_context(mock.patch.object(state_engine, "_update_portfolio_performance"))
            stack.enter_context(mock.patch.object(
                state_engine,
                "build_report_root",
                return_value={"portfolio": {"cash": {"usd": 1.0}, "positions": []}, "_report_meta": {"mode": "Intraday", "mode_key": "intraday", "version_anchor_et": "2026-03-18"}},
            ))
            stack.enter_context(mock.patch.object(state_engine, "ensure_report_root_fields", return_value=[]))
            stack.enter_context(mock.patch.object(state_engine, "_save_trades_payload", return_value="trades.json"))
            stack.enter_context(mock.patch.object(state_engine, "_save_json", side_effect=lambda obj, path: saved_paths.append(str(path)) or str(path)))
            stack.enter_context(mock.patch.object(state_engine, "_render_report_output", return_value=("# Daily Investment Report (Intraday)\n", "report/2026-03-18_intraday.md")))
            stack.enter_context(mock.patch.object(Path, "write_text", return_value=0))
            exit_code = state_engine._run_main(args)

        self.assertEqual(exit_code, 0)
        self.assertEqual(saved_paths, ["report/2026-03-18_intraday.json"])

    def test_hydrate_positions_from_trade_ledger_if_needed_rebuilds_minimal_states(self) -> None:
        states = {"portfolio": {"positions": [{"ticker": "AAA", "shares": 2}], "cash": {"usd": 0.0}}}
        runtime = {"config": {"numeric_precision": _numeric_precision()}, "history": {}}
        trades = [
            {
                "trade_id": 1,
                "trade_date_et": "2026-03-10",
                "time_tw": "2026/03/10 23:00:00",
                "ticker": "AAA",
                "side": "BUY",
                "shares": 2,
                "cash_amount": 101.0,
            }
        ]

        state_engine._hydrate_positions_from_trade_ledger_if_needed(states, runtime, trades)

        self.assertEqual(states["portfolio"]["positions"], [{"ticker": "AAA", "bucket": "tactical", "shares": 2, "cost_usd": 101.0}])


if __name__ == "__main__":
    unittest.main()
