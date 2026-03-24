from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

from core import state_engine
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
        state_engine._update_signals_and_thresholds(
            states,
            runtime,
            derive_signals_inputs="never",
            derive_threshold_inputs="never",
            mode="Premarket",
            trades=list(states.get("trades") or []),
        )

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

    def test_refresh_csv_history_for_mode_updates_downloads_only_stale_active_tickers(self) -> None:
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
                    refresh_policy="auto",
                )

        self.assertEqual(set(refreshed), {"AAA", "CCC", "DDD", "TWD=X"})
        requested = [call.args[0] for call in mocked_download.call_args_list]
        self.assertEqual(set(requested), {"AAA", "CCC", "DDD", "TWD=X"})
        self.assertNotIn("BBB", requested)

    def test_refresh_csv_history_for_mode_updates_skips_without_mode_under_auto_policy(self) -> None:
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
                    refresh_policy="auto",
                )

        self.assertEqual(refreshed, [])
        mocked_download.assert_not_called()


if __name__ == "__main__":
    unittest.main()
