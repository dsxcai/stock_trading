from __future__ import annotations

import unittest

from core import state_engine


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


if __name__ == "__main__":
    unittest.main()
