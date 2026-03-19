from __future__ import annotations

import unittest

from core import state_engine


class StateEngineSignalTests(unittest.TestCase):
    def test_sell_signal_blocked_within_recent_buy_protection(self) -> None:
        states = {
            "config": {"tactical_indicators": {"AAA": "SMA50"}},
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
        state_engine._ensure_trading_calendar(states)
        state_engine._update_signals_and_thresholds(states, derive_signals_inputs="never", derive_threshold_inputs="never", mode="Premarket")

        row = (states.get("signals", {}).get("tactical") or [])[0]
        self.assertTrue(bool(row.get("sell_signal")))
        self.assertTrue(bool(row.get("sell_blocked_by_recent_buy")))
        self.assertFalse(bool(row.get("buy_signal")))
        self.assertEqual(row.get("t_plus_1_action"), "HOLD")
        self.assertIn("SELL_BLOCKED", str(row.get("t5_filter_label") or ""))

    def test_buy_budget_includes_estimated_sell_reclaim(self) -> None:
        states = {
            "config": {"tactical_indicators": {"AAA": "SMA50", "BBB": "SMA50"}},
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
        state_engine._ensure_trading_calendar(states)
        state_engine._update_signals_and_thresholds(states, derive_signals_inputs="never", derive_threshold_inputs="never", mode="Premarket")

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
        self.assertEqual(int(buy_row.get("action_shares") or 0), 20)

    def test_recent_buy_protection_does_not_turn_failed_buy_into_buy(self) -> None:
        states = {
            "config": {"tactical_indicators": {"SMH": "SMA100"}},
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
        state_engine._ensure_trading_calendar(states)
        state_engine._update_signals_and_thresholds(states, derive_signals_inputs="never", derive_threshold_inputs="never", mode="Premarket")

        row = (states.get("signals", {}).get("tactical") or [])[0]
        self.assertFalse(bool(row.get("buy_signal")))
        self.assertTrue(bool(row.get("sell_signal")))
        self.assertTrue(bool(row.get("sell_blocked_by_recent_buy")))
        self.assertEqual(row.get("t5_filter_label"), "SELL_BLOCKED (1d<=5d)")
        self.assertEqual(row.get("t_plus_1_action"), "HOLD")


if __name__ == "__main__":
    unittest.main()
