# Copyright (c) 2026 Sheng-Hsin Tsai
# SPDX-License-Identifier: MIT

from __future__ import annotations

import unittest
from pathlib import Path

from core.reconciliation import _upsert_trades
from utils.precision import load_state_engine_numeric_precision


REPO_ROOT = Path(__file__).resolve().parents[1]


def _numeric_precision() -> dict:
    return load_state_engine_numeric_precision(str(REPO_ROOT / "config.json"))


class ReconciliationTests(unittest.TestCase):
    def test_upsert_trades_assigns_incrementing_integer_trade_ids(self) -> None:
        trades = [
            {
                "trade_id": 7,
                "trade_date_et": "2026-03-16",
                "time_tw": "2026/03/16 23:15:59",
                "ticker": "NVDA",
                "side": "BUY",
                "shares": 33,
                "gross": 6090.12,
                "fee": 12.18,
            }
        ]
        incoming = [
            {
                "trade_date_et": "2026-03-20",
                "time_tw": "2026/03/20 21:31:18",
                "ticker": "NVDA",
                "side": "SELL",
                "shares": 33,
                "gross": 5847.60,
                "fee": 11.70,
            }
        ]

        precision = _numeric_precision()
        added, dup = _upsert_trades(
            trades,
            incoming,
            cash_amount_ndigits=int(precision["trade_cash_amount"]),
            trade_dedupe_amount_ndigits=int(precision["trade_dedupe_amount"]),
        )

        self.assertEqual((added, dup), (1, 0))
        self.assertEqual(trades[-1]["trade_id"], 8)


if __name__ == "__main__":
    unittest.main()
