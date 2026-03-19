from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from core.strategy import _allocate_buy_shares_across_triggered_signals, _calc_threshold_row, _read_ohlcv_csv
from download_1y import _normalize_history_frame


class StrategyAndDownloadTests(unittest.TestCase):
    def test_calc_threshold_row_uses_expected_thresholds(self) -> None:
        row = _calc_threshold_row(
            ticker="GOOG",
            ma_rule="SMA50",
            window=50,
            inputs={
                "close_t": 309.41,
                "ma_sum_prev": 15627.97,
                "close_t_minus_5_next": 308.42,
            },
        )
        self.assertEqual(row["ticker"], "GOOG")
        self.assertEqual(row["ma_rule"], "SMA50")
        self.assertAlmostEqual(row["threshold_from_ma"], 318.9381632653061)
        self.assertAlmostEqual(row["threshold"], 318.9381632653061)
        self.assertEqual(row["display"], "318.94+")
        self.assertNotIn("sum_n_minus_1", row)

    def test_allocate_buy_shares_respects_budget(self) -> None:
        allocation = _allocate_buy_shares_across_triggered_signals(
            candidates=[
                {"ticker": "NVDA", "price_usd": 100.0},
                {"ticker": "SMH", "price_usd": 50.0},
            ],
            investable_cash_usd=260.0,
        )
        self.assertEqual(allocation, {"SMH": 3, "NVDA": 1})

    def test_read_ohlcv_csv_dedupes_and_keeps_last_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "AAPL.csv"
            csv_path.write_text(
                "Date,Open,High,Low,Close,Volume\n"
                "2026-03-17,1,2,0.5,1.5,100\n"
                "2026-03-17,3,4,2.5,3.5,200\n"
                "2026-03-18,5,6,4.5,5.5,300\n",
                encoding="utf-8",
            )
            rows = _read_ohlcv_csv(str(csv_path), keep_last_n=2)
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["Date"], "2026-03-17")
            self.assertEqual(rows[0]["Close"], 3.5)
            self.assertEqual(rows[1]["Date"], "2026-03-18")

    def test_normalize_history_frame_flattens_and_formats_download_output(self) -> None:
        index = pd.to_datetime(["2022-12-05", "2022-12-06", "2022-12-07"])
        columns = pd.MultiIndex.from_tuples([
            ("Adj Close", "AAPL"),
            ("Close", "AAPL"),
            ("High", "AAPL"),
            ("Low", "AAPL"),
            ("Open", "AAPL"),
            ("Volume", "AAPL"),
        ])
        frame = pd.DataFrame([
            [144.3157501220703, 146.6300048828125, 150.9199981689453, 145.77000427246094, 147.77000427246094, 68826400],
            [140.6544647216797, 142.91000366210938, 147.3000030517578, 141.9199981689453, 147.07000732421875, 64727200],
            [138.715576171875, 140.94000244140625, 143.3699951171875, 140.0, 142.19000244140625, 69721100],
        ], index=index, columns=columns)
        normalized = _normalize_history_frame(frame, "AAPL")
        self.assertEqual(list(normalized.columns), ["Open", "High", "Low", "Close", "Volume"])
        csv_text = normalized.to_csv(float_format="%.4f")
        self.assertEqual(
            csv_text,
            "Date,Open,High,Low,Close,Volume\n"
            "2022-12-05,147.7700,150.9200,145.7700,146.6300,68826400\n"
            "2022-12-06,147.0700,147.3000,141.9200,142.9100,64727200\n"
            "2022-12-07,142.1900,143.3700,140.0000,140.9400,69721100\n",
        )


if __name__ == "__main__":
    unittest.main()
