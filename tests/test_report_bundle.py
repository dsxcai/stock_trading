from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path

from core.models import TacticalPlan
from core.report_bundle import build_report_root


REPO_ROOT = Path(__file__).resolve().parents[1]


class ReportBundleTests(unittest.TestCase):
    def test_build_report_root_keeps_state_pure(self) -> None:
        states = {
            "market": {"prices_now": {}},
            "portfolio": {"positions": [], "cash": {}, "totals": {}},
        }
        baseline = copy.deepcopy(states)
        plan = TacticalPlan(
            signals_inputs={"AAA": {"close_t": 10.0}},
            threshold_inputs={"AAA": {"close_t": 10.0}},
            tactical_rows=[{"ticker": "AAA", "t_plus_1_action": "BUY"}],
            threshold_rows=[{"ticker": "AAA", "threshold": 11.0}],
        )

        report_root = build_report_root(
            states,
            config={"doc": "x", "trades_file": "trades.json"},
            trades=[{"trade_id": 1, "ticker": "AAA"}],
            tactical_plan=plan,
        )

        self.assertEqual(states, baseline)
        self.assertEqual(report_root["config"]["doc"], "x")
        self.assertEqual(report_root["trades"][0]["ticker"], "AAA")
        self.assertEqual(report_root["signals"]["tactical"][0]["t_plus_1_action"], "BUY")
        self.assertEqual(report_root["thresholds"]["buy_signal_close_price_thresholds"][0]["threshold"], 11.0)
        self.assertEqual(report_root["market"]["signals_inputs"]["AAA"]["close_t"], 10.0)
        self.assertNotIn("config", states)
        self.assertNotIn("trades", states)
        self.assertNotIn("signals", states)
        self.assertNotIn("thresholds", states)

    def test_build_report_root_derives_position_notes_from_trades(self) -> None:
        states = {
            "portfolio": {
                "positions": [
                    {"ticker": "AAA", "shares": 2, "notes": "stale position note"},
                    {"ticker": "BBB", "shares": 1, "notes": "should be cleared"},
                ],
                "cash": {},
                "totals": {},
            }
        }
        baseline = copy.deepcopy(states)

        report_root = build_report_root(
            states,
            trades=[
                {"trade_id": 1, "ticker": "AAA", "trade_date_et": "2026-03-18", "time_tw": "2026/03/18 22:00:00", "side": "BUY", "shares": 1, "notes": "first AAA note"},
                {"trade_id": 2, "ticker": "AAA", "trade_date_et": "2026-03-19", "time_tw": "2026/03/19 22:00:00", "side": "BUY", "shares": 1, "notes": "second AAA note"},
                {"trade_id": 3, "ticker": "AAA", "trade_date_et": "2026-03-20", "time_tw": "2026/03/20 21:00:00", "side": "BUY", "shares": 1, "notes": "third AAA note"},
                {"trade_id": 4, "ticker": "AAA", "trade_date_et": "2026-03-21", "time_tw": "2026/03/21 21:00:00", "side": "SELL", "shares": 1, "notes": "sell should not override notes"},
                {"trade_id": 5, "ticker": "BBB", "trade_date_et": "2026-03-19", "time_tw": "2026/03/19 22:00:00", "side": "BUY", "shares": 1, "notes": ""},
            ],
        )

        positions = {item["ticker"]: item for item in report_root["portfolio"]["positions"]}
        self.assertEqual(positions["AAA"]["notes"], "second AAA note x1 | third AAA note x1")
        self.assertEqual(positions["BBB"]["notes"], "")
        self.assertEqual(states, baseline)

    def test_build_report_root_loads_trade_notes_for_all_current_positions(self) -> None:
        states = json.loads((REPO_ROOT / "tests" / "fixtures" / "golden_premarket_states.json").read_text(encoding="utf-8"))
        trades = json.loads((REPO_ROOT / "tests" / "fixtures" / "golden_premarket_trades.json").read_text(encoding="utf-8"))

        current_tickers = {str(item["ticker"]).upper() for item in states["portfolio"]["positions"]}
        lots_by_ticker = {ticker: [] for ticker in current_tickers}
        for trade in sorted(
            trades,
            key=lambda item: (
                str(item.get("trade_date_et") or "").strip(),
                str(item.get("time_tw") or "").strip(),
                int(item.get("trade_id") or 0),
            ),
        ):
            ticker = str(trade.get("ticker") or "").upper().strip()
            note = str(trade.get("notes") or "").strip()
            side = str(trade.get("side") or "").upper().strip()
            try:
                shares = int(float(trade.get("shares") or 0))
            except Exception:
                shares = 0
            if ticker not in current_tickers or shares <= 0:
                continue
            ticker_lots = lots_by_ticker.setdefault(ticker, [])
            if side.startswith("B"):
                ticker_lots.append({"shares": shares, "note": note})
                continue
            if side.startswith("S"):
                remaining = shares
                while remaining > 0 and ticker_lots:
                    lot = ticker_lots[0]
                    lot_shares = int(lot["shares"])
                    used = min(remaining, lot_shares)
                    remaining -= used
                    lot_shares -= used
                    if lot_shares <= 0:
                        ticker_lots.pop(0)
                    else:
                        lot["shares"] = lot_shares

        expected_notes = {}
        for ticker, lots in lots_by_ticker.items():
            note_shares = {}
            ordered_notes = []
            for lot in lots:
                note = str(lot.get("note") or "").strip()
                try:
                    shares = int(lot.get("shares") or 0)
                except Exception:
                    shares = 0
                if not note or shares <= 0:
                    continue
                if note not in note_shares:
                    ordered_notes.append(note)
                    note_shares[note] = 0
                note_shares[note] += shares
            expected_notes[ticker] = " | ".join(f"{note} x{note_shares[note]}" for note in ordered_notes)

        report_root = build_report_root(states, trades=trades)
        actual_notes = {
            str(item["ticker"]).upper(): str(item.get("notes") or "")
            for item in report_root["portfolio"]["positions"]
        }

        self.assertEqual(set(actual_notes), current_tickers)
        self.assertEqual(actual_notes, expected_notes)


if __name__ == "__main__":
    unittest.main()
