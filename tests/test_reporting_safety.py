from __future__ import annotations

import copy
import unittest

from core import reporting
from core import state_engine


class ReportingSafetyTests(unittest.TestCase):
    def test_row_computed_does_not_mutate_state_rows(self) -> None:
        states = {
            "trades": [
                {"trade_id": 1, "cash_amount": 100.0},
                {"trade_id": 2, "cash_amount": 200.0},
            ]
        }
        baseline = copy.deepcopy(states)
        schema = {
            "datasets": {
                "trades_ds": {
                    "row_source": {"path": "$.trades[*]"},
                }
            }
        }
        table_spec = {
            "dataset": "trades_ds",
            "row_computed": {
                "cash_amount": {"const": 999.99},
            },
        }

        rows = reporting.build_dataset(schema, states, "trades_ds")
        reporting.apply_row_computed(table_spec, rows, states)

        self.assertEqual(states, baseline)
        self.assertEqual(rows[0]["cash_amount"], 999.99)
        self.assertEqual(rows[1]["cash_amount"], 999.99)

    def test_render_report_uses_transient_report_meta(self) -> None:
        states = {
            "config": {"doc": "Daily Investment Report"},
            "_report_meta": {
                "mode": "Premarket",
                "mode_key": "premarket",
                "signal_basis": {"t_et": "2026-03-17", "basis": "NYSE Close"},
                "execution_basis": {"t_plus_1_et": "2026-03-18", "basis": "NYSE Trading Day"},
                "version_anchor_et": "2026-03-18",
                "price_notes": ["Estimated Price: Premarket Unrealized PnL (TWD) uses the latest TWD=X CSV quote from 2026-03-24."],
            },
        }
        markdown = reporting.render_report(states, {"tables": []}, "Premarket")

        self.assertIn("# Daily Investment Report (Premarket)", markdown)
        self.assertIn("- Signal Basis: t=2026-03-17 (NYSE Close)", markdown)
        self.assertIn("- Execution Basis: t+1=2026-03-18 (NYSE Trading Day)", markdown)
        self.assertIn("- Estimated Price: Premarket Unrealized PnL (TWD) uses the latest TWD=X CSV quote from 2026-03-24.", markdown)

    def test_strip_persisted_report_transients_removes_by_mode(self) -> None:
        states = {
            "by_mode": {
                "intraday": {
                    "mode": "Intraday",
                    "signal_basis": {"t_et": "2026-03-20", "basis": "NYSE Intraday"},
                }
            },
            "signals": {"tactical": []},
            "thresholds": {"buy_signal_close_price_thresholds": []},
            "market": {"signals_inputs": {}, "next_close_threshold_inputs": {}},
        }

        state_engine._strip_persisted_report_transients(states)

        self.assertNotIn("by_mode", states)
        self.assertNotIn("signals", states)
        self.assertNotIn("thresholds", states)


if __name__ == "__main__":
    unittest.main()
