from __future__ import annotations

import copy
import unittest

from core.models import TacticalPlan
from core.report_bundle import build_report_root


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


if __name__ == "__main__":
    unittest.main()
