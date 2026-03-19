from __future__ import annotations

import copy
import unittest

from core import reporting


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


if __name__ == "__main__":
    unittest.main()
