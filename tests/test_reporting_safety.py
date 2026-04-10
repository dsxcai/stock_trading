from __future__ import annotations

import copy
import unittest
from pathlib import Path

from core import reporting
from core import state_engine

REPO_ROOT = Path(__file__).resolve().parents[1]


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
            "config": {
                "meta": {"doc": "Daily Investment Report"},
                "data": {
                    "trading_calendar": {
                        "years": {
                            "2026": {
                                "closed": {
                                    "2026-04-03": "Good Friday",
                                }
                            }
                        }
                    }
                },
            },
            "_report_meta": {
                "mode": "Premarket",
                "mode_key": "premarket",
                "generated_at_et": "2026-03-18T08:00:00-04:00",
                "signal_basis": {"t_et": "2026-03-17", "basis": "NYSE Close"},
                "execution_basis": {"t_plus_1_et": "2026-03-18", "basis": "NYSE Trading Day"},
                "version_anchor_et": "2026-03-18",
                "price_notes": ["Estimated Price: Premarket Unrealized PnL (TWD) uses the latest TWD=X CSV quote from 2026-03-24."],
            },
        }
        markdown = reporting.render_report(states, {"tables": []}, "Premarket")

        self.assertIn("# Daily Investment Report (Premarket)", markdown)
        self.assertIn("- Generated At (ET): 2026/03/18 08:00:00", markdown)
        self.assertIn("- Signal Basis: t=2026-03-17 (NYSE Close)", markdown)
        self.assertIn("- Execution Basis: t+1=2026-03-18 (NYSE Trading Day)", markdown)
        self.assertIn("- Estimated Price: Premarket Unrealized PnL (TWD) uses the latest TWD=X CSV quote from 2026-03-24.", markdown)
        self.assertIn("- Nearby Trading Calendar: 2026-03-17 Open; 2026-03-18 Open; 2026-03-19 Open.", markdown)

    def test_render_report_trading_calendar_line_handles_closed_and_trade_no_settlement(self) -> None:
        states = {
            "config": {
                "meta": {"doc": "Daily Investment Report"},
                "data": {
                    "trading_calendar": {
                        "years": {
                            "2026": {
                                "closed": {
                                    "2026-04-03": "Good Friday",
                                },
                                "trade_no_settlement": {
                                    "2026-10-12": "Columbus Day",
                                },
                            }
                        }
                    }
                },
            },
            "_report_meta": {
                "mode": "Premarket",
                "mode_key": "premarket",
                "generated_at_et": "2026-04-03T08:00:00-04:00",
                "signal_basis": {"t_et": "2026-04-02", "basis": "NYSE Close"},
                "execution_basis": {"t_plus_1_et": "2026-04-03", "basis": "NYSE Trading Day"},
                "version_anchor_et": "2026-04-03",
            },
        }

        markdown = reporting.render_report(states, {"tables": []}, "Premarket")

        self.assertIn("- Nearby Trading Calendar: 2026-04-02 Open; 2026-04-03 Closed (Good Friday); 2026-04-06 Open.", markdown)

        states["_report_meta"]["generated_at_et"] = "2026-10-12T08:00:00-04:00"
        states["_report_meta"]["signal_basis"] = {"t_et": "2026-10-09", "basis": "NYSE Close"}
        states["_report_meta"]["execution_basis"] = {"t_plus_1_et": "2026-10-12", "basis": "NYSE Trading Day"}
        states["_report_meta"]["version_anchor_et"] = "2026-10-12"

        markdown = reporting.render_report(states, {"tables": []}, "Premarket")

        self.assertIn(
            "- Nearby Trading Calendar: 2026-10-09 Open; 2026-10-12 Open (Trade, No Settlement: Columbus Day); 2026-10-13 Open.",
            markdown,
        )

    def test_render_report_renders_table_after_lines(self) -> None:
        states = {
            "config": {"meta": {"doc": "Daily Investment Report"}},
            "_report_meta": {
                "mode": "Intraday",
                "mode_key": "intraday",
                "generated_at_et": "2026-03-25T10:30:00-04:00",
                "signal_basis": {"t_et": "2026-03-25", "basis": "NYSE Intraday"},
                "execution_basis": {"t_plus_1_et": "2026-03-26", "basis": "NYSE Trading Day"},
                "version_anchor_et": "2026-03-25",
            },
            "portfolio": {"positions": [{"ticker": "AAA", "price_now": 110.0}]},
        }
        schema = {
            "tables": [
                {
                    "title": "Current Positions",
                    "dataset": "positions",
                    "columns": [
                        {"header": "Ticker", "value": {"path": "ticker"}},
                        {"header": "Price (Now)", "value": {"path": "price_now"}},
                    ],
                    "after_lines": [
                        "Note: Price (Now) = Close(t) in Premarket / AfterClose. In Intraday, it is the current price."
                    ],
                }
            ],
            "datasets": {
                "positions": {
                    "row_source": {"path": "$.portfolio.positions[*]"},
                }
            },
        }

        markdown = reporting.render_report(states, schema, "Intraday")

        self.assertIn("- Generated At (ET): 2026/03/25 10:30:00", markdown)
        self.assertIn("| Ticker | Price (Now) |", markdown)
        self.assertIn("Note: Price (Now) = Close(t) in Premarket / AfterClose. In Intraday, it is the current price.", markdown)

    def test_render_grouped_trade_table_supports_group_footer_rows(self) -> None:
        schema = {
            "formatters": {
                "int": {"type": "integer"},
                "usd2": {"type": "currency", "currency": "USD", "decimals": 2},
            }
        }
        table_spec = {
            "column_sets": {
                "simple": [
                    {"header": "Trade ID", "value": {"path": "trade_id"}, "format": "int"},
                    {"header": "Ticker", "value": {"path": "ticker"}},
                    {"header": "Shares", "value": {"path": "shares"}, "format": "int", "align": "right"},
                    {"header": "Buy Fee", "value": {"path": "buy_fee"}, "format": "usd2", "align": "right"},
                    {"header": "Sell Fee", "value": {"path": "sell_fee"}, "format": "usd2", "align": "right"},
                    {"header": "Net Cash", "value": {"path": "cash_effect"}, "format": "usd2", "align": "right"},
                ]
            },
            "grouping": {
                "group_by": {"path": "trade_date_et"},
                "order": "desc",
                "keep_groups": {"latest_full_groups": 0, "prev_simplified_groups": {"default": 10}},
            },
            "group_rendering": {
                "columns_selector": [
                    {"when": {"group_index_between": [0, 9]}, "use": "simple"},
                ]
            },
            "group_footer_rows": {
                "simple": [
                    {
                        "label": "Total",
                        "cells": {
                            "Shares": {"path": "totals.shares", "format": "int"},
                            "Buy Fee": {"path": "totals.buy_fee", "format": "usd2"},
                            "Sell Fee": {"path": "totals.sell_fee", "format": "usd2"},
                            "Net Cash": {"path": "totals.cash_effect", "format": "usd2"},
                        },
                    }
                ]
            },
        }
        rows = [
            {"trade_id": 1, "trade_date_et": "2026-03-18", "ticker": "AAA", "shares": 2, "buy_fee": 1.0, "cash_effect": -101.0},
            {"trade_id": 2, "trade_date_et": "2026-03-18", "ticker": "BBB", "shares": 3, "sell_fee": 2.0, "cash_effect": 198.0},
            {"trade_id": 3, "trade_date_et": "2026-03-18", "ticker": "CASH", "shares": None, "cash_effect": -50.0},
        ]

        markdown = reporting.render_grouped_trade_table(table_spec, rows, schema, {}, "-")

        self.assertIn("### Trade Date (ET): 2026-03-18", markdown)
        self.assertIn("| Total | - | 5 | $1.00 | $2.00 | $47.00 |", markdown)

    def test_report_spec_sorts_signal_status_by_b_minus_a_desc(self) -> None:
        schema = reporting.load_schema(str(REPO_ROOT / "report_spec.json"))
        states = {
            "signals": {
                "tactical": [
                    {"ticker": "AAA", "close_t": 100.0, "ma_t": 110.0},
                    {"ticker": "BBB", "close_t": 100.0, "ma_t": 120.0},
                    {"ticker": "CCC", "close_t": 100.0, "ma_t": 100.0},
                ]
            }
        }

        rows = reporting.build_dataset(schema, states, "signals")

        self.assertEqual([row["ticker"] for row in rows], ["BBB", "AAA", "CCC"])

    def test_report_spec_uses_precomputed_threshold_display_value(self) -> None:
        schema = reporting.load_schema(str(REPO_ROOT / "report_spec.json"))
        states = {
            "config": {"meta": {"doc": "Daily Investment Report"}},
            "_report_meta": {
                "mode": "Premarket",
                "mode_key": "premarket",
                "generated_at_et": "2026-03-18T08:00:00-04:00",
                "signal_basis": {"t_et": "2026-03-17", "basis": "NYSE Close"},
                "execution_basis": {"t_plus_1_et": "2026-03-18", "basis": "NYSE Trading Day"},
                "version_anchor_et": "2026-03-18",
            },
            "thresholds": {
                "buy_signal_close_price_thresholds": [
                    {
                        "ticker": "GOOG",
                        "ma_rule": "SMA50",
                        "ma_sum_prev": 15627.97,
                        "close_t_minus_5_next": 308.42,
                        "threshold_from_ma": 318.9382,
                        "threshold": 318.9382,
                        "display": "318.938+",
                    }
                ]
            },
        }

        markdown = reporting.render_report(states, schema, "Premarket")

        self.assertIn("| GOOG | SMA50 | $15,627.97 | 308.4200 | 318.9382 | 318.9382 | 318.938+ |", markdown)

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
