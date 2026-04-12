from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import gui_app
from gui.markdown import render_markdown
from gui.server import GuiApplication
from gui.services import GuiServices, OperationResult


class MarkdownRendererTests(unittest.TestCase):
    def test_render_markdown_supports_headings_lists_and_tables(self) -> None:
        markdown_text = """# Daily Report
- Signal Basis: t=2026-03-17

| Ticker | Price |
| --- | ---: |
| AAA | 100.0 |
"""

        rendered = render_markdown(markdown_text)

        self.assertIn("<h1>Daily Report</h1>", rendered)
        self.assertIn("<ul>", rendered)
        self.assertIn("<table>", rendered)
        self.assertIn('<td class="align-right">100.0</td>', rendered)


class GuiServicesTests(unittest.TestCase):
    def _write_base_repo(self, root: Path) -> None:
        (root / "report").mkdir()
        (root / "data").mkdir()
        (root / "logs").mkdir()
        (root / "config.json").write_text(
            json.dumps(
                {
                    "state_engine": {
                        "portfolio": {
                            "buckets": {
                                "core": {"tickers": ["SPY"]},
                                "tactical": {"cash_pool_ticker": "META"},
                            }
                        },
                        "strategy": {
                            "tactical": {
                                "indicators": {
                                    "GOOG": {"ma_type": "SMA", "window": 50},
                                    "SMH": {"ma_type": "SMA", "window": 100},
                                }
                            }
                        },
                        "data": {
                            "fx_pairs": {
                                "usd_twd": {"ticker": "TWD=X"},
                            }
                        },
                    }
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        (root / "states.json").write_text(
            json.dumps(
                {
                    "portfolio": {
                        "positions": [
                            {"ticker": "NVDA"},
                        ]
                    }
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        (root / "trades.json").write_text("[]\n", encoding="utf-8")
        (root / "report_spec.json").write_text("{}\n", encoding="utf-8")
        (root / "data" / "AAPL.csv").write_text("Date,Open,High,Low,Close,Volume\n", encoding="utf-8")
        (root / "data" / "TWD=X.csv").write_text("Date,Open,High,Low,Close,Volume\n", encoding="utf-8")

    def test_list_recent_reports_sorts_by_mtime_and_ignores_nonstandard_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)
            older = root / "report" / "2026-03-30_intraday.md"
            newer = root / "report" / "2026-03-31_premarket.md"
            ignored = root / "report" / "notes.md"
            older.write_text("# older\n", encoding="utf-8")
            newer.write_text("# newer\n", encoding="utf-8")
            ignored.write_text("# ignored\n", encoding="utf-8")
            os.utime(older, (1, 1))
            os.utime(newer, (2, 2))

            reports = GuiServices(root).list_recent_reports()

            self.assertEqual([item.name for item in reports], ["2026-03-31_premarket.md", "2026-03-30_intraday.md"])
            self.assertEqual(reports[0].mode_label, "Premarket")
            self.assertEqual(reports[1].report_date, "2026-03-30")

    def test_load_signal_config_collects_candidates_and_excludes_fx(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)

            snapshot = GuiServices(root).load_signal_config()

            self.assertEqual(snapshot.selected_windows["GOOG"], 50)
            self.assertEqual(snapshot.selected_windows["SMH"], 100)
            self.assertIn("AAPL", snapshot.candidate_tickers)
            self.assertIn("NVDA", snapshot.candidate_tickers)
            self.assertNotIn("TWD=X", snapshot.candidate_tickers)

    def test_delete_all_reports_removes_standard_markdown_and_json_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)
            report_md = root / "report" / "2026-03-31_premarket.md"
            report_json = root / "report" / "2026-03-31_premarket.json"
            extra_md = root / "report" / "2026-03-30_intraday.md"
            note = root / "report" / "notes.md"
            report_md.write_text("# report\n", encoding="utf-8")
            report_json.write_text("{}\n", encoding="utf-8")
            extra_md.write_text("# older\n", encoding="utf-8")
            note.write_text("# keep\n", encoding="utf-8")

            result = GuiServices(root).delete_all_reports()

            self.assertTrue(result.success)
            self.assertFalse(report_md.exists())
            self.assertFalse(report_json.exists())
            self.assertFalse(extra_md.exists())
            self.assertTrue(note.exists())
            self.assertIn("Deleted 3 report artifacts across 2 reports.", result.message)

    def test_delete_report_removes_matching_markdown_and_json_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)
            report_md = root / "report" / "2026-03-31_premarket.md"
            report_json = root / "report" / "2026-03-31_premarket.json"
            note = root / "report" / "notes.md"
            report_md.write_text("# report\n", encoding="utf-8")
            report_json.write_text("{}\n", encoding="utf-8")
            note.write_text("# keep\n", encoding="utf-8")

            result = GuiServices(root).delete_report(str(report_md))

            self.assertTrue(result.success)
            self.assertFalse(report_md.exists())
            self.assertFalse(report_json.exists())
            self.assertTrue(note.exists())
            self.assertIn("Deleted 2 report artifacts for 2026-03-31_premarket.md.", result.message)

    def test_save_signal_config_rewrites_indicator_block(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)
            services = GuiServices(root)

            result = services.save_signal_config({"AAPL": 50, "NVDA": 100})

            self.assertTrue(result.success)
            saved = json.loads((root / "config.json").read_text(encoding="utf-8"))
            indicators = (((saved.get("state_engine") or {}).get("strategy") or {}).get("tactical") or {}).get("indicators") or {}
            self.assertEqual(indicators, {"AAPL": {"ma_type": "SMA", "window": 50}, "NVDA": {"ma_type": "SMA", "window": 100}})

    def test_save_runtime_config_rewrites_structured_sections_and_preserves_indicators(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)
            config_path = root / "config.json"
            existing = json.loads(config_path.read_text(encoding="utf-8"))
            existing.setdefault("state_engine", {})["gui"] = {
                "window": {"width": 1200, "height": 780, "x": 30, "y": 50}
            }
            config_path.write_text(json.dumps(existing, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            services = GuiServices(root)

            result = services.save_runtime_config(
                {
                    "doc": "Daily Investment Report",
                    "trades_file": "ledger/trades.json",
                    "cash_events_file": "ledger/cash_events.json",
                    "buy_fee_rate": "0.0015",
                    "sell_fee_rate": "0.0025",
                    "core_tickers": "SPY, ARKQ",
                    "tactical_tickers": "QQQ\nSMH",
                    "tactical_cash_pool_ticker": "META",
                    "tactical_cash_pool_tickers": "META",
                    "fx_pairs": "usd_twd=TWD=X",
                    "csv_sources": "AAPL=prices/AAPL.csv",
                    "closed_days": "2026-12-25=Christmas Day",
                    "early_close_days": "2026-12-24=13:00|Christmas Eve",
                    "usd_amount": "2",
                    "display_price": "2",
                    "display_pct": "2",
                    "trade_cash_amount": "4",
                    "trade_dedupe_amount": "6",
                    "state_selected_fields": "4",
                    "backtest_amount": "4",
                    "backtest_price": "4",
                    "backtest_rate": "6",
                    "backtest_cost_param": "6",
                    "keep_prev_trade_days_simplified": "7",
                }
            )

            self.assertTrue(result.success)
            saved = json.loads((root / "config.json").read_text(encoding="utf-8"))
            state_engine = saved["state_engine"]
            self.assertEqual(state_engine["meta"]["doc"], "Daily Investment Report")
            self.assertEqual(state_engine["meta"]["trades_file"], "ledger/trades.json")
            self.assertEqual(state_engine["meta"]["cash_events_file"], "ledger/cash_events.json")
            self.assertEqual(state_engine["execution"]["buy_fee_rate"], 0.0015)
            self.assertEqual(state_engine["execution"]["sell_fee_rate"], 0.0025)
            self.assertEqual(state_engine["portfolio"]["buckets"]["core"]["tickers"], ["SPY", "ARKQ"])
            self.assertEqual(state_engine["portfolio"]["buckets"]["tactical"]["tickers"], ["QQQ", "SMH"])
            self.assertEqual(state_engine["portfolio"]["buckets"]["tactical"]["cash_pool_ticker"], "META")
            self.assertEqual(state_engine["portfolio"]["buckets"]["tactical_cash_pool"]["tickers"], ["META"])
            self.assertEqual(state_engine["data"]["fx_pairs"], {"usd_twd": {"ticker": "TWD=X"}})
            self.assertEqual(state_engine["data"]["csv_sources"], {"AAPL": "prices/AAPL.csv"})
            self.assertEqual(
                state_engine["data"]["trading_calendar"]["years"]["2026"]["closed"],
                {"2026-12-25": "Christmas Day"},
            )
            self.assertEqual(
                state_engine["data"]["trading_calendar"]["years"]["2026"]["early_close"]["2026-12-24"],
                {"close_time_et": "13:00", "reason": "Christmas Eve"},
            )
            indicators = (((state_engine.get("strategy") or {}).get("tactical") or {}).get("indicators")) or {}
            self.assertEqual(indicators, {"GOOG": {"ma_type": "SMA", "window": 50}, "SMH": {"ma_type": "SMA", "window": 100}})
            self.assertEqual(
                state_engine["reporting"]["trade_render_policy"]["keep_prev_trade_days_simplified"],
                7,
            )
            self.assertEqual(
                state_engine["gui"]["window"],
                {"width": 1200, "height": 780, "x": 30, "y": 50},
            )

    def test_run_import_trades_uses_module_entrypoint_and_replace_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)
            services = GuiServices(root)
            xls_path = root / "sample.xls"
            xls_path.write_text("", encoding="utf-8")
            captured = {}

            def fake_run_command(command, *, name):
                captured["command"] = list(command)
                return OperationResult(
                    name=name,
                    success=True,
                    returncode=0,
                    command=" ".join(command),
                    stdout="",
                    message="ok",
                )

            with mock.patch.object(services, "_run_command", side_effect=fake_run_command):
                result = services.run_import_trades(str(xls_path))

            self.assertTrue(result.success)
            self.assertEqual(captured["command"][1:4], ["-m", "extensions.capital_xls_import", str(xls_path)])
            self.assertIn("--trades-import-mode", captured["command"])
            self.assertEqual(captured["command"][-1], "replace")

    def test_run_cash_adjustment_uses_update_states_entrypoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)
            services = GuiServices(root)
            captured = {}

            def fake_run_command(command, *, name):
                captured["command"] = list(command)
                return OperationResult(
                    name=name,
                    success=True,
                    returncode=0,
                    command=" ".join(command),
                    stdout="",
                    message="ok",
                )

            with mock.patch.object(services, "_run_command", side_effect=fake_run_command):
                result = services.run_cash_adjustment("-3600", cash_adjust_note="wire out")

            self.assertTrue(result.success)
            self.assertEqual(captured["command"][:2], [sys.executable, "update_states.py"])
            self.assertEqual(captured["command"][captured["command"].index("--out") + 1], "states.json")
            self.assertEqual(captured["command"][captured["command"].index("--cash-adjust-usd") + 1], "-3600")
            self.assertEqual(captured["command"][captured["command"].index("--cash-adjust-note") + 1], "wire out")
            self.assertEqual(captured["command"][captured["command"].index("--trades-file") + 1], "trades.json")
            self.assertEqual(captured["command"][captured["command"].index("--cash-events-file") + 1], "cash_events.json")

    def test_cash_adjustment_and_report_refresh_use_configured_ledger_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)
            config_path = root / "config.json"
            config = json.loads(config_path.read_text(encoding="utf-8"))
            state_engine = config.setdefault("state_engine", {})
            state_engine["meta"] = {
                "trades_file": "ledger/trades_live.json",
                "cash_events_file": "ledger/cash_live.json",
            }
            config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

            services = GuiServices(root)
            report_path = root / "report" / "2026-03-31_premarket.md"
            report_path.write_text("# Daily Report\n", encoding="utf-8")
            commands: list[list[str]] = []

            def fake_run_command(command, *, name):
                commands.append(list(command))
                if len(commands) == 1:
                    return OperationResult(
                        name=name,
                        success=True,
                        returncode=0,
                        command=" ".join(command),
                        stdout="cash adjusted",
                        message="ok",
                    )
                return OperationResult(
                    name=name,
                    success=True,
                    returncode=0,
                    command=" ".join(command),
                    stdout="report refreshed",
                    message="refreshed",
                    report_path=str(report_path),
                    report_json_path=str(report_path.with_suffix(".json")),
                )

            with mock.patch.object(services, "_run_command", side_effect=fake_run_command):
                result = services.run_cash_adjustment(
                    "-3600",
                    cash_adjust_note="wire out",
                    selected_report_path=str(report_path),
                )

            self.assertTrue(result.success)
            self.assertEqual(len(commands), 2)
            self.assertEqual(commands[0][commands[0].index("--trades-file") + 1], "ledger/trades_live.json")
            self.assertEqual(commands[0][commands[0].index("--cash-events-file") + 1], "ledger/cash_live.json")
            self.assertEqual(commands[1][commands[1].index("--trades-file") + 1], "ledger/trades_live.json")
            self.assertEqual(commands[1][commands[1].index("--cash-events-file") + 1], "ledger/cash_live.json")

    def test_run_command_uses_subprocess_in_repo_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)
            services = GuiServices(root)
            completed = subprocess.CompletedProcess(
                args=[sys.executable, "generate_report.py"],
                returncode=0,
                stdout="[OK] wrote report/2026-03-31_premarket.json\n[OK] wrote report/2026-03-31_premarket.md\n",
            )

            with mock.patch("gui.services.subprocess.run", return_value=completed) as mocked_run:
                result = services._run_command([sys.executable, "generate_report.py", "--mode", "premarket"], name="Generate report")

            mocked_run.assert_called_once_with(
                [sys.executable, "generate_report.py", "--mode", "premarket"],
                cwd=root.resolve(),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
            self.assertTrue(result.success)
            self.assertEqual(result.report_path, "report/2026-03-31_premarket.md")
            self.assertEqual(result.report_json_path, "report/2026-03-31_premarket.json")


class GuiServerTests(unittest.TestCase):
    def _write_base_repo(self, root: Path) -> None:
        GuiServicesTests()._write_base_repo(root)

    def test_failed_operation_switches_right_panel_to_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)

            app = GuiApplication(root, session_token="test-session")
            app.set_right_tab("status")
            app.set_last_result(
                OperationResult(
                    name="Premarket run",
                    success=False,
                    returncode=1,
                    command="python update_states.py",
                    stdout="[ERR] broken\n",
                    message="run failed",
                )
            )

            self.assertEqual(app.snapshot().right_tab, "status")

    def test_render_page_exposes_right_side_tabs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)
            report_path = root / "report" / "2026-03-31_premarket.md"
            report_path.write_text("# Daily Report\n", encoding="utf-8")

            app = GuiApplication(root, session_token="test-session")
            app.set_selected_report(str(report_path))

            rendered = app.render_page()

            self.assertIn(">Report</a>", rendered)
            self.assertIn(">Status</a>", rendered)
            self.assertIn(">Config</a>", rendered)
            self.assertNotIn(">Error Log</a>", rendered)
            self.assertIn(">Reload</button>", rendered)
            self.assertIn(">Close</button>", rendered)
            self.assertIn('action="/server-control"', rendered)
            self.assertIn('action="/delete-report"', rendered)
            self.assertIn('action="/delete-all-reports"', rendered)
            self.assertIn('action="/cash-adjust"', rendered)
            self.assertIn("Delete All Reports", rendered)
            self.assertIn("Cash Adjustment", rendered)
            self.assertIn('name="cash_adjust_usd"', rendered)
            self.assertIn('name="cash_adjust_note"', rendered)
            self.assertIn('id="allow_incomplete_report"', rendered)
            self.assertNotIn('id="allow_incomplete_cash_adjust"', rendered)
            self.assertNotIn("<h2>Signal Config</h2>", rendered)
            self.assertIn('data-async-submit="1"', rendered)
            self.assertIn("Estimated progress", rendered)
            self.assertIn('id="busy_progress_fill"', rendered)
            self.assertIn('class="danger report-delete"', rendered)
            self.assertIn('>X</button>', rendered)
            self.assertIn("if (button !== submitter)", rendered)
            self.assertIn('form.dataset.submitting = "1"', rendered)
            self.assertIn("window.fetch(form.action", rendered)
            self.assertIn(".raw-report {", rendered)
            self.assertIn("color: var(--ink);", rendered)
            self.assertIn(".log-error-line {", rendered)
            self.assertIn('<option value="replace" selected>replace</option>', rendered)

    def test_render_page_config_tab_exposes_structured_runtime_config_forms(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)

            app = GuiApplication(root, session_token="test-session")
            app.set_right_tab("config")

            rendered = app.render_page()

            self.assertIn("Runtime Config", rendered)
            self.assertIn('action="/save-runtime-config"', rendered)
            self.assertIn('action="/save-signal-config"', rendered)
            self.assertIn('name="doc"', rendered)
            self.assertIn('name="trades_file"', rendered)
            self.assertIn('name="cash_events_file"', rendered)
            self.assertIn('name="buy_fee_rate"', rendered)
            self.assertIn('name="sell_fee_rate"', rendered)
            self.assertIn('name="core_tickers"', rendered)
            self.assertIn('name="tactical_tickers"', rendered)
            self.assertIn('name="tactical_cash_pool_ticker"', rendered)
            self.assertIn('name="tactical_cash_pool_tickers"', rendered)
            self.assertIn('name="fx_pairs"', rendered)
            self.assertIn('name="csv_sources"', rendered)
            self.assertIn('name="closed_days"', rendered)
            self.assertIn('name="early_close_days"', rendered)
            self.assertIn('name="usd_amount"', rendered)
            self.assertIn('name="display_price"', rendered)
            self.assertIn('name="display_pct"', rendered)
            self.assertIn('name="trade_cash_amount"', rendered)
            self.assertIn('name="trade_dedupe_amount"', rendered)
            self.assertIn('name="state_selected_fields"', rendered)
            self.assertIn('name="backtest_amount"', rendered)
            self.assertIn('name="backtest_price"', rendered)
            self.assertIn('name="backtest_rate"', rendered)
            self.assertIn('name="backtest_cost_param"', rendered)
            self.assertIn('name="keep_prev_trade_days_simplified"', rendered)
            self.assertIn("YYYY-MM-DD=Reason", rendered)
            self.assertIn("YYYY-MM-DD=HH:MM|Reason", rendered)
            self.assertIn("alias=ticker", rendered)
            self.assertNotIn('id="allow_incomplete_runtime_config"', rendered)
            self.assertNotIn('id="allow_incomplete_config"', rendered)
            ordered_sections = [
                "General",
                "Ledger Paths",
                "Execution Costs",
                "Portfolio Buckets",
                "Market Data",
                "Trading Calendar",
                "Reporting",
                "Signal Config",
            ]
            section_positions = [rendered.index(label) for label in ordered_sections]
            self.assertEqual(section_positions, sorted(section_positions))

    def test_render_page_auto_selects_latest_report_when_none_selected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)
            older = root / "report" / "2026-03-30_intraday.md"
            newer = root / "report" / "2026-03-31_premarket.md"
            older.write_text("# older\n", encoding="utf-8")
            newer.write_text("# newer\n", encoding="utf-8")
            os.utime(older, (1, 1))
            os.utime(newer, (2, 2))

            app = GuiApplication(root, session_token="test-session")

            rendered = app.render_page()

            self.assertEqual(Path(app.snapshot().selected_report_path).resolve(), newer.resolve())
            self.assertIn("2026-03-31_premarket.md", rendered)

    def test_render_page_raw_view_includes_report_body(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)
            report_path = root / "report" / "2026-03-31_premarket.md"
            report_path.write_text("# Daily Report\n\n- hello\n", encoding="utf-8")

            app = GuiApplication(root, session_token="test-session")
            app.set_selected_report(str(report_path))
            app.set_view_mode("raw")

            rendered = app.render_page()

            self.assertIn('<pre class="raw-report"># Daily Report', rendered)

    def test_render_page_status_highlights_error_lines(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)

            app = GuiApplication(root, session_token="test-session")
            app.set_last_result(
                OperationResult(
                    name="Import trades",
                    success=False,
                    returncode=1,
                    command="python -m extensions.capital_xls_import",
                    stdout="Traceback (most recent call last):\nModuleNotFoundError: No module named 'utils'\n",
                    message="import failed",
                )
            )

            rendered = app.render_page()

            self.assertIn("Operation Status", rendered)
            self.assertIn('<span class="log-error-line">Traceback (most recent call last):</span>', rendered)
            self.assertIn("ModuleNotFoundError", rendered)


class GuiAppTests(unittest.TestCase):
    def test_build_client_url_uses_loopback_for_wildcard_host(self) -> None:
        self.assertEqual(gui_app._build_client_url("0.0.0.0", 8765), "http://127.0.0.1:8765/")

    def test_build_authenticated_client_url_embeds_session_token(self) -> None:
        self.assertEqual(
            gui_app._build_authenticated_client_url("127.0.0.1", 8765, "secret"),
            "http://127.0.0.1:8765/?__gui_token=secret",
        )

    def test_load_window_geometry_uses_defaults_when_config_has_no_gui_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.json"
            config_path.write_text(json.dumps({"state_engine": {}}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

            geometry = gui_app._load_window_geometry(config_path)

        self.assertEqual(
            geometry,
            {"width": gui_app.WINDOW_WIDTH, "height": gui_app.WINDOW_HEIGHT, "x": None, "y": None},
        )

    def test_save_window_geometry_writes_to_config_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.json"
            config_path.write_text(json.dumps({"state_engine": {}}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

            gui_app._save_window_geometry(config_path, width=1280, height=820, x=44, y=66)

            saved = json.loads(config_path.read_text(encoding="utf-8"))

        self.assertEqual(
            saved["state_engine"]["gui"]["window"],
            {"width": 1280, "height": 820, "x": 44, "y": 66},
        )

    def test_run_desktop_app_restores_and_persists_window_geometry(self) -> None:
        class FakeEvent:
            def __init__(self) -> None:
                self.handlers = []

            def __iadd__(self, callback):
                self.handlers.append(callback)
                return self

        class FakeEvents:
            def __init__(self) -> None:
                self.moved = FakeEvent()
                self.resized = FakeEvent()
                self.closing = FakeEvent()

        class FakeWindow:
            def __init__(self) -> None:
                self.width = 1350
                self.height = 840
                self.x = 75
                self.y = 95
                self.events = FakeEvents()

            def destroy(self) -> None:
                return None

        class FakeWebview:
            def __init__(self, window) -> None:
                self.window = window
                self.create_window = mock.Mock(return_value=window)
                self.start = mock.Mock()

        class FakeServerThread:
            def __init__(self, repo_root, host, port, session_token) -> None:
                self.repo_root = repo_root
                self.host = host
                self.port = port
                self.session_token = session_token
                self.error = None
                self.final_action = "shutdown"

            def start(self) -> None:
                return None

            def is_alive(self) -> bool:
                return False

            def join(self, timeout=None) -> None:
                return None

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "state_engine": {
                            "gui": {
                                "window": {"width": 1100, "height": 720, "x": 12, "y": 18}
                            }
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            window = FakeWindow()
            fake_webview = FakeWebview(window)

            with mock.patch.object(gui_app, "_load_webview_module", return_value=fake_webview):
                with mock.patch.object(gui_app, "ServerLoopThread", FakeServerThread):
                    with mock.patch.object(gui_app, "_wait_for_server"):
                        with mock.patch.object(gui_app, "_request_server_shutdown"):
                            action = gui_app.run_desktop_app(root, "127.0.0.1", 8765, "session-token")

            self.assertEqual(action, "shutdown")
            fake_webview.create_window.assert_called_once()
            self.assertEqual(fake_webview.create_window.call_args.kwargs["width"], 1100)
            self.assertEqual(fake_webview.create_window.call_args.kwargs["height"], 720)
            self.assertEqual(fake_webview.create_window.call_args.kwargs["x"], 12)
            self.assertEqual(fake_webview.create_window.call_args.kwargs["y"], 18)
            for callback in window.events.closing.handlers:
                callback(window)
            saved = json.loads(config_path.read_text(encoding="utf-8"))

        self.assertEqual(
            saved["state_engine"]["gui"]["window"],
            {"width": 1350, "height": 840, "x": 75, "y": 95},
        )

    def test_main_uses_desktop_mode_by_default(self) -> None:
        with mock.patch.object(sys, "argv", ["gui_app.py"]):
            with mock.patch.object(gui_app, "run_desktop_app", return_value="shutdown") as mocked_desktop:
                with mock.patch.object(gui_app, "run_browser_app") as mocked_browser:
                    exit_code = gui_app.main()

        self.assertEqual(exit_code, 0)
        mocked_desktop.assert_called_once()
        mocked_browser.assert_not_called()

    def test_main_reexecs_current_process_when_restart_requested_in_browser_mode(self) -> None:
        with mock.patch.object(sys, "argv", ["gui_app.py", "--open-browser"]):
            with mock.patch.object(gui_app, "run_browser_app", return_value="restart") as mocked_browser:
                with mock.patch.object(gui_app, "_restart_current_process") as mocked_restart:
                    exit_code = gui_app.main()

        self.assertEqual(exit_code, 0)
        mocked_browser.assert_called_once()
        self.assertTrue(mocked_browser.call_args.kwargs["open_browser"])
        self.assertIn("session_token", mocked_browser.call_args.kwargs)
        mocked_restart.assert_called_once()

    def test_main_does_not_reopen_browser_after_process_restart(self) -> None:
        with mock.patch.object(sys, "argv", ["gui_app.py", "--open-browser"]):
            with mock.patch.dict(os.environ, {gui_app._RESTARTED_ENV_VAR: "1"}, clear=False):
                with mock.patch.object(gui_app, "run_browser_app", return_value="shutdown") as mocked_browser:
                    exit_code = gui_app.main()

        self.assertEqual(exit_code, 0)
        mocked_browser.assert_called_once()
        self.assertFalse(mocked_browser.call_args.kwargs["open_browser"])

    def test_main_reexecs_current_process_when_restart_requested_in_desktop_mode(self) -> None:
        with mock.patch.object(sys, "argv", ["gui_app.py"]):
            with mock.patch.object(gui_app, "run_desktop_app", return_value="restart") as mocked_desktop:
                with mock.patch.object(gui_app, "_restart_current_process") as mocked_restart:
                    exit_code = gui_app.main()

        self.assertEqual(exit_code, 0)
        mocked_desktop.assert_called_once()
        self.assertEqual(len(mocked_desktop.call_args.args), 4)
        self.assertTrue(str(mocked_desktop.call_args.args[3] or "").strip())
        mocked_restart.assert_called_once()

    def test_main_reports_missing_pywebview(self) -> None:
        with mock.patch.object(sys, "argv", ["gui_app.py"]):
            with mock.patch.object(gui_app, "run_desktop_app", side_effect=RuntimeError("pywebview missing")):
                stderr = io.StringIO()
                with mock.patch("sys.stderr", stderr):
                    exit_code = gui_app.main()

        self.assertEqual(exit_code, 1)
        self.assertIn("pywebview missing", stderr.getvalue())

    def test_main_rejects_non_loopback_host(self) -> None:
        with mock.patch.object(sys, "argv", ["gui_app.py", "--host", "192.168.1.20"]):
            stderr = io.StringIO()
            with mock.patch("sys.stderr", stderr):
                exit_code = gui_app.main()

        self.assertEqual(exit_code, 1)
        self.assertIn("loopback hosts", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
