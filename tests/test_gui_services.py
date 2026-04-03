from __future__ import annotations

import json
import os
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


class GuiServerTests(unittest.TestCase):
    def _write_base_repo(self, root: Path) -> None:
        GuiServicesTests()._write_base_repo(root)

    def test_failed_operation_switches_right_panel_to_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)

            app = GuiApplication(root)
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

            app = GuiApplication(root)
            app.set_selected_report(str(report_path))

            rendered = app.render_page()

            self.assertIn(">Report</a>", rendered)
            self.assertIn(">Status</a>", rendered)
            self.assertNotIn(">Error Log</a>", rendered)
            self.assertIn("Restart Server", rendered)
            self.assertIn("Stop Server", rendered)
            self.assertIn('action="/server-control"', rendered)
            self.assertIn('action="/delete-report"', rendered)
            self.assertIn('action="/delete-all-reports"', rendered)
            self.assertIn("Delete All Reports", rendered)
            self.assertIn('class="danger report-delete"', rendered)
            self.assertIn('>X</button>', rendered)
            self.assertIn("if (button !== submitter)", rendered)
            self.assertIn('form.dataset.submitting = "1"', rendered)
            self.assertIn(".raw-report {", rendered)
            self.assertIn("color: var(--ink);", rendered)
            self.assertIn(".log-error-line {", rendered)
            self.assertIn('<option value="replace" selected>replace</option>', rendered)

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

            app = GuiApplication(root)

            rendered = app.render_page()

            self.assertEqual(Path(app.snapshot().selected_report_path).resolve(), newer.resolve())
            self.assertIn("2026-03-31_premarket.md", rendered)

    def test_render_page_raw_view_includes_report_body(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)
            report_path = root / "report" / "2026-03-31_premarket.md"
            report_path.write_text("# Daily Report\n\n- hello\n", encoding="utf-8")

            app = GuiApplication(root)
            app.set_selected_report(str(report_path))
            app.set_view_mode("raw")

            rendered = app.render_page()

            self.assertIn('<pre class="raw-report"># Daily Report', rendered)

    def test_render_page_status_highlights_error_lines(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)

            app = GuiApplication(root)
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
    def test_main_restarts_server_when_requested(self) -> None:
        with mock.patch.object(sys, "argv", ["gui_app.py", "--open-browser"]):
            with mock.patch.object(gui_app, "run_server", side_effect=["restart", "shutdown"]) as mocked:
                gui_app.main()

        self.assertEqual(mocked.call_count, 2)
        first_kwargs = mocked.call_args_list[0].kwargs
        second_kwargs = mocked.call_args_list[1].kwargs
        self.assertTrue(first_kwargs["open_browser"])
        self.assertFalse(second_kwargs["open_browser"])


if __name__ == "__main__":
    unittest.main()
