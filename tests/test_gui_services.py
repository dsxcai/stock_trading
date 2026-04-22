# Copyright (c) 2026 Sheng-Hsin Tsai
# SPDX-License-Identifier: MIT

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import gui_app
import gui_ipc
from gui.desktop_backend import DesktopSessionState, GuiDesktopBackend
from gui.services import GuiServices, OperationResult


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

    def test_run_report_uses_update_states_for_latest_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)
            services = GuiServices(root)
            captured = {}

            def fake_run_command(command, *, name):
                captured["command"] = list(command)
                captured["name"] = name
                return OperationResult(name=name, success=True, returncode=0, command=" ".join(command), stdout="", message="ok")

            with mock.patch.object(services, "_run_command", side_effect=fake_run_command):
                services.run_report("premarket", "", force_mode=True, allow_incomplete_csv_rows=True)

            self.assertEqual(captured["command"][1], "update_states.py")
            self.assertIn("--force-mode", captured["command"])
            self.assertIn("--allow-incomplete-csv-rows", captured["command"])
            self.assertIn("--render-report", captured["command"])

    def test_run_report_uses_generate_report_for_historical_date(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)
            services = GuiServices(root)
            captured = {}

            def fake_run_command(command, *, name):
                captured["command"] = list(command)
                captured["name"] = name
                return OperationResult(name=name, success=True, returncode=0, command=" ".join(command), stdout="", message="ok")

            with mock.patch.object(services, "_run_command", side_effect=fake_run_command):
                services.run_report("afterclose", "2026-03-31", allow_incomplete_csv_rows=True)

            self.assertEqual(captured["command"][1], "generate_report.py")
            self.assertIn("--date", captured["command"])
            self.assertEqual(captured["command"][captured["command"].index("--date") + 1], "2026-03-31")
            self.assertIn("--allow-incomplete-csv-rows", captured["command"])

    def test_run_report_rejects_intraday_for_historical_date(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)
            services = GuiServices(root)

            with self.assertRaisesRegex(ValueError, "Intraday"):
                services.run_report("intraday", "2026-03-31")


class GuiBackendStateTests(unittest.TestCase):
    def _write_base_repo(self, root: Path) -> None:
        GuiServicesTests()._write_base_repo(root)

    def test_build_state_includes_selected_report_runtime_signal_config_and_last_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)
            report_path = root / "report" / "2026-03-31_premarket.md"
            report_path.write_text("# Daily Report\n\n- hello\n", encoding="utf-8")
            log_path = root / "logs" / "latest.log"
            log_path.write_text("[ERR] failure\n", encoding="utf-8")

            backend = GuiDesktopBackend(root)
            payload = backend.build_state(
                DesktopSessionState(
                    selected_report_path=str(report_path),
                    last_result=OperationResult(
                        name="Generate report",
                        success=False,
                        returncode=1,
                        command="python generate_report.py",
                        stdout="failed",
                        message="generation failed",
                        log_path=str(log_path),
                    ),
                )
            )

            self.assertEqual(payload["ui"]["selected_report_path"], str(report_path))
            self.assertEqual(payload["report"]["selected"]["name"], "2026-03-31_premarket.md")
            self.assertIn("# Daily Report", payload["report"]["text"])
            self.assertEqual(payload["report"]["error_log_text"], "[ERR] failure\n")
            self.assertEqual(payload["last_result"]["message"], "generation failed")
            self.assertIn("GOOG", payload["signal_config"]["selected_windows"])
            self.assertTrue(payload["recent_reports"])

    def test_perform_action_select_report_and_error_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)
            report_path = root / "report" / "2026-03-31_premarket.md"
            report_path.write_text("# Daily Report\n", encoding="utf-8")

            backend = GuiDesktopBackend(root)
            selected = backend.perform_action("select-report", {"report_path": str(report_path)})
            self.assertEqual(selected.selected_report_path, str(report_path))

            error_state = backend.perform_action(
                "select-report",
                {
                    "report_path": str(report_path),
                    "last_result": GuiDesktopBackend.serialize_operation_result(
                        OperationResult(
                            name="Generate report",
                            success=False,
                            returncode=1,
                            command="python generate_report.py",
                            stdout="failed",
                            message="generation failed",
                        )
                    ),
                    "selected_report_path": str(report_path),
                },
            )
            self.assertEqual(error_state.selected_report_path, str(report_path))
            self.assertIsNotNone(error_state.last_result)

    def test_perform_action_run_mode_merges_operation_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)
            backend = GuiDesktopBackend(root)
            report_path = root / "report" / "2026-03-31_premarket.md"

            fake_result = OperationResult(
                name="Premarket run",
                success=True,
                returncode=0,
                command="python update_states.py",
                stdout="ok",
                message="completed",
                report_path=str(report_path),
                report_json_path=str(report_path.with_suffix(".json")),
            )

            with mock.patch.object(backend.services, "run_report", return_value=fake_result):
                session_state = backend.perform_action(
                    "run-mode",
                    {"mode": "premarket", "force_mode": True, "allow_incomplete_csv_rows": False},
                )

            self.assertEqual(session_state.selected_report_path, str(report_path))
            self.assertEqual(session_state.last_result.message, "completed")

    def test_perform_action_generate_report_uses_unified_report_flow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)
            backend = GuiDesktopBackend(root)
            report_path = root / "report" / "2026-03-31_afterclose.md"

            fake_result = OperationResult(
                name="Generate AfterClose report",
                success=True,
                returncode=0,
                command="python generate_report.py",
                stdout="ok",
                message="generated",
                report_path=str(report_path),
                report_json_path=str(report_path.with_suffix(".json")),
            )

            with mock.patch.object(backend.services, "run_report", return_value=fake_result) as mocked_run:
                session_state = backend.perform_action(
                    "generate-report",
                    {"mode": "afterclose", "report_date": "2026-03-31", "allow_incomplete_csv_rows": False},
                )

            mocked_run.assert_called_once_with(
                "afterclose",
                "2026-03-31",
                force_mode=False,
                allow_incomplete_csv_rows=False,
            )
            self.assertEqual(session_state.selected_report_path, str(report_path))
            self.assertEqual(session_state.last_result.message, "generated")

    def test_serialize_and_deserialize_operation_result_roundtrip(self) -> None:
        result = OperationResult(
            name="Generate report",
            success=True,
            returncode=0,
            command="python generate_report.py",
            stdout="ok",
            message="done",
            log_path="logs/latest.log",
            report_path="report/2026-03-31_premarket.md",
            report_json_path="report/2026-03-31_premarket.json",
        )
        serialized = GuiDesktopBackend.serialize_operation_result(result)
        restored = GuiDesktopBackend.deserialize_operation_result(serialized)

        self.assertIsNotNone(restored)
        self.assertEqual(restored.command, result.command)
        self.assertEqual(restored.report_path, result.report_path)


class GuiLauncherTests(unittest.TestCase):
    def test_run_npm_clears_electron_run_as_node(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            desktop_dir = Path(tmp)
            captured = {}

            def fake_run(command, cwd, env, check):
                captured["command"] = command
                captured["cwd"] = cwd
                captured["env"] = dict(env)
                captured["check"] = check
                return mock.Mock(returncode=0)

            with mock.patch.dict(os.environ, {"ELECTRON_RUN_AS_NODE": "1"}, clear=False):
                with mock.patch("shutil.which", return_value="npm"):
                    with mock.patch("subprocess.run", side_effect=fake_run):
                        exit_code = gui_app._run_npm(desktop_dir, "run", "dev")

        self.assertEqual(exit_code, 0)
        self.assertEqual(captured["command"], ["npm", "run", "dev"])
        self.assertEqual(captured["cwd"], desktop_dir)
        self.assertNotIn("ELECTRON_RUN_AS_NODE", captured["env"])

    def test_main_runs_desktop_dev_script_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            desktop_dir = Path(tmp)

            with mock.patch.object(sys, "argv", ["gui_app.py", "--dev"]):
                with mock.patch.object(gui_app, "_desktop_dir", return_value=desktop_dir):
                    with mock.patch.object(gui_app, "_require_binary"):
                        with mock.patch.object(gui_app, "_ensure_desktop_dependencies") as mocked_install:
                            with mock.patch.object(gui_app, "_run_npm", return_value=0) as mocked_run:
                                exit_code = gui_app.main()

        self.assertEqual(exit_code, 0)
        mocked_install.assert_called_once_with(desktop_dir, skip_install=False)
        mocked_run.assert_called_once_with(desktop_dir, "run", "dev")

    def test_main_builds_then_starts_desktop_in_production_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            desktop_dir = Path(tmp)

            with mock.patch.object(sys, "argv", ["gui_app.py"]):
                with mock.patch.object(gui_app, "_desktop_dir", return_value=desktop_dir):
                    with mock.patch.object(gui_app, "_require_binary"):
                        with mock.patch.object(gui_app, "_ensure_desktop_dependencies") as mocked_install:
                            with mock.patch.object(gui_app, "_ensure_desktop_build") as mocked_build:
                                with mock.patch.object(gui_app, "_run_npm", return_value=0) as mocked_run:
                                    exit_code = gui_app.main()

        self.assertEqual(exit_code, 0)
        mocked_install.assert_called_once_with(desktop_dir, skip_install=False)
        mocked_build.assert_called_once_with(desktop_dir, force_rebuild=False)
        mocked_run.assert_called_once_with(desktop_dir, "start")

    def test_main_restarts_launcher_when_restart_flag_is_written(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            desktop_dir = repo_root / "desktop"
            desktop_dir.mkdir()
            restart_flag = repo_root / ".restart_flag"
            run_calls = []

            def fake_run_npm(_desktop_dir, *npm_args):
                run_calls.append(npm_args)
                if len(run_calls) == 1:
                    restart_flag.write_text("", encoding="utf-8")
                return 0

            with mock.patch.object(sys, "argv", ["gui_app.py"]):
                with mock.patch.object(gui_app, "_desktop_dir", return_value=desktop_dir):
                    with mock.patch.object(gui_app, "_require_binary"):
                        with mock.patch.object(gui_app, "_ensure_desktop_dependencies"):
                            with mock.patch.object(gui_app, "_ensure_desktop_build") as mocked_build:
                                with mock.patch.object(gui_app, "_run_npm", side_effect=fake_run_npm):
                                    with mock.patch.object(gui_app, "__file__", str(repo_root / "gui_app.py")):
                                        exit_code = gui_app.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(run_calls, [("start",), ("start",)])
        self.assertEqual(mocked_build.call_count, 2)
        self.assertFalse(restart_flag.exists())

    def test_main_reports_missing_node_binary(self) -> None:
        with mock.patch.object(sys, "argv", ["gui_app.py"]):
            with mock.patch.object(gui_app, "_require_binary", side_effect=RuntimeError("node missing")):
                stderr = io.StringIO()
                with mock.patch("sys.stderr", stderr):
                    exit_code = gui_app.main()

        self.assertEqual(exit_code, 1)
        self.assertIn("node missing", stderr.getvalue())


    def test_run_npm_uses_resolved_absolute_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            desktop_dir = Path(tmp)
            captured = {}

            def fake_run(command, cwd, env, check):
                captured["command"] = command
                return mock.Mock(returncode=0)

            with mock.patch("shutil.which", return_value="/absolute/path/to/npm"):
                with mock.patch("subprocess.run", side_effect=fake_run):
                    gui_app._run_npm(desktop_dir, "install")

            self.assertEqual(captured["command"][0], "/absolute/path/to/npm")
            self.assertEqual(captured["command"][1], "install")

    def test_ensure_desktop_build_force_rebuild_removes_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            desktop_dir = Path(tmp)
            dist_dir = desktop_dir / "dist"
            dist_electron_dir = desktop_dir / "dist-electron"
            dist_dir.mkdir()
            dist_electron_dir.mkdir()
            (dist_dir / "index.html").touch()
            (dist_electron_dir / "main.js").touch()

            with mock.patch.object(gui_app, "_run_npm", return_value=0) as mocked_run:
                gui_app._ensure_desktop_build(desktop_dir, force_rebuild=True)

            self.assertFalse(dist_dir.exists())
            self.assertFalse(dist_electron_dir.exists())
            mocked_run.assert_called_once_with(desktop_dir, "run", "build")

    def test_ensure_desktop_build_rebuilds_when_sources_are_newer_than_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            desktop_dir = Path(tmp)
            dist_dir = desktop_dir / "dist"
            dist_electron_dir = desktop_dir / "dist-electron"
            src_dir = desktop_dir / "src"
            electron_dir = desktop_dir / "electron"
            dist_dir.mkdir()
            dist_electron_dir.mkdir()
            src_dir.mkdir()
            electron_dir.mkdir()

            renderer_index = dist_dir / "index.html"
            electron_main = dist_electron_dir / "main.js"
            source_file = src_dir / "App.tsx"
            electron_source = electron_dir / "main.ts"
            renderer_index.write_text("", encoding="utf-8")
            electron_main.write_text("", encoding="utf-8")
            source_file.write_text("", encoding="utf-8")
            electron_source.write_text("", encoding="utf-8")
            old_time = 10
            new_time = 20
            os.utime(renderer_index, (old_time, old_time))
            os.utime(electron_main, (old_time, old_time))
            os.utime(source_file, (new_time, new_time))
            os.utime(electron_source, (new_time, new_time))

            with mock.patch.object(gui_app, "_run_npm", return_value=0) as mocked_run:
                gui_app._ensure_desktop_build(desktop_dir, force_rebuild=False)

            self.assertFalse(dist_dir.exists())
            self.assertFalse(dist_electron_dir.exists())
            mocked_run.assert_called_once_with(desktop_dir, "run", "build")

    def test_is_build_stale_returns_false_when_outputs_are_current(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            desktop_dir = Path(tmp)
            (desktop_dir / "dist").mkdir()
            (desktop_dir / "dist-electron").mkdir()
            (desktop_dir / "src").mkdir()
            (desktop_dir / "electron").mkdir()
            renderer_index = desktop_dir / "dist" / "index.html"
            electron_main = desktop_dir / "dist-electron" / "main.js"
            source_file = desktop_dir / "src" / "App.tsx"
            renderer_index.write_text("", encoding="utf-8")
            electron_main.write_text("", encoding="utf-8")
            source_file.write_text("", encoding="utf-8")
            os.utime(source_file, (10, 10))
            os.utime(renderer_index, (20, 20))
            os.utime(electron_main, (20, 20))

            self.assertFalse(gui_app._is_build_stale(desktop_dir))

    def test_vite_config_uses_relative_base_path(self) -> None:
        # Prevents the "white screen of death" by ensuring Vite uses relative paths in Electron
        repo_root = Path(__file__).resolve().parent.parent
        vite_config = repo_root / "desktop" / "vite.config.ts"
        if vite_config.exists():
            content = vite_config.read_text(encoding="utf-8")
            self.assertRegex(content, r'base:\s*["\']\./["\']', "vite.config.ts must use relative base path './' to work in Electron")


class GuiIpcTests(unittest.TestCase):
    def test_read_payload_uses_readline_to_prevent_deadlock(self) -> None:
        with mock.patch("sys.stdin.readline", return_value='{"action": "ping"}\n'):
            payload = gui_ipc._read_payload()
        self.assertEqual(payload, {"action": "ping"})

    def test_read_payload_handles_empty_input(self) -> None:
        with mock.patch("sys.stdin.readline", return_value=''):
            payload = gui_ipc._read_payload()
        self.assertEqual(payload, {})


if __name__ == "__main__":
    unittest.main()
