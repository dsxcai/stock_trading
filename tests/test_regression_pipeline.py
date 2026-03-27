from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from utils.config_access import discover_state_engine_tickers, load_state_engine_config
from utils.precision import load_state_engine_numeric_precision


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES_DIR = REPO_ROOT / "tests" / "fixtures"
FIXTURE_MARKET_DATA_DIR = FIXTURES_DIR / "market_data"
FIXTURE_STATES = FIXTURES_DIR / "golden_premarket_states.json"
FIXTURE_TRADES = FIXTURES_DIR / "golden_premarket_trades.json"
FIXED_NOW_ET = "2026-03-18T08:00:00-04:00"
PROJECT_CORE_ITEMS = [
    "config.json",
    "report_spec.json",
    "update_states.py",
    "generate_report.py",
    "download_1y.py",
    "core",
    "utils",
]


def _copy_project(
    dst: Path,
    *,
    states_src: Path,
    trades_src: Path,
    data_src: Path,
) -> None:
    for name in PROJECT_CORE_ITEMS:
        src = REPO_ROOT / name
        target = dst / name
        if not src.exists():
            raise FileNotFoundError(src)
        if src.is_dir():
            shutil.copytree(src, target)
        else:
            shutil.copy2(src, target)
    shutil.copy2(states_src, dst / "states.json")
    shutil.copy2(trades_src, dst / "trades.json")
    shutil.copytree(data_src, dst / "data")


def _expected_active_tickers(config_path: Path, states_path: Path) -> set[str]:
    cfg = load_state_engine_config(str(config_path))
    states = json.loads(states_path.read_text(encoding="utf-8"))
    expected = discover_state_engine_tickers(cfg)
    seen = {ticker for ticker in expected}
    for position in (((states.get("portfolio") or {}).get("positions")) or []):
        if isinstance(position, dict):
            ticker_norm = str(position.get("ticker") or "").upper().strip()
            if ticker_norm and ticker_norm not in seen:
                seen.add(ticker_norm)
                expected.append(ticker_norm)
    return set(expected)


def _run_premarket_update(workdir: Path, *, states_name: str = "states.json", out_states: str = "out_states.json", out_report: str = "out_report.md") -> None:
    subprocess.run(
        [
            sys.executable,
            "update_states.py",
            "--states",
            states_name,
            "--out",
            out_states,
            "--csv-dir",
            "data",
            "--derive-signals-inputs",
            "force",
            "--derive-threshold-inputs",
            "force",
            "--mode",
            "Premarket",
            "--render-report",
            "--report-schema",
            "report_spec.json",
            "--report-out",
            out_report,
            "--now-et",
            FIXED_NOW_ET,
        ],
        cwd=workdir,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=True,
    )


class RegressionPipelineTests(unittest.TestCase):
    maxDiff = None

    def _assert_selected_fields_rounded(self, obj: object, ndigits: int) -> None:
        keys = {
            "fee_rate_pct",
            "ma_sum_prev",
            "threshold_from_ma",
            "threshold",
            "ma_t",
            "profit_rate",
            "holdings_cost_usd",
            "holdings_mv_usd",
            "market_value_usd",
            "nav_usd",
            "total_assets_usd",
            "unrealized_pnl_usd",
            "unrealized_pnl_pct",
        }

        def walk(node: object) -> None:
            if isinstance(node, dict):
                for k, v in node.items():
                    if k in keys and isinstance(v, (int, float)) and not isinstance(v, bool):
                        self.assertEqual(v, round(float(v), ndigits), f"{k} must be rounded to {ndigits} decimals")
                    walk(v)
                return
            if isinstance(node, list):
                for item in node:
                    walk(item)

        walk(obj)

    def test_premarket_pipeline_matches_golden_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workdir = Path(tmp)
            _copy_project(
                workdir,
                states_src=FIXTURE_STATES,
                trades_src=FIXTURE_TRADES,
                data_src=FIXTURE_MARKET_DATA_DIR,
            )
            _run_premarket_update(workdir)
            actual_states = (workdir / "out_states.json").read_text(encoding="utf-8")
            actual_report = (workdir / "out_report.md").read_text(encoding="utf-8")
            expected_states = (FIXTURES_DIR / "golden_premarket_states.json").read_text(encoding="utf-8")
            expected_report = (FIXTURES_DIR / "golden_premarket_report.md").read_text(encoding="utf-8")
            self.assertEqual(actual_states, expected_states)
            self.assertEqual(actual_report, expected_report)
            precision = load_state_engine_numeric_precision(str(workdir / "config.json"))
            self._assert_selected_fields_rounded(json.loads(actual_states), int(precision["state_selected_fields"]))

    def test_generate_report_matches_golden_markdown(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workdir = Path(tmp)
            _copy_project(
                workdir,
                states_src=FIXTURE_STATES,
                trades_src=FIXTURE_TRADES,
                data_src=FIXTURE_MARKET_DATA_DIR,
            )
            subprocess.run(
                [
                    sys.executable,
                    "generate_report.py",
                    "--states",
                    "states.json",
                    "--trades-file",
                    "trades.json",
                    "--schema",
                    "report_spec.json",
                    "--mode",
                    "Premarket",
                    "--date",
                    "2026-03-18",
                    "--out",
                    "rendered_report.md",
                ],
                cwd=workdir,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
            )
            actual_report = (workdir / "rendered_report.md").read_text(encoding="utf-8")
            expected_report = (FIXTURES_DIR / "golden_premarket_report.md").read_text(encoding="utf-8")
            self.assertEqual(actual_report, expected_report)

    def test_premarket_pipeline_keeps_position_notes_out_of_states(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workdir = Path(tmp)
            _copy_project(
                workdir,
                states_src=FIXTURE_STATES,
                trades_src=FIXTURE_TRADES,
                data_src=FIXTURE_MARKET_DATA_DIR,
            )
            _run_premarket_update(workdir)

            out_states = json.loads((workdir / "out_states.json").read_text(encoding="utf-8"))
            for position in (out_states.get("portfolio") or {}).get("positions") or []:
                self.assertNotIn("notes", position)

            current_positions = (workdir / "out_report.md").read_text(encoding="utf-8").split(
                "## Current Positions\n",
                1,
            )[1].split(
                "\n## Signal Status",
                1,
            )[0]
            self.assertIn("Imported from Capital XLS (ARKQ ARKQUS) x70", current_positions)
            self.assertIn("Imported from Capital XLS (SPY SPDR標普500ETF) x17", current_positions)
            self.assertIn("Imported from Capital XLS (SMH VanEck半導體ETF) x29", current_positions)
            self.assertIn("Unrealized PnL (TWD)", current_positions)
            self.assertIn("Unrealized PnL % (TWD)", current_positions)
            self.assertIn("Estimated Price", (workdir / "out_report.md").read_text(encoding="utf-8"))

    def test_premarket_pipeline_rebuilds_market_snapshot_to_functional_held_and_fx_tickers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workdir = Path(tmp)
            _copy_project(
                workdir,
                states_src=FIXTURE_STATES,
                trades_src=FIXTURE_TRADES,
                data_src=FIXTURE_MARKET_DATA_DIR,
            )
            _run_premarket_update(workdir)

            report_snapshot = json.loads((workdir / "report" / "2026-03-18_premarket.json").read_text(encoding="utf-8"))
            prices_now = ((report_snapshot.get("market") or {}).get("prices_now") or {})
            self.assertEqual(set(prices_now), _expected_active_tickers(workdir / "config.json", workdir / "states.json"))

    def test_force_mode_allows_session_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workdir = Path(tmp)
            _copy_project(
                workdir,
                states_src=FIXTURE_STATES,
                trades_src=FIXTURE_TRADES,
                data_src=FIXTURE_MARKET_DATA_DIR,
            )
            base_cmd = [
                sys.executable,
                "update_states.py",
                "--states",
                "states.json",
                "--out",
                "forced_intraday_states.json",
                "--csv-dir",
                "data",
                "--derive-signals-inputs",
                "force",
                "--derive-threshold-inputs",
                "force",
                "--mode",
                "Intraday",
                "--render-report",
                "--report-schema",
                "report_spec.json",
                "--report-out",
                "forced_intraday_report.md",
                "--now-et",
                FIXED_NOW_ET,
            ]
            rejected = subprocess.run(
                base_cmd,
                cwd=workdir,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("current ET session is premarket", rejected.stdout)
            self.assertFalse((workdir / "forced_intraday_states.json").exists())

            forced = subprocess.run(
                [*base_cmd, "--force-mode"],
                cwd=workdir,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
            self.assertEqual(forced.returncode, 0, forced.stdout)
            self.assertIn("forcing mode=Intraday via -f/--force-mode despite ET/session mismatch", forced.stdout)
            self.assertTrue((workdir / "forced_intraday_states.json").exists())
            self.assertTrue((workdir / "forced_intraday_report.md").exists())
            self.assertIn(
                "# Daily Investment Report (Intraday)",
                (workdir / "forced_intraday_report.md").read_text(encoding="utf-8"),
            )


class LiveDataSmokeTests(unittest.TestCase):
    def test_live_premarket_update_runs_and_emits_core_sections(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workdir = Path(tmp)
            _copy_project(
                workdir,
                states_src=REPO_ROOT / "states.json",
                trades_src=REPO_ROOT / "trades.json",
                data_src=REPO_ROOT / "data",
            )
            _run_premarket_update(workdir, out_states="live_out_states.json", out_report="live_out_report.md")

            report_text = (workdir / "live_out_report.md").read_text(encoding="utf-8")
            self.assertIn("# Daily Investment Report (Premarket)", report_text)
            self.assertIn("## Performance Summary", report_text)
            self.assertIn("## Current Positions", report_text)
            self.assertIn("## Signal Status", report_text)
            self.assertIn("## t+1 Hypothetical Trigger Close Threshold (P_min)", report_text)

            report_snapshot = json.loads((workdir / "report" / "2026-03-18_premarket.json").read_text(encoding="utf-8"))
            prices_now = ((report_snapshot.get("market") or {}).get("prices_now") or {})
            self.assertEqual(set(prices_now), _expected_active_tickers(workdir / "config.json", workdir / "states.json"))

            out_states = json.loads((workdir / "live_out_states.json").read_text(encoding="utf-8"))
            for position in (out_states.get("portfolio") or {}).get("positions") or []:
                self.assertNotIn("notes", position)


if __name__ == "__main__":
    unittest.main()
