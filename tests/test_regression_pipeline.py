from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from utils.precision import load_state_engine_numeric_precision


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES_DIR = REPO_ROOT / "tests" / "fixtures"
FIXED_NOW_ET = "2026-03-18T08:00:00-04:00"
PROJECT_ITEMS = [
    "config.json",
    "report_spec.json",
    "states.json",
    "trades.json",
    "update_states.py",
    "generate_report.py",
    "download_1y.py",
    "core",
    "utils",
    "data",
]


def _copy_project(dst: Path) -> None:
    for name in PROJECT_ITEMS:
        src = REPO_ROOT / name
        target = dst / name
        if not src.exists():
            if name == "data":
                target.mkdir(parents=True, exist_ok=True)
                continue
            raise FileNotFoundError(src)
        if src.is_dir():
            shutil.copytree(src, target)
        else:
            shutil.copy2(src, target)


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
            _copy_project(workdir)
            subprocess.run(
                [
                    sys.executable,
                    "update_states.py",
                    "--states",
                    "states.json",
                    "--out",
                    "out_states.json",
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
                    "out_report.md",
                    "--now-et",
                    FIXED_NOW_ET,
                ],
                cwd=workdir,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
            )
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
            _copy_project(workdir)
            shutil.copy2(FIXTURES_DIR / "golden_premarket_states.json", workdir / "golden_states.json")
            shutil.copy2(FIXTURES_DIR / "golden_premarket_trades.json", workdir / "trades.json")
            subprocess.run(
                [
                    sys.executable,
                    "generate_report.py",
                    "--states",
                    "golden_states.json",
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


if __name__ == "__main__":
    unittest.main()
