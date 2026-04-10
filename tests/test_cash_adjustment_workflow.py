from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from gui.services import GuiServices, OperationResult
from tests.test_gui_services import GuiServicesTests


class CashAdjustmentWorkflowTests(unittest.TestCase):
    def _write_base_repo(self, root: Path) -> None:
        GuiServicesTests()._write_base_repo(root)

    def test_gui_cash_adjustment_refreshes_selected_report_on_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_base_repo(root)
            services = GuiServices(root)
            report_path = root / "report" / "2026-03-31_premarket.md"
            report_path.write_text("# Daily Report\n", encoding="utf-8")

            primary = OperationResult(
                name="Cash adjustment",
                success=True,
                returncode=0,
                command="python update_states.py",
                stdout="cash adjusted",
                message="ok",
            )
            refreshed = OperationResult(
                name="Refresh 2026-03-31_premarket.md",
                success=True,
                returncode=0,
                command="python generate_report.py",
                stdout="report refreshed",
                message="refreshed",
                log_path="logs/generate_report.log",
                report_path=str(report_path),
                report_json_path=str(report_path.with_suffix(".json")),
            )

            with mock.patch.object(services, "_run_command", return_value=primary) as mocked_run:
                with mock.patch.object(services, "refresh_selected_report", return_value=refreshed) as mocked_refresh:
                    result = services.run_cash_adjustment(
                        "-3600",
                        cash_adjust_note="wire out",
                        selected_report_path=str(report_path),
                    )

            mocked_run.assert_called_once()
            mocked_refresh.assert_called_once_with(str(report_path))
            self.assertTrue(result.success)
            self.assertIn("Refreshed 2026-03-31_premarket.md.", result.message)
            self.assertEqual(result.report_path, str(report_path))
            self.assertEqual(result.report_json_path, str(report_path.with_suffix(".json")))
            self.assertEqual(result.log_path, "logs/generate_report.log")
            self.assertIn("cash adjusted", result.stdout)
            self.assertIn("report refreshed", result.stdout)


if __name__ == "__main__":
    unittest.main()
