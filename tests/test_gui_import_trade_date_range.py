# Copyright (c) 2026 Sheng-Hsin Tsai
# SPDX-License-Identifier: MIT

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from gui.services import GuiServices, OperationResult
from tests.test_gui_services import GuiServicesTests


class GuiImportTradeDateRangeTests(unittest.TestCase):
    def _write_base_repo(self, root: Path) -> None:
        GuiServicesTests()._write_base_repo(root)

    def test_run_import_trades_passes_optional_trade_date_range(self) -> None:
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
                result = services.run_import_trades(
                    str(xls_path),
                    trade_date_from="2026-03-20",
                    trade_date_to="2026-03-31",
                )

            self.assertTrue(result.success)
            self.assertIn("--trade-date-from", captured["command"])
            self.assertIn("--trade-date-to", captured["command"])
            self.assertIn("2026-03-20", captured["command"])
            self.assertIn("2026-03-31", captured["command"])

if __name__ == "__main__":
    unittest.main()
