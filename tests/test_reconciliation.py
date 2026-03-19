from __future__ import annotations

import tempfile
import textwrap
import unittest
from pathlib import Path

from core.reconciliation import _import_trades_from_os_history_xml


class ReconciliationImportTests(unittest.TestCase):
    def _write_xml(self, xml_body: str) -> str:
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        path = Path(tmpdir.name) / "OSHistoryDealAll.xml"
        path.write_text(xml_body, encoding="utf-8")
        return str(path)

    def test_import_trades_supports_english_field_aliases(self) -> None:
        xml_path = self._write_xml(
            textwrap.dedent(
                """\
                <root>
                  <row
                    trade_date="2026/03/16"
                    trade_time="2026/03/16 23:15:59"
                    side="BUY"
                    product="NVDA Nvidia"
                    qty="33"
                    amount="6090.12"
                    fee="12.18"
                    net="6102.30"
                    price="184.549"
                  />
                </root>
                """
            )
        )

        rows = _import_trades_from_os_history_xml(xml_path)

        self.assertEqual(len(rows), 1)
        trade = rows[0]
        self.assertEqual(trade["trade_date_et"], "2026-03-16")
        self.assertEqual(trade["ticker"], "NVDA")
        self.assertEqual(trade["side"], "BUY")
        self.assertEqual(trade["shares"], 33)
        self.assertEqual(trade["cash_amount"], 6102.30)
        self.assertEqual(trade["cash_basis"], "Total")
        self.assertEqual(trade["notes"], "Imported from OSHistoryDealAll (NVDA)")
        self.assertEqual(trade["source"], "xml:OSHistoryDealAll.xml")

    def test_import_trades_supports_chinese_field_aliases(self) -> None:
        xml_path = self._write_xml(
            textwrap.dedent(
                """\
                <root>
                  <row
                    成交日期="2026/03/16"
                    成交時間="2026/03/16 23:15:59"
                    買賣="買進"
                    商品名稱="NVDA 輝達"
                    成交股數="33"
                    成交金額="6090.12"
                    手續費="12.18"
                    淨額="6102.30"
                    成交均價="184.549"
                  />
                </root>
                """
            )
        )

        rows = _import_trades_from_os_history_xml(xml_path)

        self.assertEqual(len(rows), 1)
        trade = rows[0]
        self.assertEqual(trade["trade_date_et"], "2026-03-16")
        self.assertEqual(trade["ticker"], "NVDA")
        self.assertEqual(trade["side"], "BUY")
        self.assertEqual(trade["shares"], 33)
        self.assertEqual(trade["cash_amount"], 6102.30)
        self.assertAlmostEqual(trade["fee_rate_pct"], 12.18 / 6090.12, places=10)
        self.assertEqual(trade["notes"], "Imported from OSHistoryDealAll (NVDA)")
        self.assertEqual(trade["source"], "xml:OSHistoryDealAll.xml")


if __name__ == "__main__":
    unittest.main()
