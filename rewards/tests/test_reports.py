import argparse
import json
import unittest
from datetime import date, datetime
from decimal import Decimal
from io import StringIO

from rewards import reports


class FakeReportAdapter:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.queries = []

    def query_rows(self, query, params=()):
        self.queries.append((query, tuple(params)))
        return self.rows


def report_args(**overrides):
    values = {
        "top": 25,
        "chapter_code": None,
        "status": None,
        "aging_status": None,
        "processor": None,
        "source_type": None,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


class RewardsReportsTest(unittest.TestCase):
    def test_fetch_balances_filters_by_chapter_code(self):
        adapter = FakeReportAdapter([{"Chapter_Code": "NYG"}])

        rows = reports.fetch_balances(adapter, report_args(top=10, chapter_code="NYG"))

        self.assertEqual(rows, [{"Chapter_Code": "NYG"}])
        self.assertEqual(adapter.queries, [(reports.BALANCES_SQL, (10, "NYG", "NYG"))])

    def test_fetch_transactions_filters_by_chapter_and_source(self):
        adapter = FakeReportAdapter()

        reports.fetch_transactions(
            adapter,
            report_args(top=5, chapter_code="SEA", source_type="rated_game_participation"),
        )

        self.assertEqual(
            adapter.queries,
            [(reports.TRANSACTIONS_SQL, (5, "SEA", "SEA", "rated_game_participation", "rated_game_participation"))],
        )

    def test_fetch_chapter_renewal_notices_filters_by_chapter_and_status(self):
        adapter = FakeReportAdapter()

        reports.fetch_chapter_renewal_notices(
            adapter,
            report_args(top=8, chapter_code="NYG", status="insufficient_points"),
        )

        self.assertEqual(
            adapter.queries,
            [(reports.CHAPTER_RENEWAL_NOTICES_SQL, (8, "NYG", "NYG", "insufficient_points", "insufficient_points"))],
        )

    def test_fetch_pending_chapter_renewals_filters_by_chapter(self):
        adapter = FakeReportAdapter()

        reports.fetch_pending_chapter_renewals(adapter, report_args(top=7, chapter_code="SHPO"))

        self.assertEqual(
            adapter.queries,
            [(reports.PENDING_CHAPTER_RENEWALS_SQL, (7, "SHPO", "SHPO"))],
        )

    def test_format_table_renders_dates_datetimes_and_decimals(self):
        rows = [
            {
                "Chapter_Code": "NYG",
                "Available_Points": Decimal("1200"),
                "Latest_Snapshot_Date": date(2026, 5, 2),
                "Last_Transaction_Posted_At": datetime(2026, 5, 2, 17, 30, 4),
            }
        ]

        text = reports.format_table(
            rows,
            (
                ("Chapter_Code", "Chapter"),
                ("Available_Points", "Available"),
                ("Latest_Snapshot_Date", "Snapshot"),
                ("Last_Transaction_Posted_At", "Posted"),
            ),
        )

        self.assertIn("NYG", text)
        self.assertIn("1200", text)
        self.assertIn("2026-05-02", text)
        self.assertIn("2026-05-02 17:30:04", text)

    def test_rows_as_json_serializes_dates_and_decimals(self):
        data = json.loads(
            reports.rows_as_json(
                [
                    {
                        "date": date(2026, 5, 2),
                        "points": Decimal("500"),
                    }
                ]
            )
        )

        self.assertEqual(data, [{"date": "2026-05-02", "points": 500}])

    def test_print_report_handles_empty_rows(self):
        output = StringIO()

        reports.print_report("Chapter Balances", [], reports.BALANCE_COLUMNS, output)

        self.assertIn("Chapter Balances", output.getvalue())
        self.assertIn("(no rows)", output.getvalue())


if __name__ == "__main__":
    unittest.main()
