import unittest
from datetime import date

from rewards import opening_balances


class FakeOpeningBalanceAdapter:
    def __init__(self, *, preview=None, result=None):
        self.preview = preview or {
            "RunID": None,
            "EffectiveDate": date(2026, 5, 2),
            "DryRun": True,
            "InputRowCount": 2,
            "SetupRowCount": 3,
            "AddedZeroChapterCount": 1,
            "PositiveBalanceRowCount": 1,
            "ZeroBalanceRowCount": 1,
            "MissingSnapshotCount": 0,
            "ReconciliationIssueCount": 0,
            "AlreadyImportedCount": 0,
            "NewImportCount": 1,
            "InputPointTotal": 684000,
            "NewPointTotal": 684000,
        }
        self.result = result or {
            "RunID": 12,
            "Snapshot_Date": date(2026, 5, 2),
            "SummaryJson": (
                '{"processor":"opening_balance","input_row_count":2,'
                '"setup_row_count":3,"added_zero_chapter_count":1,'
                '"positive_balance_row_count":1,"zero_balance_row_count":1,'
                '"missing_snapshot_count":0,"reconciliation_issue_count":0,'
                '"already_imported_count":0,"new_import_count":1,'
                '"input_point_total":684000,"new_point_total":684000}'
            ),
        }
        self.queries = []
        self.statements = []

    def query_rows(self, query, params=()):
        self.queries.append((query, tuple(params)))
        if query == opening_balances.IMPORT_OPENING_BALANCES_SQL:
            return [self.preview]
        if query == opening_balances.OPENING_BALANCE_RESULT_SQL:
            return [self.result]
        raise AssertionError("Unexpected query")

    def execute_statements(self, statements):
        self.statements.extend(list(statements))


class OpeningBalancesTest(unittest.TestCase):
    def test_parse_line_with_used_points(self):
        row = opening_balances.parse_balance_line(
            "19927 Albuquerque Go ALBQ 532500 306500 155000 684000",
            1,
        )

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row.legacy_agaid, 19927)
        self.assertEqual(row.chapter_name, "Albuquerque Go")
        self.assertEqual(row.chapter_code, "ALBQ")
        self.assertEqual(row.used_points, 155000)
        self.assertEqual(row.opening_balance_points, 684000)
        self.assertEqual(row.reconciliation_delta, 0)

    def test_parse_line_without_used_points(self):
        row = opening_balances.parse_balance_line(
            "14412 Ames Go Club AMES 37000 0 37000",
            2,
        )

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row.used_points, 0)
        self.assertEqual(row.opening_balance_points, 37000)

    def test_parse_line_with_code_attached_to_name(self):
        row = opening_balances.parse_balance_line(
            "31309 Rio Grande Valley Shogi & Go SocietyRGVG 0 0 0",
            3,
        )

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row.chapter_name, "Rio Grande Valley Shogi & Go Society")
        self.assertEqual(row.chapter_code, "RGVG")

    def test_parse_line_with_three_character_code(self):
        row = opening_balances.parse_balance_line(
            "13529 Providence Go Club PVD 120000 128000 248000",
            4,
        )

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row.chapter_name, "Providence Go Club")
        self.assertEqual(row.chapter_code, "PVD")
        self.assertEqual(row.opening_balance_points, 248000)

    def test_dry_run_returns_preview_without_writes(self):
        adapter = FakeOpeningBalanceAdapter()
        rows = [
            opening_balances.OpeningBalanceRow(1, 19927, "Albuquerque Go", "ALBQ", 532500, 306500, 155000, 684000)
        ]

        result = opening_balances.import_opening_balances(
            adapter,
            rows,
            date(2026, 5, 2),
            dry_run=True,
        )

        self.assertTrue(result.dry_run)
        self.assertIsNone(result.run_id)
        self.assertEqual(result.setup_row_count, 3)
        self.assertEqual(result.added_zero_chapter_count, 1)
        self.assertEqual(result.new_import_count, 1)
        self.assertEqual(adapter.statements, [])

    def test_write_executes_proc_and_reads_summary(self):
        adapter = FakeOpeningBalanceAdapter()
        rows = [
            opening_balances.OpeningBalanceRow(1, 19927, "Albuquerque Go", "ALBQ", 532500, 306500, 155000, 684000)
        ]

        result = opening_balances.import_opening_balances(
            adapter,
            rows,
            date(2026, 5, 2),
            run_type="import",
        )

        self.assertFalse(result.dry_run)
        self.assertEqual(result.run_id, 12)
        self.assertEqual(result.new_point_total, 684000)
        self.assertEqual(len(adapter.statements), 1)
        sql, params = adapter.statements[0]
        self.assertEqual(sql, opening_balances.IMPORT_OPENING_BALANCES_SQL)
        self.assertEqual(params[1:], (date(2026, 5, 2), "import", False))
        self.assertEqual(adapter.queries[-1][1], ("import", date(2026, 5, 2), opening_balances.SOURCE_TYPE))


if __name__ == "__main__":
    unittest.main()
