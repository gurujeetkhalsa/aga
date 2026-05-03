import unittest
from datetime import date

from rewards import expirations


class FakeExpirationAdapter:
    def __init__(self, *, preview=None, result=None):
        self.preview = preview or {
            "RunID": None,
            "AsOfDate": date(2028, 5, 3),
            "DryRun": True,
            "ExpiringLotCount": 3,
            "AlreadyExpiredCount": 1,
            "NewExpirationCount": 2,
            "ExpiredPointTotal": 7500,
            "ChapterCount": 2,
        }
        self.result = result or {
            "RunID": 12,
            "Snapshot_Date": date(2028, 5, 3),
            "SummaryJson": (
                '{"processor":"point_expiration","expiring_lot_count":3,'
                '"already_expired_count":1,"new_expiration_count":2,'
                '"expired_point_total":7500,"chapter_count":2}'
            ),
        }
        self.queries = []
        self.statements = []

    def query_rows(self, query, params=()):
        self.queries.append((query, tuple(params)))
        if query == expirations.PROCESS_POINT_EXPIRATIONS_SQL:
            return [self.preview]
        if query == expirations.POINT_EXPIRATION_RESULT_SQL:
            return [self.result]
        raise AssertionError("Unexpected query")

    def execute_statements(self, statements):
        self.statements.extend(list(statements))


class PointExpirationsTest(unittest.TestCase):
    def test_dry_run_returns_preview_without_writes(self):
        adapter = FakeExpirationAdapter()

        result = expirations.process_point_expirations(
            adapter,
            date(2028, 5, 3),
            dry_run=True,
        )

        self.assertTrue(result.dry_run)
        self.assertIsNone(result.run_id)
        self.assertEqual(result.expiring_lot_count, 3)
        self.assertEqual(result.new_expiration_count, 2)
        self.assertEqual(result.expired_point_total, 7500)
        self.assertEqual(adapter.statements, [])

    def test_write_executes_proc_and_reads_summary(self):
        adapter = FakeExpirationAdapter()

        result = expirations.process_point_expirations(
            adapter,
            date(2028, 5, 3),
            run_type="manual",
        )

        self.assertFalse(result.dry_run)
        self.assertEqual(result.run_id, 12)
        self.assertEqual(result.already_expired_count, 1)
        self.assertEqual(len(adapter.statements), 1)
        sql, params = adapter.statements[0]
        self.assertEqual(sql, expirations.PROCESS_POINT_EXPIRATIONS_SQL)
        self.assertEqual(params, (date(2028, 5, 3), "manual", False))
        self.assertEqual(adapter.queries[-1][1], ("manual", date(2028, 5, 3), expirations.SOURCE_TYPE))


if __name__ == "__main__":
    unittest.main()
