import argparse
import unittest
from datetime import date

from rewards import snapshot_generator as snapshots


class FakeAdapter:
    def __init__(self, *, existing=None, preview=None, result=None):
        self.existing = existing or {
            "ExistingMemberSnapshotCount": 0,
            "ExistingChapterSnapshotCount": 0,
        }
        self.preview = preview or {
            "MemberSnapshotCount": 10,
            "ActiveMemberCount": 8,
            "TournamentPassCount": 1,
            "ChapterSnapshotCount": 3,
            "CurrentChapterCount": 3,
            "Multiplier1ChapterCount": 1,
            "Multiplier2ChapterCount": 1,
            "Multiplier3ChapterCount": 1,
        }
        self.result = result or dict(self.preview, RunID=42, SummaryJson="{}")
        self.queries = []
        self.statements = []

    def query_rows(self, query, params=()):
        self.queries.append((query, tuple(params)))
        if query == snapshots.SNAPSHOT_EXISTING_COUNTS_SQL:
            return [self.existing]
        if query == snapshots.SNAPSHOT_PREVIEW_SQL:
            return [self.preview]
        if query == snapshots.SNAPSHOT_RESULT_SQL:
            return [self.result]
        raise AssertionError("Unexpected query")

    def execute_statements(self, statements):
        self.statements.extend(list(statements))


class SnapshotGeneratorTest(unittest.TestCase):
    def test_dry_run_returns_preview_without_writes(self):
        adapter = FakeAdapter()

        result = snapshots.create_daily_snapshot(adapter, date(2026, 5, 2), dry_run=True)

        self.assertTrue(result.dry_run)
        self.assertIsNone(result.run_id)
        self.assertEqual(result.member_snapshot_count, 10)
        self.assertEqual(result.chapter_snapshot_count, 3)
        self.assertEqual(adapter.statements, [])

    def test_existing_snapshot_requires_replace(self):
        adapter = FakeAdapter(
            existing={
                "ExistingMemberSnapshotCount": 10,
                "ExistingChapterSnapshotCount": 3,
            }
        )

        with self.assertRaisesRegex(ValueError, "already exist"):
            snapshots.create_daily_snapshot(adapter, date(2026, 5, 2))

        self.assertEqual(adapter.statements, [])

    def test_write_passes_replace_flag_and_returns_result(self):
        adapter = FakeAdapter()

        result = snapshots.create_daily_snapshot(adapter, date(2026, 5, 2), run_type="manual", replace=True)

        self.assertFalse(result.dry_run)
        self.assertEqual(result.run_id, 42)
        self.assertEqual(len(adapter.statements), 1)
        sql, params = adapter.statements[0]
        self.assertEqual(sql, snapshots.CREATE_SNAPSHOT_SQL)
        self.assertEqual(params, (date(2026, 5, 2), "manual", snapshots.MAX_MEMBER_AGAID, True))

    def test_parse_snapshot_date_rejects_bad_format(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            snapshots.parse_snapshot_date("05/02/2026")


if __name__ == "__main__":
    unittest.main()
