import unittest
from datetime import date

from rewards import membership_awards


class FakeMembershipAwardAdapter:
    def __init__(self, *, preview=None, result=None):
        self.preview = preview or {
            "RunID": None,
            "AsOfDate": date(2026, 5, 2),
            "DryRun": True,
            "PendingEventCount": 5,
            "EligibleEventCount": 2,
            "AlreadyAwardedCount": 1,
            "NewAwardCount": 1,
            "PointTotal": 10000,
            "ExpiringNoChapterCount": 1,
            "WaitingForChapterCount": 2,
            "MissingSnapshotCoverageCount": 0,
            "IneligibleCount": 0,
        }
        self.result = result or {
            "RunID": 9,
            "Snapshot_Date": date(2026, 5, 2),
            "SummaryJson": (
                '{"processor":"membership_event","pending_event_count":5,'
                '"eligible_event_count":2,"already_awarded_count":1,'
                '"new_award_count":1,"point_total":10000,'
                '"expired_no_chapter_count":1,"waiting_for_chapter_count":2,'
                '"missing_snapshot_coverage_count":0,"ineligible_count":0}'
            ),
        }
        self.queries = []
        self.statements = []

    def query_rows(self, query, params=()):
        self.queries.append((query, tuple(params)))
        if query == membership_awards.PROCESS_MEMBERSHIP_AWARDS_SQL:
            return [self.preview]
        if query == membership_awards.MEMBERSHIP_AWARD_RESULT_SQL:
            return [self.result]
        raise AssertionError("Unexpected query")

    def execute_statements(self, statements):
        self.statements.extend(list(statements))


class MembershipAwardsTest(unittest.TestCase):
    def test_dry_run_returns_preview_without_writes(self):
        adapter = FakeMembershipAwardAdapter()

        result = membership_awards.process_membership_awards(
            adapter,
            date(2026, 5, 2),
            dry_run=True,
        )

        self.assertTrue(result.dry_run)
        self.assertIsNone(result.run_id)
        self.assertEqual(result.pending_event_count, 5)
        self.assertEqual(result.new_award_count, 1)
        self.assertEqual(result.point_total, 10000)
        self.assertEqual(adapter.statements, [])

    def test_write_executes_proc_and_reads_summary(self):
        adapter = FakeMembershipAwardAdapter()

        result = membership_awards.process_membership_awards(
            adapter,
            date(2026, 5, 2),
            run_type="manual",
        )

        self.assertFalse(result.dry_run)
        self.assertEqual(result.run_id, 9)
        self.assertEqual(result.already_awarded_count, 1)
        self.assertEqual(len(adapter.statements), 1)
        sql, params = adapter.statements[0]
        self.assertEqual(sql, membership_awards.PROCESS_MEMBERSHIP_AWARDS_SQL)
        self.assertEqual(params, (date(2026, 5, 2), "manual", False))
        self.assertEqual(adapter.queries[-1][1], ("manual", date(2026, 5, 2), membership_awards.SOURCE_TYPE))


if __name__ == "__main__":
    unittest.main()
