import json
import unittest
from datetime import date

from rewards import legacy_membership_awards


class FakeLegacyMembershipAwardAdapter:
    def __init__(self, *, preview=None, result=None):
        self.preview = preview or {
            "RunID": None,
            "DryRun": True,
            "SourceAsOfDate": date(2026, 2, 8),
            "LedgerStartDate": date(2026, 5, 2),
            "ValuationSnapshotDate": date(2026, 5, 2),
            "InputRowCount": 3,
            "ChapterCount": 2,
            "MemberCountTotal": 8,
            "EligibleAwardCount": 3,
            "AlreadyAwardedCount": 1,
            "NewAwardCount": 2,
            "PointTotal": 45000,
            "MissingChapterSnapshotCount": 0,
            "ChapterNotCurrentCount": 0,
            "UnsupportedMemberTypeCount": 0,
            "IneligibleMemberTypeCount": 0,
        }
        self.result = result or {
            "RunID": 12,
            "Snapshot_Date": date(2026, 5, 2),
            "SummaryJson": (
                '{"processor":"legacy_membership_gap_award","legacy_gap":true,'
                '"source_balance_as_of_date":"2026-02-08",'
                '"ledger_start_date":"2026-05-02",'
                '"valuation_snapshot_date":"2026-05-02",'
                '"input_row_count":3,"chapter_count":2,'
                '"member_count_total":8,"eligible_award_count":3,'
                '"already_awarded_count":1,"new_award_count":2,'
                '"point_total":45000,"missing_chapter_snapshot_count":0,'
                '"chapter_not_current_count":0,"unsupported_member_type_count":0,'
                '"ineligible_member_type_count":0}'
            ),
        }
        self.queries = []
        self.statements = []

    def query_rows(self, query, params=()):
        self.queries.append((query, tuple(params)))
        if query == legacy_membership_awards.IMPORT_LEGACY_MEMBERSHIP_AWARDS_SQL:
            return [self.preview]
        if query == legacy_membership_awards.LEGACY_MEMBERSHIP_AWARD_RESULT_SQL:
            return [self.result]
        raise AssertionError("Unexpected query")

    def execute_statements(self, statements):
        self.statements.extend(list(statements))


def _rows():
    return [
        legacy_membership_awards.LegacyMembershipAwardRow(
            source_row_number=1,
            chapter_id=667,
            chapter_code="SFGC",
            chapter_name="San Francisco Go Club",
            member_type="Adult Full",
            event_type="renewal",
            count=5,
        )
    ]


class LegacyMembershipAwardsTest(unittest.TestCase):
    def test_record_parser_normalizes_csv_values(self):
        row = legacy_membership_awards.legacy_membership_award_from_record(
            {
                "ChapterID": "667",
                "ChapterCode": " sfgc ",
                "ChapterName": "San Francisco Go Club",
                "MemberType": "Adult Full",
                "EventType": "New_Membership",
                "Count": "3",
            },
            7,
        )

        self.assertEqual(row.source_row_number, 7)
        self.assertEqual(row.chapter_id, 667)
        self.assertEqual(row.chapter_code, "SFGC")
        self.assertEqual(row.event_type, "new_membership")
        self.assertEqual(row.count, 3)

    def test_dry_run_returns_preview_without_writes(self):
        adapter = FakeLegacyMembershipAwardAdapter()

        result = legacy_membership_awards.import_legacy_membership_awards(
            adapter,
            _rows(),
            dry_run=True,
        )

        self.assertTrue(result.dry_run)
        self.assertIsNone(result.run_id)
        self.assertEqual(result.member_count_total, 8)
        self.assertEqual(result.new_award_count, 2)
        self.assertEqual(result.point_total, 45000)
        self.assertEqual(adapter.statements, [])

    def test_write_executes_proc_and_reads_summary(self):
        adapter = FakeLegacyMembershipAwardAdapter()

        result = legacy_membership_awards.import_legacy_membership_awards(
            adapter,
            _rows(),
            run_type="import",
        )

        self.assertFalse(result.dry_run)
        self.assertEqual(result.run_id, 12)
        self.assertEqual(result.valuation_snapshot_date, date(2026, 5, 2))
        self.assertEqual(len(adapter.statements), 1)
        sql, params = adapter.statements[0]
        self.assertEqual(sql, legacy_membership_awards.IMPORT_LEGACY_MEMBERSHIP_AWARDS_SQL)
        self.assertEqual(params[1:], (
            False,
            "import",
            legacy_membership_awards.DEFAULT_SOURCE_AS_OF_DATE,
            legacy_membership_awards.DEFAULT_LEDGER_START_DATE,
            legacy_membership_awards.DEFAULT_VALUATION_SNAPSHOT_DATE,
            legacy_membership_awards.SOURCE_TYPE,
            legacy_membership_awards.RULE_VERSION,
        ))
        self.assertEqual(json.loads(params[0])[0]["chapter_id"], 667)
        self.assertEqual(
            adapter.queries[-1][1],
            ("import", date(2026, 5, 2), legacy_membership_awards.SOURCE_TYPE),
        )

    def test_rejects_empty_import(self):
        with self.assertRaisesRegex(ValueError, "At least one"):
            legacy_membership_awards.import_legacy_membership_awards(
                FakeLegacyMembershipAwardAdapter(),
                [],
            )


if __name__ == "__main__":
    unittest.main()
