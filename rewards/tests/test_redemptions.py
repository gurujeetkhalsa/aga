import unittest
from datetime import date

from rewards import redemptions


class FakeRedemptionAdapter:
    def __init__(self, *, preview=None, result=None):
        self.preview = preview or {
            "RunID": None,
            "DryRun": True,
            "SourceAsOfDate": date(2026, 2, 8),
            "LedgerStartDate": date(2026, 5, 2),
            "InputRowCount": 2,
            "ExistingRequestCount": 0,
            "AlreadyPostedCount": 0,
            "NewPostCount": 2,
            "MissingOpeningLotCount": 0,
            "InsufficientBalanceCount": 0,
            "InputPointTotal": 70000,
            "NewPointTotal": 70000,
            "ChapterCount": 2,
            "DuesCreditCount": 2,
            "DuesCreditPoints": 70000,
            "ReimbursementCount": 0,
            "ReimbursementPoints": 0,
        }
        self.result = result or {
            "RunID": 14,
            "Snapshot_Date": date(2026, 5, 2),
            "SummaryJson": (
                '{"processor":"redemption","legacy_gap":true,'
                '"source_balance_as_of_date":"2026-02-08",'
                '"ledger_start_date":"2026-05-02",'
                '"input_row_count":2,"existing_request_count":0,'
                '"already_posted_count":0,"new_post_count":2,'
                '"input_point_total":70000,"new_point_total":70000,'
                '"chapter_count":2,"dues_credit_count":2,'
                '"dues_credit_points":70000,"reimbursement_count":0,'
                '"reimbursement_points":0}'
            ),
        }
        self.queries = []
        self.statements = []

    def query_rows(self, query, params=()):
        self.queries.append((query, tuple(params)))
        if query in {
            redemptions.IMPORT_LEGACY_REDEMPTIONS_SQL,
            redemptions.IMPORT_LEGACY_REDEMPTIONS_WITH_ADJUSTMENTS_SQL,
        }:
            return [self.preview]
        if query == redemptions.LEGACY_REDEMPTION_RESULT_SQL:
            return [self.result]
        raise AssertionError("Unexpected query")

    def execute_statements(self, statements):
        self.statements.extend(list(statements))


def sample_rows():
    return [
        redemptions.LegacyRedemptionRow(
            source_row_number=1,
            request_id="546",
            chapter_id=13529,
            chapter_name="Providence Go Club",
            request_date=date(2026, 2, 13),
            points=35000,
            notes="Chapter Renewal",
            redemption_category="chapter_renewal",
            payment_mode="dues_credit",
            description="Chapter Renewal",
            receipt_ref="546",
        ),
        redemptions.LegacyRedemptionRow(
            source_row_number=2,
            request_id="548",
            chapter_id=25495,
            chapter_name="Ghost City Go",
            request_date=date(2026, 2, 16),
            points=35000,
            notes="Chapter Renewal",
            redemption_category="chapter_renewal",
            payment_mode="dues_credit",
            description="Chapter Renewal",
            receipt_ref="548",
        ),
    ]


class RedemptionsTest(unittest.TestCase):
    def test_legacy_redemption_from_record_maps_chapter_renewal(self):
        row = redemptions.legacy_redemption_from_record(
            {
                "request_id": 546,
                "chapter_id": 13529,
                "chapter_name": "Providence Go Club",
                "request_date": 46066,
                "points": 35000,
                "notes": "Chapter Renewal",
            },
            1,
        )

        self.assertEqual(row.request_id, "546")
        self.assertEqual(row.request_date, date(2026, 2, 13))
        self.assertEqual(row.redemption_category, "chapter_renewal")
        self.assertEqual(row.payment_mode, "dues_credit")

    def test_legacy_redemption_from_record_maps_go_promotion(self):
        row = redemptions.legacy_redemption_from_record(
            {
                "request_id": 547,
                "chapter_id": 19235,
                "chapter_name": "Middle TN Go Club",
                "request_date": "2026-02-09",
                "points": 533000,
                "notes": "Go Promotion",
            },
            1,
        )

        self.assertEqual(row.redemption_category, "go_promotion")
        self.assertEqual(row.payment_mode, "reimbursement")

    def test_dry_run_returns_preview_without_writes(self):
        adapter = FakeRedemptionAdapter()

        result = redemptions.import_legacy_redemptions(
            adapter,
            sample_rows(),
            dry_run=True,
        )

        self.assertTrue(result.dry_run)
        self.assertEqual(result.new_post_count, 2)
        self.assertEqual(result.new_point_total, 70000)
        self.assertEqual(adapter.statements, [])

    def test_write_executes_proc_and_reads_summary(self):
        adapter = FakeRedemptionAdapter()

        result = redemptions.import_legacy_redemptions(
            adapter,
            sample_rows(),
            posted_by_principal_name="operator@example.org",
        )

        self.assertFalse(result.dry_run)
        self.assertEqual(result.run_id, 14)
        self.assertEqual(result.new_post_count, 2)
        self.assertEqual(len(adapter.statements), 1)
        sql, params = adapter.statements[0]
        self.assertEqual(sql, redemptions.IMPORT_LEGACY_REDEMPTIONS_SQL)
        self.assertEqual(params[1], False)
        self.assertEqual(params[2], "import")
        self.assertEqual(params[5], "operator@example.org")
        self.assertEqual(adapter.queries[-1][1], ("import", date(2026, 5, 2), redemptions.SOURCE_TYPE))

    def test_allow_shortfall_adjustments_uses_adjustment_proc(self):
        adapter = FakeRedemptionAdapter()

        redemptions.import_legacy_redemptions(
            adapter,
            sample_rows(),
            dry_run=True,
            allow_dues_credit_shortfall_adjustment=True,
        )

        sql, params = adapter.queries[0]
        self.assertEqual(sql, redemptions.IMPORT_LEGACY_REDEMPTIONS_WITH_ADJUSTMENTS_SQL)
        self.assertEqual(params[-1], True)


if __name__ == "__main__":
    unittest.main()
