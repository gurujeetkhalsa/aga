# Copyright 2026, American Go Association, All rights reserved

import json
import sys
import unittest
from datetime import date
from pathlib import Path


APP_ROOT = Path(__file__).resolve().parents[1]
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

import chapter_rewards_admin_support as rewards  # noqa: E402


class ChapterTransferSupportTests(unittest.TestCase):
    """Verify chapter-transfer request validation and response shaping."""

    def setUp(self):
        self.authorization = {
            "principal_name": "rewards-admin@example.org",
            "principal_id": "principal-123",
        }

    def test_valid_transfer_request_builds_procedure_params(self):
        body = {
            "from_chapter_id": 101,
            "from_chapter_code": "SRC",
            "to_chapter_id": 202,
            "to_chapter_code": "DEST",
            "transfer_date": "2026-09-01",
            "points": "12,500",
            "description": "Support a regional youth event",
            "external_transfer_id": "board-2026-09-01-01",
            "notes": "Approved by the chapter board.",
        }

        request, error = rewards._rewards_chapter_transfer_request_from_body(
            body,
            self.authorization,
            generate_transfer_id=False,
        )

        self.assertIsNone(error)
        self.assertEqual(request["transfer_date"], date(2026, 9, 1))
        self.assertEqual(request["points"], 12_500)
        self.assertEqual(request["from_chapter_id"], 101)
        self.assertEqual(request["to_chapter_id"], 202)
        source_payload = json.loads(request["source_payload_json"])
        self.assertEqual(source_payload["entry_source"], "rewards_admin_transfer_form")
        self.assertEqual(source_payload["entered_by_principal_id"], "principal-123")

        params = rewards._rewards_chapter_transfer_params(request, dry_run=True)
        self.assertEqual(len(params), 13)
        self.assertEqual(params[:7], (101, "SRC", 202, "DEST", date(2026, 9, 1), 12_500, "Support a regional youth event"))
        self.assertTrue(params[10])
        self.assertEqual(params[-2:], ("rewards-admin@example.org", "principal-123"))

    def test_post_request_generates_id(self):
        request, error = rewards._rewards_chapter_transfer_request_from_body(
            {
                "from_chapter_code": "SRC",
                "to_chapter_code": "DEST",
                "points": 500,
                "description": "Shared equipment",
            },
            self.authorization,
            generate_transfer_id=True,
        )

        self.assertIsNone(error)
        self.assertRegex(request["external_transfer_id"], r"^transfer-\d{8}-[0-9a-f]{12}$")

    def test_same_chapter_is_rejected(self):
        request, error = rewards._rewards_chapter_transfer_request_from_body(
            {
                "from_chapter_id": 101,
                "from_chapter_code": "SAME",
                "to_chapter_id": 101,
                "to_chapter_code": "SAME",
                "points": 500,
                "description": "Invalid",
            },
            self.authorization,
            generate_transfer_id=False,
        )

        self.assertIsNone(request)
        self.assertIsNotNone(error)
        payload = json.loads(error.get_body())
        self.assertIn("must be different", payload["error"])

    def test_transfer_payload_includes_both_balances_and_lot_counts(self):
        payload = rewards._rewards_chapter_transfer_payload(
            {
                "RunID": 42,
                "TransferID": 9,
                "External_Transfer_ID": "transfer-9",
                "From_ChapterID": 101,
                "From_Chapter_Code": "SRC",
                "To_ChapterID": 202,
                "To_Chapter_Code": "DEST",
                "Transfer_Date": date(2026, 9, 1),
                "Points": 12_500,
                "From_Available_Points": 50_000,
                "From_Available_After_Points": 37_500,
                "To_Available_Points": 1_000,
                "To_Available_After_Points": 13_500,
                "Transferred_Lot_Count": 3,
                "In_Transaction_Count": 3,
            }
        )

        self.assertEqual(payload["from_chapter_code"], "SRC")
        self.assertEqual(payload["to_chapter_code"], "DEST")
        self.assertEqual(payload["from_available_after_points"], 37_500)
        self.assertEqual(payload["to_available_after_points"], 13_500)
        self.assertEqual(payload["transferred_lot_count"], 3)
        self.assertEqual(payload["in_transaction_count"], 3)

    def test_transfer_sql_preserves_lot_dates_and_balances_both_ledger_sides(self):
        sql_path = APP_ROOT.parent / "rewards" / "sql" / "transfer_processing.sql"
        sql = sql_path.read_text(encoding="utf-8")

        self.assertEqual(rewards.REWARDS_CHAPTER_TRANSFER_SQL.count("?"), 13)
        self.assertIn("CREATE OR ALTER PROCEDURE [rewards].[sp_post_chapter_transfer]", sql)
        self.assertIn("N'transfer_out'", sql)
        self.assertIn("N'transfer_in'", sql)
        self.assertIn("INSERT INTO [rewards].[lot_allocations]", sql)
        self.assertIn("lot.[Remaining_Points] - allocation.[Points_Allocated]", sql)
        self.assertIn("allocation.[Earned_Date]", sql)
        self.assertIn("allocation.[Expires_On]", sql)
        self.assertIn("BEGIN TRANSACTION", sql)
        self.assertIn("ROLLBACK TRANSACTION", sql)


if __name__ == "__main__":
    unittest.main()
