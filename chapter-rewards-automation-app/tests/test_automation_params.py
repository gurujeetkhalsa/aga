import importlib.util
import os
from datetime import date, datetime
from pathlib import Path
import sys
import unittest


APP_DIR = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

spec = importlib.util.spec_from_file_location("chapter_rewards_automation_app", APP_DIR / "function_app.py")
automation = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(automation)


class ChapterRewardsAutomationParamsTest(unittest.TestCase):
    """Represent chapter rewards automation params test."""
    def test_snapshot_params_do_not_replace_existing_snapshots(self):
        """Verify that snapshot params do not replace existing snapshots."""
        params = automation._rewards_snapshot_params(date(2026, 5, 6))

        self.assertEqual(
            params,
            {
                "SnapshotDate": date(2026, 5, 6),
                "RunType": "daily",
                "ReplaceExisting": 0,
            },
        )

    def test_membership_awards_params_write_daily_run(self):
        """Verify that membership awards params write daily run."""
        params = automation._rewards_membership_awards_params(date(2026, 5, 6))

        self.assertEqual(
            params,
            {
                "AsOfDate": date(2026, 5, 6),
                "RunType": "daily",
                "DryRun": 0,
            },
        )

    def test_rated_game_awards_params_scan_from_ledger_start(self):
        """Verify that rated game awards params scan from ledger start."""
        previous = os.environ.get("REWARDS_LEDGER_START_DATE")
        os.environ["REWARDS_LEDGER_START_DATE"] = "2026-05-02"
        try:
            params = automation._rewards_rated_game_awards_params(date(2026, 5, 6))
        finally:
            if previous is None:
                os.environ.pop("REWARDS_LEDGER_START_DATE", None)
            else:
                os.environ["REWARDS_LEDGER_START_DATE"] = previous

        self.assertEqual(params["GameDateFrom"], date(2026, 5, 2))
        self.assertEqual(params["GameDateTo"], date(2026, 5, 6))
        self.assertEqual(params["RunType"], "daily")
        self.assertEqual(params["DryRun"], 0)

    def test_tournament_awards_params_scan_through_daily_date(self):
        """Verify that tournament awards params scan through daily date."""
        params = automation._rewards_tournament_awards_params(date(2026, 5, 6))

        self.assertEqual(
            params,
            {
                "TournamentDateFrom": None,
                "TournamentDateTo": date(2026, 5, 6),
                "RunType": "daily",
                "DryRun": 0,
            },
        )

    def test_point_expirations_params_write_daily_run(self):
        """Verify that point expirations params write daily run."""
        params = automation._rewards_point_expirations_params(date(2026, 5, 6))

        self.assertEqual(
            params,
            {
                "AsOfDate": date(2026, 5, 6),
                "RunType": "daily",
                "DryRun": 0,
            },
        )

    def test_stored_procedure_call_orders_params(self):
        """Verify that stored procedure call orders params."""
        sql, values = automation._stored_procedure_call(
            "rewards.sp_process_membership_awards",
            {
                "AsOfDate": date(2026, 5, 6),
                "RunType": "daily",
                "DryRun": 0,
            },
        )

        self.assertEqual(
            sql,
            "EXEC rewards.sp_process_membership_awards @AsOfDate = ?, @RunType = ?, @DryRun = ?",
        )
        self.assertEqual(values, [date(2026, 5, 6), "daily", 0])

    def test_pending_chapter_renewals_email_body_lists_debited_chapters(self):
        """Verify that pending chapter renewals email body lists debited chapters."""
        body = automation._pending_chapter_renewals_email_body(
            [
                {
                    "ChapterID": 13529,
                    "Chapter_Code": "AUST",
                    "Chapter_Name": "Austin Go Club",
                    "Notice_Date": datetime(2026, 5, 5, 12, 0, 0),
                    "Pending_Days": 3,
                    "Points_Required": 35000,
                    "TransactionID": 42,
                }
            ],
            date(2026, 5, 8),
        )

        self.assertIn("Pending debited chapters: 1", body)
        self.assertIn("AUST Austin Go Club (13529)", body)
        self.assertIn("txn 42", body)

    def test_pending_chapter_renewals_email_body_handles_empty_list(self):
        """Verify that pending chapter renewals email body handles empty list."""
        body = automation._pending_chapter_renewals_email_body([], date(2026, 5, 8))

        self.assertIn("Pending debited chapters: 0", body)
        self.assertIn("No chapters are currently debited", body)


if __name__ == "__main__":
    unittest.main()
