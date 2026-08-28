# Copyright 2026, American Go Association, All rights reserved

import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
PROCESSING_SQL = REPO_ROOT / "rewards" / "sql" / "bayrate_reconciliation_processing.sql"
SCHEMA_SQL = REPO_ROOT / "rewards" / "sql" / "chapter_rewards_schema.sql"
ADMIN_SUPPORT = REPO_ROOT / "chapter-rewards-admin-app" / "chapter_rewards_admin_support.py"
ADMIN_APP = REPO_ROOT / "chapter-rewards-admin-app" / "function_app.py"
ADMIN_PAGE = REPO_ROOT / "chapter-rewards-admin-app" / "chapter_rewards_admin.html"
DISPLAY_SUPPORT = REPO_ROOT / "chapter-rewards-display-app" / "chapter_rewards_support.py"


class BayrateReconciliationTest(unittest.TestCase):
    """Verify the durable, idempotent Bayrate-to-rewards adjustment workflow."""

    @classmethod
    def setUpClass(cls):
        cls.processing_sql = PROCESSING_SQL.read_text(encoding="utf-8")
        cls.schema_sql = SCHEMA_SQL.read_text(encoding="utf-8")

    def test_schema_records_one_application_per_bayrate_run(self):
        self.assertIn("[rewards].[bayrate_reconciliation_applications]", self.schema_sql)
        self.assertIn("PRIMARY KEY CLUSTERED ([Bayrate_RunID])", self.schema_sql)
        self.assertIn("UNIQUE ([Reward_RunID])", self.schema_sql)
        self.assertIn("FOREIGN KEY ([Bayrate_RunID]) REFERENCES [ratings].[bayrate_runs]", self.schema_sql)

    def test_procedure_uses_persisted_old_and_new_chapter_totals(self):
        self.assertIn("OPENJSON(@ReconciliationJson, N'$.chapters')", self.processing_sql)
        self.assertIn("[Point_Difference] <> [New_Total_Points] - [Old_Total_Points]", self.processing_sql)
        self.assertIn("$.old_played_game_points", self.processing_sql)
        self.assertIn("$.new_total_games_points", self.processing_sql)
        self.assertIn("$.new_state_championship_points", self.processing_sql)

    def test_procedure_is_confirmable_and_idempotent(self):
        self.assertIn("@DryRun bit = 1", self.processing_sql)
        self.assertIn("WITH (UPDLOCK, HOLDLOCK)", self.processing_sql)
        self.assertIn("IF @ExistingStatus IS NOT NULL", self.processing_sql)
        self.assertIn("CAST(1 AS bit) AS [Already_Applied]", self.processing_sql)
        self.assertIn("CASE WHEN @TransactionCount = 0 THEN N'no_changes' ELSE N'applied' END", self.processing_sql)

    def test_positive_and_negative_changes_preserve_lot_accounting(self):
        self.assertIn("INSERT INTO [rewards].[point_lots]", self.processing_sql)
        self.assertIn("WHERE inserted.[Points_Delta] > 0", self.processing_sql)
        self.assertIn("INSERT INTO [rewards].[lot_allocations]", self.processing_sql)
        self.assertIn("WHERE inserted.[Points_Delta] < 0", self.processing_sql)
        self.assertIn("ORDER BY available.[Expires_On], available.[Earned_Date], available.[LotID]", self.processing_sql)
        self.assertIn("[Remaining_Points] = lots.[Remaining_Points] - usage.[Points_Allocated]", self.processing_sql)

    def test_negative_change_blocks_if_unexpired_points_are_insufficient(self):
        self.assertIn("lots.[Expires_On] >= @Today", self.processing_sql)
        self.assertIn("@InsufficientChapterCount", self.processing_sql)
        self.assertIn("THROW 52905", self.processing_sql)

    def test_admin_api_requires_explicit_confirmation(self):
        app = ADMIN_APP.read_text(encoding="utf-8")
        self.assertIn('route="chapter-rewards/admin/reconciliation-preview"', app)
        self.assertIn('route="chapter-rewards/admin/reconciliation-apply"', app)
        self.assertIn('body.get("confirm_reconciliation") is not True', app)

    def test_admin_page_shows_old_new_and_component_changes(self):
        page = ADMIN_PAGE.read_text(encoding="utf-8")
        self.assertIn("Bayrate Reconciliations", page)
        self.assertIn("old_played_game_points", page)
        self.assertIn("new_total_games_points", page)
        self.assertIn("new_state_championship_points", page)
        self.assertIn("Apply Reconciliation", page)

    def test_admin_and_public_history_label_reconciliation_transactions(self):
        for support_path in (ADMIN_SUPPORT, DISPLAY_SUPPORT):
            support = support_path.read_text(encoding="utf-8")
            self.assertIn("bayrate_rerun_reconciliation", support)
            self.assertIn("Bayrate rerun reconciliation", support)


if __name__ == "__main__":
    unittest.main()
