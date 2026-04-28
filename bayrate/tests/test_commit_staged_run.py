import unittest
from datetime import date

from bayrate.commit_staged_run import build_commit_plan, build_commit_statements, commit_staged_run, printable_commit_plan


class CommitAdapter:
    def __init__(self, *, status="ready_for_rating", staged_ratings=None, production_games=None, production_ratings=None, production_tournaments=None):
        self.status = status
        self.staged_ratings = list(self.default_staged_ratings() if staged_ratings is None else staged_ratings)
        self.production_games = list(production_games or [])
        self.production_ratings = list(production_ratings or [])
        self.production_tournaments = list(production_tournaments or [])
        self.statements = []

    def query_rows(self, query, params=()):
        if "FROM [ratings].[bayrate_runs]" in query:
            return [
                {
                    "RunID": 1,
                    "Status": self.status,
                    "Source_Report_Count": 1,
                    "Source_Report_Names": '["new-report.txt"]',
                    "Tournament_Count": 1,
                    "Game_Count": 1,
                    "Validation_Error_Count": 0,
                    "Ready_Tournament_Count": 1 if self.status == "ready_for_rating" else 0,
                    "Needs_Review_Count": 1 if self.status == "needs_review" else 0,
                    "Validation_Failed_Count": 1 if self.status == "validation_failed" else 0,
                }
            ]
        if "FROM [ratings].[bayrate_staged_tournaments]" in query:
            return [
                {
                    "RunID": 1,
                    "Source_Report_Ordinal": 1,
                    "Source_Report_Name": "new-report.txt",
                    "Source_Report_Sha256": "abc123",
                    "Tournament_Code": "new20260101",
                    "Original_Tournament_Code": None,
                    "Tournament_Code_Source": "generated",
                    "Tournament_Descr": "New Test Tournament",
                    "Normalized_Title": "new test tournament",
                    "Tournament_Date": date(2026, 1, 1),
                    "City": "Seattle",
                    "State_Code": "WA",
                    "Country_Code": "US",
                    "Rounds": 1,
                    "Total_Players": 2,
                    "Wallist": None,
                    "Elab_Date": date(2026, 1, 1),
                    "Validation_Status": self.status,
                    "Validation_Errors": "[]",
                    "Parser_Warnings": "[]",
                    "Duplicate_Candidate_Code": None,
                    "Duplicate_Score": None,
                    "Review_Reason": None,
                    "MetadataJson": "{}",
                }
            ]
        if "FROM [ratings].[bayrate_staged_games]" in query:
            return [
                {
                    "RunID": 1,
                    "Source_Report_Ordinal": 1,
                    "Source_Game_Ordinal": 1,
                    "Source_Report_Name": "new-report.txt",
                    "Game_ID": None,
                    "Tournament_Code": "new20260101",
                    "Game_Date": date(2026, 1, 1),
                    "Round": 1,
                    "Pin_Player_1": 1001,
                    "Color_1": "W",
                    "Rank_1": "1d",
                    "Pin_Player_2": 1002,
                    "Color_2": "B",
                    "Rank_2": "1d",
                    "Handicap": 0,
                    "Komi": 6.5,
                    "Result": "W",
                    "Sgf_Code": None,
                    "Online": 0,
                    "Exclude": 0,
                    "Rated": 1,
                    "Elab_Date": date(2026, 1, 1),
                    "Validation_Status": self.status,
                    "Validation_Errors": "[]",
                }
            ]
        if "FROM [ratings].[bayrate_staged_ratings]" in query:
            return self.staged_ratings
        if "MAX([Game_ID])" in query and "MAX([id])" in query:
            return [{"MaxGameID": 100, "MaxRatingID": 200}]
        if "FROM [ratings].[tournaments]" in query:
            return self.production_tournaments
        if "FROM [ratings].[games]" in query:
            return self.production_games
        if "FROM [ratings].[ratings]" in query:
            return self.production_ratings
        return []

    def execute_statements(self, statements):
        self.statements.extend(list(statements))

    @staticmethod
    def default_staged_ratings():
        return [
            {
                "RunID": 1,
                "Event_Ordinal": 1,
                "Player_Ordinal": 1,
                "Event_Source": "staged",
                "Event_Key": "2026-01-01|new20260101",
                "Tournament_Code": "new20260101",
                "Staged_Tournament_Code": "new20260101",
                "Replaced_Production_Code": None,
                "Source_Report_Ordinal": 1,
                "Pin_Player": 1001,
                "Rating": 1.2,
                "Sigma": 0.4,
                "Elab_Date": date(2026, 1, 1),
                "Rank_Seed": 1.0,
                "Seed_Before_Closing_Boundary": 1.0,
                "Prior_Rating": 1.1,
                "Prior_Sigma": 0.5,
                "Planned_Rating_Row_ID": None,
                "Production_Rating_Row_ID": None,
                "Rating_Delta": None,
                "Sigma_Delta": None,
                "MetadataJson": "{}",
            },
            {
                "RunID": 1,
                "Event_Ordinal": 1,
                "Player_Ordinal": 2,
                "Event_Source": "staged",
                "Event_Key": "2026-01-01|new20260101",
                "Tournament_Code": "new20260101",
                "Staged_Tournament_Code": "new20260101",
                "Replaced_Production_Code": None,
                "Source_Report_Ordinal": 1,
                "Pin_Player": 1002,
                "Rating": 0.9,
                "Sigma": 0.45,
                "Elab_Date": date(2026, 1, 1),
                "Rank_Seed": 1.0,
                "Seed_Before_Closing_Boundary": 1.0,
                "Prior_Rating": 1.0,
                "Prior_Sigma": 0.5,
                "Planned_Rating_Row_ID": None,
                "Production_Rating_Row_ID": None,
                "Rating_Delta": None,
                "Sigma_Delta": None,
                "MetadataJson": "{}",
            },
        ]


class CommitStagedRunTest(unittest.TestCase):
    def test_build_commit_plan_allocates_new_game_and_rating_ids(self) -> None:
        plan = build_commit_plan(CommitAdapter(), 1)

        self.assertEqual(plan["run_id"], 1)
        self.assertEqual(plan["staged_tournament_codes"], ["new20260101"])
        self.assertEqual(plan["affected_tournament_codes"], ["new20260101"])
        self.assertEqual(plan["game_insert_count"], 1)
        self.assertEqual(plan["rating_insert_count"], 2)
        self.assertEqual([row["planned_game_id"] for row in plan["planned_games"]], [101])
        self.assertEqual([row["planned_rating_row_id"] for row in plan["planned_ratings"]], [201, 202])

    def test_build_commit_plan_appends_rating_ids_when_replacing_existing_ratings(self) -> None:
        plan = build_commit_plan(
            CommitAdapter(
                production_ratings=[
                    {
                        "Tournament_Code": "new20260101",
                        "RatingRowCount": 2,
                        "FirstRatingRowID": 10,
                        "LastRatingRowID": 11,
                    }
                ]
            ),
            1,
        )

        self.assertEqual([row["planned_rating_row_id"] for row in plan["planned_ratings"]], [201, 202])

    def test_build_commit_plan_rejects_runs_without_replay_rows(self) -> None:
        with self.assertRaisesRegex(ValueError, "Run Replay before commit"):
            build_commit_plan(CommitAdapter(staged_ratings=[]), 1)

    def test_build_commit_plan_rejects_already_committed_rows(self) -> None:
        staged_ratings = CommitAdapter.default_staged_ratings()
        staged_ratings[0]["Planned_Rating_Row_ID"] = 201

        with self.assertRaisesRegex(ValueError, "appears to have been committed"):
            build_commit_plan(CommitAdapter(staged_ratings=staged_ratings), 1)

    def test_build_commit_plan_rejects_non_ready_runs(self) -> None:
        with self.assertRaisesRegex(ValueError, "only ready_for_rating"):
            build_commit_plan(CommitAdapter(status="needs_review"), 1)

    def test_build_commit_statements_include_production_and_staging_updates(self) -> None:
        plan = build_commit_plan(CommitAdapter(), 1)
        statements = build_commit_statements(plan)
        sql_text = "\n".join(statement[0] for statement in statements)

        self.assertIn("DELETE FROM [ratings].[ratings]", sql_text)
        self.assertIn("DELETE FROM [ratings].[games]", sql_text)
        self.assertIn("INSERT INTO [ratings].[tournaments]", sql_text)
        self.assertIn("INSERT INTO [ratings].[games]", sql_text)
        self.assertIn("INSERT INTO [ratings].[ratings]", sql_text)
        self.assertIn("UPDATE [ratings].[bayrate_staged_games]", sql_text)
        self.assertIn("UPDATE [ratings].[bayrate_staged_ratings]", sql_text)
        self.assertIn("THROW 51021", sql_text)
        self.assertIn("UPDATE [ratings].[bayrate_runs]", sql_text)

    def test_printable_commit_plan_includes_stable_plan_hash(self) -> None:
        plan = build_commit_plan(CommitAdapter(), 1)
        preview = printable_commit_plan(plan)

        plan["executed"] = True
        executed = printable_commit_plan(plan)

        self.assertEqual(len(preview["plan_hash"]), 64)
        self.assertEqual(preview["plan_hash"], executed["plan_hash"])

    def test_game_insert_uses_production_integer_komi_convention(self) -> None:
        plan = build_commit_plan(CommitAdapter(), 1)
        statements = build_commit_statements(plan)
        game_insert = next(statement for statement in statements if "INSERT INTO [ratings].[games]" in statement[0])

        self.assertEqual(game_insert[1][11], 6)

    def test_commit_staged_run_requires_confirmation(self) -> None:
        with self.assertRaisesRegex(ValueError, "confirm_production_commit"):
            commit_staged_run(CommitAdapter(), 1)

    def test_commit_staged_run_rejects_stale_preview_hash(self) -> None:
        with self.assertRaisesRegex(ValueError, "changed since preview"):
            commit_staged_run(
                CommitAdapter(),
                1,
                confirm_production_commit=True,
                expected_plan_hash="not-the-current-plan",
            )

    def test_commit_staged_run_requires_sgf_acknowledgement_for_sgf_replacement(self) -> None:
        production_games = [
            {
                "Game_ID": 700,
                "Tournament_Code": "new20260101",
                "Game_Date": date(2026, 1, 1),
                "Round": 1,
                "Pin_Player_1": 1001,
                "Pin_Player_2": 1002,
                "Sgf_Code": "linked-game",
            }
        ]

        with self.assertRaisesRegex(ValueError, "SGF-linked"):
            commit_staged_run(
                CommitAdapter(production_games=production_games),
                1,
                confirm_production_commit=True,
            )

    def test_commit_staged_run_executes_generated_statements(self) -> None:
        adapter = CommitAdapter()

        plan = commit_staged_run(adapter, 1, confirm_production_commit=True)

        self.assertTrue(plan["executed"])
        self.assertGreater(len(adapter.statements), 0)


if __name__ == "__main__":
    unittest.main()
