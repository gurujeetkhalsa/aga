# SPDX-FileCopyrightText: 2010 Philip Waldron
# SPDX-FileCopyrightText: 2026 American Go Association
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import unittest
from datetime import date
from unittest.mock import patch

from bayrate.commit_staged_run import build_commit_plan, build_commit_statements, commit_staged_run, printable_commit_plan
from bayrate.reward_reconciliation import tournament_host_points


class CommitAdapter:
    """Represent commit adapter."""
    def __init__(
        self,
        *,
        status="ready_for_rating",
        staged_ratings=None,
        staged_games=None,
        production_games=None,
        production_ratings=None,
        production_tournaments=None,
        previous_committed_runs=None,
        reward_snapshots=None,
    ):
        """Initialize the commit adapter instance."""
        self.status = status
        self.staged_ratings = list(self.default_staged_ratings() if staged_ratings is None else staged_ratings)
        self.staged_games = list(self.default_staged_games() if staged_games is None else staged_games)
        self.production_games = list(production_games or [])
        self.production_ratings = list(production_ratings or [])
        self.production_tournaments = list(production_tournaments or [])
        self.previous_committed_runs = list(previous_committed_runs or [])
        self.reward_snapshots = list(reward_snapshots or [])
        self.statements = []

    def query_rows(self, query, params=()):
        """Query rows."""
        if "FROM [ratings].[bayrate_runs] AS r" in query:
            return [{"RunID": run_id} for run_id in self.previous_committed_runs]
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
                    "Host_ChapterID": 10,
                    "Host_ChapterCode": "SEAG",
                    "Host_ChapterName": "Seattle Go Center",
                    "Reward_Event_Key": "new20260101",
                    "Reward_Event_Name": "New Test Tournament",
                    "Reward_Is_State_Championship": 1,
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
            return self.staged_games
        if "FROM [ratings].[bayrate_staged_ratings]" in query:
            return self.staged_ratings
        if "MAX([Game_ID])" in query and "MAX([id])" in query:
            return [{"MaxGameID": max([100, *[int(row["Game_ID"]) for row in self.production_games]]), "MaxRatingID": 200}]
        if "FROM [rewards].[member_daily_snapshot]" in query:
            return self.reward_snapshots
        if "FROM [ratings].[tournaments]" in query:
            return self.production_tournaments
        if "FROM [ratings].[games]" in query:
            return self.production_games
        if "FROM [ratings].[ratings]" in query:
            return self.production_ratings
        return []

    def execute_statements(self, statements):
        """Execute statements."""
        self.statements.extend(list(statements))

    @staticmethod
    def default_staged_games():
        """Return one valid staged game."""
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
                "Validation_Status": "ready_for_rating",
                "Validation_Errors": "[]",
            }
        ]

    @staticmethod
    def default_staged_ratings():
        """Execute the default staged ratings routine."""
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
    """Represent commit staged run test."""
    def test_build_commit_plan_allocates_new_game_and_rating_ids(self) -> None:
        """Verify that build commit plan allocates new game and rating ids."""
        with patch("bayrate.commit_staged_run.build_reward_reconciliation") as build_reconciliation:
            plan = build_commit_plan(CommitAdapter(), 1)

        self.assertEqual(plan["run_id"], 1)
        self.assertEqual(plan["staged_tournament_codes"], ["new20260101"])
        self.assertEqual(plan["affected_tournament_codes"], ["new20260101"])
        self.assertEqual(plan["game_insert_count"], 1)
        self.assertEqual(plan["rating_insert_count"], 2)
        self.assertEqual(plan["rerun_tournament_codes"], [])
        self.assertFalse(plan["reward_automation_suppressed"])
        self.assertNotIn("reward_reconciliation", plan)
        self.assertNotIn("reward_reconciliation", printable_commit_plan(plan))
        build_reconciliation.assert_not_called()
        self.assertEqual([row["planned_game_id"] for row in plan["planned_games"]], [101])
        self.assertEqual([row["planned_rating_row_id"] for row in plan["planned_ratings"]], [201, 202])

    def test_build_commit_plan_appends_rating_ids_when_replacing_existing_ratings(self) -> None:
        """Verify that build commit plan appends rating ids when replacing existing ratings."""
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

    def test_cascade_rating_events_do_not_suppress_or_create_rewards(self) -> None:
        """Later production events are ratings-only cascade work, not reward-bearing reports."""
        staged_ratings = CommitAdapter.default_staged_ratings()
        cascade = dict(staged_ratings[0])
        cascade.update(
            {
                "Event_Ordinal": 2,
                "Player_Ordinal": 1,
                "Event_Source": "production",
                "Event_Key": "2026-01-02|later20260102",
                "Tournament_Code": "later20260102",
                "Staged_Tournament_Code": None,
                "Source_Report_Ordinal": None,
                "Pin_Player": 1003,
            }
        )

        plan = build_commit_plan(CommitAdapter(staged_ratings=staged_ratings + [cascade]), 1)

        self.assertEqual(plan["production_cascade_tournament_codes"], ["later20260102"])
        self.assertEqual(plan["rerun_tournament_codes"], [])
        self.assertFalse(plan["reward_automation_suppressed"])
        self.assertNotIn("reward_reconciliation", plan)

    def test_build_commit_plan_marks_previous_committed_run_superseded(self) -> None:
        """Verify that build commit plan marks previous committed run superseded."""
        plan = build_commit_plan(CommitAdapter(previous_committed_runs=[35]), 1)

        self.assertEqual(plan["superseded_run_ids"], [35])
        self.assertTrue(
            any("marked superseded: 35" in warning for warning in plan["warnings"]),
            plan["warnings"],
        )
        preview = printable_commit_plan(plan)
        self.assertEqual(preview["superseded_run_ids"], [35])

        statements = build_commit_statements(plan)
        sql_text = "\n".join(statement[0] for statement in statements)
        self.assertIn("N'$.commit_status'", sql_text)
        self.assertIn("N'superseded'", sql_text)
        self.assertIn("N'$.superseded_by_run_id'", sql_text)

    def test_build_commit_plan_rejects_runs_without_replay_rows(self) -> None:
        """Verify that build commit plan rejects runs without replay rows."""
        with self.assertRaisesRegex(ValueError, "Run Replay before commit"):
            build_commit_plan(CommitAdapter(staged_ratings=[]), 1)

    def test_build_commit_plan_rejects_already_committed_rows(self) -> None:
        """Verify that build commit plan rejects already committed rows."""
        staged_ratings = CommitAdapter.default_staged_ratings()
        staged_ratings[0]["Planned_Rating_Row_ID"] = 201

        with self.assertRaisesRegex(ValueError, "appears to have been committed"):
            build_commit_plan(CommitAdapter(staged_ratings=staged_ratings), 1)

    def test_build_commit_plan_rejects_non_ready_runs(self) -> None:
        """Verify that build commit plan rejects non ready runs."""
        with self.assertRaisesRegex(ValueError, "only ready_for_rating"):
            build_commit_plan(CommitAdapter(status="needs_review"), 1)

    def test_build_commit_plan_rejects_ready_run_without_host_chapter(self) -> None:
        """Verify that build commit plan rejects ready run without host chapter."""
        adapter = CommitAdapter()
        original_query_rows = adapter.query_rows

        def query_rows(query, params=()):
            """Query rows."""
            rows = original_query_rows(query, params)
            if "FROM [ratings].[bayrate_staged_tournaments]" in query:
                rows[0]["Host_ChapterID"] = None
                rows[0]["Host_ChapterCode"] = None
                rows[0]["Host_ChapterName"] = None
            return rows

        adapter.query_rows = query_rows

        with self.assertRaisesRegex(ValueError, "Host chapter is required"):
            build_commit_plan(adapter, 1)

    def test_build_commit_statements_include_production_and_staging_updates(self) -> None:
        """Verify that build commit statements include production and staging updates."""
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
        self.assertNotIn("bayrate_reward_reconciliations", sql_text)

    def test_rerun_commit_records_reward_reconciliation_suppression(self) -> None:
        """Only a true replacement writes the durable automatic-reward suppression record."""
        production_tournaments = [
            {
                "Tournament_Code": "new20260101",
                "Tournament_Descr": "New Test Tournament",
                "Tournament_Date": date(2026, 1, 1),
                "Host_ChapterID": 10,
                "Host_ChapterCode": "SEAG",
                "Host_ChapterName": "Seattle Go Center",
                "Reward_Event_Key": "new20260101",
                "Reward_Event_Name": "New Test Tournament",
                "Reward_Is_State_Championship": 1,
            }
        ]
        plan = build_commit_plan(CommitAdapter(production_tournaments=production_tournaments), 1)
        statements = build_commit_statements(plan, operator_principal_name="operator@example.org")
        sql_text = "\n".join(statement[0] for statement in statements)
        insert = next(statement for statement in statements if "INSERT INTO [ratings].[bayrate_reward_reconciliations]" in statement[0])
        report = json.loads(insert[1][1])

        self.assertEqual(plan["rerun_tournament_codes"], ["new20260101"])
        self.assertTrue(plan["reward_automation_suppressed"])
        self.assertTrue(report["required"])
        self.assertEqual(report["rerun_tournament_codes"], ["new20260101"])
        self.assertIn("THROW 51023", sql_text)
        self.assertEqual(insert[1][2], "operator@example.org")

    def test_commit_audit_persists_reconciliation_report(self) -> None:
        """Committed runs retain the approved report for later download and adjustment."""
        plan = build_commit_plan(
            CommitAdapter(
                production_tournaments=[
                    {
                        "Tournament_Code": "new20260101",
                        "Tournament_Descr": "New Test Tournament",
                        "Tournament_Date": date(2026, 1, 1),
                        "Host_ChapterID": 10,
                        "Host_ChapterCode": "SEAG",
                        "Host_ChapterName": "Seattle Go Center",
                        "Reward_Event_Key": "new20260101",
                        "Reward_Event_Name": "New Test Tournament",
                        "Reward_Is_State_Championship": 1,
                    }
                ]
            ),
            1,
        )
        statements = build_commit_statements(plan)
        audit_statement = next(statement for statement in statements if "[SummaryJson] = ?" in statement[0])
        audit = json.loads(audit_statement[1][0])

        self.assertTrue(audit["executed"])
        self.assertEqual(audit["plan_hash"], printable_commit_plan(plan)["plan_hash"])
        self.assertEqual(audit["reward_reconciliation"]["report_type"], "chapter_rewards_reconciliation")
        self.assertIn("game_id_reconciliation", audit)

    def test_commit_audit_omits_reconciliation_report_for_first_time_tournament(self) -> None:
        """A first-time tournament has no old reward entitlement to reconcile."""
        plan = build_commit_plan(CommitAdapter(), 1)
        statements = build_commit_statements(plan)
        audit_statement = next(statement for statement in statements if "[SummaryJson] = ?" in statement[0])
        audit = json.loads(audit_statement[1][0])

        self.assertNotIn("reward_reconciliation", audit)

    def test_printable_commit_plan_includes_stable_plan_hash(self) -> None:
        """Verify that printable commit plan includes stable plan hash."""
        plan = build_commit_plan(CommitAdapter(), 1)
        preview = printable_commit_plan(plan)

        plan["executed"] = True
        executed = printable_commit_plan(plan)

        self.assertEqual(len(preview["plan_hash"]), 64)
        self.assertEqual(preview["plan_hash"], executed["plan_hash"])

    def test_game_insert_uses_production_integer_komi_convention(self) -> None:
        """Verify that game insert uses production integer komi convention."""
        plan = build_commit_plan(CommitAdapter(), 1)
        statements = build_commit_statements(plan)
        game_insert = next(statement for statement in statements if "INSERT INTO [ratings].[games]" in statement[0])

        self.assertEqual(game_insert[1][11], 6)

    def test_tournament_upsert_carries_host_chapter(self) -> None:
        """Verify that tournament upsert carries host chapter."""
        plan = build_commit_plan(CommitAdapter(), 1)
        statements = build_commit_statements(plan)
        tournament_upsert = next(statement for statement in statements if "INSERT INTO [ratings].[tournaments]" in statement[0])

        self.assertIn("[Host_ChapterID]", tournament_upsert[0])
        self.assertEqual(tournament_upsert[1][5:8], (10, "SEAG", "Seattle Go Center"))
        self.assertEqual(tournament_upsert[1][8:10], ("new20260101", "New Test Tournament"))
        self.assertEqual(tournament_upsert[1][10], 1)

    def test_commit_staged_run_requires_confirmation(self) -> None:
        """Verify that commit staged run requires confirmation."""
        with self.assertRaisesRegex(ValueError, "confirm_production_commit"):
            commit_staged_run(CommitAdapter(), 1)

    def test_commit_staged_run_rejects_stale_preview_hash(self) -> None:
        """Verify that commit staged run rejects stale preview hash."""
        with self.assertRaisesRegex(ValueError, "changed since preview"):
            commit_staged_run(
                CommitAdapter(),
                1,
                confirm_production_commit=True,
                expected_plan_hash="not-the-current-plan",
            )

    def test_commit_staged_run_requires_sgf_acknowledgement_for_sgf_replacement(self) -> None:
        """Verify that commit staged run requires sgf acknowledgement for sgf replacement."""
        production_games = [
            {
                "Game_ID": 700,
                "Tournament_Code": "new20260101",
                "Game_Date": date(2026, 1, 1),
                "Round": 1,
                "Pin_Player_1": 1001,
                "Pin_Player_2": 1003,
                "Sgf_Code": "linked-game",
            }
        ]

        with self.assertRaisesRegex(ValueError, "SGF-linked"):
            commit_staged_run(
                CommitAdapter(production_games=production_games),
                1,
                confirm_production_commit=True,
            )

    def test_matching_game_retains_game_id_and_sgf_without_acknowledgement(self) -> None:
        """A physical-game match retains both its stable ID and SGF link."""
        production_games = [
            {
                "Game_ID": 700,
                "Tournament_Code": "new20260101",
                "Game_Date": date(2026, 1, 1),
                "Round": 1,
                "Pin_Player_1": 1002,
                "Pin_Player_2": 1001,
                "Sgf_Code": "linked-game",
                "Rated": 1,
                "Exclude": 0,
            }
        ]

        plan = build_commit_plan(CommitAdapter(production_games=production_games), 1)

        self.assertEqual(plan["planned_games"][0]["planned_game_id"], 700)
        self.assertEqual(plan["planned_games"][0]["game_row"]["Sgf_Code"], "linked-game")
        self.assertEqual(plan["game_id_reconciliation"]["retained_game_count"], 1)
        self.assertFalse(plan["requires_sgf_acknowledgement"])

    def test_changed_bye_retires_old_id_and_allocates_new_id(self) -> None:
        """Changing one participant creates a new physical game instead of reusing an old ID."""
        production_games = [
            {
                "Game_ID": 100,
                "Tournament_Code": "new20260101",
                "Game_Date": date(2026, 1, 1),
                "Round": 1,
                "Pin_Player_1": 1001,
                "Pin_Player_2": 1003,
                "Sgf_Code": None,
                "Rated": 1,
                "Exclude": 0,
            }
        ]

        plan = build_commit_plan(CommitAdapter(production_games=production_games), 1)

        self.assertEqual(plan["planned_games"][0]["planned_game_id"], 101)
        self.assertEqual(plan["game_id_reconciliation"]["retired_games"][0]["game_id"], 100)
        self.assertEqual(plan["game_id_reconciliation"]["new_game_count"], 1)

    def test_rerun_allows_game_count_change_and_retains_matches(self) -> None:
        """Added and removed games no longer require equal old/new row counts."""
        staged_games = CommitAdapter.default_staged_games()
        second = dict(staged_games[0])
        second.update({"Source_Game_Ordinal": 2, "Round": 2, "Pin_Player_1": 1001, "Pin_Player_2": 1003})
        production_games = [
            {
                "Game_ID": 100,
                "Tournament_Code": "new20260101",
                "Game_Date": date(2026, 1, 1),
                "Round": 1,
                "Pin_Player_1": 1001,
                "Pin_Player_2": 1002,
                "Sgf_Code": None,
                "Rated": 1,
                "Exclude": 0,
            }
        ]

        plan = build_commit_plan(
            CommitAdapter(staged_games=staged_games + [second], production_games=production_games),
            1,
        )

        self.assertEqual([row["planned_game_id"] for row in plan["planned_games"]], [100, 101])
        self.assertEqual(plan["game_id_reconciliation"]["retained_game_count"], 1)
        self.assertEqual(plan["game_id_reconciliation"]["new_game_count"], 1)
        self.assertEqual(plan["game_id_reconciliation"]["retired_game_count"], 0)

    def test_rerun_can_remove_all_games_from_replaced_tournament(self) -> None:
        """A zero-game replacement still records and retires every old Game_ID."""
        production_games = [
            {
                "Game_ID": 100,
                "Tournament_Code": "new20260101",
                "Game_Date": date(2026, 1, 1),
                "Round": 1,
                "Pin_Player_1": 1001,
                "Pin_Player_2": 1002,
            }
        ]

        plan = build_commit_plan(
            CommitAdapter(staged_games=[], production_games=production_games),
            1,
        )

        self.assertEqual(plan["planned_games"], [])
        self.assertEqual(plan["game_id_reconciliation"]["retired_game_count"], 1)
        self.assertEqual(plan["game_id_reconciliation"]["retired_games"][0]["game_id"], 100)

    def test_rerun_blocks_ambiguous_duplicate_physical_games(self) -> None:
        """Duplicate identity buckets require corrected round/game data before ID reuse."""
        staged_games = CommitAdapter.default_staged_games()
        duplicate = dict(staged_games[0])
        duplicate["Source_Game_Ordinal"] = 2
        production_games = [
            {
                "Game_ID": game_id,
                "Tournament_Code": "new20260101",
                "Game_Date": date(2026, 1, 1),
                "Round": 1,
                "Pin_Player_1": 1001,
                "Pin_Player_2": 1002,
            }
            for game_id in (100, 101)
        ]

        with self.assertRaisesRegex(ValueError, "ambiguous duplicate games"):
            build_commit_plan(
                CommitAdapter(staged_games=staged_games + [duplicate], production_games=production_games),
                1,
            )

    def test_reward_reconciliation_compares_played_game_points_by_chapter(self) -> None:
        """The report moves participant points when the player receiving a bye changes."""
        production_games = [
            {
                "Game_ID": 100,
                "Tournament_Code": "new20260101",
                "Game_Date": date(2026, 1, 1),
                "Round": 1,
                "Pin_Player_1": 1001,
                "Pin_Player_2": 1003,
                "Online": 0,
                "Exclude": 0,
                "Rated": 1,
            }
        ]
        production_tournaments = [
            {
                "Tournament_Code": "new20260101",
                "Tournament_Descr": "New Test Tournament",
                "Tournament_Date": date(2026, 1, 1),
                "Host_ChapterID": 10,
                "Host_ChapterCode": "SEAG",
                "Host_ChapterName": "Seattle Go Center",
                "Reward_Event_Key": "new20260101",
                "Reward_Event_Name": "New Test Tournament",
                "Reward_Is_State_Championship": 1,
            }
        ]
        snapshots = [
            self._reward_snapshot(1001, 20, "AAA", "Alpha", 1),
            self._reward_snapshot(1002, 30, "BBB", "Beta", 2),
            self._reward_snapshot(1003, 40, "CCC", "Gamma", 3),
        ]

        report = build_commit_plan(
            CommitAdapter(
                production_games=production_games,
                production_tournaments=production_tournaments,
                reward_snapshots=snapshots,
            ),
            1,
        )["reward_reconciliation"]
        by_code = {row["chapter_code"]: row for row in report["chapters"]}

        self.assertEqual((report["old_total_points"], report["new_total_points"]), (202000, 201500))
        self.assertEqual((by_code["AAA"]["old_total_points"], by_code["AAA"]["new_total_points"]), (500, 500))
        self.assertEqual((by_code["BBB"]["old_total_points"], by_code["BBB"]["new_total_points"]), (0, 1000))
        self.assertEqual((by_code["CCC"]["old_total_points"], by_code["CCC"]["new_total_points"]), (1500, 0))
        self.assertEqual(
            (by_code["SEAG"]["old_state_championship_points"], by_code["SEAG"]["new_state_championship_points"]),
            (200000, 200000),
        )

    def test_reward_reconciliation_compares_sponsor_total_game_points(self) -> None:
        """The report recalculates the sponsoring chapter across the affected reward group."""
        staged_games = []
        production_games = []
        for index in range(1, 18):
            staged = dict(CommitAdapter.default_staged_games()[0])
            staged.update(
                {
                    "Source_Game_Ordinal": index,
                    "Round": index,
                    "Pin_Player_1": 1000 + index * 2,
                    "Pin_Player_2": 1001 + index * 2,
                }
            )
            staged_games.append(staged)
            if index <= 16:
                production_games.append(
                    {
                        "Game_ID": 100 + index,
                        "Tournament_Code": "new20260101",
                        "Game_Date": date(2026, 1, 1),
                        "Round": index,
                        "Pin_Player_1": staged["Pin_Player_1"],
                        "Pin_Player_2": staged["Pin_Player_2"],
                        "Online": 0,
                        "Exclude": 0,
                        "Rated": 1,
                    }
                )
        production_tournaments = [
            {
                "Tournament_Code": "new20260101",
                "Tournament_Descr": "New Test Tournament",
                "Tournament_Date": date(2026, 1, 1),
                "Host_ChapterID": 10,
                "Host_ChapterCode": "SEAG",
                "Host_ChapterName": "Seattle Go Center",
                "Reward_Event_Key": "new20260101",
                "Reward_Event_Name": "New Test Tournament",
                "Reward_Is_State_Championship": 1,
            }
        ]

        report = build_commit_plan(
            CommitAdapter(
                staged_games=staged_games,
                production_games=production_games,
                production_tournaments=production_tournaments,
            ),
            1,
        )["reward_reconciliation"]
        sponsor = next(row for row in report["chapters"] if row["chapter_code"] == "SEAG")

        self.assertEqual(sponsor["old_total_games_points"], tournament_host_points(16, "SEAG"))
        self.assertEqual(sponsor["new_total_games_points"], tournament_host_points(17, "SEAG"))
        self.assertGreater(sponsor["point_difference"], 0)

    def test_reward_reconciliation_includes_state_championship_once(self) -> None:
        """A corrected State Championship flag is held as a single manual entitlement."""
        production_tournaments = [
            {
                "Tournament_Code": "new20260101",
                "Tournament_Descr": "New Test Tournament",
                "Tournament_Date": date(2026, 1, 1),
                "Host_ChapterID": 10,
                "Host_ChapterCode": "SEAG",
                "Host_ChapterName": "Seattle Go Center",
                "Reward_Event_Key": "new20260101",
                "Reward_Event_Name": "New Test Tournament",
                "Reward_Is_State_Championship": 0,
            }
        ]

        report = build_commit_plan(
            CommitAdapter(production_tournaments=production_tournaments),
            1,
        )["reward_reconciliation"]
        sponsor = next(row for row in report["chapters"] if row["chapter_code"] == "SEAG")
        event_group = report["event_groups"][0]

        self.assertEqual(sponsor["old_state_championship_points"], 0)
        self.assertEqual(sponsor["new_state_championship_points"], 200000)
        self.assertEqual(event_group["new_state_championship_points"], 200000)
        self.assertEqual(event_group["state_championship_point_difference"], 200000)

    @staticmethod
    def _reward_snapshot(agaid, chapter_id, chapter_code, chapter_name, multiplier):
        return {
            "Snapshot_Date": date(2026, 1, 1),
            "AGAID": agaid,
            "Member_ChapterID": chapter_id,
            "Member_Chapter_Code": chapter_code,
            "Member_Is_Active": 1,
            "ChapterID": chapter_id,
            "Chapter_Code": chapter_code,
            "Chapter_Name": chapter_name,
            "Chapter_Is_Current": 1,
            "Multiplier": multiplier,
        }

    def test_commit_staged_run_executes_generated_statements(self) -> None:
        """Verify that commit staged run executes generated statements."""
        adapter = CommitAdapter()

        plan = commit_staged_run(adapter, 1, confirm_production_commit=True)

        self.assertTrue(plan["executed"])
        self.assertGreater(len(adapter.statements), 0)


if __name__ == "__main__":
    unittest.main()
