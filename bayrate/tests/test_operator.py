import io
import unittest
from datetime import date
from pathlib import Path

from bayrate.operator import run_existing_run_review, run_operator


FIXTURE_DIR = Path(__file__).parent / "fixtures"


class FakeAdapter:
    def __init__(self, candidate_rows=None, membership_rows=None) -> None:
        self.candidate_rows = list(candidate_rows or [])
        self.membership_rows = None if membership_rows is None else list(membership_rows)
        self.statements = []

    def query_rows(self, query, params=()):
        if "FROM [membership].[chapters]" in query:
            return [
                {
                    "ChapterID": 10,
                    "ChapterCode": "SEAG",
                    "ChapterName": "Seattle Go Center",
                    "City": "Seattle",
                    "State": "WA",
                }
            ]
        if "FROM [membership].[members]" in query:
            if self.membership_rows is None:
                return [{"AGAID": player_id, "ExpirationDate": date(2099, 1, 1)} for player_id in params]
            requested_ids = {int(player_id) for player_id in params}
            return [row for row in self.membership_rows if int(row.get("AGAID")) in requested_ids]
        if "FROM [ratings].[ratings] AS r" in query and "ROW_NUMBER() OVER" in query:
            return []
        return list(self.candidate_rows)

    def execute_statements(self, statements):
        self.statements.extend(list(statements))


class ScriptedInput:
    def __init__(self, answers):
        self.answers = list(answers)

    def __call__(self):
        if not self.answers:
            raise AssertionError("No scripted answer remains.")
        return self.answers.pop(0)


class ExistingRunAdapter:
    def __init__(self) -> None:
        self.statements = []

    def query_rows(self, query, params=()):
        if "FROM [membership].[chapters]" in query:
            return [
                {
                    "ChapterID": 10,
                    "ChapterCode": "SEAG",
                    "ChapterName": "Seattle Go Center",
                    "City": "Seattle",
                    "State": "WA",
                }
            ]
        if "FROM [ratings].[bayrate_runs]" in query:
            return [
                {
                    "RunID": 555,
                    "Status": "needs_review",
                    "Source_Report_Count": 1,
                    "Source_Report_Names": '["report_compact_one.txt"]',
                    "Tournament_Count": 1,
                    "Game_Count": 2,
                    "Validation_Error_Count": 0,
                    "Ready_Tournament_Count": 0,
                    "Needs_Review_Count": 1,
                    "Validation_Failed_Count": 0,
                }
            ]
        if "FROM [ratings].[bayrate_staged_tournaments]" in query:
            return [
                {
                    "RunID": 555,
                    "Source_Report_Ordinal": 1,
                    "Source_Report_Name": "report_compact_one.txt",
                    "Source_Report_Sha256": "abc123",
                    "Tournament_Code": "migrations20260101",
                    "Original_Tournament_Code": None,
                    "Tournament_Code_Source": "generated",
                    "Tournament_Descr": "Migration Sample One",
                    "Normalized_Title": "migration sample one",
                    "Tournament_Date": date(2026, 1, 1),
                    "City": "New York",
                    "State_Code": "NY",
                    "Country_Code": "US",
                    "Host_ChapterID": None,
                    "Host_ChapterCode": None,
                    "Host_ChapterName": None,
                    "Reward_Event_Key": "migrations20260101",
                    "Reward_Event_Name": "Migration Sample One",
                    "Reward_Is_State_Championship": 0,
                    "Rounds": 2,
                    "Total_Players": 3,
                    "Wallist": None,
                    "Elab_Date": date(2026, 1, 1),
                    "Validation_Status": "needs_review",
                    "Validation_Errors": "[]",
                    "Parser_Warnings": "[]",
                    "Duplicate_Candidate_Code": "OLD-SAMPLE-1",
                    "Duplicate_Score": 0.91,
                    "Review_Reason": "Likely duplicate.",
                    "MetadataJson": "{}",
                }
            ]
        if "FROM [ratings].[bayrate_staged_games]" in query:
            return [
                {
                    "RunID": 555,
                    "Source_Report_Ordinal": 1,
                    "Source_Game_Ordinal": 1,
                    "Source_Report_Name": "report_compact_one.txt",
                    "Game_ID": None,
                    "Tournament_Code": "migrations20260101",
                    "Game_Date": date(2026, 1, 1),
                    "Round": 1,
                    "Pin_Player_1": 1001,
                    "Color_1": "W",
                    "Rank_1": "1d",
                    "Pin_Player_2": 1002,
                    "Color_2": "B",
                    "Rank_2": "1d",
                    "Handicap": 0,
                    "Komi": 7,
                    "Result": "W",
                    "Sgf_Code": None,
                    "Online": 0,
                    "Exclude": 0,
                    "Rated": 1,
                    "Elab_Date": date(2026, 1, 1),
                    "Validation_Status": "needs_review",
                    "Validation_Errors": "[]",
                },
                {
                    "RunID": 555,
                    "Source_Report_Ordinal": 1,
                    "Source_Game_Ordinal": 2,
                    "Source_Report_Name": "report_compact_one.txt",
                    "Game_ID": None,
                    "Tournament_Code": "migrations20260101",
                    "Game_Date": date(2026, 1, 1),
                    "Round": 2,
                    "Pin_Player_1": 1002,
                    "Color_1": "W",
                    "Rank_1": "1d",
                    "Pin_Player_2": 1003,
                    "Color_2": "B",
                    "Rank_2": "2d",
                    "Handicap": 0,
                    "Komi": 7,
                    "Result": "B",
                    "Sgf_Code": None,
                    "Online": 0,
                    "Exclude": 0,
                    "Rated": 1,
                    "Elab_Date": date(2026, 1, 1),
                    "Validation_Status": "needs_review",
                    "Validation_Errors": "[]",
                },
            ]
        return []

    def execute_statements(self, statements):
        self.statements.extend(list(statements))


def duplicate_candidate_rows():
    return [
        {
            "Tournament_Code": "OLD-SAMPLE-1",
            "Tournament_Descr": "Migration Sample One Open",
            "Tournament_Date": date(2026, 1, 1),
            "City": "New York",
            "State_Code": "NY",
            "Country_Code": "US",
            "Game_ID": 501,
            "Game_Date": date(2026, 1, 1),
            "Round": 1,
            "Pin_Player_1": 1001,
            "Pin_Player_2": 1002,
            "Handicap": 0,
            "Komi": 7,
            "Result": "W",
        },
        {
            "Tournament_Code": "OLD-SAMPLE-1",
            "Tournament_Descr": "Migration Sample One Open",
            "Tournament_Date": date(2026, 1, 1),
            "City": "New York",
            "State_Code": "NY",
            "Country_Code": "US",
            "Game_ID": 502,
            "Game_Date": date(2026, 1, 1),
            "Round": 2,
            "Pin_Player_1": 1002,
            "Pin_Player_2": 1003,
            "Handicap": 0,
            "Komi": 7,
            "Result": "B",
        },
    ]


class OperatorWorkflowTest(unittest.TestCase):
    def test_operator_confirms_duplicate_code_marks_ready_and_writes(self) -> None:
        adapter = FakeAdapter(duplicate_candidate_rows())

        payload = run_operator(
            [FIXTURE_DIR / "report_compact_one.txt"],
            adapter=adapter,
            input_func=ScriptedInput(["y", "y", "SEAG", "", "", "", "Round correction approved.", "y"]),
            output=io.StringIO(),
            run_id=222,
        )

        tournament = payload["staged_tournaments"][0]
        self.assertTrue(payload["written"])
        self.assertEqual(payload["status"], "ready_for_rating")
        self.assertEqual(tournament["tournament_row"]["Tournament_Code"], "OLD-SAMPLE-1")
        self.assertEqual(tournament["original_tournament_code"], "migrations20260101")
        self.assertEqual(tournament["metadata"]["operator_note"], "Round correction approved.")
        self.assertIn("Round correction approved.", tournament["review_reason"])
        self.assertEqual({entry["game_row"]["Tournament_Code"] for entry in payload["staged_games"]}, {"OLD-SAMPLE-1"})
        self.assertEqual(len(adapter.statements), 4)

    def test_operator_can_decline_sql_write(self) -> None:
        adapter = FakeAdapter()

        payload = run_operator(
            [FIXTURE_DIR / "report_compact_two.txt"],
            adapter=adapter,
            input_func=ScriptedInput(["n", "n"]),
            output=io.StringIO(),
            run_id=333,
        )

        self.assertFalse(payload["written"])
        self.assertEqual(payload["status"], "needs_review")
        self.assertEqual(adapter.statements, [])

    def test_operator_can_keep_duplicate_run_in_review(self) -> None:
        adapter = FakeAdapter(duplicate_candidate_rows())

        payload = run_operator(
            [FIXTURE_DIR / "report_compact_one.txt"],
            adapter=adapter,
            input_func=ScriptedInput(["n", "n", "y"]),
            output=io.StringIO(),
            run_id=444,
        )

        tournament = payload["staged_tournaments"][0]
        self.assertTrue(payload["written"])
        self.assertEqual(payload["status"], "needs_review")
        self.assertEqual(tournament["tournament_row"]["Tournament_Code"], "migrations20260101")
        self.assertEqual(tournament["duplicate_candidate"]["tournament_code"], "OLD-SAMPLE-1")
        self.assertEqual(len(adapter.statements), 4)

    def test_operator_can_review_existing_run_and_save_updates(self) -> None:
        adapter = ExistingRunAdapter()

        payload = run_existing_run_review(
            555,
            adapter=adapter,
            input_func=ScriptedInput(["y", "y", "SEAG", "", "", "", "Existing run approval note.", "y"]),
            output=io.StringIO(),
        )

        tournament = payload["staged_tournaments"][0]
        self.assertTrue(payload["updated"])
        self.assertEqual(payload["status"], "ready_for_rating")
        self.assertEqual(tournament["tournament_row"]["Tournament_Code"], "OLD-SAMPLE-1")
        self.assertEqual(tournament["original_tournament_code"], "migrations20260101")
        self.assertEqual(tournament["metadata"]["operator_note"], "Existing run approval note.")
        self.assertIn("Existing run approval note.", tournament["review_reason"])
        self.assertEqual({entry["game_row"]["Tournament_Code"] for entry in payload["staged_games"]}, {"OLD-SAMPLE-1"})
        self.assertEqual(len(adapter.statements), 4)


if __name__ == "__main__":
    unittest.main()
