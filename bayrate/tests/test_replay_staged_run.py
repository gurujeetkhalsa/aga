import unittest
from datetime import date
from pathlib import Path

from bayrate.replay_staged_run import build_staged_replay_input, run_staged_replay
from bayrate.stage_reports import build_staging_payload


FIXTURE_DIR = Path(__file__).parent / "fixtures"


class DuplicateCandidateAdapter:
    def query_rows(self, query, params=()):
        if "FROM [membership].[members]" in query:
            return [{"AGAID": player_id, "ExpirationDate": date(2099, 1, 1)} for player_id in params]
        if "FROM [ratings].[ratings] AS r" in query and "ROW_NUMBER() OVER" in query:
            return []
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

    def execute_statements(self, statements):
        raise AssertionError("Duplicate candidate test adapter should not write SQL.")


class ReplayAdapter:
    def __init__(self) -> None:
        self.queries = []
        self.statements = []

    def query_rows(self, query, params=()):
        self.queries.append((query, tuple(params)))
        if "WHERE t.[Tournament_Code] = ?" in query:
            return self._event_summary(params[0])
        if "WHERE t.[Tournament_Date] >= ?" in query:
            return [
                *self._event_summary("PREV-SAME-DAY"),
                *self._event_summary("OLD-SAMPLE-1"),
                *self._event_summary("LATER-SAME-DAY"),
            ]
        if "ROW_NUMBER() OVER" in query and "r.[Elab_Date] < ?" in query:
            return [
                {
                    "Pin_Player": 1001,
                    "Rating": 1.2,
                    "Sigma": 0.4,
                    "Elab_Date": date(2025, 12, 15),
                    "id": 8900,
                },
                {
                    "Pin_Player": 1002,
                    "Rating": 1.1,
                    "Sigma": 0.5,
                    "Elab_Date": date(2025, 12, 15),
                    "id": 8901,
                },
                {
                    "Pin_Player": 1003,
                    "Rating": 2.2,
                    "Sigma": 0.4,
                    "Elab_Date": date(2025, 12, 15),
                    "id": 8902,
                },
            ]
        if "FROM [ratings].[games] AS g" in query:
            if params[0] == "LATER-SAME-DAY":
                return [
                    {
                        "Game_ID": 7001,
                        "Tournament_Code": "LATER-SAME-DAY",
                        "Game_Date": date(2026, 1, 1),
                        "Round": 1,
                        "Pin_Player_1": 1001,
                        "Color_1": "B",
                        "Rank_1": "1d",
                        "Pin_Player_2": 1004,
                        "Color_2": "W",
                        "Rank_2": "1d",
                        "Handicap": 0,
                        "Komi": 7,
                        "Result": "W",
                        "Sgf_Code": None,
                        "Online": 0,
                        "Exclude": 0,
                        "Rated": 1,
                        "Elab_Date": date(2026, 1, 1),
                    }
                ]
            return []
        return []

    def execute_statements(self, statements):
        self.statements.extend(list(statements))

    def _event_summary(self, code):
        summaries = {
            "OLD-SAMPLE-1": {
                "Tournament_Code": "OLD-SAMPLE-1",
                "Tournament_Descr": "Migration Sample One Open",
                "Tournament_Date": date(2026, 1, 1),
                "GameCount": 2,
                "FirstGameID": 501,
                "FirstRatingRowID": 9001,
                "LastRatingRowID": 9003,
                "RatingRowCount": 3,
            },
            "PREV-SAME-DAY": {
                "Tournament_Code": "PREV-SAME-DAY",
                "Tournament_Descr": "Previous Same Day",
                "Tournament_Date": date(2026, 1, 1),
                "GameCount": 1,
                "FirstGameID": 401,
                "FirstRatingRowID": 8901,
                "LastRatingRowID": 8902,
                "RatingRowCount": 2,
            },
            "LATER-SAME-DAY": {
                "Tournament_Code": "LATER-SAME-DAY",
                "Tournament_Descr": "Later Same Day",
                "Tournament_Date": date(2026, 1, 1),
                "GameCount": 1,
                "FirstGameID": 7001,
                "FirstRatingRowID": 9010,
                "LastRatingRowID": 9011,
                "RatingRowCount": 2,
            },
        }
        return [summaries[code]] if code in summaries else []


def needs_review_payload():
    return build_staging_payload(
        [
            (
                "report_compact_one.txt",
                (FIXTURE_DIR / "report_compact_one.txt").read_text(encoding="utf-8"),
            )
        ],
        adapter=DuplicateCandidateAdapter(),
        run_id=777,
    )


class ReplayStagedRunTest(unittest.TestCase):
    def test_replay_input_replaces_duplicate_and_adds_later_same_day_cascade(self) -> None:
        payload = needs_review_payload()
        adapter = ReplayAdapter()

        replay_input = build_staged_replay_input(adapter, payload=payload)
        plan = replay_input["plan"]

        self.assertEqual(plan["anchor"]["tournament_code"], "OLD-SAMPLE-1")
        self.assertEqual(plan["anchor"]["first_rating_row_id"], 9001)
        self.assertEqual(
            [event["tournament_code"] for event in plan["events"]],
            ["OLD-SAMPLE-1", "LATER-SAME-DAY"],
        )
        self.assertEqual(plan["staged_event_count"], 1)
        self.assertEqual(plan["production_cascade_event_count"], 1)
        self.assertEqual(plan["game_count"], 3)
        self.assertIn("duplicate candidate OLD-SAMPLE-1", plan["warnings"][0])
        self.assertEqual(plan["starter"]["same_day_predecessor_tournament_codes"], ["PREV-SAME-DAY"])
        self.assertIn("same-day predecessors: PREV-SAME-DAY", plan["starter"]["source"])
        self.assertEqual([game.tournament_code for game in replay_input["games"]], ["OLD-SAMPLE-1", "OLD-SAMPLE-1", "LATER-SAME-DAY"])

    def test_run_staged_replay_writes_artifact_without_sql_writes(self) -> None:
        payload = needs_review_payload()
        adapter = ReplayAdapter()
        output_path = Path(__file__).parent / "tmp_replay_artifact.json"
        try:
            artifact = run_staged_replay(adapter, payload=payload, output_path=output_path)

            self.assertTrue(output_path.exists())
            self.assertEqual(adapter.statements, [])
            self.assertEqual(artifact["plan"]["production_write_count"], 0)
            self.assertEqual(artifact["plan"]["staged_rating_count"], 0)
            self.assertEqual(artifact["bayrate_result"]["event_count"], 2)
        finally:
            if output_path.exists():
                output_path.unlink()

    def test_run_staged_replay_can_stage_rating_rows(self) -> None:
        payload = needs_review_payload()
        adapter = ReplayAdapter()

        artifact = run_staged_replay(
            adapter,
            payload=payload,
            write_artifact=False,
            persist_staged_ratings=True,
        )

        staged_rating_count = artifact["plan"]["staged_rating_count"]
        self.assertGreater(staged_rating_count, 0)
        self.assertEqual(artifact["plan"]["production_write_count"], 0)
        self.assertEqual(artifact["plan"]["staging_write_count"], staged_rating_count)
        self.assertEqual(artifact["staged_rating_summary"]["rating_count"], staged_rating_count)
        self.assertEqual(len(adapter.statements), staged_rating_count + 1)
        self.assertIn("DELETE FROM [ratings].[bayrate_staged_ratings]", adapter.statements[0][0])
        self.assertTrue(
            all("INSERT INTO [ratings].[bayrate_staged_ratings]" in statement[0] for statement in adapter.statements[1:])
        )

    def test_replay_rejects_validation_failed_payload(self) -> None:
        report = """TOURNEY Numeric Rank Sample
start=2026-03-01
finish=2026-03-01
location=Seattle, WA
rules=AGA

PLAYERS (2)
3001 Numeric One 1.0
3002 Numeric Two 2.0

GAMES (1)
3001 3002 W 0 7
END
"""
        payload = build_staging_payload([("bad.txt", report)], adapter=DuplicateCandidateAdapter())

        with self.assertRaisesRegex(ValueError, "validation_failed"):
            build_staged_replay_input(ReplayAdapter(), payload=payload)


if __name__ == "__main__":
    unittest.main()
