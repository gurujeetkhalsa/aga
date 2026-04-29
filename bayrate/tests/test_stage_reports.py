import csv
import unittest
from datetime import date
from pathlib import Path

from bayrate.core import BayrateConfig, load_games_from_csv
from bayrate.stage_reports import (
    bayrate_game_csv_rows,
    build_staging_payload,
    compare_staged_to_production_games,
    explain_staged_run_review,
    stage_report_files,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures"


class FakeAdapter:
    def __init__(self, candidate_rows=None, membership_rows=None, rating_rows=None) -> None:
        self.candidate_rows = list(candidate_rows or [])
        self.membership_rows = None if membership_rows is None else list(membership_rows)
        self.rating_rows = list(rating_rows or [])
        self.queries = []
        self.statements = []

    def query_rows(self, query, params=()):
        self.queries.append((query, tuple(params)))
        if "NEXT VALUE FOR [ratings].[bayrate_run_id_seq]" in query:
            return [{"RunID": 42}]
        if "FROM [membership].[members]" in query:
            if self.membership_rows is None:
                return [{"AGAID": player_id, "ExpirationDate": date(2099, 1, 1)} for player_id in params]
            requested_ids = {int(player_id) for player_id in params}
            return [row for row in self.membership_rows if int(row.get("AGAID")) in requested_ids]
        if "FROM [ratings].[ratings] AS r" in query and "ROW_NUMBER() OVER" in query:
            requested_ids = {int(player_id) for player_id in params[:-1]}
            return [row for row in self.rating_rows if int(row.get("Pin_Player")) in requested_ids]
        return list(self.candidate_rows)

    def execute_statements(self, statements):
        self.statements.extend(list(statements))


class StageReportsTest(unittest.TestCase):
    def test_stage_multiple_reports_writes_run_tournaments_and_games(self) -> None:
        adapter = FakeAdapter()
        run_id = 111

        payload = stage_report_files(
            [FIXTURE_DIR / "report_compact_one.txt", FIXTURE_DIR / "report_compact_two.txt"],
            adapter=adapter,
            run_id=run_id,
        )

        self.assertEqual(payload["run_id"], run_id)
        self.assertEqual(payload["status"], "ready_for_rating")
        self.assertEqual(payload["tournament_count"], 2)
        self.assertEqual(payload["game_count"], 3)
        self.assertTrue(payload["written"])
        self.assertEqual(len(adapter.statements), 6)
        self.assertEqual([entry["status"] for entry in payload["staged_tournaments"]], ["ready_for_rating", "ready_for_rating"])

    def test_stage_write_reserves_incremental_run_id(self) -> None:
        adapter = FakeAdapter()

        payload = stage_report_files(
            [FIXTURE_DIR / "report_compact_one.txt"],
            adapter=adapter,
        )

        self.assertEqual(payload["run_id"], 42)
        self.assertTrue(payload["written"])
        self.assertTrue(any("NEXT VALUE FOR [ratings].[bayrate_run_id_seq]" in query for query, _ in adapter.queries))
        self.assertEqual(adapter.statements[0][1][0], 42)
        self.assertEqual({entry["run_id"] for entry in payload["staged_tournaments"]}, {42})
        self.assertEqual({entry["run_id"] for entry in payload["staged_games"]}, {42})

    def test_duplicate_candidate_with_different_code_needs_review(self) -> None:
        adapter = FakeAdapter(
            [
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
        )

        payload = stage_report_files([FIXTURE_DIR / "report_compact_one.txt"], adapter=adapter, dry_run=True)

        self.assertEqual(payload["status"], "needs_review")
        tournament = payload["staged_tournaments"][0]
        self.assertEqual(tournament["status"], "needs_review")
        self.assertEqual(tournament["duplicate_candidate"]["tournament_code"], "OLD-SAMPLE-1")
        self.assertFalse(adapter.statements)

    def test_exact_existing_code_is_reused(self) -> None:
        adapter = FakeAdapter(
            [
                {
                    "Tournament_Code": "REUSED-1",
                    "Tournament_Descr": "Migration Sample One",
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
                }
            ]
        )

        payload = stage_report_files([FIXTURE_DIR / "report_compact_one.txt"], adapter=adapter, dry_run=True)
        tournament = payload["staged_tournaments"][0]

        self.assertEqual(payload["status"], "ready_for_rating")
        self.assertEqual(tournament["tournament_row"]["Tournament_Code"], "REUSED-1")
        self.assertEqual(tournament["code_source"], "reused")
        self.assertTrue(tournament["original_tournament_code"])
        self.assertEqual({entry["game_row"]["Tournament_Code"] for entry in payload["staged_games"]}, {"REUSED-1"})

    def test_generated_code_collision_with_different_production_tournament_gets_unique_code(self) -> None:
        adapter = FakeAdapter(
            [
                {
                    "Tournament_Code": "2026badukp20260425",
                    "Tournament_Descr": "2026 BadukPop Tournament",
                    "Tournament_Date": date(2026, 4, 25),
                    "City": "San Francisco",
                    "State_Code": "CA",
                    "Country_Code": "US",
                    "Game_ID": 501,
                    "Game_Date": date(2026, 4, 25),
                    "Round": 1,
                    "Pin_Player_1": 9001,
                    "Pin_Player_2": 9002,
                    "Handicap": 0,
                    "Komi": 7,
                    "Result": "W",
                }
            ]
        )
        report = """TOURNEY 2026 BadukPop Open, San Francisco
start=2026-04-25
finish=2026-04-25
location=San Francisco, CA
rules=AGA

PLAYERS (2)
3001 Open One 1D
3002 Open Two 2D

GAMES (1)
3001 3002 W 0 7
END
"""

        payload = build_staging_payload([("2026BPO.txt", report)], adapter=adapter)
        tournament = payload["staged_tournaments"][0]

        self.assertEqual(payload["status"], "ready_for_rating")
        self.assertEqual(tournament["tournament_row"]["Tournament_Code"], "2026badukp20260425-2")
        self.assertEqual(tournament["original_tournament_code"], "2026badukp20260425")
        self.assertEqual(tournament["code_source"], "generated")
        self.assertEqual({entry["game_row"]["Tournament_Code"] for entry in payload["staged_games"]}, {"2026badukp20260425-2"})
        self.assertTrue(
            any(warning.get("type") == "generated_tournament_code_collision" for warning in tournament["parser_warnings"])
        )

    def test_validation_failure_for_non_bayrate_rank(self) -> None:
        report = """TOURNEY Decimal Rank Suffix Sample
start=2026-03-01
finish=2026-03-01
location=Seattle, WA
rules=AGA

PLAYERS (2)
3001 Decimal Suffix One 1.5D
3002 Numeric Two 2D

GAMES (1)
3001 3002 W 0 7
END
"""
        payload = build_staging_payload(
            [("decimal_rank_suffix_report.txt", report)],
            adapter=FakeAdapter(),
        )

        self.assertEqual(payload["status"], "validation_failed")
        self.assertIn("Rank_1 must be a BayRate rank", payload["staged_tournaments"][0]["validation_errors"][0])

    def test_preview_collects_later_report_warnings_after_parse_failure(self) -> None:
        broken_report = """TOURNEY Broken Upload
start=2026-04-01
finish=2026-04-01
"""
        later_warning_report = """TOURNEY Later Pasted Report
start=2026-04-02
finish=2026-04-02
location=Seattle, WA

PLAYERS (2)
4001 Later One 1D
4002 Later Two 2D

GAMES (1)
4001 4002 W 0 7
END
"""

        payload = build_staging_payload(
            [
                ("broken_upload.txt", broken_report),
                ("pasted_report.txt", later_warning_report),
            ]
        )

        self.assertEqual(payload["status"], "validation_failed")
        self.assertEqual(payload["tournament_count"], 2)
        self.assertEqual([warning["type"] for warning in payload["warnings"]], ["sql_adapter_unavailable"])
        self.assertEqual(payload["staged_tournaments"][0]["status"], "validation_failed")
        self.assertIn("Report parse failed", payload["staged_tournaments"][0]["validation_errors"][0])
        later_warnings = payload["staged_tournaments"][1]["parser_warnings"]
        self.assertTrue(any(warning.get("type") == "missing_rules" for warning in later_warnings))
        self.assertFalse(any(warning.get("type") == "membership_validation_skipped" for warning in later_warnings))
        self.assertEqual(payload["staged_tournaments"][1]["status"], "staged")

    def test_unreported_game_result_is_warned_and_not_staged(self) -> None:
        report = """TOURNEY Unreported Result Sample
start=2026-05-01
finish=2026-05-01
location=Seattle, WA
rules=AGA

PLAYERS (3)
5001 Ready One 1D
5002 Ready Two 2D
5003 Waiting Three 3D

GAMES (2)
5001 5002 W 0 7
5001 5003 ? 0 7
END
"""

        payload = build_staging_payload(
            [("unreported_result.txt", report)],
            adapter=FakeAdapter(),
        )

        self.assertEqual(payload["status"], "ready_for_rating")
        self.assertEqual(payload["game_count"], 1)
        warnings = payload["staged_tournaments"][0]["parser_warnings"]
        unreported = [warning for warning in warnings if warning.get("type") == "unreported_game_results"]
        self.assertEqual(len(unreported), 1)
        self.assertEqual(unreported[0]["ignored_game_count"], 1)
        self.assertEqual(payload["staged_games"][0]["game_row"]["Pin_Player_2"], 5002)

    def test_expired_membership_is_valid_when_covered_on_event_date(self) -> None:
        adapter = FakeAdapter(
            membership_rows=[
                {"AGAID": 1001, "ExpirationDate": date(2026, 1, 1)},
                {"AGAID": 1002, "ExpirationDate": date(2026, 1, 1)},
                {"AGAID": 1003, "ExpirationDate": date(2026, 1, 1)},
            ]
        )

        payload = stage_report_files(
            [FIXTURE_DIR / "report_compact_one.txt"],
            adapter=adapter,
            dry_run=True,
            today=date(2026, 4, 25),
        )

        self.assertEqual(payload["status"], "ready_for_rating")
        self.assertEqual(payload["validation_error_count"], 0)

    def test_expired_membership_before_event_date_requires_review(self) -> None:
        adapter = FakeAdapter(
            membership_rows=[
                {"AGAID": 1001, "ExpirationDate": date(2025, 12, 31)},
                {"AGAID": 1002, "ExpirationDate": date(2026, 1, 1)},
                {"AGAID": 1003, "ExpirationDate": date(2099, 1, 1)},
            ]
        )

        payload = stage_report_files(
            [FIXTURE_DIR / "report_compact_one.txt"],
            adapter=adapter,
            dry_run=True,
            today=date(2026, 4, 25),
        )

        self.assertEqual(payload["status"], "needs_review")
        self.assertEqual(payload["validation_error_count"], 0)
        warnings = payload["staged_tournaments"][0]["parser_warnings"]
        membership_warnings = [warning for warning in warnings if warning.get("type") == "expired_membership_on_event_date"]
        self.assertEqual(len(membership_warnings), 1)
        self.assertTrue(membership_warnings[0]["review_required"])
        self.assertIn("AGAID 1001 membership expired on 2025-12-31 before event date 2026-01-01", membership_warnings[0]["message"])

    def test_missing_membership_record_requires_review(self) -> None:
        adapter = FakeAdapter(
            membership_rows=[
                {"AGAID": 1001, "ExpirationDate": date(2099, 1, 1)},
                {"AGAID": 1002, "ExpirationDate": date(2099, 1, 1)},
            ]
        )

        payload = stage_report_files(
            [FIXTURE_DIR / "report_compact_one.txt"],
            adapter=adapter,
            dry_run=True,
            today=date(2026, 4, 25),
        )

        self.assertEqual(payload["status"], "needs_review")
        self.assertEqual(payload["validation_error_count"], 0)
        warnings = payload["staged_tournaments"][0]["parser_warnings"]
        membership_warnings = [warning for warning in warnings if warning.get("type") == "missing_membership_record"]
        self.assertEqual(len(membership_warnings), 1)
        self.assertTrue(membership_warnings[0]["review_required"])
        self.assertIn("AGAID 1003 is missing a membership record", membership_warnings[0]["message"])

    def test_membership_name_mismatch_adds_operator_warning(self) -> None:
        adapter = FakeAdapter(
            membership_rows=[
                {"AGAID": 1001, "FirstName": "Alice", "LastName": "Example", "ExpirationDate": date(2099, 1, 1)},
                {"AGAID": 1002, "FirstName": "Robert", "LastName": "Example", "ExpirationDate": date(2099, 1, 1)},
                {"AGAID": 1003, "FirstName": "Cara", "LastName": "Example", "ExpirationDate": date(2099, 1, 1)},
            ]
        )

        payload = stage_report_files(
            [FIXTURE_DIR / "report_compact_one.txt"],
            adapter=adapter,
            dry_run=True,
        )

        warnings = payload["staged_tournaments"][0]["parser_warnings"]
        mismatch = [warning for warning in warnings if warning.get("type") == "membership_name_mismatch"]
        self.assertEqual(payload["status"], "ready_for_rating")
        self.assertEqual(len(mismatch), 1)
        self.assertEqual(mismatch[0]["agaid"], 1002)
        self.assertIn("Bob Example", mismatch[0]["message"])
        self.assertIn("Robert Example", mismatch[0]["message"])

    def test_entry_rank_mismatch_adds_operator_warning_with_highlight(self) -> None:
        adapter = FakeAdapter(
            rating_rows=[
                {
                    "Pin_Player": 1001,
                    "Rating": 2.2,
                    "Sigma": 0.4,
                    "Elab_Date": date(2025, 12, 1),
                    "Tournament_Code": "PREV",
                    "id": 10,
                },
                {
                    "Pin_Player": 1002,
                    "Rating": -1.2,
                    "Sigma": 0.4,
                    "Elab_Date": date(2025, 12, 1),
                    "Tournament_Code": "PREV",
                    "id": 11,
                },
            ]
        )

        payload = stage_report_files(
            [FIXTURE_DIR / "report_compact_one.txt"],
            adapter=adapter,
            dry_run=True,
        )

        warnings = [
            warning
            for warning in payload["staged_tournaments"][0]["parser_warnings"]
            if warning.get("type") == "entry_rank_mismatch"
        ]
        self.assertEqual(payload["status"], "ready_for_rating")
        self.assertEqual(len(warnings), 2)
        highlighted = [warning for warning in warnings if warning.get("highlight")]
        self.assertEqual(len(highlighted), 1)
        self.assertEqual(highlighted[0]["agaid"], 1001)
        self.assertTrue(highlighted[0]["entry_below_current"])
        self.assertIn("Entry rank is below", highlighted[0]["message"])

    def test_staged_games_can_be_rendered_as_bayrate_game_csv_rows(self) -> None:
        payload = stage_report_files(
            [FIXTURE_DIR / "report_compact_one.txt"],
            adapter=FakeAdapter(),
            dry_run=True,
        )
        rows = bayrate_game_csv_rows(payload)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["Game_ID"], 1)
        self.assertEqual(rows[0]["Tournament_Code"], payload["staged_tournaments"][0]["tournament_row"]["Tournament_Code"])

        path = FIXTURE_DIR.parent / "tmp_stage_reports_games.csv"
        try:
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)

            loaded_games = load_games_from_csv(path, BayrateConfig())
        finally:
            if path.exists():
                path.unlink()

        self.assertEqual(len(loaded_games), 2)

    def test_review_explanation_reports_game_diff_and_same_date_order(self) -> None:
        adapter = FakeAdapter()
        payload = build_staging_payload(
            [("report_compact_one.txt", (FIXTURE_DIR / "report_compact_one.txt").read_text(encoding="utf-8"))],
            adapter=adapter,
        )
        payload["staged_tournaments"][0]["status"] = "needs_review"
        payload["staged_tournaments"][0]["duplicate_candidate"] = {
            "tournament_code": "OLD-SAMPLE-1",
            "score": 0.9,
            "score_parts": {},
        }
        adapter.candidate_rows = [
            {
                "Tournament_Code": "OLD-SAMPLE-1",
                "Tournament_Descr": "Migration Sample One",
                "Tournament_Date": date(2026, 1, 1),
                "Game_ID": 501,
                "Game_Date": date(2026, 1, 1),
                "Round": 1,
                "Pin_Player_1": 1001,
                "Pin_Player_2": 1002,
                "Handicap": 0,
                "Komi": 7,
                "Result": "W",
                "GameCount": 2,
                "FirstGameID": 501,
                "FirstRatingRowID": 9001,
                "LastRatingRowID": 9003,
                "RatingRowCount": 3,
            },
            {
                "Tournament_Code": "OLD-SAMPLE-1",
                "Tournament_Descr": "Migration Sample One",
                "Tournament_Date": date(2026, 1, 1),
                "Game_ID": 502,
                "Game_Date": date(2026, 1, 1),
                "Round": 2,
                "Pin_Player_1": 1002,
                "Pin_Player_2": 1003,
                "Handicap": 0,
                "Komi": 7,
                "Result": "W",
                "GameCount": 2,
                "FirstGameID": 501,
                "FirstRatingRowID": 9001,
                "LastRatingRowID": 9003,
                "RatingRowCount": 3,
            },
        ]

        explanation = explain_staged_run_review(adapter, payload)["tournaments"][0]

        self.assertEqual(explanation["game_diff"]["matched_game_count"], 1)
        self.assertEqual(explanation["game_diff"]["staged_only_count"], 1)
        self.assertEqual(explanation["game_diff"]["production_only_count"], 1)
        self.assertEqual(explanation["same_date_order"][0]["tournament_code"], "OLD-SAMPLE-1")

    def test_compare_games_handles_exact_match(self) -> None:
        payload = stage_report_files(
            [FIXTURE_DIR / "report_compact_one.txt"],
            adapter=FakeAdapter(),
            dry_run=True,
        )
        production_rows = [
            {
                "Game_ID": 1,
                "Game_Date": row["game_row"]["Game_Date"],
                "Round": row["game_row"]["Round"],
                "Pin_Player_1": row["game_row"]["Pin_Player_1"],
                "Pin_Player_2": row["game_row"]["Pin_Player_2"],
                "Handicap": row["game_row"]["Handicap"],
                "Komi": row["game_row"]["Komi"],
                "Result": row["game_row"]["Result"],
            }
            for row in payload["staged_games"]
        ]

        diff = compare_staged_to_production_games(payload["staged_games"], production_rows)

        self.assertEqual(diff["matched_game_count"], 2)
        self.assertEqual(diff["staged_only_count"], 0)
        self.assertEqual(diff["production_only_count"], 0)


if __name__ == "__main__":
    unittest.main()
