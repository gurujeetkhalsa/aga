import csv
import shutil
import unittest
from pathlib import Path

from bayrate.report_parser import _write_csv_exports, parse_report_to_rows, parse_reports_to_rows


FIXTURE_DIR = Path(__file__).parent / "fixtures"


class RatingsReportParserTest(unittest.TestCase):
    def test_parse_single_report_to_rating_rows(self) -> None:
        payload = parse_report_to_rows((FIXTURE_DIR / "report_compact_one.txt").read_text(encoding="utf-8"))

        self.assertEqual(payload["tournament_row"]["Tournament_Descr"], "Migration Sample One")
        self.assertEqual(payload["tournament_row"]["Tournament_Date"].isoformat(), "2026-01-01")
        self.assertEqual(payload["tournament_row"]["Rounds"], 2)
        self.assertEqual(len(payload["game_rows"]), 2)
        self.assertEqual(payload["game_rows"][0]["Pin_Player_1"], 1001)
        self.assertEqual(payload["game_rows"][0]["Color_1"], "W")
        self.assertEqual(payload["game_rows"][0]["Result"], "W")
        self.assertEqual(payload["game_rows"][1]["Round"], "2")

    def test_parse_multiple_reports_and_export_combined_csvs(self) -> None:
        reports = [
            ("report_compact_one.txt", (FIXTURE_DIR / "report_compact_one.txt").read_text(encoding="utf-8")),
            ("report_compact_two.txt", (FIXTURE_DIR / "report_compact_two.txt").read_text(encoding="utf-8")),
        ]

        payload = parse_reports_to_rows(reports)

        self.assertEqual(len(payload["reports"]), 2)
        self.assertEqual(len(payload["tournament_rows"]), 2)
        self.assertEqual(len(payload["game_rows"]), 3)
        self.assertEqual(
            [row["Tournament_Descr"] for row in payload["tournament_rows"]],
            ["Migration Sample One", "Migration Sample Two"],
        )

        output_dir = FIXTURE_DIR.parent / "tmp_report_parser_output"
        if output_dir.exists():
            shutil.rmtree(output_dir)
        try:
            _write_csv_exports(output_dir, payload)

            with (output_dir / "tournaments.csv").open(newline="", encoding="utf-8") as handle:
                tournaments = list(csv.DictReader(handle))
            with (output_dir / "games.csv").open(newline="", encoding="utf-8") as handle:
                games = list(csv.DictReader(handle))
        finally:
            if output_dir.exists():
                shutil.rmtree(output_dir)

        self.assertEqual(len(tournaments), 2)
        self.assertEqual(len(games), 3)
        self.assertEqual(tournaments[0]["Tournament_Descr"], "Migration Sample One")
        self.assertEqual(tournaments[1]["Tournament_Descr"], "Migration Sample Two")

    def test_parse_multiple_reports_can_continue_after_one_failure(self) -> None:
        reports = [
            ("broken.txt", "TOURNEY Broken\n"),
            ("report_compact_one.txt", (FIXTURE_DIR / "report_compact_one.txt").read_text(encoding="utf-8")),
        ]

        with self.assertRaises(ValueError):
            parse_reports_to_rows(reports)

        payload = parse_reports_to_rows(reports, continue_on_error=True)

        self.assertEqual(len(payload["reports"]), 2)
        self.assertEqual(payload["reports"][0]["tournament_row"]["Tournament_Code"], "PARSE-ERROR-1")
        self.assertEqual(payload["reports"][1]["tournament_row"]["Tournament_Descr"], "Migration Sample One")
        self.assertEqual(payload["warnings"][0]["type"], "report_parse_failed")

    def test_question_mark_game_result_is_warned_and_ignored(self) -> None:
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

        payload = parse_report_to_rows(report)

        self.assertEqual(len(payload["game_rows"]), 1)
        self.assertEqual(payload["game_rows"][0]["Pin_Player_2"], 5002)
        warnings = [warning for warning in payload["warnings"] if warning.get("type") == "unreported_game_results"]
        self.assertEqual(len(warnings), 1)
        self.assertEqual(warnings[0]["ignored_game_count"], 1)
        self.assertIn("will not be staged or rated", warnings[0]["message"])
        self.assertFalse(any(warning.get("type") == "game_count_mismatch" for warning in payload["warnings"]))


if __name__ == "__main__":
    unittest.main()
