import unittest
from contextlib import redirect_stderr
from datetime import date
from io import StringIO

from rewards import tournament_awards


class FakeTournamentAwardAdapter:
    def __init__(self, *, preview=None, result=None):
        self.preview = preview or {
            "RunID": None,
            "TournamentDateFrom": None,
            "TournamentDateTo": date(2026, 5, 3),
            "DryRun": True,
            "EventGroupCount": 3,
            "TournamentSectionCount": 5,
            "RatedGameCount": 420,
            "HostEligibleAwardCount": 2,
            "HostAlreadyAwardedCount": 1,
            "HostNewAwardCount": 1,
            "HostPointTotal": 514161,
            "StateChampionshipGroupCount": 1,
            "StateAlreadyAwardedCount": 0,
            "StateNewAwardCount": 1,
            "StateChampionshipPointTotal": 200000,
            "NewAwardCount": 2,
            "PointTotal": 714161,
            "MissingHostChapterCount": 0,
            "MissingRewardEventKeyCount": 0,
        }
        self.result = result or {
            "RunID": 14,
            "Snapshot_Date": date(2026, 5, 3),
            "SummaryJson": (
                '{"processor":"tournament_awards","event_group_count":3,'
                '"tournament_section_count":5,"rated_game_count":420,'
                '"host_eligible_award_count":2,"host_already_awarded_count":1,'
                '"host_new_award_count":1,"host_point_total":514161,'
                '"state_championship_group_count":1,"state_already_awarded_count":0,'
                '"state_new_award_count":1,"state_championship_point_total":200000,'
                '"new_award_count":2,"point_total":714161,'
                '"missing_host_chapter_count":0,"missing_reward_event_key_count":0}'
            ),
        }
        self.queries = []
        self.statements = []

    def query_rows(self, query, params=()):
        self.queries.append((query, tuple(params)))
        if query == tournament_awards.PROCESS_TOURNAMENT_AWARDS_SQL:
            return [self.preview]
        if query == tournament_awards.TOURNAMENT_AWARD_RESULT_SQL:
            return [self.result]
        raise AssertionError("Unexpected query")

    def execute_statements(self, statements):
        self.statements.extend(list(statements))


class TournamentAwardsTest(unittest.TestCase):
    def test_formula_matches_configured_curve(self):
        self.assertEqual(tournament_awards.calculate_tournament_host_points(15), 0)
        self.assertEqual(tournament_awards.calculate_tournament_host_points(16), 2306)
        self.assertEqual(tournament_awards.calculate_tournament_host_points(350), 514161)
        self.assertEqual(tournament_awards.calculate_tournament_host_points(700), 1000000)

    def test_dry_run_returns_preview_without_writes(self):
        adapter = FakeTournamentAwardAdapter()

        result = tournament_awards.process_tournament_awards(
            adapter,
            None,
            date(2026, 5, 3),
            dry_run=True,
        )

        self.assertTrue(result.dry_run)
        self.assertIsNone(result.run_id)
        self.assertIsNone(result.date_from)
        self.assertEqual(result.event_group_count, 3)
        self.assertEqual(result.host_new_award_count, 1)
        self.assertEqual(result.state_new_award_count, 1)
        self.assertEqual(result.point_total, 714161)
        self.assertEqual(adapter.statements, [])

    def test_write_executes_proc_and_reads_summary(self):
        adapter = FakeTournamentAwardAdapter()

        result = tournament_awards.process_tournament_awards(
            adapter,
            date(2026, 5, 1),
            date(2026, 5, 3),
            run_type="manual",
        )

        self.assertFalse(result.dry_run)
        self.assertEqual(result.run_id, 14)
        self.assertEqual(result.state_championship_point_total, 200000)
        self.assertEqual(len(adapter.statements), 1)
        sql, params = adapter.statements[0]
        self.assertEqual(sql, tournament_awards.PROCESS_TOURNAMENT_AWARDS_SQL)
        self.assertEqual(
            params,
            (
                date(2026, 5, 1),
                date(2026, 5, 3),
                "manual",
                False,
                tournament_awards.MIN_GAMES,
                tournament_awards.MAX_GAMES,
                tournament_awards.MAX_SUPPORT,
                tournament_awards.EXPONENT,
                tournament_awards.STATE_CHAMPIONSHIP_POINTS,
                tournament_awards.HOST_SOURCE_TYPE,
                tournament_awards.STATE_SOURCE_TYPE,
                tournament_awards.RULE_VERSION,
            ),
        )
        self.assertEqual(
            adapter.queries[-1][1],
            ("manual", date(2026, 5, 3), tournament_awards.PROCESSOR_NAME),
        )

    def test_rejects_reversed_date_range(self):
        with self.assertRaisesRegex(ValueError, "date_to"):
            tournament_awards.process_tournament_awards(
                FakeTournamentAwardAdapter(),
                date(2026, 5, 4),
                date(2026, 5, 3),
            )

    def test_cli_rejects_date_and_range_together(self):
        with redirect_stderr(StringIO()):
            with self.assertRaises(SystemExit):
                tournament_awards.main(
                    [
                        "--date",
                        "2026-05-03",
                        "--date-from",
                        "2026-05-01",
                        "--connection-string",
                        "ignored",
                    ]
                )


if __name__ == "__main__":
    unittest.main()
