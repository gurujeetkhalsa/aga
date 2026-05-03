import unittest
from contextlib import redirect_stderr
from datetime import date
from io import StringIO

from rewards import rated_game_awards


class FakeAwardAdapter:
    def __init__(self, *, preview=None, result=None):
        self.preview = preview or {
            "ParticipantCount": 4,
            "EligibleAwardCount": 3,
            "AlreadyAwardedCount": 1,
            "NewAwardCount": 2,
            "PointTotal": 1500,
            "MissingMemberSnapshotCount": 0,
            "MissingChapterSnapshotCount": 0,
            "InactivePlayerCount": 1,
            "NoChapterCount": 0,
            "ChapterNotCurrentCount": 0,
        }
        self.result = result or {
            "RunID": 7,
            "SummaryJson": (
                '{"participant_count":4,"eligible_award_count":3,'
                '"already_awarded_count":1,"new_award_count":2,'
                '"point_total":1500,"missing_member_snapshot_count":0,'
                '"missing_chapter_snapshot_count":0,"inactive_player_count":1,'
                '"no_chapter_count":0,"chapter_not_current_count":0}'
            ),
        }
        self.queries = []
        self.statements = []

    def query_rows(self, query, params=()):
        self.queries.append((query, tuple(params)))
        if query == rated_game_awards.RATED_GAME_AWARD_PREVIEW_SQL:
            return [self.preview]
        if query == rated_game_awards.RATED_GAME_AWARD_RESULT_SQL:
            return [self.result]
        raise AssertionError("Unexpected query")

    def execute_statements(self, statements):
        self.statements.extend(list(statements))


class RatedGameAwardsTest(unittest.TestCase):
    def test_dry_run_returns_preview_without_writes(self):
        adapter = FakeAwardAdapter()

        result = rated_game_awards.process_rated_game_awards(
            adapter,
            date(2026, 5, 2),
            date(2026, 5, 2),
            dry_run=True,
        )

        self.assertTrue(result.dry_run)
        self.assertIsNone(result.run_id)
        self.assertEqual(result.participant_count, 4)
        self.assertEqual(result.new_award_count, 2)
        self.assertEqual(result.point_total, 1500)
        self.assertEqual(adapter.statements, [])

    def test_write_executes_award_batch_and_reads_summary(self):
        adapter = FakeAwardAdapter()

        result = rated_game_awards.process_rated_game_awards(
            adapter,
            date(2026, 5, 2),
            date(2026, 5, 2),
            run_type="manual",
        )

        self.assertFalse(result.dry_run)
        self.assertEqual(result.run_id, 7)
        self.assertEqual(result.already_awarded_count, 1)
        self.assertEqual(len(adapter.statements), 1)
        sql, params = adapter.statements[0]
        self.assertEqual(sql, rated_game_awards.CREATE_RATED_GAME_AWARDS_SQL)
        self.assertEqual(
            params,
            (
                date(2026, 5, 2),
                date(2026, 5, 2),
                rated_game_awards.MAX_MEMBER_AGAID,
                rated_game_awards.BASE_POINTS,
                rated_game_awards.SOURCE_TYPE,
                "manual",
                rated_game_awards.RULE_VERSION,
            ),
        )

    def test_rejects_reversed_date_range(self):
        with self.assertRaisesRegex(ValueError, "date_to"):
            rated_game_awards.process_rated_game_awards(
                FakeAwardAdapter(),
                date(2026, 5, 3),
                date(2026, 5, 2),
            )

    def test_cli_rejects_date_and_range_together(self):
        with redirect_stderr(StringIO()):
            with self.assertRaises(SystemExit):
                rated_game_awards.main(
                    [
                        "--date",
                        "2026-05-02",
                        "--date-from",
                        "2026-05-01",
                        "--connection-string",
                        "ignored",
                    ]
                )


if __name__ == "__main__":
    unittest.main()
