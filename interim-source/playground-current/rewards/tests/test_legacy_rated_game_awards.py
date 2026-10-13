import unittest
from datetime import date

from rewards import legacy_rated_game_awards


class FakeLegacyRatedGameAwardAdapter:
    def __init__(self, *, preview=None, result=None):
        self.preview = preview or {
            "RunID": None,
            "DryRun": True,
            "GameIDFrom": 1290000,
            "GameIDTo": 1290100,
            "SourceAsOfDate": date(2026, 2, 8),
            "LedgerStartDate": date(2026, 5, 2),
            "ValuationSnapshotDate": date(2026, 5, 2),
            "GameCount": 10,
            "ParticipantCount": 20,
            "EligibleAwardCount": 12,
            "AlreadyAwardedCount": 2,
            "NewAwardCount": 10,
            "PointTotal": 6500,
            "MissingMemberSnapshotCount": 1,
            "MissingChapterSnapshotCount": 0,
            "InactivePlayerCount": 3,
            "NoChapterCount": 4,
            "ChapterNotCurrentCount": 0,
        }
        self.result = result or {
            "RunID": 11,
            "Snapshot_Date": date(2026, 5, 2),
            "SummaryJson": (
                '{"processor":"rated_game_participation","legacy_gap":true,'
                '"game_id_from":1290000,"game_id_to":1290100,'
                '"source_balance_as_of_date":"2026-02-08",'
                '"ledger_start_date":"2026-05-02",'
                '"valuation_snapshot_date":"2026-05-02",'
                '"game_count":10,"participant_count":20,'
                '"eligible_award_count":12,"already_awarded_count":2,'
                '"new_award_count":10,"point_total":6500,'
                '"missing_member_snapshot_count":1,'
                '"missing_chapter_snapshot_count":0,'
                '"inactive_player_count":3,"no_chapter_count":4,'
                '"chapter_not_current_count":0}'
            ),
        }
        self.queries = []
        self.statements = []

    def query_rows(self, query, params=()):
        self.queries.append((query, tuple(params)))
        if query == legacy_rated_game_awards.PROCESS_LEGACY_RATED_GAME_AWARDS_SQL:
            return [self.preview]
        if query == legacy_rated_game_awards.LEGACY_RATED_GAME_AWARD_RESULT_SQL:
            return [self.result]
        raise AssertionError("Unexpected query")

    def execute_statements(self, statements):
        self.statements.extend(list(statements))


class LegacyRatedGameAwardsTest(unittest.TestCase):
    def test_dry_run_returns_preview_without_writes(self):
        adapter = FakeLegacyRatedGameAwardAdapter()

        result = legacy_rated_game_awards.process_legacy_rated_game_awards(
            adapter,
            1290000,
            1290100,
            dry_run=True,
        )

        self.assertTrue(result.dry_run)
        self.assertIsNone(result.run_id)
        self.assertEqual(result.game_count, 10)
        self.assertEqual(result.new_award_count, 10)
        self.assertEqual(result.point_total, 6500)
        self.assertEqual(adapter.statements, [])

    def test_write_executes_proc_and_reads_summary(self):
        adapter = FakeLegacyRatedGameAwardAdapter()

        result = legacy_rated_game_awards.process_legacy_rated_game_awards(
            adapter,
            1290000,
            1290100,
            run_type="import",
        )

        self.assertFalse(result.dry_run)
        self.assertEqual(result.run_id, 11)
        self.assertEqual(result.valuation_snapshot_date, date(2026, 5, 2))
        self.assertEqual(len(adapter.statements), 1)
        sql, params = adapter.statements[0]
        self.assertEqual(sql, legacy_rated_game_awards.PROCESS_LEGACY_RATED_GAME_AWARDS_SQL)
        self.assertEqual(
            params,
            (
                1290000,
                1290100,
                legacy_rated_game_awards.DEFAULT_SOURCE_AS_OF_DATE,
                legacy_rated_game_awards.DEFAULT_LEDGER_START_DATE,
                legacy_rated_game_awards.DEFAULT_VALUATION_SNAPSHOT_DATE,
                "import",
                False,
                legacy_rated_game_awards.MAX_MEMBER_AGAID,
                legacy_rated_game_awards.BASE_POINTS,
            ),
        )
        self.assertEqual(
            adapter.queries[-1][1],
            ("import", date(2026, 5, 2), legacy_rated_game_awards.SOURCE_TYPE),
        )

    def test_rejects_reversed_game_id_range(self):
        with self.assertRaisesRegex(ValueError, "game_id_to"):
            legacy_rated_game_awards.process_legacy_rated_game_awards(
                FakeLegacyRatedGameAwardAdapter(),
                1290100,
                1290000,
            )


if __name__ == "__main__":
    unittest.main()
