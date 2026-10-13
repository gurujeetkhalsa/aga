import json
import unittest
from datetime import date

from rewards import legacy_tournament_awards


class FakeLegacyTournamentAwardAdapter:
    def __init__(self, *, preview=None, result=None):
        self.preview = preview or {
            "RunID": None,
            "DryRun": True,
            "SourceAsOfDate": date(2026, 2, 8),
            "LedgerStartDate": date(2026, 5, 2),
            "InputRowCount": 2,
            "ChapterCount": 2,
            "TournamentCount": 2,
            "RatedGameCount": 197,
            "HostEligibleAwardCount": 2,
            "HostAlreadyAwardedCount": 0,
            "HostNewAwardCount": 2,
            "HostPointTotal": 275932,
            "StateChampionshipGroupCount": 1,
            "StateAlreadyAwardedCount": 0,
            "StateNewAwardCount": 1,
            "StateChampionshipPointTotal": 200000,
            "NewAwardCount": 3,
            "PointTotal": 475932,
        }
        self.result = result or {
            "RunID": 13,
            "Snapshot_Date": date(2026, 5, 2),
            "SummaryJson": (
                '{"processor":"legacy_tournament_gap_award","legacy_gap":true,'
                '"source_balance_as_of_date":"2026-02-08",'
                '"ledger_start_date":"2026-05-02",'
                '"input_row_count":2,"chapter_count":2,"tournament_count":2,'
                '"rated_game_count":197,"host_eligible_award_count":2,'
                '"host_already_awarded_count":0,"host_new_award_count":2,'
                '"host_point_total":275932,"state_championship_group_count":1,'
                '"state_already_awarded_count":0,"state_new_award_count":1,'
                '"state_championship_point_total":200000,'
                '"new_award_count":3,"point_total":475932}'
            ),
        }
        self.queries = []
        self.statements = []

    def query_rows(self, query, params=()):
        self.queries.append((query, tuple(params)))
        if query == legacy_tournament_awards.IMPORT_LEGACY_TOURNAMENT_AWARDS_SQL:
            return [self.preview]
        if query == legacy_tournament_awards.LEGACY_TOURNAMENT_AWARD_RESULT_SQL:
            return [self.result]
        raise AssertionError("Unexpected query")

    def execute_statements(self, statements):
        self.statements.extend(list(statements))


def _rows():
    return [
        legacy_tournament_awards.LegacyTournamentAwardRow(
            source_row_number=1,
            chapter_id=2047,
            chapter_code="DALL",
            chapter_name="Dallas Go Club",
            tournament_date=date(2026, 2, 14),
            tournament_code="schdi20260214",
            rated_game_count=164,
            is_state_championship=True,
        )
    ]


class LegacyTournamentAwardsTest(unittest.TestCase):
    def test_record_parser_accepts_current_csv_headers(self):
        headers = legacy_tournament_awards._resolve_headers(
            ["ChapterID", "ChapterCode", "ChapterName", "Tournament_Date", "Tournament_Code", "Game_ID", "State Championship"]
        )

        row = legacy_tournament_awards.legacy_tournament_award_from_record(
            {
                "ChapterID": "2047",
                "ChapterCode": " dall ",
                "ChapterName": "Dallas Go Club",
                "Tournament_Date": "2/14/2026 0:00",
                "Tournament_Code": "schdi20260214",
                "Game_ID": "164",
                "State Championship": "Y",
            },
            3,
            headers,
        )

        self.assertEqual(row.source_row_number, 3)
        self.assertEqual(row.chapter_id, 2047)
        self.assertEqual(row.chapter_code, "DALL")
        self.assertEqual(row.tournament_date, date(2026, 2, 14))
        self.assertEqual(row.rated_game_count, 164)
        self.assertTrue(row.is_state_championship)

    def test_dry_run_returns_preview_without_writes(self):
        adapter = FakeLegacyTournamentAwardAdapter()

        result = legacy_tournament_awards.import_legacy_tournament_awards(
            adapter,
            _rows(),
            dry_run=True,
        )

        self.assertTrue(result.dry_run)
        self.assertIsNone(result.run_id)
        self.assertEqual(result.new_award_count, 3)
        self.assertEqual(result.point_total, 475932)
        self.assertEqual(adapter.statements, [])

    def test_write_executes_proc_and_reads_summary(self):
        adapter = FakeLegacyTournamentAwardAdapter()

        result = legacy_tournament_awards.import_legacy_tournament_awards(
            adapter,
            _rows(),
            run_type="import",
        )

        self.assertFalse(result.dry_run)
        self.assertEqual(result.run_id, 13)
        self.assertEqual(result.ledger_start_date, date(2026, 5, 2))
        self.assertEqual(len(adapter.statements), 1)
        sql, params = adapter.statements[0]
        self.assertEqual(sql, legacy_tournament_awards.IMPORT_LEGACY_TOURNAMENT_AWARDS_SQL)
        self.assertEqual(params[1:5], (
            False,
            "import",
            legacy_tournament_awards.DEFAULT_SOURCE_AS_OF_DATE,
            legacy_tournament_awards.DEFAULT_LEDGER_START_DATE,
        ))
        self.assertEqual(json.loads(params[0])[0]["tournament_code"], "schdi20260214")
        self.assertEqual(
            adapter.queries[-1][1],
            ("import", date(2026, 5, 2), legacy_tournament_awards.PROCESSOR_NAME),
        )

    def test_rejects_empty_import(self):
        with self.assertRaisesRegex(ValueError, "At least one"):
            legacy_tournament_awards.import_legacy_tournament_awards(
                FakeLegacyTournamentAwardAdapter(),
                [],
            )


if __name__ == "__main__":
    unittest.main()
