import unittest
from datetime import date

from bayrate.merge_member_ratings import (
    build_member_merge_plan,
    build_member_merge_statements,
    merge_member_ratings,
    printable_member_merge_plan,
    validate_member_merge_plan_for_execution,
)


class MergeAdapter:
    def __init__(self, *, rating_rows=None, game_rows=None) -> None:
        self.rating_rows = list(rating_rows or [])
        self.game_rows = list(game_rows or [])
        self.statements = []

    def query_rows(self, query, params=()):
        requested_ids = {int(value) for value in params}
        if "FROM [ratings].[ratings]" in query:
            return [row for row in self.rating_rows if int(row.get("Pin_Player")) in requested_ids]
        if "FROM [ratings].[games]" in query:
            return [
                row
                for row in self.game_rows
                if int(row.get("Pin_Player_1")) in requested_ids or int(row.get("Pin_Player_2")) in requested_ids
            ]
        return []

    def execute_statements(self, statements):
        self.statements.extend(list(statements))


def sample_rating_rows():
    return [
        {
            "Rating_Row_ID": 10,
            "Pin_Player": 31575,
            "Rating": 2.1,
            "Sigma": 0.42,
            "Elab_Date": date(2025, 1, 1),
            "Tournament_Code": "OLD-A",
        },
        {
            "Rating_Row_ID": 11,
            "Pin_Player": 31575,
            "Rating": 2.3,
            "Sigma": 0.4,
            "Elab_Date": date(2025, 5, 1),
            "Tournament_Code": "OLD-B",
        },
        {
            "Rating_Row_ID": 12,
            "Pin_Player": 32194,
            "Rating": 2.4,
            "Sigma": 0.38,
            "Elab_Date": date(2026, 1, 1),
            "Tournament_Code": "CURRENT-C",
        },
    ]


def sample_game_rows():
    return [
        {
            "Game_ID": 100,
            "Tournament_Code": "OLD-A",
            "Game_Date": date(2025, 1, 1),
            "Round": 1,
            "Pin_Player_1": 31575,
            "Pin_Player_2": 20001,
            "Sgf_Code": None,
        },
        {
            "Game_ID": 101,
            "Tournament_Code": "OLD-B",
            "Game_Date": date(2025, 5, 1),
            "Round": 2,
            "Pin_Player_1": 20002,
            "Pin_Player_2": 31575,
            "Sgf_Code": "sgf-old",
        },
        {
            "Game_ID": 102,
            "Tournament_Code": "CURRENT-C",
            "Game_Date": date(2026, 1, 1),
            "Round": 1,
            "Pin_Player_1": 32194,
            "Pin_Player_2": 20003,
            "Sgf_Code": None,
        },
    ]


class MergeMemberRatingsTest(unittest.TestCase):
    def test_build_member_merge_plan_counts_source_updates_without_replay(self) -> None:
        plan = build_member_merge_plan(
            MergeAdapter(rating_rows=sample_rating_rows(), game_rows=sample_game_rows()),
            source_agaids=[31575],
            target_agaid=32194,
        )
        printable = printable_member_merge_plan(plan)

        self.assertEqual(plan["source_agaids"], [31575])
        self.assertEqual(plan["target_agaid"], 32194)
        self.assertEqual(plan["rating_update_count"], 2)
        self.assertEqual(plan["game_player_1_update_count"], 1)
        self.assertEqual(plan["game_player_2_update_count"], 1)
        self.assertEqual(plan["production_write_count"], 4)
        self.assertEqual(printable["source_rating_id_range"], {"first": 10, "last": 11})
        self.assertEqual(printable["source_game_id_range"], {"first": 100, "last": 101})
        self.assertTrue(printable["ready_to_execute"])
        self.assertEqual(len(printable["plan_hash"]), 64)

    def test_build_member_merge_statements_update_rating_and_game_player_columns(self) -> None:
        plan = build_member_merge_plan(
            MergeAdapter(rating_rows=sample_rating_rows(), game_rows=sample_game_rows()),
            source_agaids=[31575],
            target_agaid=32194,
        )

        statements = build_member_merge_statements(plan)
        sql_text = "\n".join(statement[0] for statement in statements)

        self.assertEqual(len(statements), 4)
        self.assertIn("THROW 51042", statements[0][0])
        self.assertIn("UPDATE [ratings].[ratings]", sql_text)
        self.assertIn("UPDATE [ratings].[games]", sql_text)
        self.assertIn("[Pin_Player_1] = ?", sql_text)
        self.assertIn("[Pin_Player_2] = ?", sql_text)
        self.assertEqual(statements[1][1], (32194, 31575))
        self.assertEqual(statements[2][1], (32194, 31575))
        self.assertEqual(statements[3][1], (32194, 31575))

    def test_merge_member_ratings_requires_confirmation(self) -> None:
        with self.assertRaisesRegex(ValueError, "confirm_member_rating_merge"):
            merge_member_ratings(
                MergeAdapter(rating_rows=sample_rating_rows(), game_rows=sample_game_rows()),
                source_agaids=[31575],
                target_agaid=32194,
            )

    def test_merge_member_ratings_executes_generated_statements(self) -> None:
        adapter = MergeAdapter(rating_rows=sample_rating_rows(), game_rows=sample_game_rows())
        preview = build_member_merge_plan(adapter, source_agaids=[31575], target_agaid=32194)
        plan_hash = printable_member_merge_plan(preview)["plan_hash"]

        plan = merge_member_ratings(
            adapter,
            source_agaids=[31575],
            target_agaid=32194,
            confirm_member_rating_merge=True,
            expected_plan_hash=plan_hash,
        )

        self.assertTrue(plan["executed"])
        self.assertEqual(len(adapter.statements), 4)

    def test_merge_member_ratings_rejects_stale_preview_hash(self) -> None:
        with self.assertRaisesRegex(ValueError, "changed since preview"):
            merge_member_ratings(
                MergeAdapter(rating_rows=sample_rating_rows(), game_rows=sample_game_rows()),
                source_agaids=[31575],
                target_agaid=32194,
                confirm_member_rating_merge=True,
                expected_plan_hash="not-the-current-plan",
            )

    def test_validation_rejects_duplicate_tournament_rating_rows_after_merge(self) -> None:
        rows = sample_rating_rows()
        rows[2]["Tournament_Code"] = "OLD-B"
        plan = build_member_merge_plan(
            MergeAdapter(rating_rows=rows, game_rows=[]),
            source_agaids=[31575],
            target_agaid=32194,
        )

        with self.assertRaisesRegex(ValueError, "duplicate ratings rows"):
            validate_member_merge_plan_for_execution(plan)

    def test_preexisting_same_id_duplicate_ratings_are_warnings_not_blockers(self) -> None:
        rows = sample_rating_rows()
        rows.insert(
            2,
            {
                "Rating_Row_ID": 13,
                "Pin_Player": 31575,
                "Rating": 2.35,
                "Sigma": 0.39,
                "Elab_Date": date(2025, 5, 1),
                "Tournament_Code": "OLD-B",
            },
        )

        plan = build_member_merge_plan(
            MergeAdapter(rating_rows=rows, game_rows=[]),
            source_agaids=[31575],
            target_agaid=32194,
        )
        printable = printable_member_merge_plan(plan)

        self.assertEqual(plan["rating_conflicts"], [])
        self.assertEqual(len(plan["preexisting_rating_duplicates"]), 1)
        self.assertTrue(printable["ready_to_execute"])

    def test_validation_rejects_games_that_would_become_self_play(self) -> None:
        game_rows = [
            {
                "Game_ID": 200,
                "Tournament_Code": "BAD",
                "Game_Date": date(2025, 7, 1),
                "Round": 1,
                "Pin_Player_1": 31575,
                "Pin_Player_2": 32194,
                "Sgf_Code": None,
            }
        ]
        plan = build_member_merge_plan(
            MergeAdapter(rating_rows=sample_rating_rows(), game_rows=game_rows),
            source_agaids=[31575],
            target_agaid=32194,
        )

        with self.assertRaisesRegex(ValueError, "plays itself"):
            validate_member_merge_plan_for_execution(plan)


if __name__ == "__main__":
    unittest.main()
