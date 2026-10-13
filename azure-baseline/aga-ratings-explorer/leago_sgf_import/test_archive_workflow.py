from __future__ import annotations

import base64
import csv
import io
import unittest
import zipfile
from datetime import date
from unittest.mock import patch

from leago_sgf_import import archive_workflow
from leago_sgf_import.build_leago_sgf_manifest import MANIFEST_COLUMNS


class ArchiveWorkflowTests(unittest.TestCase):
    def test_preview_is_stable_and_uses_exact_game_mapping(self) -> None:
        encoded = self._archive_base64()
        game = self._game()
        with patch.object(
            archive_workflow.bulk_upload.explorer,
            "load_game_for_sgf_upload",
            return_value=game,
        ):
            first = archive_workflow.preview_archive_import("unused", encoded, archive_name="records.zip")
            second = archive_workflow.preview_archive_import("unused", encoded, archive_name="records.zip")

        self.assertEqual(first["summary"], {"ready": 1})
        self.assertEqual(first["plan_hash"], second["plan_hash"])
        self.assertEqual(first["results"][0]["game_id"], 12345)
        self.assertEqual(first["results"][0]["sgf_file"], "sgf/game.sgf")
        self.assertRegex(first["results"][0]["new_sgf_code"], r"^archive-usopenbost20260725-12345-")

    def test_apply_rejects_changed_plan_hash(self) -> None:
        encoded = self._archive_base64()
        with patch.object(
            archive_workflow.bulk_upload.explorer,
            "load_game_for_sgf_upload",
            return_value=self._game(),
        ):
            with self.assertRaisesRegex(ValueError, "plan changed"):
                archive_workflow.apply_archive_import(
                    "unused",
                    encoded,
                    archive_name="records.zip",
                    expected_plan_hash="not-the-preview-hash",
                )

    def test_plan_hash_binds_the_archive_bytes(self) -> None:
        first_archive = self._archive_base64(sgf_code="fixed-code")
        changed_archive = self._archive_base64(
            sgf_code="fixed-code",
            sgf_text="(;GM[1]FF[4]SZ[19]PB[Black Player]PW[White Player]RE[B+R];B[qd];W[dc])",
        )
        with patch.object(
            archive_workflow.bulk_upload.explorer,
            "load_game_for_sgf_upload",
            return_value=self._game(),
        ):
            preview = archive_workflow.preview_archive_import("unused", first_archive, archive_name="records.zip")
            with self.assertRaisesRegex(ValueError, "plan changed"):
                archive_workflow.apply_archive_import(
                    "unused",
                    changed_archive,
                    archive_name="records.zip",
                    expected_plan_hash=preview["plan_hash"],
                )

    def test_apply_uploads_and_updates_the_exact_planned_game(self) -> None:
        encoded = self._archive_base64()
        game = self._game()
        with patch.object(
            archive_workflow.bulk_upload.explorer,
            "load_game_for_sgf_upload",
            return_value=game,
        ):
            preview = archive_workflow.preview_archive_import("unused", encoded, archive_name="records.zip")
        expected_code = preview["results"][0]["new_sgf_code"]
        updated_game = dict(game, Sgf_Code=expected_code)
        with (
            patch.object(
                archive_workflow.bulk_upload.explorer,
                "load_game_for_sgf_upload",
                side_effect=[game, updated_game],
            ),
            patch.object(archive_workflow.bulk_upload.explorer, "upload_sgf_blob") as upload,
            patch.object(archive_workflow.bulk_upload.explorer, "update_game_sgf_code") as update,
        ):
            applied = archive_workflow.apply_archive_import(
                "unused",
                encoded,
                archive_name="records.zip",
                expected_plan_hash=preview["plan_hash"],
            )

        self.assertEqual(applied["summary"], {"applied": 1})
        upload.assert_called_once()
        self.assertEqual(upload.call_args.args[0], expected_code)
        update.assert_called_once_with("unused", 12345, expected_code)

    def test_archive_rejects_path_traversal(self) -> None:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("../ratings-explorer-manifest.csv", "bad")
        encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
        with self.assertRaisesRegex(ValueError, "Unsafe archive path"):
            archive_workflow.preview_archive_import("unused", encoded, archive_name="records.zip")

    def test_replacement_requires_manifest_and_operator_opt_in(self) -> None:
        encoded = self._archive_base64(replace_existing="true")
        existing_game = self._game()
        existing_game["Sgf_Code"] = "existing-code"
        with patch.object(
            archive_workflow.bulk_upload.explorer,
            "load_game_for_sgf_upload",
            return_value=existing_game,
        ):
            protected = archive_workflow.preview_archive_import("unused", encoded, archive_name="records.zip")
            allowed = archive_workflow.preview_archive_import(
                "unused",
                encoded,
                archive_name="records.zip",
                replace_existing=True,
            )

        self.assertEqual(protected["summary"], {"skipped": 1})
        self.assertEqual(allowed["summary"], {"ready": 1})
        self.assertEqual(allowed["results"][0]["action"], "replace")

    @staticmethod
    def _game() -> dict:
        return {
            "Game_ID": 12345,
            "Tournament_Code": "usopenbost20260725",
            "Game_Date": date(2026, 7, 25),
            "Round": 1,
            "Result": "B+R",
            "Handicap": 0,
            "Komi": 7.5,
            "Sgf_Code": None,
            "Pin_Player_1": 10001,
            "Pin_Player_2": 10002,
            "Player1FirstName": "Black",
            "Player1LastName": "Player",
            "Player2FirstName": "White",
            "Player2LastName": "Player",
        }

    @staticmethod
    def _archive_base64(
        *,
        replace_existing: str = "false",
        sgf_code: str = "",
        sgf_text: str = "(;GM[1]FF[4]SZ[19]PB[Black Player]PW[White Player]RE[B+R];B[pd];W[dd])",
    ) -> str:
        row = {
            "Game_ID": "12345",
            "Tournament_Code": "usopenbost20260725",
            "Round": "1",
            "Pin_Player_1": "10001",
            "Pin_Player_2": "10002",
            "Game_Date": "2026-07-25",
            "Sgf_File": "sgf/game.sgf",
            "Replace_Existing": replace_existing,
            "Sgf_Code": sgf_code,
        }
        manifest = io.StringIO(newline="")
        writer = csv.DictWriter(manifest, fieldnames=MANIFEST_COLUMNS)
        writer.writeheader()
        writer.writerow(row)
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("package/ratings-explorer-manifest.csv", manifest.getvalue())
            archive.writestr("package/sgf/game.sgf", sgf_text)
        return base64.b64encode(buffer.getvalue()).decode("ascii")


if __name__ == "__main__":
    unittest.main()
