from __future__ import annotations

import base64
import csv
import io
import sys
import unittest
import zipfile
from datetime import date
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import importer


def archive_payload(*, sgf_path: str = "sgf/game.sgf", manifest_path: str = "ratings-explorer-manifest.csv") -> str:
    manifest = io.StringIO(newline="")
    writer = csv.DictWriter(manifest, fieldnames=[
        "Game_ID", "Tournament_Code", "Round", "Pin_Player_1", "Pin_Player_2",
        "Game_Date", "Sgf_File", "Replace_Existing", "Sgf_Code",
    ])
    writer.writeheader()
    writer.writerow({
        "Game_ID": "12", "Tournament_Code": "event1", "Round": "2",
        "Pin_Player_1": "100", "Pin_Player_2": "200", "Game_Date": "2026-07-25",
        "Sgf_File": sgf_path, "Replace_Existing": "false", "Sgf_Code": "",
    })
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(manifest_path, manifest.getvalue())
        archive.writestr(sgf_path, "(;GM[1]FF[4]SZ[19];B[pd];W[dd])")
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def game(existing: str | None = None):
    return {
        12: {
            "Game_ID": 12, "Tournament_Code": "event1", "Round": 2,
            "Pin_Player_1": 100, "Pin_Player_2": 200, "Game_Date": date(2026, 7, 25),
            "Sgf_Code": existing,
        }
    }


class ImporterTests(unittest.TestCase):
    def test_preview_builds_stable_ready_plan(self):
        with patch.object(importer.storage, "load_games", side_effect=lambda ids: game()):
            first = importer.preview(archive_payload(), archive_name="games.zip")
            second = importer.preview(archive_payload(), archive_name="games.zip")
        self.assertEqual(first["summary"], {"ready": 1})
        self.assertEqual(first["plan_hash"], second["plan_hash"])
        self.assertTrue(first["results"][0]["new_sgf_code"].startswith("archive-event1-12-"))

    def test_existing_record_is_skipped_without_two_part_replace(self):
        with patch.object(importer.storage, "load_games", side_effect=lambda ids: game("already-there")):
            payload = importer.preview(archive_payload(), archive_name="games.zip", replace_existing=True)
        self.assertEqual(payload["summary"], {"skipped": 1})

    def test_apply_requires_matching_preview_hash(self):
        with patch.object(importer.storage, "load_games", side_effect=lambda ids: game()):
            with self.assertRaisesRegex(ValueError, "plan changed"):
                importer.apply(archive_payload(), archive_name="games.zip", expected_plan_hash="wrong")

    def test_apply_uploads_and_updates(self):
        uploaded = []
        updated = []
        with (
            patch.object(importer.storage, "load_games", side_effect=lambda ids: game()),
            patch.object(importer.storage, "upload_sgf", side_effect=lambda code, text: uploaded.append((code, text))),
            patch.object(importer.storage, "update_game_sgf_code", side_effect=lambda game_id, code: updated.append((game_id, code))),
        ):
            preview = importer.preview(archive_payload(), archive_name="games.zip")
            result = importer.apply(archive_payload(), archive_name="games.zip", expected_plan_hash=preview["plan_hash"])
        self.assertEqual(result["summary"], {"applied": 1})
        self.assertTrue(uploaded)
        self.assertEqual(updated, [(12, uploaded[0][0])])

    def test_archive_rejects_traversal(self):
        with patch.object(importer.storage, "load_games", side_effect=lambda ids: game()):
            with self.assertRaisesRegex(ValueError, "Unsafe archive path"):
                importer.preview(archive_payload(sgf_path="../game.sgf"), archive_name="games.zip")


if __name__ == "__main__":
    unittest.main()
