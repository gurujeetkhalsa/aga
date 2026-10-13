from __future__ import annotations

import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from azure.storage.blob import BlobServiceClient

import storage


tournament_code = sys.argv[1]
summary = storage.query_rows(
    """
SELECT
    COUNT(*) AS [Tournament_Games],
    SUM(CASE WHEN NULLIF(LTRIM(RTRIM([Sgf_Code])), '') IS NOT NULL THEN 1 ELSE 0 END) AS [Linked_Games],
    COUNT(DISTINCT NULLIF(LTRIM(RTRIM([Sgf_Code])), '')) AS [Distinct_Sgf_Codes]
FROM [ratings].[games]
WHERE [Tournament_Code] = ?
""",
    (tournament_code,),
)[0]
rounds = storage.query_rows(
    """
SELECT [Round], COUNT(*) AS [Linked_Games]
FROM [ratings].[games]
WHERE [Tournament_Code] = ?
  AND NULLIF(LTRIM(RTRIM([Sgf_Code])), '') IS NOT NULL
GROUP BY [Round]
ORDER BY [Round]
""",
    (tournament_code,),
)
samples = storage.query_rows(
    """
WITH ranked AS
(
    SELECT [Game_ID], [Round], [Pin_Player_1], [Pin_Player_2], [Sgf_Code],
           ROW_NUMBER() OVER (PARTITION BY [Round] ORDER BY [Game_ID]) AS rn
    FROM [ratings].[games]
    WHERE [Tournament_Code] = ?
      AND NULLIF(LTRIM(RTRIM([Sgf_Code])), '') IS NOT NULL
)
SELECT [Game_ID], [Round], [Pin_Player_1], [Pin_Player_2], [Sgf_Code]
FROM ranked
WHERE rn = 1
ORDER BY [Round]
""",
    (tournament_code,),
)
all_codes = storage.query_rows(
    """
SELECT [Game_ID], [Sgf_Code]
FROM [ratings].[games]
WHERE [Tournament_Code] = ?
  AND NULLIF(LTRIM(RTRIM([Sgf_Code])), '') IS NOT NULL
ORDER BY [Game_ID]
""",
    (tournament_code,),
)

service = BlobServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"])
container = service.get_container_client(os.environ.get("RATINGS_SGF_CONTAINER", "ratings-game-sgf"))


def blob_exists(row):
    code = str(row["Sgf_Code"])
    return int(row["Game_ID"]), code, container.get_blob_client(f"games/{code}.sgf").exists()


with ThreadPoolExecutor(max_workers=16) as pool:
    blob_checks = list(pool.map(blob_exists, all_codes))
missing_blobs = [{"game_id": game_id, "sgf_code": code} for game_id, code, exists in blob_checks if not exists]

verified = []
for sample in samples:
    code = str(sample["Sgf_Code"])
    payload = container.get_blob_client(f"games/{code}.sgf").download_blob().readall()
    text = payload.decode("utf-8-sig")
    player_black = re.search(r"PB\[([^]]*)\]", text)
    player_white = re.search(r"PW\[([^]]*)\]", text)
    moves = len(re.findall(r";[BW]\[[a-z]{0,2}\]", text))
    verified.append({
        "round": sample["Round"],
        "game_id": sample["Game_ID"],
        "player_1": sample["Pin_Player_1"],
        "player_2": sample["Pin_Player_2"],
        "sgf_code": code,
        "bytes": len(payload),
        "complete": text.lstrip().startswith("(;") and text.rstrip().endswith(")"),
        "moves": moves,
        "black": player_black.group(1) if player_black else None,
        "white": player_white.group(1) if player_white else None,
    })

print(json.dumps({
    "tournament_code": tournament_code,
    "sql": summary,
    "rounds": rounds,
    "exact_sql_referenced_blob_count": len(blob_checks) - len(missing_blobs),
    "missing_blobs": missing_blobs,
    "verified_round_samples": verified,
}, indent=2, default=str))
