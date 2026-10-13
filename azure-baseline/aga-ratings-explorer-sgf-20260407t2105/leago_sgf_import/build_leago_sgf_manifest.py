import argparse
import csv
import json
import re
import sys
from pathlib import Path
from typing import Any


MANIFEST_COLUMNS = [
    "Game_ID",
    "Tournament_Code",
    "Round",
    "Pin_Player_1",
    "Pin_Player_2",
    "Game_Date",
    "Sgf_File",
    "Replace_Existing",
    "Sgf_Code",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a conservative SGF upload manifest from a Leago AGA download index.")
    parser.add_argument("download_index")
    parser.add_argument("--output-csv")
    parser.add_argument("--replace-existing", action="store_true")
    parser.add_argument("--resolve-sql", action="store_true", help="Fill Game_ID and SQL match fields using ratings.games.")
    return parser.parse_args()


def sgf_prop(text: str, prop: str) -> str:
    match = re.search(rf"{re.escape(prop)}\[((?:\\.|[^\]])*)\]", text)
    if not match:
        return ""
    return match.group(1).replace("\\]", "]").replace("\\\\", "\\").strip()


def normalize_date(value: str) -> str:
    text = str(value or "").strip()
    match = re.search(r"\d{4}-\d{2}-\d{2}", text)
    if match:
        return match.group(0)
    match = re.search(r"(\d{4})(\d{2})(\d{2})", text)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    return text[:10]


def first_value(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def read_sgf(path: Path) -> str:
    for encoding in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    return ""


def value_from_match(match: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = match.get(key)
        if value is not None:
            return str(value)
    return ""


def player_member_id_by_color(match: dict[str, Any], color: str) -> str:
    expected = color.strip().lower()
    for player in match.get("players") or []:
        if not isinstance(player, dict):
            continue
        if str(player.get("color") or "").strip().lower() != expected:
            continue
        return first_value(player.get("memberId"), player.get("organizationMemberKey"), player.get("agaid"))
    return ""


def rows_from_index(index: dict[str, Any], replace_existing: bool) -> list[dict[str, str]]:
    if not index.get("aga_only"):
        raise ValueError("Download index is not marked aga_only=true.")
    rows: list[dict[str, str]] = []
    for record in index.get("records") or []:
        if not record.get("aga_only"):
            raise ValueError(f"Refusing non-AGA record: {record.get('tournament_key')}")
        tournament_code = first_value(record.get("aga_tournament_code"), record.get("tournament_code"))
        for sgf in record.get("sgfs") or []:
            if sgf.get("status") != "downloaded" or not sgf.get("path"):
                continue
            sgf_path = Path(sgf["path"])
            sgf_text = read_sgf(sgf_path)
            match = sgf.get("match") or {}
            rows.append(
                {
                    "Game_ID": first_value(value_from_match(match, "gameId", "game_id", "agaGameId")),
                    "Tournament_Code": first_value(
                        tournament_code,
                        value_from_match(match, "tournamentCode", "tournament_code", "agaTournamentCode"),
                        sgf_prop(sgf_text, "EV"),
                    ),
                    "Round": first_value(value_from_match(match, "round", "roundNumber"), sgf_prop(sgf_text, "RO")),
                    "Pin_Player_1": first_value(value_from_match(match, "pinPlayer1", "blackAgaId", "player1AgaId"), player_member_id_by_color(match, "Black")),
                    "Pin_Player_2": first_value(value_from_match(match, "pinPlayer2", "whiteAgaId", "player2AgaId"), player_member_id_by_color(match, "White")),
                    "Game_Date": normalize_date(first_value(value_from_match(match, "gameDate", "date", "wallTime"), sgf_prop(sgf_text, "DT"))),
                    "Sgf_File": str(sgf_path),
                    "Replace_Existing": "true" if replace_existing else "false",
                    "Sgf_Code": "",
                }
            )
    return rows


def resolve_rows_with_sql(rows: list[dict[str, str]]) -> None:
    app_root = Path(__file__).resolve().parents[1]
    if str(app_root) not in sys.path:
        sys.path.insert(0, str(app_root))
    try:
        import ratings_explorer_support as explorer
    except Exception as exc:
        raise RuntimeError("Could not import ratings_explorer_support for --resolve-sql.") from exc
    conn_str = explorer.get_sql_connection_string()
    if not conn_str:
        raise RuntimeError("Missing SQL connection string for --resolve-sql.")
    for row in rows:
        p1 = row.get("Pin_Player_1")
        p2 = row.get("Pin_Player_2")
        round_text = row.get("Round")
        if not (p1 and p2 and round_text):
            continue
        matches = explorer.query_rows(
            conn_str,
            """
SELECT
    g.[Game_ID],
    g.[Tournament_Code],
    g.[Game_Date],
    g.[Round],
    g.[Pin_Player_1],
    g.[Pin_Player_2]
FROM [ratings].[games] AS g
WHERE ((g.[Pin_Player_1] = ? AND g.[Pin_Player_2] = ?) OR (g.[Pin_Player_1] = ? AND g.[Pin_Player_2] = ?))
  AND g.[Round] = ?
  AND g.[Game_Date] >= '2026-01-01'
ORDER BY g.[Game_Date] DESC, g.[Game_ID] DESC
""",
            [p1, p2, p2, p1, round_text],
        )
        if len(matches) != 1:
            row["Game_ID"] = ""
            continue
        match = matches[0]
        row["Game_ID"] = str(match.get("Game_ID") or "")
        row["Tournament_Code"] = str(match.get("Tournament_Code") or "")
        row["Round"] = str(match.get("Round") or "")
        row["Pin_Player_1"] = str(match.get("Pin_Player_1") or "")
        row["Pin_Player_2"] = str(match.get("Pin_Player_2") or "")
        row["Game_Date"] = normalize_date(str(match.get("Game_Date") or ""))


def main() -> int:
    args = parse_args()
    index_path = Path(args.download_index).resolve()
    index = json.loads(index_path.read_text(encoding="utf-8"))
    rows = rows_from_index(index, args.replace_existing)
    if args.resolve_sql:
        resolve_rows_with_sql(rows)
    output_csv = Path(args.output_csv).resolve() if args.output_csv else index_path.with_name("leago-sgf-manifest.csv")
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANIFEST_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    missing_match_fields = sum(
        1
        for row in rows
        if not row["Game_ID"]
        and not all(row[name] for name in ("Tournament_Code", "Round", "Pin_Player_1", "Pin_Player_2", "Game_Date"))
    )
    print(json.dumps({"rows": len(rows), "missing_match_fields": missing_match_fields, "output_csv": str(output_csv)}, indent=2))
    return 0 if missing_match_fields == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
