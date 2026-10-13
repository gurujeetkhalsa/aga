"""Add regional territory to the June 1 chapter directory CSV."""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bayrate.sql_adapter import SqlAdapter, get_sql_connection_string


EASTERN = {
    "ME",
    "RI",
    "VT",
    "NH",
    "CT",
    "MA",
    "NY",
    "PA",
    "NJ",
    "DE",
    "MD",
    "VA",
    "NC",
    "SC",
    "GA",
    "FL",
    "DC",
    "PR",
    "VI",
}

CENTRAL = {
    "WV",
    "IA",
    "AR",
    "OH",
    "IL",
    "IN",
    "MI",
    "WI",
    "MN",
    "ND",
    "SD",
    "MT",
    "ID",
    "WY",
    "CO",
    "NV",
    "NE",
    "TN",
    "OK",
    "KS",
    "UT",
    "MO",
    "MS",
    "LA",
    "AL",
    "TX",
    "NM",
    "KY",
    "AZ",
}

WESTERN = {"WA", "OR", "CA", "AK", "HI", "AS", "MP", "GU", "UM"}

CHAPTER_STATE_OVERRIDES = {
    # SQL has a blank state for BLGO, but the source chapter export lists KY.
    32292: "KY",
}

STATE_ALIASES = {
    "WASHINGTON DC": "DC",
    "WASHINGTON, DC": "DC",
    "DISTRICT OF COLUMBIA": "DC",
    "D.C.": "DC",
    "PUERTO RICO": "PR",
    "VIRGIN ISLANDS": "VI",
    "U.S. VIRGIN ISLANDS": "VI",
    "US VIRGIN ISLANDS": "VI",
    "VIRGIN ISLANDS OF THE UNITED STATES": "VI",
    "AMERICAN SAMOA": "AS",
    "COMMONWEALTH OF NORTHERN MARIANAS": "MP",
    "COMMONWEALTH OF THE NORTHERN MARIANA ISLANDS": "MP",
    "NORTHERN MARIANA ISLANDS": "MP",
    "GUAM": "GU",
    "WAKE ISLAND": "UM",
    "MIDWAY ATOLL": "UM",
}

STATE_LOOKUP_SQL = """
SELECT
    [ChapterID],
    NULLIF(LTRIM(RTRIM([State])), N'') AS [State]
FROM [membership].[chapters]
WHERE [ChapterID] IN ({placeholders})
"""


def normalize_state(value: object) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    if text in STATE_ALIASES:
        return STATE_ALIASES[text]
    return text


def territory_for_state(value: object) -> str:
    state = normalize_state(value)
    if state in EASTERN:
        return "Eastern"
    if state in CENTRAL:
        return "Central"
    if state in WESTERN:
        return "Western"
    return ""


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def lookup_chapter_states(chapter_ids: list[int]) -> dict[int, str]:
    conn_str = get_sql_connection_string()
    if not conn_str:
        raise RuntimeError("Missing SQL connection string.")
    placeholders = ", ".join("?" for _ in chapter_ids)
    query = STATE_LOOKUP_SQL.format(placeholders=placeholders)
    rows = SqlAdapter(conn_str).query_rows(query, chapter_ids)
    states = {int(row["ChapterID"]): str(row.get("State") or "").strip() for row in rows}
    for chapter_id, state in CHAPTER_STATE_OVERRIDES.items():
        if chapter_id in chapter_ids:
            states[chapter_id] = state
    return states


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    rows = read_csv(args.input)
    if not rows:
        raise RuntimeError(f"No rows found in {args.input}")
    fieldnames = list(rows[0].keys())
    if "ChapterID" not in fieldnames:
        raise RuntimeError("Input CSV must include ChapterID.")

    chapter_ids = sorted({int(row["ChapterID"]) for row in rows if str(row.get("ChapterID") or "").strip()})
    states_by_id = lookup_chapter_states(chapter_ids)

    unresolved_state_count = 0
    unresolved_territory_count = 0
    for row in rows:
        chapter_id = int(row["ChapterID"])
        state = states_by_id.get(chapter_id, "")
        territory = territory_for_state(state)
        if not state:
            unresolved_state_count += 1
        if not territory:
            unresolved_territory_count += 1
        row["Territory"] = territory

    output_fields = fieldnames + ([] if "Territory" in fieldnames else ["Territory"])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    write_csv(args.output, rows, output_fields)

    print(f"input_rows={len(rows)}")
    print(f"looked_up_chapters={len(states_by_id)}")
    print(f"unresolved_state_count={unresolved_state_count}")
    print(f"unresolved_territory_count={unresolved_territory_count}")
    print(f"output={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
