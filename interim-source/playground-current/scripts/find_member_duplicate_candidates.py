"""Generate read-only AGA member duplicate candidate reports.

The report groups membership rows by a conservative normalized first/last name
key and prioritizes groups where multiple AGAIDs have rating history.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bayrate.sql_adapter import SqlAdapter, get_sql_connection_string  # noqa: E402


DUPLICATE_CANDIDATE_SQL = """
WITH rating_rows AS
(
    SELECT
        r.[Pin_Player] AS [AGAID],
        COUNT_BIG(*) AS [RatingRowCount],
        COUNT(DISTINCT NULLIF(LTRIM(RTRIM(r.[Tournament_Code])), N'')) AS [RatingTournamentCount],
        MIN(CAST(r.[Elab_Date] AS date)) AS [FirstRatingDate],
        MAX(CAST(r.[Elab_Date] AS date)) AS [LastRatingDate]
    FROM [ratings].[ratings] AS r
    WHERE r.[Pin_Player] IS NOT NULL
      AND r.[Pin_Player] < 50000
    GROUP BY r.[Pin_Player]
),
current_ratings AS
(
    SELECT
        ranked.[AGAID],
        ranked.[Rating],
        ranked.[Sigma],
        ranked.[RatingDate],
        ranked.[TournamentCode]
    FROM
    (
        SELECT
            r.[Pin_Player] AS [AGAID],
            r.[Rating],
            r.[Sigma],
            r.[Elab_Date] AS [RatingDate],
            r.[Tournament_Code] AS [TournamentCode],
            ROW_NUMBER() OVER
            (
                PARTITION BY r.[Pin_Player]
                ORDER BY r.[Elab_Date] DESC, r.[id] DESC
            ) AS [rn]
        FROM [ratings].[ratings] AS r
        WHERE r.[Pin_Player] IS NOT NULL
          AND r.[Pin_Player] < 50000
    ) AS ranked
    WHERE ranked.[rn] = 1
),
game_rows AS
(
    SELECT
        player_rows.[AGAID],
        COUNT_BIG(*) AS [GameCount],
        COUNT(DISTINCT NULLIF(LTRIM(RTRIM(player_rows.[Tournament_Code])), N'')) AS [GameTournamentCount],
        MIN(CAST(player_rows.[Game_Date] AS date)) AS [FirstGameDate],
        MAX(CAST(player_rows.[Game_Date] AS date)) AS [LastGameDate]
    FROM
    (
        SELECT
            g.[Pin_Player_1] AS [AGAID],
            g.[Tournament_Code],
            g.[Game_Date]
        FROM [ratings].[games] AS g
        WHERE g.[Pin_Player_1] IS NOT NULL
          AND g.[Pin_Player_1] < 50000
        UNION ALL
        SELECT
            g.[Pin_Player_2] AS [AGAID],
            g.[Tournament_Code],
            g.[Game_Date]
        FROM [ratings].[games] AS g
        WHERE g.[Pin_Player_2] IS NOT NULL
          AND g.[Pin_Player_2] < 50000
    ) AS player_rows
    GROUP BY player_rows.[AGAID]
),
member_base AS
(
    SELECT
        m.[AGAID],
        NULLIF(LTRIM(RTRIM(m.[FirstName])), N'') AS [FirstName],
        NULLIF(LTRIM(RTRIM(m.[LastName])), N'') AS [LastName],
        LOWER(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(COALESCE(m.[FirstName], N''))), N' ', N''), N'.', N''), N'''', N''), N'-', N'')) AS [NormalizedFirstName],
        LOWER(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(COALESCE(m.[LastName], N''))), N' ', N''), N'.', N''), N'''', N''), N'-', N'')) AS [NormalizedLastName],
        NULLIF(LOWER(LTRIM(RTRIM(m.[EmailAddress]))), N'') AS [NormalizedEmail],
        CAST(m.[DateOfBirth] AS date) AS [DateOfBirth],
        m.[Status],
        m.[MemberType],
        CAST(m.[ExpirationDate] AS date) AS [ExpirationDate],
        m.[State],
        c.[ChapterCode],
        c.[ChapterName],
        cr.[Rating],
        cr.[Sigma],
        cr.[RatingDate],
        cr.[TournamentCode] AS [CurrentRatingTournamentCode],
        COALESCE(rr.[RatingRowCount], 0) AS [RatingRowCount],
        COALESCE(rr.[RatingTournamentCount], 0) AS [RatingTournamentCount],
        rr.[FirstRatingDate],
        rr.[LastRatingDate],
        COALESCE(gr.[GameCount], 0) AS [GameCount],
        COALESCE(gr.[GameTournamentCount], 0) AS [GameTournamentCount],
        gr.[FirstGameDate],
        gr.[LastGameDate]
    FROM [membership].[members] AS m
    LEFT JOIN [membership].[chapters] AS c
        ON c.[ChapterID] = m.[ChapterID]
    LEFT JOIN current_ratings AS cr
        ON cr.[AGAID] = m.[AGAID]
    LEFT JOIN rating_rows AS rr
        ON rr.[AGAID] = m.[AGAID]
    LEFT JOIN game_rows AS gr
        ON gr.[AGAID] = m.[AGAID]
    WHERE m.[AGAID] < 50000
      AND NULLIF(LTRIM(RTRIM(m.[FirstName])), N'') IS NOT NULL
      AND NULLIF(LTRIM(RTRIM(m.[LastName])), N'') IS NOT NULL
),
duplicate_keys AS
(
    SELECT
        b.[NormalizedFirstName],
        b.[NormalizedLastName],
        COUNT(*) AS [MemberCount],
        SUM(CASE WHEN b.[RatingRowCount] > 0 THEN 1 ELSE 0 END) AS [RatedAGAIDCount],
        SUM(CAST(b.[RatingRowCount] AS bigint)) AS [TotalRatingRows],
        SUM(CAST(b.[GameCount] AS bigint)) AS [TotalGameRows],
        CASE
            WHEN COUNT(b.[NormalizedEmail]) > COUNT(DISTINCT b.[NormalizedEmail]) THEN 1
            ELSE 0
        END AS [HasSharedEmail],
        CASE
            WHEN COUNT(b.[DateOfBirth]) > COUNT(DISTINCT b.[DateOfBirth]) THEN 1
            ELSE 0
        END AS [HasSharedBirthdate]
    FROM member_base AS b
    GROUP BY b.[NormalizedFirstName], b.[NormalizedLastName]
    HAVING COUNT(*) >= ?
       AND SUM(CASE WHEN b.[RatingRowCount] > 0 THEN 1 ELSE 0 END) >= ?
)
SELECT
    CONCAT(dk.[NormalizedLastName], N'|', dk.[NormalizedFirstName]) AS [MatchKey],
    dk.[MemberCount],
    dk.[RatedAGAIDCount],
    dk.[TotalRatingRows],
    dk.[TotalGameRows],
    dk.[HasSharedEmail],
    dk.[HasSharedBirthdate],
    b.[AGAID],
    b.[FirstName],
    b.[LastName],
    b.[Status],
    b.[MemberType],
    b.[ExpirationDate],
    b.[State],
    b.[ChapterCode],
    b.[ChapterName],
    b.[Rating],
    b.[Sigma],
    b.[RatingDate],
    b.[CurrentRatingTournamentCode],
    b.[RatingRowCount],
    b.[RatingTournamentCount],
    b.[FirstRatingDate],
    b.[LastRatingDate],
    b.[GameCount],
    b.[GameTournamentCount],
    b.[FirstGameDate],
    b.[LastGameDate]
FROM member_base AS b
INNER JOIN duplicate_keys AS dk
    ON dk.[NormalizedFirstName] = b.[NormalizedFirstName]
   AND dk.[NormalizedLastName] = b.[NormalizedLastName]
ORDER BY
    CASE WHEN dk.[RatedAGAIDCount] = dk.[MemberCount] THEN 0 ELSE 1 END,
    dk.[RatedAGAIDCount] DESC,
    dk.[TotalRatingRows] DESC,
    dk.[MemberCount] DESC,
    b.[LastName],
    b.[FirstName],
    b.[AGAID];
"""


DETAIL_COLUMNS = [
    "GroupRank",
    "MatchKey",
    "MemberCount",
    "RatedAGAIDCount",
    "AllAGAIDsHaveRatings",
    "HasSharedEmail",
    "HasSharedBirthdate",
    "AGAID",
    "FirstName",
    "LastName",
    "Status",
    "MemberType",
    "ExpirationDate",
    "State",
    "ChapterCode",
    "ChapterName",
    "Rating",
    "Sigma",
    "RatingDate",
    "CurrentRatingTournamentCode",
    "RatingRowCount",
    "RatingTournamentCount",
    "FirstRatingDate",
    "LastRatingDate",
    "GameCount",
    "GameTournamentCount",
    "FirstGameDate",
    "LastGameDate",
]

SUMMARY_COLUMNS = [
    "GroupRank",
    "MatchKey",
    "MemberCount",
    "RatedAGAIDCount",
    "AllAGAIDsHaveRatings",
    "HasSharedEmail",
    "HasSharedBirthdate",
    "TotalRatingRows",
    "TotalGameRows",
    "AGAIDs",
    "Names",
    "Statuses",
    "MemberTypes",
    "States",
    "ChapterCodes",
    "RatingsByAGAID",
    "RatingRowsByAGAID",
    "LastRatingDatesByAGAID",
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-members", type=int, default=2, help="Minimum members in an exact-name group.")
    parser.add_argument(
        "--min-rated-agaids",
        type=int,
        default=2,
        help="Minimum AGAIDs in the group that must have at least one rating row.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "docs"),
        help="Directory for generated CSV/JSON summary files.",
    )
    args = parser.parse_args()

    if args.min_members < 2:
        raise SystemExit("--min-members must be at least 2.")
    if args.min_rated_agaids < 0:
        raise SystemExit("--min-rated-agaids must be 0 or greater.")

    conn_str = get_sql_connection_string()
    if not conn_str:
        raise SystemExit("SQL_CONNECTION_STRING was not found in the environment or local.settings.json.")

    adapter = SqlAdapter(conn_str)
    rows = adapter.query_rows(DUPLICATE_CANDIDATE_SQL, (args.min_members, args.min_rated_agaids))
    rows = [_json_safe_row(row) for row in rows]
    groups = _rank_groups(rows)

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    detail_path = output_dir / f"aga_duplicate_member_candidates_detail_{timestamp}.csv"
    summary_path = output_dir / f"aga_duplicate_member_candidates_summary_{timestamp}.csv"
    high_confidence_summary_path = output_dir / f"aga_duplicate_member_candidates_high_confidence_{timestamp}.csv"
    metadata_path = output_dir / f"aga_duplicate_member_candidates_metadata_{timestamp}.json"

    detail_rows = _detail_rows(groups)
    summary_rows = _summary_rows(groups)
    high_confidence_rows = [
        row
        for row in summary_rows
        if row["AllAGAIDsHaveRatings"] and (row["HasSharedEmail"] or row["HasSharedBirthdate"])
    ]
    _write_csv(detail_path, DETAIL_COLUMNS, detail_rows)
    _write_csv(summary_path, SUMMARY_COLUMNS, summary_rows)
    _write_csv(high_confidence_summary_path, SUMMARY_COLUMNS, high_confidence_rows)
    metadata = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "min_members": args.min_members,
        "min_rated_agaids": args.min_rated_agaids,
        "group_count": len(groups),
        "member_row_count": len(rows),
        "all_agaids_have_ratings_group_count": sum(1 for group in groups if group["all_agaids_have_ratings"]),
        "high_confidence_group_count": len(high_confidence_rows),
        "detail_path": str(detail_path),
        "summary_path": str(summary_path),
        "high_confidence_summary_path": str(high_confidence_summary_path),
    }
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")

    print(f"Duplicate candidate groups: {metadata['group_count']}")
    print(f"Member rows in candidate groups: {metadata['member_row_count']}")
    print(f"Groups where every AGAID has ratings: {metadata['all_agaids_have_ratings_group_count']}")
    print(f"High-confidence groups with shared email or birthdate: {metadata['high_confidence_group_count']}")
    print(f"Summary CSV: {summary_path}")
    print(f"High-confidence CSV: {high_confidence_summary_path}")
    print(f"Detail CSV: {detail_path}")
    print(f"Metadata JSON: {metadata_path}")
    return 0


def _json_safe_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _json_safe_value(value) for key, value in row.items()}


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    return value


def _rank_groups(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["MatchKey"])].append(row)

    groups = []
    for match_key, members in grouped.items():
        first = members[0]
        member_count = int(first.get("MemberCount") or len(members))
        rated_count = int(first.get("RatedAGAIDCount") or 0)
        groups.append(
            {
                "match_key": match_key,
                "members": members,
                "member_count": member_count,
                "rated_agaid_count": rated_count,
                "all_agaids_have_ratings": rated_count == member_count,
                "total_rating_rows": int(first.get("TotalRatingRows") or 0),
                "total_game_rows": int(first.get("TotalGameRows") or 0),
                "has_shared_email": bool(first.get("HasSharedEmail")),
                "has_shared_birthdate": bool(first.get("HasSharedBirthdate")),
            }
        )

    groups.sort(
        key=lambda group: (
            not group["all_agaids_have_ratings"],
            -group["rated_agaid_count"],
            -group["total_rating_rows"],
            -group["member_count"],
            group["match_key"],
        )
    )
    for index, group in enumerate(groups, start=1):
        group["rank"] = index
    return groups


def _detail_rows(groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    detail_rows: list[dict[str, Any]] = []
    for group in groups:
        for member in group["members"]:
            detail = {key: member.get(key) for key in DETAIL_COLUMNS}
            detail.update(
                {
                    "GroupRank": group["rank"],
                    "MatchKey": group["match_key"],
                    "MemberCount": group["member_count"],
                    "RatedAGAIDCount": group["rated_agaid_count"],
                    "AllAGAIDsHaveRatings": group["all_agaids_have_ratings"],
                    "HasSharedEmail": group["has_shared_email"],
                    "HasSharedBirthdate": group["has_shared_birthdate"],
                }
            )
            detail_rows.append(detail)
    return detail_rows


def _summary_rows(groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summary_rows = []
    for group in groups:
        members = group["members"]
        summary_rows.append(
            {
                "GroupRank": group["rank"],
                "MatchKey": group["match_key"],
                "MemberCount": group["member_count"],
                "RatedAGAIDCount": group["rated_agaid_count"],
                "AllAGAIDsHaveRatings": group["all_agaids_have_ratings"],
                "HasSharedEmail": group["has_shared_email"],
                "HasSharedBirthdate": group["has_shared_birthdate"],
                "TotalRatingRows": group["total_rating_rows"],
                "TotalGameRows": group["total_game_rows"],
                "AGAIDs": _join(member.get("AGAID") for member in members),
                "Names": _join(_display_name(member) for member in members),
                "Statuses": _join_unique(member.get("Status") for member in members),
                "MemberTypes": _join_unique(member.get("MemberType") for member in members),
                "States": _join_unique(member.get("State") for member in members),
                "ChapterCodes": _join_unique(member.get("ChapterCode") for member in members),
                "RatingsByAGAID": _join(
                    f"{member.get('AGAID')}={_format_blank(member.get('Rating'))}" for member in members
                ),
                "RatingRowsByAGAID": _join(
                    f"{member.get('AGAID')}={_format_blank(member.get('RatingRowCount'))}" for member in members
                ),
                "LastRatingDatesByAGAID": _join(
                    f"{member.get('AGAID')}={_format_blank(member.get('LastRatingDate'))}" for member in members
                ),
            }
        )
    return summary_rows


def _display_name(member: dict[str, Any]) -> str:
    first = str(member.get("FirstName") or "").strip()
    last = str(member.get("LastName") or "").strip()
    return " ".join(part for part in (first, last) if part)


def _join(values: Any) -> str:
    return " | ".join(_format_blank(value) for value in values)


def _join_unique(values: Any) -> str:
    return " | ".join(sorted({text for value in values if (text := _format_blank(value))}))


def _format_blank(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


if __name__ == "__main__":
    raise SystemExit(main())
