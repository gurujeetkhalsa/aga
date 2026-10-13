"""Generate one-to-one AGA member duplicate candidate reports.

Each candidate pair must have matching normalized first and last names plus at
least one shared member field: exact email, email name before @, birthdate,
state, address, or phone. By default the report only includes pairs where both
AGAIDs have rating rows, which keeps the output focused on BayRate merge
candidates.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
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


MEMBER_DUPLICATE_PAIR_SQL = """
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
      AND r.[Pin_Player] < ?
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
          AND r.[Pin_Player] < ?
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
          AND g.[Pin_Player_1] < ?
        UNION ALL
        SELECT
            g.[Pin_Player_2] AS [AGAID],
            g.[Tournament_Code],
            g.[Game_Date]
        FROM [ratings].[games] AS g
        WHERE g.[Pin_Player_2] IS NOT NULL
          AND g.[Pin_Player_2] < ?
    ) AS player_rows
    GROUP BY player_rows.[AGAID]
)
SELECT
    m.[AGAID],
    NULLIF(LTRIM(RTRIM(m.[FirstName])), N'') AS [FirstName],
    NULLIF(LTRIM(RTRIM(m.[LastName])), N'') AS [LastName],
    m.[Status],
    m.[MemberType],
    CAST(m.[ExpirationDate] AS date) AS [ExpirationDate],
    NULLIF(LTRIM(RTRIM(m.[EmailAddress])), N'') AS [EmailAddress],
    CAST(m.[DateOfBirth] AS date) AS [DateOfBirth],
    NULLIF(LTRIM(RTRIM(m.[CellPhone])), N'') AS [CellPhone],
    NULLIF(LTRIM(RTRIM(m.[PhoneNumber])), N'') AS [PhoneNumber],
    NULLIF(LTRIM(RTRIM(m.[Address1])), N'') AS [Address1],
    NULLIF(LTRIM(RTRIM(m.[Address2])), N'') AS [Address2],
    NULLIF(LTRIM(RTRIM(m.[City])), N'') AS [City],
    NULLIF(LTRIM(RTRIM(m.[State])), N'') AS [State],
    NULLIF(LTRIM(RTRIM(m.[ZipCode])), N'') AS [ZipCode],
    NULLIF(LTRIM(RTRIM(m.[Country])), N'') AS [Country],
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
WHERE m.[AGAID] < ?
  AND NULLIF(LTRIM(RTRIM(m.[FirstName])), N'') IS NOT NULL
  AND NULLIF(LTRIM(RTRIM(m.[LastName])), N'') IS NOT NULL
ORDER BY m.[LastName], m.[FirstName], m.[AGAID];
"""


PAIR_COLUMNS = [
    "PairRank",
    "MatchKey",
    "SharedFields",
    "SharedFieldCount",
    "SameEmail",
    "SameEmailName",
    "SameBirthdate",
    "SameState",
    "SameAddress",
    "SamePhone",
    "BothAGAIDsHaveRatings",
    "TotalRatingRows",
    "AGAID1",
    "AGAID2",
    "Name1",
    "Name2",
    "Status1",
    "Status2",
    "MemberType1",
    "MemberType2",
    "ExpirationDate1",
    "ExpirationDate2",
    "State1",
    "State2",
    "City1",
    "City2",
    "ZipCode1",
    "ZipCode2",
    "ChapterCode1",
    "ChapterCode2",
    "Rating1",
    "Rating2",
    "Sigma1",
    "Sigma2",
    "RatingRowCount1",
    "RatingRowCount2",
    "RatingTournamentCount1",
    "RatingTournamentCount2",
    "LastRatingDate1",
    "LastRatingDate2",
    "GameCount1",
    "GameCount2",
    "GameTournamentCount1",
    "GameTournamentCount2",
    "LastGameDate1",
    "LastGameDate2",
]

MATCH_FIELD_LABELS = {
    "email": "email",
    "email_name": "email name",
    "birthdate": "birthdate",
    "state": "state",
    "address": "address",
    "phone": "phone",
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--include-unrated",
        action="store_true",
        help="Include pairs where only one AGAID, or neither AGAID, has rating rows.",
    )
    parser.add_argument(
        "--max-agaid",
        type=int,
        default=50000,
        help="Only compare member AGAIDs below this value.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "docs"),
        help="Directory for generated CSV/JSON files.",
    )
    args = parser.parse_args()

    if args.max_agaid <= 0:
        raise SystemExit("--max-agaid must be greater than zero.")

    conn_str = get_sql_connection_string()
    if not conn_str:
        raise SystemExit("SQL_CONNECTION_STRING was not found in the environment or local.settings.json.")

    adapter = SqlAdapter(conn_str)
    rows = adapter.query_rows(MEMBER_DUPLICATE_PAIR_SQL, (args.max_agaid,) * 5)
    members = [_json_safe_row(row) for row in rows]
    pairs, exact_name_group_count = build_pair_candidates(
        members,
        require_both_rated=not args.include_unrated,
    )

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    pairs_path = output_dir / f"aga_duplicate_member_pair_candidates_{timestamp}.csv"
    metadata_path = output_dir / f"aga_duplicate_member_pair_candidates_metadata_{timestamp}.json"

    _write_csv(pairs_path, PAIR_COLUMNS, pairs)
    field_counts = _field_counts(pairs)
    metadata = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "max_agaid": args.max_agaid,
        "require_both_rated": not args.include_unrated,
        "member_row_count": len(members),
        "exact_name_group_count": exact_name_group_count,
        "pair_count": len(pairs),
        "both_agaids_have_ratings_pair_count": sum(1 for pair in pairs if pair["BothAGAIDsHaveRatings"]),
        "same_email_pair_count": field_counts["email"],
        "same_email_name_pair_count": field_counts["email_name"],
        "same_birthdate_pair_count": field_counts["birthdate"],
        "same_state_pair_count": field_counts["state"],
        "same_address_pair_count": field_counts["address"],
        "same_phone_pair_count": field_counts["phone"],
        "pairs_path": str(pairs_path),
    }
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")

    print(f"Members compared: {metadata['member_row_count']}")
    print(f"Exact-name groups with at least two members: {metadata['exact_name_group_count']}")
    print(f"Pairwise duplicate candidates: {metadata['pair_count']}")
    print(f"Pairs where both AGAIDs have ratings: {metadata['both_agaids_have_ratings_pair_count']}")
    print(
        "Matched fields: "
        f"email={metadata['same_email_pair_count']}, "
        f"email_name={metadata['same_email_name_pair_count']}, "
        f"birthdate={metadata['same_birthdate_pair_count']}, "
        f"state={metadata['same_state_pair_count']}, "
        f"address={metadata['same_address_pair_count']}, "
        f"phone={metadata['same_phone_pair_count']}"
    )
    print(f"Pairs CSV: {pairs_path}")
    print(f"Metadata JSON: {metadata_path}")
    return 0


def build_pair_candidates(
    members: list[dict[str, Any]],
    *,
    require_both_rated: bool = True,
) -> tuple[list[dict[str, Any]], int]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for member in members:
        first_key = _normalize_name(member.get("FirstName"))
        last_key = _normalize_name(member.get("LastName"))
        if not first_key or not last_key:
            continue
        grouped[f"{last_key}|{first_key}"].append(member)

    exact_name_group_count = sum(1 for group in grouped.values() if len(group) >= 2)
    pairs: list[dict[str, Any]] = []
    for match_key, group in grouped.items():
        sorted_group = sorted(group, key=lambda row: int(row.get("AGAID") or 0))
        for left_index, left in enumerate(sorted_group):
            for right in sorted_group[left_index + 1 :]:
                shared_fields = _shared_fields(left, right)
                if not shared_fields:
                    continue
                both_rated = _has_rating_rows(left) and _has_rating_rows(right)
                if require_both_rated and not both_rated:
                    continue
                pairs.append(_pair_row(match_key, left, right, shared_fields, both_rated))

    pairs.sort(key=_pair_sort_key)
    for index, pair in enumerate(pairs, start=1):
        pair["PairRank"] = index
    return pairs, exact_name_group_count


def _shared_fields(left: dict[str, Any], right: dict[str, Any]) -> list[str]:
    fields = []
    if _normalize_email(left.get("EmailAddress")) and _normalize_email(left.get("EmailAddress")) == _normalize_email(
        right.get("EmailAddress")
    ):
        fields.append("email")
    elif _email_name(left.get("EmailAddress")) and _email_name(left.get("EmailAddress")) == _email_name(
        right.get("EmailAddress")
    ):
        fields.append("email_name")
    if _normalize_birthdate(left.get("DateOfBirth")) and _normalize_birthdate(
        left.get("DateOfBirth")
    ) == _normalize_birthdate(right.get("DateOfBirth")):
        fields.append("birthdate")
    if _normalize_state(left.get("State")) and _normalize_state(left.get("State")) == _normalize_state(
        right.get("State")
    ):
        fields.append("state")
    if _same_address(left, right):
        fields.append("address")
    if _normalized_phones(left) & _normalized_phones(right):
        fields.append("phone")
    return fields


def _pair_row(
    match_key: str,
    left: dict[str, Any],
    right: dict[str, Any],
    shared_fields: list[str],
    both_rated: bool,
) -> dict[str, Any]:
    flags = set(shared_fields)
    return {
        "PairRank": "",
        "MatchKey": match_key,
        "SharedFields": " | ".join(MATCH_FIELD_LABELS[field] for field in shared_fields),
        "SharedFieldCount": len(shared_fields),
        "SameEmail": "email" in flags,
        "SameEmailName": "email_name" in flags,
        "SameBirthdate": "birthdate" in flags,
        "SameState": "state" in flags,
        "SameAddress": "address" in flags,
        "SamePhone": "phone" in flags,
        "BothAGAIDsHaveRatings": both_rated,
        "TotalRatingRows": _int_value(left.get("RatingRowCount")) + _int_value(right.get("RatingRowCount")),
        "AGAID1": left.get("AGAID"),
        "AGAID2": right.get("AGAID"),
        "Name1": _display_name(left),
        "Name2": _display_name(right),
        "Status1": left.get("Status"),
        "Status2": right.get("Status"),
        "MemberType1": left.get("MemberType"),
        "MemberType2": right.get("MemberType"),
        "ExpirationDate1": left.get("ExpirationDate"),
        "ExpirationDate2": right.get("ExpirationDate"),
        "State1": left.get("State"),
        "State2": right.get("State"),
        "City1": left.get("City"),
        "City2": right.get("City"),
        "ZipCode1": left.get("ZipCode"),
        "ZipCode2": right.get("ZipCode"),
        "ChapterCode1": left.get("ChapterCode"),
        "ChapterCode2": right.get("ChapterCode"),
        "Rating1": left.get("Rating"),
        "Rating2": right.get("Rating"),
        "Sigma1": left.get("Sigma"),
        "Sigma2": right.get("Sigma"),
        "RatingRowCount1": left.get("RatingRowCount"),
        "RatingRowCount2": right.get("RatingRowCount"),
        "RatingTournamentCount1": left.get("RatingTournamentCount"),
        "RatingTournamentCount2": right.get("RatingTournamentCount"),
        "LastRatingDate1": left.get("LastRatingDate"),
        "LastRatingDate2": right.get("LastRatingDate"),
        "GameCount1": left.get("GameCount"),
        "GameCount2": right.get("GameCount"),
        "GameTournamentCount1": left.get("GameTournamentCount"),
        "GameTournamentCount2": right.get("GameTournamentCount"),
        "LastGameDate1": left.get("LastGameDate"),
        "LastGameDate2": right.get("LastGameDate"),
    }


def _pair_sort_key(pair: dict[str, Any]) -> tuple[Any, ...]:
    return (
        -_int_value(pair.get("SharedFieldCount")),
        0 if pair.get("SameEmail") else 1,
        0 if pair.get("SameEmailName") else 1,
        0 if pair.get("SameBirthdate") else 1,
        0 if pair.get("SamePhone") else 1,
        0 if pair.get("SameAddress") else 1,
        0 if pair.get("SameState") else 1,
        -_int_value(pair.get("TotalRatingRows")),
        str(pair.get("MatchKey") or ""),
        _int_value(pair.get("AGAID1")),
        _int_value(pair.get("AGAID2")),
    )


def _field_counts(pairs: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "email": sum(1 for pair in pairs if pair["SameEmail"]),
        "email_name": sum(1 for pair in pairs if pair["SameEmailName"]),
        "birthdate": sum(1 for pair in pairs if pair["SameBirthdate"]),
        "state": sum(1 for pair in pairs if pair["SameState"]),
        "address": sum(1 for pair in pairs if pair["SameAddress"]),
        "phone": sum(1 for pair in pairs if pair["SamePhone"]),
    }


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


def _normalize_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _normalize_email(value: Any) -> str:
    email = str(value or "").strip().lower()
    if "@" not in email:
        return ""
    return email


def _email_name(value: Any) -> str:
    email = _normalize_email(value)
    if not email:
        return ""
    local_part = email.split("@", 1)[0]
    return re.sub(r"[^a-z0-9]+", "", local_part)


def _normalize_birthdate(value: Any) -> str:
    text = str(value or "").strip()
    if not text or text.startswith("1900-01-01"):
        return ""
    return text[:10]


def _normalize_state(value: Any) -> str:
    return str(value or "").strip().upper()


def _same_address(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_address = _normalize_address(left)
    right_address = _normalize_address(right)
    if not left_address or left_address != right_address:
        return False
    left_address2 = _address_part(left.get("Address2"))
    right_address2 = _address_part(right.get("Address2"))
    return not (left_address2 and right_address2 and left_address2 != right_address2)


def _normalize_address(row: dict[str, Any]) -> str:
    address1 = _address_part(row.get("Address1"))
    if not address1:
        return ""
    city = _address_part(row.get("City"))
    state = _address_part(row.get("State"))
    zip_code = _address_part(row.get("ZipCode"))
    if not (city or state or zip_code):
        return ""
    return "|".join((address1, city, state, zip_code))


def _address_part(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _normalized_phones(row: dict[str, Any]) -> set[str]:
    phones = set()
    for field in ("CellPhone", "PhoneNumber"):
        digits = re.sub(r"\D+", "", str(row.get(field) or ""))
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        if len(digits) >= 7:
            phones.add(digits)
    return phones


def _has_rating_rows(row: dict[str, Any]) -> bool:
    return _int_value(row.get("RatingRowCount")) > 0


def _int_value(value: Any) -> int:
    if value in (None, ""):
        return 0
    return int(value)


def _display_name(member: dict[str, Any]) -> str:
    first = str(member.get("FirstName") or "").strip()
    last = str(member.get("LastName") or "").strip()
    return " ".join(part for part in (first, last) if part)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


if __name__ == "__main__":
    raise SystemExit(main())
