"""Generate read-only duplicate candidate reports for production tournaments."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bayrate.report_parser import _normalize_title_for_match  # noqa: E402
from bayrate.sql_adapter import SqlAdapter, get_sql_connection_string  # noqa: E402


DEFAULT_START_DATE = "1800-01-01"
DEFAULT_END_DATE = "2003-12-31"
DEFAULT_DATE_WINDOW_DAYS = 7
DUPLICATE_SCORE_THRESHOLD = 0.74


TOURNAMENT_ROWS_SQL = """
WITH rating_summary AS
(
    SELECT
        r.[Tournament_Code],
        COUNT_BIG(*) AS [RatingRowCount],
        COUNT(DISTINCT r.[Pin_Player]) AS [RatedPlayerCount],
        MIN(r.[id]) AS [FirstRatingRowID],
        MAX(r.[id]) AS [LastRatingRowID],
        MIN(CAST(r.[Elab_Date] AS date)) AS [FirstRatingDate],
        MAX(CAST(r.[Elab_Date] AS date)) AS [LastRatingDate]
    FROM [ratings].[ratings] AS r
    WHERE r.[Tournament_Code] IS NOT NULL
    GROUP BY r.[Tournament_Code]
),
game_summary AS
(
    SELECT
        g.[Tournament_Code],
        COUNT_BIG(*) AS [GameCount],
        COUNT(DISTINCT g.[Pin_Player_1]) + COUNT(DISTINCT g.[Pin_Player_2]) AS [PlayerColumnDistinctCount],
        MIN(g.[Game_ID]) AS [FirstGameID],
        MAX(g.[Game_ID]) AS [LastGameID],
        MIN(CAST(g.[Game_Date] AS date)) AS [FirstGameDate],
        MAX(CAST(g.[Game_Date] AS date)) AS [LastGameDate]
    FROM [ratings].[games] AS g
    WHERE g.[Tournament_Code] IS NOT NULL
    GROUP BY g.[Tournament_Code]
)
SELECT
    t.[Tournament_Code],
    t.[Tournament_Descr],
    CAST(t.[Tournament_Date] AS date) AS [Tournament_Date],
    t.[City],
    t.[State_Code],
    t.[Country_Code],
    COALESCE(gs.[GameCount], 0) AS [GameCount],
    COALESCE(gs.[PlayerColumnDistinctCount], 0) AS [PlayerColumnDistinctCount],
    gs.[FirstGameID],
    gs.[LastGameID],
    gs.[FirstGameDate],
    gs.[LastGameDate],
    COALESCE(rs.[RatingRowCount], 0) AS [RatingRowCount],
    COALESCE(rs.[RatedPlayerCount], 0) AS [RatedPlayerCount],
    rs.[FirstRatingRowID],
    rs.[LastRatingRowID],
    rs.[FirstRatingDate],
    rs.[LastRatingDate],
    g.[Game_ID],
    CAST(g.[Game_Date] AS date) AS [Game_Date],
    g.[Round],
    g.[Pin_Player_1],
    g.[Pin_Player_2],
    g.[Handicap],
    g.[Komi],
    g.[Result]
FROM [ratings].[tournaments] AS t
LEFT JOIN game_summary AS gs
    ON gs.[Tournament_Code] = t.[Tournament_Code]
LEFT JOIN rating_summary AS rs
    ON rs.[Tournament_Code] = t.[Tournament_Code]
LEFT JOIN [ratings].[games] AS g
    ON g.[Tournament_Code] = t.[Tournament_Code]
WHERE t.[Tournament_Date] >= ?
  AND t.[Tournament_Date] <= ?
ORDER BY t.[Tournament_Date], t.[Tournament_Code], g.[Game_Date], g.[Round], g.[Game_ID];
"""


PAIR_COLUMNS = [
    "PairRank",
    "Score",
    "Reasons",
    "TitleScore",
    "DateScore",
    "LocationScore",
    "PlayerOverlapScore",
    "GameOverlapScore",
    "DateDiffDays",
    "SharedPlayerCount",
    "SharedGameCount",
    "TournamentCode1",
    "TournamentCode2",
    "TournamentDate1",
    "TournamentDate2",
    "TournamentDescr1",
    "TournamentDescr2",
    "Location1",
    "Location2",
    "GameCount1",
    "GameCount2",
    "RatingRowCount1",
    "RatingRowCount2",
    "RatedPlayerCount1",
    "RatedPlayerCount2",
    "FirstGameID1",
    "FirstGameID2",
    "LastGameID1",
    "LastGameID2",
    "FirstRatingRowID1",
    "FirstRatingRowID2",
    "LastRatingRowID1",
    "LastRatingRowID2",
    "FirstRatingDate1",
    "FirstRatingDate2",
    "LastRatingDate1",
    "LastRatingDate2",
]


@dataclass
class TournamentCandidate:
    tournament_code: str
    tournament_descr: str | None = None
    tournament_date: date | None = None
    city: str | None = None
    state_code: str | None = None
    country_code: str | None = None
    game_count: int = 0
    player_column_distinct_count: int = 0
    first_game_id: int | None = None
    last_game_id: int | None = None
    first_game_date: date | None = None
    last_game_date: date | None = None
    rating_row_count: int = 0
    rated_player_count: int = 0
    first_rating_row_id: int | None = None
    last_rating_row_id: int | None = None
    first_rating_date: date | None = None
    last_rating_date: date | None = None
    players: set[int] = field(default_factory=set)
    games: set[tuple[Any, ...]] = field(default_factory=set)

    @property
    def normalized_title(self) -> str:
        return _normalize_title_for_match(self.tournament_descr or "")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", default=DEFAULT_START_DATE, help="Inclusive tournament date lower bound.")
    parser.add_argument("--end-date", default=DEFAULT_END_DATE, help="Inclusive tournament date upper bound.")
    parser.add_argument(
        "--window-days",
        type=int,
        default=DEFAULT_DATE_WINDOW_DAYS,
        help="Only compare tournaments within this many days of each other.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "docs"),
        help="Directory for generated CSV/JSON files.",
    )
    args = parser.parse_args()

    start_date = _parse_date(args.start_date, "--start-date")
    end_date = _parse_date(args.end_date, "--end-date")
    if start_date > end_date:
        raise SystemExit("--start-date must be on or before --end-date.")
    if args.window_days < 0:
        raise SystemExit("--window-days must be zero or greater.")

    conn_str = get_sql_connection_string()
    if not conn_str:
        raise SystemExit("SQL_CONNECTION_STRING was not found in the environment or local.settings.json.")

    adapter = SqlAdapter(conn_str)
    rows = adapter.query_rows(TOURNAMENT_ROWS_SQL, (start_date, end_date))
    tournaments = _build_tournaments(rows)
    pairs = _find_duplicate_pairs(tournaments, window_days=args.window_days)

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    pairs_path = output_dir / f"aga_duplicate_tournament_candidates_{timestamp}.csv"
    metadata_path = output_dir / f"aga_duplicate_tournament_candidates_metadata_{timestamp}.json"

    _write_csv(pairs_path, PAIR_COLUMNS, pairs)
    metadata = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "window_days": args.window_days,
        "tournament_count": len(tournaments),
        "candidate_pair_count": len(pairs),
        "same_date_pair_count": sum(1 for pair in pairs if _int_value(pair["DateDiffDays"]) == 0),
        "shared_game_pair_count": sum(1 for pair in pairs if _int_value(pair["SharedGameCount"]) > 0),
        "shared_player_pair_count": sum(1 for pair in pairs if _int_value(pair["SharedPlayerCount"]) > 0),
        "pairs_path": str(pairs_path),
    }
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")

    print(f"Tournaments compared: {metadata['tournament_count']}")
    print(f"Candidate duplicate pairs: {metadata['candidate_pair_count']}")
    print(f"Same-date candidate pairs: {metadata['same_date_pair_count']}")
    print(f"Pairs with shared games: {metadata['shared_game_pair_count']}")
    print(f"Pairs with shared players: {metadata['shared_player_pair_count']}")
    print(f"Pairs CSV: {pairs_path}")
    print(f"Metadata JSON: {metadata_path}")
    return 0


def _build_tournaments(rows: list[dict[str, Any]]) -> list[TournamentCandidate]:
    by_code: dict[str, TournamentCandidate] = {}
    for raw_row in rows:
        row = _json_safe_row(raw_row)
        code = _clean_text(row.get("Tournament_Code"))
        if not code:
            continue
        tournament = by_code.get(code)
        if tournament is None:
            tournament = TournamentCandidate(
                tournament_code=code,
                tournament_descr=_clean_text(row.get("Tournament_Descr")),
                tournament_date=_coerce_date(row.get("Tournament_Date")),
                city=_clean_text(row.get("City")),
                state_code=_clean_text(row.get("State_Code")),
                country_code=_clean_text(row.get("Country_Code")),
                game_count=_int_value(row.get("GameCount")),
                player_column_distinct_count=_int_value(row.get("PlayerColumnDistinctCount")),
                first_game_id=_int_or_none(row.get("FirstGameID")),
                last_game_id=_int_or_none(row.get("LastGameID")),
                first_game_date=_coerce_date(row.get("FirstGameDate")),
                last_game_date=_coerce_date(row.get("LastGameDate")),
                rating_row_count=_int_value(row.get("RatingRowCount")),
                rated_player_count=_int_value(row.get("RatedPlayerCount")),
                first_rating_row_id=_int_or_none(row.get("FirstRatingRowID")),
                last_rating_row_id=_int_or_none(row.get("LastRatingRowID")),
                first_rating_date=_coerce_date(row.get("FirstRatingDate")),
                last_rating_date=_coerce_date(row.get("LastRatingDate")),
            )
            by_code[code] = tournament
        for player_column in ("Pin_Player_1", "Pin_Player_2"):
            player_id = _int_or_none(row.get(player_column))
            if player_id is not None:
                tournament.players.add(player_id)
        game_signature = _game_signature(row)
        if game_signature is not None:
            tournament.games.add(game_signature)
    return sorted(by_code.values(), key=lambda item: (item.tournament_date or date.min, item.tournament_code))


def _find_duplicate_pairs(tournaments: list[TournamentCandidate], *, window_days: int) -> list[dict[str, Any]]:
    pairs = []
    for left_index, left in enumerate(tournaments):
        for right in tournaments[left_index + 1 :]:
            days = _date_diff_days(left.tournament_date, right.tournament_date)
            if days is not None and days > window_days:
                break
            pair = _score_pair(left, right)
            if _is_likely_duplicate(pair):
                pairs.append(pair)
    pairs.sort(key=_pair_sort_key)
    for index, pair in enumerate(pairs, start=1):
        pair["PairRank"] = index
    return pairs


def _score_pair(left: TournamentCandidate, right: TournamentCandidate) -> dict[str, Any]:
    title_score = _token_jaccard(left.normalized_title, right.normalized_title)
    date_score = _date_score(left.tournament_date, right.tournament_date)
    location_score = _location_score(left, right)
    player_overlap = _overlap_score(left.players, right.players)
    game_overlap = _overlap_score(left.games, right.games)
    shared_players = len(left.players & right.players)
    shared_games = len(left.games & right.games)
    days = _date_diff_days(left.tournament_date, right.tournament_date)
    score = (
        0.34 * title_score
        + 0.20 * date_score
        + 0.14 * location_score
        + 0.22 * player_overlap
        + 0.10 * game_overlap
    )
    reasons = _reasons(
        title_score=title_score,
        date_score=date_score,
        location_score=location_score,
        player_overlap=player_overlap,
        game_overlap=game_overlap,
        shared_players=shared_players,
        shared_games=shared_games,
        days=days,
    )
    return {
        "PairRank": "",
        "Score": round(score, 4),
        "Reasons": " | ".join(reasons),
        "TitleScore": round(title_score, 4),
        "DateScore": round(date_score, 4),
        "LocationScore": round(location_score, 4),
        "PlayerOverlapScore": round(player_overlap, 4),
        "GameOverlapScore": round(game_overlap, 4),
        "DateDiffDays": days,
        "SharedPlayerCount": shared_players,
        "SharedGameCount": shared_games,
        "TournamentCode1": left.tournament_code,
        "TournamentCode2": right.tournament_code,
        "TournamentDate1": _format_date(left.tournament_date),
        "TournamentDate2": _format_date(right.tournament_date),
        "TournamentDescr1": left.tournament_descr,
        "TournamentDescr2": right.tournament_descr,
        "Location1": _location_label(left),
        "Location2": _location_label(right),
        "GameCount1": left.game_count,
        "GameCount2": right.game_count,
        "RatingRowCount1": left.rating_row_count,
        "RatingRowCount2": right.rating_row_count,
        "RatedPlayerCount1": left.rated_player_count,
        "RatedPlayerCount2": right.rated_player_count,
        "FirstGameID1": left.first_game_id,
        "FirstGameID2": right.first_game_id,
        "LastGameID1": left.last_game_id,
        "LastGameID2": right.last_game_id,
        "FirstRatingRowID1": left.first_rating_row_id,
        "FirstRatingRowID2": right.first_rating_row_id,
        "LastRatingRowID1": left.last_rating_row_id,
        "LastRatingRowID2": right.last_rating_row_id,
        "FirstRatingDate1": _format_date(left.first_rating_date),
        "FirstRatingDate2": _format_date(right.first_rating_date),
        "LastRatingDate1": _format_date(left.last_rating_date),
        "LastRatingDate2": _format_date(right.last_rating_date),
    }


def _is_likely_duplicate(pair: dict[str, Any]) -> bool:
    title = float(pair["TitleScore"])
    date_score = float(pair["DateScore"])
    location = float(pair["LocationScore"])
    player = float(pair["PlayerOverlapScore"])
    game = float(pair["GameOverlapScore"])
    score = float(pair["Score"])
    days = _int_value(pair["DateDiffDays"])
    shared_games = _int_value(pair["SharedGameCount"])
    shared_players = _int_value(pair["SharedPlayerCount"])
    return (
        score >= DUPLICATE_SCORE_THRESHOLD
        or shared_games > 0
        or (title >= 0.92 and date_score >= 0.75)
        or (days == 0 and title >= 0.75 and (location >= 0.5 or shared_players >= 2))
        or (date_score >= 0.75 and player >= 0.65 and shared_players >= 2)
        or (days == 0 and location >= 0.7 and player >= 0.40 and shared_players >= 2)
    )


def _reasons(
    *,
    title_score: float,
    date_score: float,
    location_score: float,
    player_overlap: float,
    game_overlap: float,
    shared_players: int,
    shared_games: int,
    days: int | None,
) -> list[str]:
    reasons = []
    if shared_games:
        reasons.append("shared game signatures")
    if days == 0 and title_score >= 0.92:
        reasons.append("same date/title")
    elif title_score >= 0.92 and date_score >= 0.75:
        reasons.append("near date/title")
    elif days == 0 and title_score >= 0.75:
        reasons.append("same date/similar title")
    if date_score >= 0.75 and player_overlap >= 0.65 and shared_players >= 2:
        reasons.append("heavy player overlap")
    elif shared_players >= 2:
        reasons.append("shared players")
    if days == 0 and location_score >= 0.7:
        reasons.append("same date/location")
    elif location_score >= 0.7:
        reasons.append("near date/location")
    if game_overlap > 0:
        reasons.append("game overlap")
    return reasons


def _pair_sort_key(pair: dict[str, Any]) -> tuple[Any, ...]:
    return (
        -float(pair.get("Score") or 0),
        -float(pair.get("GameOverlapScore") or 0),
        -float(pair.get("PlayerOverlapScore") or 0),
        _int_value(pair.get("DateDiffDays")),
        str(pair.get("TournamentDate1") or ""),
        str(pair.get("TournamentCode1") or ""),
        str(pair.get("TournamentCode2") or ""),
    )


def _token_jaccard(left: str, right: str) -> float:
    left_tokens = set(left.split())
    right_tokens = set(right.split())
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def _date_score(left: date | None, right: date | None) -> float:
    days = _date_diff_days(left, right)
    if days is None:
        return 0.0
    if days == 0:
        return 1.0
    if days <= 2:
        return 0.75
    if days <= DEFAULT_DATE_WINDOW_DAYS:
        return 0.45
    return 0.0


def _date_diff_days(left: date | None, right: date | None) -> int | None:
    if left is None or right is None:
        return None
    return abs((left - right).days)


def _location_score(left: TournamentCandidate, right: TournamentCandidate) -> float:
    city_match = bool(left.city and right.city and left.city.lower() == right.city.lower())
    state_match = bool(left.state_code and right.state_code and left.state_code.upper() == right.state_code.upper())
    country_match = bool(
        left.country_code and right.country_code and left.country_code.upper() == right.country_code.upper()
    )
    if city_match and state_match:
        return 1.0
    if state_match and country_match:
        return 0.7
    if city_match or state_match:
        return 0.5
    if country_match:
        return 0.2
    return 0.0


def _overlap_score(left: set[Any], right: set[Any]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / min(len(left), len(right))


def _game_signature(row: dict[str, Any]) -> tuple[Any, ...] | None:
    player_1 = _int_or_none(row.get("Pin_Player_1"))
    player_2 = _int_or_none(row.get("Pin_Player_2"))
    if player_1 is None or player_2 is None:
        return None
    players = tuple(sorted((player_1, player_2)))
    return (
        _coerce_date(row.get("Game_Date")),
        _int_or_none(row.get("Round")),
        players[0],
        players[1],
        _int_or_none(row.get("Handicap")),
        _float_or_none(row.get("Komi")),
        _clean_text(row.get("Result")).upper() or None,
    )


def _location_label(tournament: TournamentCandidate) -> str:
    return ", ".join(part for part in (tournament.city, tournament.state_code, tournament.country_code) if part)


def _parse_date(value: str, label: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise SystemExit(f"{label} must be YYYY-MM-DD.") from exc


def _coerce_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _format_date(value: date | None) -> str:
    return value.isoformat() if value is not None else ""


def _int_value(value: Any) -> int:
    if value in (None, ""):
        return 0
    return int(value)


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


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


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


if __name__ == "__main__":
    raise SystemExit(main())
