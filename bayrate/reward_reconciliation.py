# SPDX-FileCopyrightText: 2026 American Go Association
# SPDX-License-Identifier: GPL-3.0-or-later
"""Calculate Chapter Rewards impact for a BayRate tournament replacement."""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Iterable, Protocol


RATED_GAME_BASE_POINTS = 500
MAX_MEMBER_AGAID = 50000
TOURNAMENT_MIN_GAMES = 15
TOURNAMENT_MAX_GAMES = 700
TOURNAMENT_MAX_SUPPORT = 1000
TOURNAMENT_EXPONENT = 0.93
STATE_CHAMPIONSHIP_POINTS = 200000


class RewardSqlAdapter(Protocol):
    """Minimal query interface needed by reconciliation."""

    def query_rows(self, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        """Return query rows."""


def build_reward_reconciliation(
    adapter: RewardSqlAdapter,
    *,
    run_id: int,
    staged_tournaments: list[dict[str, Any]],
    staged_games: list[dict[str, Any]],
    production_tournaments: list[dict[str, Any]],
    production_games: list[dict[str, Any]],
    reconciliation_tournament_codes: list[str] | None = None,
) -> dict[str, Any]:
    """Compare calculated Chapter Rewards before and after the staged replacement."""
    staged_tournament_rows = [dict(entry.get("tournament_row") or {}) for entry in staged_tournaments]
    staged_game_rows = [dict(entry.get("game_row") or {}) for entry in staged_games]
    staged_codes = _ordered_unique(row.get("Tournament_Code") for row in staged_tournament_rows)
    reconciliation_codes = _ordered_unique(
        staged_codes if reconciliation_tournament_codes is None else reconciliation_tournament_codes
    )
    reconciliation_code_set = set(reconciliation_codes)
    reconciled_staged_tournaments = [
        row for row in staged_tournament_rows if str(row.get("Tournament_Code") or "").strip() in reconciliation_code_set
    ]

    relevant_group_keys = {
        key
        for row in [
            *(row for row in production_tournaments if str(row.get("Tournament_Code") or "").strip() in reconciliation_code_set),
            *reconciled_staged_tournaments,
        ]
        if (key := _reward_group_key(row)) is not None
    }
    context_tournaments = load_reward_group_tournaments(adapter, relevant_group_keys)
    context_tournaments = _merge_rows_by_code(context_tournaments, production_tournaments)
    context_codes = _ordered_unique(row.get("Tournament_Code") for row in context_tournaments)
    context_games = load_production_games_for_codes(adapter, context_codes)
    context_games = _merge_games_by_id(context_games, production_games)

    old_participant_games = [
        row for row in production_games if str(row.get("Tournament_Code") or "").strip() in reconciliation_code_set
    ]
    new_participant_games = [
        row for row in staged_game_rows if str(row.get("Tournament_Code") or "").strip() in reconciliation_code_set
    ]
    snapshots = load_reward_snapshots(adapter, [*old_participant_games, *new_participant_games])

    old_played, old_unallocated = calculate_played_game_points(old_participant_games, snapshots)
    new_played, new_unallocated = calculate_played_game_points(new_participant_games, snapshots)

    projected_staged_tournaments = [
        row
        for row in staged_tournament_rows
        if _reward_group_key(row) in relevant_group_keys
        or str(row.get("Tournament_Code") or "").strip() in reconciliation_code_set
    ]
    projected_codes = set(_ordered_unique(row.get("Tournament_Code") for row in projected_staged_tournaments))
    old_tournaments = context_tournaments
    new_tournaments = [
        row for row in context_tournaments if str(row.get("Tournament_Code") or "").strip() not in projected_codes
    ] + projected_staged_tournaments
    old_host_groups = calculate_host_group_points(old_tournaments, context_games, relevant_group_keys)
    new_context_games = [
        row for row in context_games if str(row.get("Tournament_Code") or "").strip() not in projected_codes
    ] + [row for row in staged_game_rows if str(row.get("Tournament_Code") or "").strip() in projected_codes]
    new_host_groups = calculate_host_group_points(new_tournaments, new_context_games, relevant_group_keys)

    chapters: dict[int, dict[str, Any]] = {}
    _add_chapter_points(chapters, old_played, side="old", award_type="played_game")
    _add_chapter_points(chapters, new_played, side="new", award_type="played_game")
    _add_host_group_points(chapters, old_host_groups, side="old")
    _add_host_group_points(chapters, new_host_groups, side="new")

    chapter_rows = []
    for chapter_id in sorted(chapters, key=lambda value: (_chapter_sort_code(chapters[value]), value)):
        row = chapters[chapter_id]
        row["old_total_points"] = (
            row["old_played_game_points"]
            + row["old_total_games_points"]
            + row["old_state_championship_points"]
        )
        row["new_total_points"] = (
            row["new_played_game_points"]
            + row["new_total_games_points"]
            + row["new_state_championship_points"]
        )
        row["point_difference"] = row["new_total_points"] - row["old_total_points"]
        if row["old_total_points"] > 0 or row["new_total_points"] > 0:
            chapter_rows.append(row)

    warnings = []
    if old_unallocated or new_unallocated:
        warnings.append(
            "Chapter Rewards snapshots leave "
            f"{old_unallocated} old and {new_unallocated} new rated-game participation(s) without chapter points."
        )
    return {
        "run_id": run_id,
        "report_type": "chapter_rewards_reconciliation",
        "required": bool(reconciliation_codes),
        "rule_version": "rated-game 2026-05-02; tournament-host 2026-05-03",
        "scope_tournament_codes": reconciliation_codes,
        "rerun_tournament_codes": reconciliation_codes,
        "old_total_points": sum(row["old_total_points"] for row in chapter_rows),
        "new_total_points": sum(row["new_total_points"] for row in chapter_rows),
        "point_difference": sum(row["point_difference"] for row in chapter_rows),
        "old_unallocated_participation_count": old_unallocated,
        "new_unallocated_participation_count": new_unallocated,
        "chapters": chapter_rows,
        "event_groups": _format_event_group_comparison(old_host_groups, new_host_groups),
        "warnings": warnings,
        "notes": [
            "Played-game points cover only the tournament codes replaced by this BayRate run.",
            "Total-games points use every production section in each affected sponsoring-chapter reward event.",
            "State Championship points are included and remain a once-per-logical-event entitlement.",
            "This report calculates old and new entitlements; BayRate does not adjust Chapter Rewards transactions.",
        ],
    }


def load_reward_group_tournaments(
    adapter: RewardSqlAdapter,
    group_keys: set[tuple[int, str]],
) -> list[dict[str, Any]]:
    """Load all production sections in affected host/reward-event groups."""
    if not group_keys:
        return []
    predicates = []
    params: list[Any] = []
    for chapter_id, event_key in sorted(group_keys):
        predicates.append(
            "([Host_ChapterID] = ? AND "
            "COALESCE(NULLIF(LTRIM(RTRIM([Reward_Event_Key])), N''), "
            "NULLIF(LTRIM(RTRIM([Tournament_Code])), N'')) = ?)"
        )
        params.extend((chapter_id, event_key))
    query = f"""
SELECT
    [Tournament_Code],
    [Tournament_Descr],
    [Tournament_Date],
    [Host_ChapterID],
    [Host_ChapterCode],
    [Host_ChapterName],
    [Reward_Event_Key],
    [Reward_Event_Name],
    [Reward_Is_State_Championship]
FROM [ratings].[tournaments]
WHERE {' OR '.join(predicates)}
ORDER BY [Tournament_Code]
"""
    return adapter.query_rows(query, tuple(params))


def load_production_games_for_codes(adapter: RewardSqlAdapter, codes: list[str]) -> list[dict[str, Any]]:
    """Load production games used by affected reward event groups."""
    if not codes:
        return []
    placeholders = ", ".join("?" for _ in codes)
    query = f"""
SELECT
    [Game_ID], [Tournament_Code], [Game_Date], [Round],
    [Pin_Player_1], [Color_1], [Rank_1], [Pin_Player_2], [Color_2], [Rank_2],
    [Handicap], [Komi], [Result], [Sgf_Code], [Online], [Exclude], [Rated], [Elab_Date]
FROM [ratings].[games]
WHERE [Tournament_Code] IN ({placeholders})
ORDER BY [Tournament_Code], [Game_Date], [Round], [Game_ID]
"""
    return adapter.query_rows(query, tuple(codes))


def load_reward_snapshots(
    adapter: RewardSqlAdapter,
    games: list[dict[str, Any]],
) -> dict[tuple[date, int], dict[str, Any]]:
    """Load member/chapter daily snapshot rows used by rated-game rewards."""
    dates = sorted({_as_date(row.get("Game_Date")) for row in games if _as_date(row.get("Game_Date")) is not None})
    players = sorted(
        {
            player
            for row in games
            for player in (_as_int(row.get("Pin_Player_1")), _as_int(row.get("Pin_Player_2")))
            if player is not None and player < MAX_MEMBER_AGAID
        }
    )
    if not dates or not players:
        return {}
    date_placeholders = ", ".join("?" for _ in dates)
    snapshots: dict[tuple[date, int], dict[str, Any]] = {}
    # Keep comfortably below SQL Server's 2,100-parameter limit for large events.
    player_chunk_size = max(1, 2000 - len(dates))
    for offset in range(0, len(players), player_chunk_size):
        player_chunk = players[offset : offset + player_chunk_size]
        player_placeholders = ", ".join("?" for _ in player_chunk)
        query = f"""
SELECT
    ms.[Snapshot_Date],
    ms.[AGAID],
    ms.[ChapterID] AS [Member_ChapterID],
    ms.[Chapter_Code] AS [Member_Chapter_Code],
    ms.[Is_Active] AS [Member_Is_Active],
    cs.[ChapterID],
    cs.[Chapter_Code],
    cs.[Chapter_Name],
    cs.[Is_Current] AS [Chapter_Is_Current],
    cs.[Multiplier]
FROM [rewards].[member_daily_snapshot] AS ms
LEFT JOIN [rewards].[chapter_daily_snapshot] AS cs
    ON cs.[Snapshot_Date] = ms.[Snapshot_Date]
   AND cs.[ChapterID] = ms.[ChapterID]
WHERE ms.[Snapshot_Date] IN ({date_placeholders})
  AND ms.[AGAID] IN ({player_placeholders})
"""
        rows = adapter.query_rows(query, (*dates, *player_chunk))
        snapshots.update(
            {
                (_as_date(row.get("Snapshot_Date")), _as_int(row.get("AGAID"))): row
                for row in rows
                if _as_date(row.get("Snapshot_Date")) is not None and _as_int(row.get("AGAID")) is not None
            }
        )
    return snapshots


def calculate_played_game_points(
    games: list[dict[str, Any]],
    snapshots: dict[tuple[date, int], dict[str, Any]],
) -> tuple[dict[int, dict[str, Any]], int]:
    """Calculate rated-game participation points using Chapter Rewards rules."""
    result: dict[int, dict[str, Any]] = {}
    unallocated = 0
    for game in games:
        if not _rated_game_participation_eligible(game):
            continue
        game_date = _as_date(game.get("Game_Date"))
        for column in ("Pin_Player_1", "Pin_Player_2"):
            player = _as_int(game.get(column))
            if game_date is None or player is None or player >= MAX_MEMBER_AGAID:
                continue
            snapshot = snapshots.get((game_date, player))
            if not _eligible_snapshot(snapshot):
                unallocated += 1
                continue
            chapter_id = _as_int(snapshot.get("ChapterID"))
            if chapter_id is None:
                unallocated += 1
                continue
            multiplier = _as_int(snapshot.get("Multiplier")) or 0
            entry = result.setdefault(
                chapter_id,
                {
                    "chapter_id": chapter_id,
                    "chapter_code": _clean(snapshot.get("Chapter_Code")),
                    "chapter_name": _clean(snapshot.get("Chapter_Name")),
                    "points": 0,
                    "participation_count": 0,
                },
            )
            entry["points"] += RATED_GAME_BASE_POINTS * multiplier
            entry["participation_count"] += 1
    return result, unallocated


def calculate_host_group_points(
    tournaments: list[dict[str, Any]],
    games: list[dict[str, Any]],
    relevant_group_keys: set[tuple[int, str]],
) -> dict[tuple[int, str], dict[str, Any]]:
    """Calculate sponsoring-chapter total-games awards for affected event groups."""
    sections: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    for row in tournaments:
        key = _reward_group_key(row)
        if key is None or key not in relevant_group_keys or _as_date(row.get("Tournament_Date")) is None:
            continue
        sections[key].append(row)
    games_by_code: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for game in games:
        games_by_code[str(game.get("Tournament_Code") or "").strip()].append(game)

    result = {}
    for key, group_sections in sections.items():
        chapter_id, event_key = key
        codes = _ordered_unique(row.get("Tournament_Code") for row in group_sections)
        game_count = sum(
            1
            for code in codes
            for game in games_by_code.get(code, [])
            if _host_game_eligible(game)
        )
        chapter_code = max((_clean(row.get("Host_ChapterCode")) or "" for row in group_sections), default="")
        chapter_name = max((_clean(row.get("Host_ChapterName")) or "" for row in group_sections), default="") or None
        result[key] = {
            "chapter_id": chapter_id,
            "chapter_code": chapter_code,
            "chapter_name": chapter_name,
            "reward_event_key": event_key,
            "reward_event_name": max(
                (_clean(row.get("Reward_Event_Name")) or _clean(row.get("Tournament_Descr")) or "" for row in group_sections),
                default="",
            ) or None,
            "tournament_codes": codes,
            "rated_game_count": game_count,
            "points": tournament_host_points(game_count, chapter_code),
            "state_championship_points": (
                STATE_CHAMPIONSHIP_POINTS
                if chapter_code.upper() != "AGA"
                and any(_coalesce_int(row.get("Reward_Is_State_Championship"), 0) == 1 for row in group_sections)
                else 0
            ),
        }
    return result


def tournament_host_points(rated_game_count: int, chapter_code: str | None) -> int:
    """Mirror rewards.sp_process_tournament_awards host-award formula."""
    if str(chapter_code or "").strip().upper() == "AGA" or rated_game_count <= TOURNAMENT_MIN_GAMES:
        return 0
    if rated_game_count >= TOURNAMENT_MAX_GAMES:
        return TOURNAMENT_MAX_SUPPORT * 1000
    raw = (
        TOURNAMENT_MAX_SUPPORT
        * math.pow(
            (rated_game_count - TOURNAMENT_MIN_GAMES) / (TOURNAMENT_MAX_GAMES - TOURNAMENT_MIN_GAMES),
            TOURNAMENT_EXPONENT,
        )
        * 1000
    )
    return int(math.floor(raw + 0.5))


def _add_chapter_points(
    chapters: dict[int, dict[str, Any]],
    awards: dict[int, dict[str, Any]],
    *,
    side: str,
    award_type: str,
) -> None:
    for chapter_id, award in awards.items():
        row = _chapter_row(chapters, award)
        row[f"{side}_{award_type}_points"] += int(award.get("points") or 0)
        if award_type == "played_game":
            row[f"{side}_played_game_participation_count"] += int(award.get("participation_count") or 0)


def _add_host_group_points(
    chapters: dict[int, dict[str, Any]],
    groups: dict[tuple[int, str], dict[str, Any]],
    *,
    side: str,
) -> None:
    for award in groups.values():
        row = _chapter_row(chapters, award)
        row[f"{side}_total_games_points"] += int(award.get("points") or 0)
        row[f"{side}_state_championship_points"] += int(award.get("state_championship_points") or 0)


def _chapter_row(chapters: dict[int, dict[str, Any]], award: dict[str, Any]) -> dict[str, Any]:
    chapter_id = int(award["chapter_id"])
    row = chapters.setdefault(
        chapter_id,
        {
            "chapter_id": chapter_id,
            "chapter_code": award.get("chapter_code"),
            "chapter_name": award.get("chapter_name"),
            "old_played_game_participation_count": 0,
            "new_played_game_participation_count": 0,
            "old_played_game_points": 0,
            "new_played_game_points": 0,
            "old_total_games_points": 0,
            "new_total_games_points": 0,
            "old_state_championship_points": 0,
            "new_state_championship_points": 0,
        },
    )
    if not row.get("chapter_code") and award.get("chapter_code"):
        row["chapter_code"] = award.get("chapter_code")
    if not row.get("chapter_name") and award.get("chapter_name"):
        row["chapter_name"] = award.get("chapter_name")
    return row


def _format_event_group_comparison(
    old_groups: dict[tuple[int, str], dict[str, Any]],
    new_groups: dict[tuple[int, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    result = []
    for key in sorted(set(old_groups) | set(new_groups)):
        old = old_groups.get(key) or {}
        new = new_groups.get(key) or {}
        sample = new or old
        result.append(
            {
                "chapter_id": key[0],
                "chapter_code": sample.get("chapter_code"),
                "chapter_name": sample.get("chapter_name"),
                "reward_event_key": key[1],
                "reward_event_name": sample.get("reward_event_name"),
                "old_tournament_codes": old.get("tournament_codes") or [],
                "new_tournament_codes": new.get("tournament_codes") or [],
                "old_rated_game_count": old.get("rated_game_count", 0),
                "new_rated_game_count": new.get("rated_game_count", 0),
                "old_points": old.get("points", 0),
                "new_points": new.get("points", 0),
                "old_total_games_points": old.get("points", 0),
                "new_total_games_points": new.get("points", 0),
                "point_difference": new.get("points", 0) - old.get("points", 0),
                "old_state_championship_points": old.get("state_championship_points", 0),
                "new_state_championship_points": new.get("state_championship_points", 0),
                "state_championship_point_difference": (
                    new.get("state_championship_points", 0) - old.get("state_championship_points", 0)
                ),
            }
        )
    return result


def _reward_group_key(row: dict[str, Any]) -> tuple[int, str] | None:
    chapter_id = _as_int(row.get("Host_ChapterID"))
    chapter_code = _clean(row.get("Host_ChapterCode"))
    event_key = _clean(row.get("Reward_Event_Key")) or _clean(row.get("Tournament_Code"))
    if chapter_id is None or not chapter_code or not event_key:
        return None
    return chapter_id, event_key


def _rated_game_participation_eligible(row: dict[str, Any]) -> bool:
    return (
        _as_date(row.get("Game_Date")) is not None
        and _coalesce_int(row.get("Rated"), 1) == 1
        and _coalesce_int(row.get("Online"), 0) == 0
        and _coalesce_int(row.get("Exclude"), 0) == 0
    )


def _host_game_eligible(row: dict[str, Any]) -> bool:
    return _coalesce_int(row.get("Rated"), 1) == 1 and _coalesce_int(row.get("Exclude"), 0) == 0


def _eligible_snapshot(row: dict[str, Any] | None) -> bool:
    return bool(
        row
        and _coalesce_int(row.get("Member_Is_Active"), 0) == 1
        and _as_int(row.get("Member_ChapterID")) is not None
        and _as_int(row.get("ChapterID")) is not None
        and _coalesce_int(row.get("Chapter_Is_Current"), 0) == 1
    )


def _merge_rows_by_code(primary: list[dict[str, Any]], extra: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = {str(row.get("Tournament_Code") or "").strip(): row for row in primary}
    for row in extra:
        rows.setdefault(str(row.get("Tournament_Code") or "").strip(), row)
    return [row for code, row in rows.items() if code]


def _merge_games_by_id(primary: list[dict[str, Any]], extra: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = {_game_merge_key(row): row for row in primary}
    for row in extra:
        rows.setdefault(_game_merge_key(row), row)
    return list(rows.values())


def _game_merge_key(row: dict[str, Any]) -> tuple[Any, ...]:
    game_id = _as_int(row.get("Game_ID"))
    if game_id is not None:
        return ("id", game_id)
    return (
        "game",
        _clean(row.get("Tournament_Code")),
        _as_date(row.get("Game_Date")),
        _as_int(row.get("Round")),
        _as_int(row.get("Pin_Player_1")),
        _as_int(row.get("Pin_Player_2")),
    )


def _ordered_unique(values: Iterable[Any]) -> list[str]:
    result = []
    seen = set()
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            result.append(text)
            seen.add(text)
    return result


def _chapter_sort_code(row: dict[str, Any]) -> str:
    return str(row.get("chapter_code") or "").upper()


def _clean(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _as_int(value: Any) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    return int(float(value))


def _coalesce_int(value: Any, default: int) -> int:
    parsed = _as_int(value)
    return default if parsed is None else parsed
