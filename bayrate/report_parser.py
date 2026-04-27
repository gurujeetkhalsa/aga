import argparse
import csv
import json
import os
import re
import sys
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

try:
    import pyodbc
except Exception:
    class _MissingPyodbc:
        Error = Exception

        @staticmethod
        def connect(*args, **kwargs):
            raise RuntimeError("pyodbc is unavailable in this environment")

    pyodbc = _MissingPyodbc()


HEADER_RE = re.compile(r"^\s*(tourney|tournament)\b(.*)$", re.IGNORECASE)
PLAYERS_RE = re.compile(r"^\s*players(?:\s*\((\d+)\))?\s*$", re.IGNORECASE)
GAMES_RE = re.compile(r"^\s*games(?:\s+(\d+)|\s*\((\d+)\))?\s*$", re.IGNORECASE)
ROUND_COMMENT_RE = re.compile(r"^\s*#\s*round\s+(\d+)\b", re.IGNORECASE)
KEY_VALUE_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$")
COMMENT_KEY_VALUE_RE = re.compile(r"^\s*#\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$")
COMMENT_TD_RE = re.compile(r"^\s*#\s*TD\s+(.+?)\s*$", re.IGNORECASE)
PLAYER_INLINE_RE = re.compile(r"^\s*(\d+)\s+(.+?)\s+([^\s]+)\s*$")
RESULT_TOKEN_RE = re.compile(r"^[WwBb]$")
NO_RESULT_TOKEN_RE = re.compile(r"^\?$")
INT_TOKEN_RE = re.compile(r"^-?\d+$")
KOMI_TOKEN_RE = re.compile(r"^-?\d+(?:\.5)?$")
RANK_TOKEN_RE = re.compile(r"^-?\d+(?:\.\d+)?[dDkK]$")
NUMERIC_STRENGTH_RE = re.compile(r"^-?\d+(?:\.\d+)?$")
STATE_CODE_RE = re.compile(r"^[A-Za-z]{2}$")
NORMALIZE_TITLE_WORD_RE = re.compile(r"[a-z0-9]+")

STOP_WORDS = {
    "the",
    "and",
    "go",
    "club",
    "tournament",
    "tourney",
    "games",
    "game",
    "rated",
}

CSV_TABLE_SPECS = {
    "games": (
        "Game_ID",
        "Tournament_Code",
        "Game_Date",
        "Round",
        "Pin_Player_1",
        "Color_1",
        "Rank_1",
        "Pin_Player_2",
        "Color_2",
        "Rank_2",
        "Handicap",
        "Komi",
        "Result",
        "Sgf_Code",
        "Online",
        "Exclude",
        "Rated",
        "Elab_Date",
    ),
    "tournaments": (
        "Tournament_Code",
        "Tournament_Descr",
        "Tournament_Date",
        "City",
        "State_Code",
        "Country_Code",
        "Rounds",
        "Total_Players",
        "Wallist",
        "Elab_Date",
        "status",
    ),
}


@dataclass
class ParsedPlayer:
    agaid: int
    name: str
    raw_strength: str
    normalized_strength: str


@dataclass
class ParsedGame:
    white_id: int
    black_id: int
    result: str
    handicap: int
    komi: int
    round_text: str | None


@dataclass
class ParsedUnreportedGame:
    white_id: int
    black_id: int
    handicap: int
    komi: int
    round_text: str | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Parse AGA ratings report text into ratings.tournaments and ratings.games rows.")
    parser.add_argument("inputs", nargs="*", type=Path, help="Optional report text files. Reads stdin when omitted.")
    parser.add_argument("--connection-string", help="Optional SQL connection string used for tournament-code reuse and membership warnings.")
    parser.add_argument("--output", type=Path, help="Optional JSON output path.")
    parser.add_argument("--csv-dir", type=Path, help="Optional output directory for tournaments.csv and games.csv exports.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    conn_str = args.connection_string or _load_connection_string_optional()
    if args.inputs:
        reports = [(str(path), path.read_text(encoding="utf-8")) for path in args.inputs]
        payload = parse_reports_to_rows(reports, conn_str)
    else:
        payload = parse_report_to_rows(sys.stdin.read(), conn_str)
    if args.csv_dir:
        _write_csv_exports(args.csv_dir, payload)
    output = json.dumps(payload, indent=2 if args.pretty or args.output else None, default=str)
    if args.output:
        args.output.write_text(output + ("\n" if not output.endswith("\n") else ""), encoding="utf-8")
    print(output)


def parse_reports_to_rows(
    reports: list[tuple[str, str]],
    conn_str: str | None = None,
    *,
    continue_on_error: bool = False,
) -> dict[str, Any]:
    parsed_reports = []
    tournament_rows = []
    game_rows = []
    players = []
    warnings = []

    for source_ordinal, (source_name, raw_text) in enumerate(reports, start=1):
        try:
            payload = parse_report_to_rows(raw_text, conn_str)
        except Exception as exc:
            if not continue_on_error:
                raise
            payload = _parse_error_report_payload(source_name, raw_text, exc, source_ordinal)
        payload["source_name"] = source_name
        parsed_reports.append(payload)

        tournament_row = dict(payload["tournament_row"])
        tournament_row["_source_name"] = source_name
        tournament_rows.append(tournament_row)

        for game_row in payload["game_rows"]:
            row = dict(game_row)
            row["_source_name"] = source_name
            game_rows.append(row)

        for player in payload.get("players") or []:
            row = dict(player)
            row["_source_name"] = source_name
            players.append(row)

        for warning in payload["warnings"]:
            warning_with_source = dict(warning)
            warning_with_source["source_name"] = source_name
            warnings.append(warning_with_source)

    return {
        "reports": parsed_reports,
        "tournament_rows": tournament_rows,
        "game_rows": game_rows,
        "players": players,
        "warnings": warnings,
    }


def _parse_error_report_payload(source_name: str, raw_text: str, exc: Exception, source_ordinal: int) -> dict[str, Any]:
    title = _best_effort_title(raw_text) or Path(source_name).name or f"Report {source_ordinal}"
    parse_error = str(exc) or exc.__class__.__name__
    return {
        "tournament_row": {
            "Tournament_Code": f"PARSE-ERROR-{source_ordinal}",
            "Tournament_Descr": title,
            "Tournament_Date": None,
            "City": None,
            "State_Code": None,
            "Country_Code": None,
            "Rounds": None,
            "Total_Players": 0,
            "Wallist": None,
            "Elab_Date": None,
            "status": None,
        },
        "game_rows": [],
        "players": [],
        "warnings": [
            {
                "type": "report_parse_failed",
                "severity": "review",
                "review_required": True,
                "message": f"Report could not be parsed: {parse_error}",
            }
        ],
        "metadata": {},
        "raw_text": raw_text,
        "parse_error": parse_error,
    }


def _best_effort_title(raw_text: str) -> str | None:
    lines = raw_text.splitlines()
    for idx, line in enumerate(lines):
        header_match = HEADER_RE.match(line)
        if header_match is None:
            continue
        title = header_match.group(2).strip()
        if title:
            return title
        for candidate in lines[idx + 1 :]:
            stripped = candidate.strip()
            if stripped:
                return stripped
        return None
    for line in lines:
        stripped = line.strip()
        if stripped:
            return stripped[:255]
    return None


def parse_report_to_rows(raw_text: str, conn_str: str | None = None) -> dict[str, Any]:
    report_lines = _extract_report_lines(raw_text)
    parsed = _parse_report_sections(report_lines)

    tournament_code, code_warnings = _resolve_tournament_code(
        parsed["title"],
        parsed["start_date"],
        conn_str,
    )
    warnings = list(parsed["warnings"])
    warnings.extend(code_warnings)

    city, state_code, country_code, location_warnings = _parse_location(parsed["metadata"], parsed["title"])
    warnings.extend(location_warnings)

    tournament_row = {
        "Tournament_Code": tournament_code,
        "Tournament_Descr": parsed["title"],
        "Tournament_Date": parsed["start_date"],
        "City": city,
        "State_Code": state_code,
        "Country_Code": country_code,
        "Rounds": parsed["round_count"],
        "Total_Players": len(parsed["players"]),
        "Wallist": None,
        "Elab_Date": parsed["finish_date"],
        "status": None,
    }

    game_rows = []
    for index, game in enumerate(parsed["games"], start=1):
        white = parsed["players"][game.white_id]
        black = parsed["players"][game.black_id]
        game_rows.append(
            {
                "Game_ID": None,
                "Tournament_Code": tournament_code,
                "Game_Date": parsed["start_date"],
                "Round": game.round_text,
                "Pin_Player_1": game.white_id,
                "Color_1": "W",
                "Rank_1": white.normalized_strength,
                "Pin_Player_2": game.black_id,
                "Color_2": "B",
                "Rank_2": black.normalized_strength,
                "Handicap": game.handicap,
                "Komi": game.komi,
                "Result": game.result,
                "Sgf_Code": None,
                "Online": 0,
                "Exclude": 0,
                "Rated": 1,
                "Elab_Date": parsed["finish_date"],
                "_row_number": index,
            }
        )

    membership_map = None
    if conn_str:
        try:
            membership_map = _load_membership_expirations(conn_str, parsed["players"].keys())
        except Exception as exc:
            warnings.append(
                {
                    "type": "membership_lookup_unavailable",
                    "message": f"Membership lookup was unavailable: {exc}",
                }
            )
    warnings.extend(_build_membership_warnings(membership_map, parsed["games"], parsed["start_date"], tournament_code))

    return {
        "tournament_row": tournament_row,
        "game_rows": game_rows,
        "players": _players_payload(parsed["players"]),
        "warnings": warnings,
        "metadata": parsed["metadata"],
        "raw_text": raw_text,
    }


def _players_payload(players: dict[int, ParsedPlayer]) -> list[dict[str, Any]]:
    return [
        {
            "agaid": player.agaid,
            "name": player.name,
            "raw_strength": player.raw_strength,
            "normalized_strength": player.normalized_strength,
        }
        for player in players.values()
    ]


def _extract_report_lines(raw_text: str) -> list[str]:
    lines = raw_text.splitlines()
    start_idx = None
    for idx, line in enumerate(lines):
        if HEADER_RE.match(line):
            start_idx = idx
            break
    if start_idx is None:
        raise ValueError("Could not find TOURNEY/TOURNAMENT header.")
    return lines[start_idx:]


def _parse_report_sections(lines: list[str]) -> dict[str, Any]:
    if not lines:
        raise ValueError("Report is empty after extracting tournament block.")

    header_match = HEADER_RE.match(lines[0])
    if header_match is None:
        raise ValueError("First extracted line is not a valid tournament header.")

    title = header_match.group(2).strip()
    idx = 1
    while not title and idx < len(lines):
        candidate = lines[idx].strip()
        if candidate:
            title = candidate
            idx += 1
            break
        idx += 1
    if not title:
        raise ValueError("Tournament title is missing.")

    metadata: dict[str, Any] = {}
    warnings: list[dict[str, Any]] = []

    while idx < len(lines):
        stripped = lines[idx].strip()
        if not stripped:
            idx += 1
            continue
        if PLAYERS_RE.match(stripped):
            break
        if stripped.upper() == "END":
            break
        if stripped.startswith("#"):
            td_match = COMMENT_TD_RE.match(stripped)
            if td_match:
                metadata["td"] = td_match.group(1).strip()
                idx += 1
                continue
            comment_match = COMMENT_KEY_VALUE_RE.match(stripped)
            if comment_match:
                metadata[comment_match.group(1).lower()] = _strip_quotes(comment_match.group(2))
                idx += 1
                continue
            warnings.append({"type": "unknown_metadata_field", "message": f"Unparsed comment metadata: {stripped}"})
            idx += 1
            continue
        key_value_match = KEY_VALUE_RE.match(stripped)
        if key_value_match:
            metadata[key_value_match.group(1).lower()] = _strip_quotes(key_value_match.group(2))
            idx += 1
            continue
        warnings.append({"type": "unknown_metadata_field", "message": f"Unparsed metadata line: {stripped}"})
        idx += 1

    players_header = None
    if idx < len(lines):
        players_header = PLAYERS_RE.match(lines[idx].strip())
    if players_header is None:
        raise ValueError("PLAYERS section is missing.")

    player_count_hint = int(players_header.group(1)) if players_header.group(1) else None
    idx += 1
    players, idx = _parse_players(lines, idx)
    if not players:
        raise ValueError("PLAYERS section did not contain any players.")
    if player_count_hint is not None and player_count_hint != len(players):
        warnings.append(
            {
                "type": "player_count_mismatch",
                "expected": player_count_hint,
                "actual": len(players),
                "message": f"PLAYERS header said {player_count_hint}, parsed {len(players)} players.",
            }
        )

    games, unreported_games, round_numbers, idx, game_count_hints = _parse_games(lines, idx)
    if not games and not unreported_games:
        raise ValueError("GAMES section did not contain any games.")
    hinted_total = sum(hint for hint in game_count_hints if hint is not None)
    parsed_total = len(games) + len(unreported_games)
    if hinted_total and hinted_total != parsed_total:
        warnings.append(
            {
                "type": "game_count_mismatch",
                "expected": hinted_total,
                "actual": parsed_total,
                "rated_game_count": len(games),
                "unreported_game_count": len(unreported_games),
                "message": f"GAMES headers said {hinted_total}, parsed {parsed_total} games.",
            }
        )
    if unreported_games:
        warnings.append(
            {
                "type": "unreported_game_results",
                "severity": "warning",
                "ignored_game_count": len(unreported_games),
                "games": [
                    {
                        "white_id": game.white_id,
                        "black_id": game.black_id,
                        "round": game.round_text,
                        "handicap": game.handicap,
                        "komi": game.komi,
                    }
                    for game in unreported_games
                ],
                "message": (
                    f"Skipped {len(unreported_games)} scheduled game"
                    f"{'' if len(unreported_games) == 1 else 's'} with no reported result (? result token). "
                    "Skipped games will not be staged or rated."
                ),
            }
        )

    start_date = _parse_date_required(metadata.get("start"), "start")
    finish_date = _parse_date_required(metadata.get("finish"), "finish")

    if not round_numbers:
        inferred_round_numbers = _infer_rounds_from_game_order(games)
        round_numbers = set(inferred_round_numbers)
        warnings.append(
            {
                "type": "inferred_round_information",
                "message": "No explicit round numbers were found. Rounds were inferred from game order.",
            }
        )

    if "rules" not in metadata:
        warnings.append({"type": "missing_rules", "message": "rules=... metadata is missing."})
    if "location" not in metadata:
        warnings.append({"type": "missing_location", "message": "location=... metadata is missing."})
    if not round_numbers:
        warnings.append({"type": "missing_round_information", "message": "No explicit round numbers were found."})

    for game in games:
        if game.white_id not in players or game.black_id not in players:
            raise ValueError(f"Game references player not found in PLAYERS: {game.white_id} vs {game.black_id}")

    round_count = max(round_numbers) if round_numbers else None
    return {
        "title": title,
        "metadata": metadata,
        "players": players,
        "games": games,
        "warnings": warnings,
        "start_date": start_date,
        "finish_date": finish_date,
        "round_count": round_count,
    }


def _parse_players(lines: list[str], start_idx: int) -> tuple[dict[int, ParsedPlayer], int]:
    players: dict[int, ParsedPlayer] = {}
    idx = start_idx
    while idx < len(lines):
        stripped = lines[idx].strip()
        if not stripped:
            idx += 1
            continue
        if GAMES_RE.match(stripped) or stripped.upper() == "END":
            break
        if stripped.startswith("#"):
            idx += 1
            continue

        inline_match = PLAYER_INLINE_RE.match(stripped)
        if inline_match and _is_strength_token(inline_match.group(3)):
            agaid = int(inline_match.group(1))
            name = inline_match.group(2).strip()
            strength = inline_match.group(3).strip()
            players[agaid] = ParsedPlayer(agaid, name, strength, _normalize_strength(strength))
            idx += 1
            continue

        if not INT_TOKEN_RE.match(stripped):
            raise ValueError(f"Could not parse player row starting at line: {stripped}")
        agaid = int(stripped)
        idx += 1
        name = _next_nonempty_line(lines, idx)
        if name is None:
            raise ValueError(f"Missing player name after AGAID {agaid}")
        idx = name[0] + 1
        strength = _next_nonempty_line(lines, idx)
        if strength is None:
            raise ValueError(f"Missing player strength after AGAID {agaid}")
        idx = strength[0] + 1
        strength_text = strength[1].strip()
        if not _is_strength_token(strength_text):
            raise ValueError(f"Invalid player strength {strength_text!r} for AGAID {agaid}")
        players[agaid] = ParsedPlayer(agaid, name[1].strip(), strength_text, _normalize_strength(strength_text))
    return players, idx


def _parse_games(
    lines: list[str],
    start_idx: int,
) -> tuple[list[ParsedGame], list[ParsedUnreportedGame], set[int], int, list[int | None]]:
    games: list[ParsedGame] = []
    unreported_games: list[ParsedUnreportedGame] = []
    round_numbers: set[int] = set()
    game_count_hints: list[int | None] = []
    idx = start_idx
    current_round: str | None = None
    buffer: list[str] = []

    while idx < len(lines):
        stripped = lines[idx].strip()
        if not stripped:
            idx += 1
            continue
        if stripped.upper() == "END":
            _flush_game_buffer(buffer, games, unreported_games, current_round)
            idx += 1
            break
        games_match = GAMES_RE.match(stripped)
        if games_match:
            _flush_game_buffer(buffer, games, unreported_games, current_round)
            round_hint = games_match.group(1)
            count_hint = games_match.group(2)
            game_count_hints.append(int(count_hint) if count_hint else None)
            if round_hint:
                current_round = str(int(round_hint))
                round_numbers.add(int(round_hint))
            idx += 1
            continue
        round_match = ROUND_COMMENT_RE.match(stripped)
        if round_match:
            _flush_game_buffer(buffer, games, unreported_games, current_round)
            current_round = str(int(round_match.group(1)))
            round_numbers.add(int(round_match.group(1)))
            idx += 1
            continue
        if stripped.startswith("#"):
            idx += 1
            continue

        buffer.extend(stripped.split())
        _consume_game_tokens(buffer, games, unreported_games, current_round)
        idx += 1

    _flush_game_buffer(buffer, games, unreported_games, current_round)
    return games, unreported_games, round_numbers, idx, game_count_hints


def _consume_game_tokens(
    buffer: list[str],
    games: list[ParsedGame],
    unreported_games: list[ParsedUnreportedGame],
    current_round: str | None,
) -> None:
    while len(buffer) >= 5:
        if not INT_TOKEN_RE.match(buffer[0]) or not INT_TOKEN_RE.match(buffer[1]):
            raise ValueError(f"Game row has invalid player IDs near tokens: {buffer[:5]}")
        if (
            not (RESULT_TOKEN_RE.match(buffer[2]) or NO_RESULT_TOKEN_RE.match(buffer[2]))
            or not INT_TOKEN_RE.match(buffer[3])
            or not KOMI_TOKEN_RE.match(buffer[4])
        ):
            raise ValueError(f"Game row has invalid tokens near: {buffer[:5]}")
        white_id = int(buffer.pop(0))
        black_id = int(buffer.pop(0))
        result = buffer.pop(0).upper()
        handicap = int(buffer.pop(0))
        komi = _normalize_komi(buffer.pop(0))
        if NO_RESULT_TOKEN_RE.match(result):
            unreported_games.append(ParsedUnreportedGame(white_id, black_id, handicap, komi, current_round))
        else:
            games.append(ParsedGame(white_id, black_id, result, handicap, komi, current_round))


def _flush_game_buffer(
    buffer: list[str],
    games: list[ParsedGame],
    unreported_games: list[ParsedUnreportedGame],
    current_round: str | None,
) -> None:
    if not buffer:
        return
    if len(buffer) % 5 != 0:
        raise ValueError(f"Incomplete game record tokens remaining: {buffer}")
    _consume_game_tokens(buffer, games, unreported_games, current_round)


def _next_nonempty_line(lines: list[str], start_idx: int) -> tuple[int, str] | None:
    idx = start_idx
    while idx < len(lines):
        stripped = lines[idx].strip()
        if stripped:
            return idx, stripped
        idx += 1
    return None


def _is_strength_token(value: str) -> bool:
    text = value.strip()
    return bool(RANK_TOKEN_RE.match(text) or NUMERIC_STRENGTH_RE.match(text))


def _normalize_strength(value: str) -> str:
    text = value.strip()
    if RANK_TOKEN_RE.match(text):
        return text.lower()
    if NUMERIC_STRENGTH_RE.match(text):
        return text
    raise ValueError(f"Unsupported strength token {value!r}")


def _normalize_komi(value: str) -> int:
    text = value.strip()
    if not KOMI_TOKEN_RE.match(text):
        raise ValueError(f"Unsupported komi value {value!r}")
    return int(float(text))


def _parse_date_required(value: Any, field_name: str) -> date:
    if value is None:
        raise ValueError(f"{field_name} is missing.")
    return _parse_date(str(value))


def _parse_date(value: str) -> date:
    text = value.strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unsupported date value: {value!r}")


def _strip_quotes(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        return text[1:-1]
    return text


def _parse_location(metadata: dict[str, Any], title: str) -> tuple[str | None, str | None, str | None, list[dict[str, Any]]]:
    warnings: list[dict[str, Any]] = []
    location = metadata.get("location")
    if location:
        return (*_parse_location_text(str(location)), warnings)
    inferred = _infer_location_from_title(title)
    if inferred[0] is not None or inferred[1] is not None:
        warnings.append({"type": "inferred_location_from_title", "message": "Location was inferred from tournament title."})
        return (*inferred, warnings)
    return None, None, "US", warnings


def _parse_location_text(location_text: str) -> tuple[str | None, str | None, str | None]:
    cleaned = location_text.strip()
    parts = [part.strip() for part in cleaned.split(",") if part.strip()]
    if not parts:
        return None, None, "US"
    if len(parts) >= 2 and STATE_CODE_RE.match(parts[-1]):
        return ", ".join(parts[:-1]), parts[-1].upper(), "US"
    return cleaned, None, "US"


def _infer_location_from_title(title: str) -> tuple[str | None, str | None, str | None]:
    parts = [part.strip() for part in title.split(",") if part.strip()]
    if len(parts) >= 2 and STATE_CODE_RE.match(parts[-1]):
        return parts[-2], parts[-1].upper(), "US"
    return None, None, "US"


def _resolve_tournament_code(title: str, start_date: date, conn_str: str | None) -> tuple[str, list[dict[str, Any]]]:
    warnings: list[dict[str, Any]] = []
    normalized_title = _normalize_title_for_match(title)
    if conn_str:
        try:
            matches = _find_existing_tournament_codes(conn_str, start_date, normalized_title)
        except Exception as exc:
            warnings.append(
                {
                    "type": "tournament_lookup_unavailable",
                    "message": f"Existing tournament lookup was unavailable: {exc}",
                }
            )
            matches = []
        if len(matches) == 1:
            return matches[0]["Tournament_Code"], warnings
        if len(matches) > 1:
            warnings.append(
                {
                    "type": "ambiguous_existing_tournament_match",
                    "matches": [row["Tournament_Code"] for row in matches],
                    "message": "Multiple existing tournaments matched this title/date. Generated a provisional code instead.",
                }
            )
    generated = _generate_tournament_code(title, start_date)
    warnings.append(
        {
            "type": "generated_tournament_code",
            "code": generated,
            "message": f"Tournament_Code {generated!r} was generated instead of reused from existing data.",
        }
    )
    return generated, warnings


def _find_existing_tournament_codes(conn_str: str, start_date: date, normalized_title: str) -> list[dict[str, Any]]:
    sql = """
SELECT Tournament_Code, Tournament_Descr
FROM ratings.tournaments
WHERE Tournament_Date = ?
"""
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(sql, start_date)
        rows = []
        for tournament_code, tournament_descr in cursor.fetchall():
            if _normalize_title_for_match(tournament_descr or "") == normalized_title:
                rows.append({"Tournament_Code": tournament_code, "Tournament_Descr": tournament_descr})
        return rows
    finally:
        conn.close()


def _normalize_title_for_match(title: str) -> str:
    ascii_text = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode("ascii")
    words = NORMALIZE_TITLE_WORD_RE.findall(ascii_text.lower())
    normalized = []
    for word in words:
        if word == "tourney":
            normalized.append("tournament")
        else:
            normalized.append(word)
    return " ".join(normalized)


def _generate_tournament_code(title: str, start_date: date) -> str:
    ascii_text = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode("ascii").lower()
    words = [word for word in NORMALIZE_TITLE_WORD_RE.findall(ascii_text) if word not in STOP_WORDS]
    if not words:
        words = NORMALIZE_TITLE_WORD_RE.findall(ascii_text) or ["report"]
    prefix = "".join(words)[:10]
    return f"{prefix}{start_date.strftime('%Y%m%d')}"


def _load_membership_expirations(conn_str: str, player_ids: Any) -> dict[int, date | None]:
    player_ids = sorted(set(int(player_id) for player_id in player_ids))
    if not player_ids:
        return {}
    placeholders = ", ".join("?" for _ in player_ids)
    sql = f"""
SELECT AGAID, ExpirationDate
FROM membership.members
WHERE AGAID IN ({placeholders})
"""
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(sql, *player_ids)
        return {int(agaid): expiration_date for agaid, expiration_date in cursor.fetchall()}
    finally:
        conn.close()


def _build_membership_warnings(
    membership_map: dict[int, date | None] | None,
    games: list[ParsedGame],
    game_date: date,
    tournament_code: str,
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    if membership_map is None:
        return warnings
    seen_missing: set[int] = set()
    seen_expired: set[int] = set()
    today = date.today()

    for game in games:
        for agaid in (game.white_id, game.black_id):
            if agaid not in membership_map:
                if agaid not in seen_missing:
                    seen_missing.add(agaid)
                    warnings.append(
                        {
                            "type": "missing_membership_record",
                            "agaid": agaid,
                            "game_date": game_date,
                            "tournament_code": tournament_code,
                            "message": f"AGAID {agaid} was not found in membership.members for game date {game_date.isoformat()}.",
                        }
                    )
                continue

            expiration_date = membership_map[agaid]
            if expiration_date is None or expiration_date >= game_date or agaid in seen_expired:
                continue
            seen_expired.add(agaid)
            warnings.append(
                {
                    "type": "expired_membership_on_game_date",
                    "agaid": agaid,
                    "game_date": game_date,
                    "expiration_date": expiration_date,
                    "currently_expired": expiration_date < today,
                    "tournament_code": tournament_code,
                    "message": (
                        f"AGAID {agaid} had membership expiration {expiration_date.isoformat()}, "
                        f"before game date {game_date.isoformat()}."
                    ),
                }
            )
    return warnings


def _load_connection_string_optional() -> str | None:
    conn = os.environ.get("SQL_CONNECTION_STRING") or os.environ.get("MYSQL_SYNC_SQL_CONNECTION_STRING")
    if conn and conn.strip():
        return conn

    settings_path = Path(__file__).with_name("local.settings.json")
    if not settings_path.exists():
        return None
    try:
        settings = json.loads(settings_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    values = settings.get("Values", {})
    conn = values.get("SQL_CONNECTION_STRING") or values.get("MYSQL_SYNC_SQL_CONNECTION_STRING")
    return conn if conn and str(conn).strip() else None



def _infer_rounds_from_game_order(games: list[ParsedGame]) -> list[int]:
    inferred_round_numbers: list[int] = []
    current_round = 1
    players_in_round: set[int] = set()

    for game in games:
        game_players = {game.white_id, game.black_id}
        if players_in_round.intersection(game_players):
            current_round += 1
            players_in_round = set()
        game.round_text = str(current_round)
        inferred_round_numbers.append(current_round)
        players_in_round.update(game_players)

    return inferred_round_numbers

def _write_csv_exports(output_dir: Path, payload: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    tournament_path = output_dir / "tournaments.csv"
    games_path = output_dir / "games.csv"
    tournament_rows = payload.get("tournament_rows") or [payload["tournament_row"]]
    game_rows = payload["game_rows"]
    _write_csv_rows(tournament_path, CSV_TABLE_SPECS["tournaments"], tournament_rows)
    _write_csv_rows(games_path, CSV_TABLE_SPECS["games"], game_rows)


def _write_csv_rows(csv_path: Path, columns: tuple[str, ...], rows: list[dict[str, Any]]) -> None:
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _format_csv_value(row.get(column)) for column in columns})


def _format_csv_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, date):
        return value.isoformat()
    return str(value)

if __name__ == "__main__":
    main()

