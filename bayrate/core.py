import csv
import json
import math
import random
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable


SQRT2 = math.sqrt(2.0)
SQRT2PI = math.sqrt(2.0 / math.pi)
LOG_SQRT_2PI = 0.5 * math.log(2.0 * math.pi)
MIN_PROBABILITY = 1e-300

INIT_SIGMA_R = [
    -49.5, -48.5, -47.5, -46.5, -45.5,
    -44.5, -43.5, -42.5, -41.5, -40.5,
    -39.5, -38.5, -37.5, -36.5, -35.5,
    -34.5, -33.5, -32.5, -31.5, -30.5,
    -29.5, -28.5, -27.5, -26.5, -25.5,
    -24.5, -23.5, -22.5, -21.5, -20.5,
    -19.5, -18.5, -17.5, -16.5, -15.5,
    -14.5, -13.5, -12.5, -11.5, -10.5,
    -9.5, -8.5, -7.5, -6.5, -5.5,
    -4.5, -3.5, -2.5, -1.5, -0.5,
    0.5, 1.5, 2.5, 3.5, 4.5,
    5.5, 6.5, 7.5, 8.5,
]

INIT_SIGMA_S = [
    5.73781, 5.63937, 5.54098, 5.44266, 5.34439,
    5.24619, 5.14806, 5.05000, 4.95202, 4.85412,
    4.75631, 4.65859, 4.56098, 4.46346, 4.36606,
    4.26878, 4.17163, 4.07462, 3.97775, 3.88104,
    3.78451, 3.68816, 3.59201, 3.49607, 3.40037,
    3.30492, 3.20975, 3.11488, 3.02035, 2.92617,
    2.83240, 2.73907, 2.64622, 2.55392, 2.46221,
    2.37118, 2.28090, 2.19146, 2.10297, 2.01556,
    1.92938, 1.84459, 1.76139, 1.68003, 1.60078,
    1.52398, 1.45000, 1.37931, 1.31244, 1.25000,
    1.19269, 1.14127, 1.09659, 1.05948, 1.03078,
    1.01119, 1.00125, 1.00000, 1.00000,
]


@dataclass(slots=True)
class BayrateConfig:
    allow_online_games: bool = False
    min_game_date: date | None = None
    max_game_date: date | None = None
    max_events: int | None = None
    optimizer_max_iterations: int = 400
    optimizer_gradient_tolerance: float = 1e-3
    optimizer_min_step: float = 1e-8
    optimizer_armijo: float = 1e-4
    optimizer_random_jitter: float = 0.1
    random_seed: int = 1
    inactivity_growth_per_day: float = 0.0005
    full_reseed_threshold: float = 3.0
    partial_reseed_threshold: float = 1.0
    partial_reseed_rating_offset: float = 0.024746
    partial_reseed_rating_factor: float = 0.32127
    partial_reseed_sigma_base: float = 0.256
    partial_reseed_sigma_power: float = 1.9475


@dataclass(slots=True)
class CsvRowError:
    path: Path
    line_number: int
    column: str
    message: str

    def format(self) -> str:
        return f"{self.path}:{self.line_number}: {self.column}: {self.message}"


class CsvValidationError(ValueError):
    def __init__(self, errors: list[CsvRowError]) -> None:
        self.errors = errors
        detail = "\n".join(error.format() for error in errors[:10])
        more = "" if len(errors) <= 10 else f"\n... {len(errors) - 10} more error(s)"
        super().__init__(f"CSV validation failed with {len(errors)} error(s):\n{detail}{more}")


@dataclass(slots=True)
class GameRecord:
    source_game_id: int
    tournament_code: str | None
    game_date: date
    round_number: int | None
    white_agaid: int
    black_agaid: int
    white_seed_rank: float
    black_seed_rank: float
    handicap: int
    komi: float
    white_wins: bool
    is_online_game: bool


@dataclass(slots=True)
class OfficialSnapshot:
    player_id: int
    rating: float
    sigma: float
    elab_date: date
    tournament_code: str | None
    row_id: int


@dataclass(slots=True)
class TdListEntry:
    player_id: int
    rating: float
    sigma: float
    last_rating_date: date


@dataclass(slots=True)
class EventRecord:
    event_key: str
    event_date: date
    tournament_code: str | None
    games: list[GameRecord]


@dataclass(slots=True)
class PreparedGame:
    source_game_id: int
    game_date: date
    tournament_code: str | None
    white_idx: int
    black_idx: int
    white_agaid: int
    black_agaid: int
    white_wins: bool
    handicap: int
    komi: float
    handicapeqv: float
    sigma_px: float


@dataclass(slots=True)
class PreparedPlayer:
    player_id: int
    index: int
    rank_seed: float
    seed: float
    prior_sigma: float
    prior_rating_for_update: float | None
    prior_sigma_for_update: float | None
    prior_date_for_update: date | None


@dataclass(slots=True)
class EventPlayerResult:
    player_id: int
    event_key: str
    event_date: date
    tournament_code: str | None
    rank_seed: float
    seed_before_closing_boundary: float
    prior_rating: float | None
    prior_sigma: float | None
    rating_after: float
    sigma_after: float


@dataclass(slots=True)
class EventGameResult:
    source_game_id: int
    event_key: str
    event_date: date
    tournament_code: str | None
    white_agaid: int
    black_agaid: int
    handicap: int
    komi: float
    white_wins: bool
    white_seed_before: float
    black_seed_before: float
    pre_event_expected_white: float
    post_event_expected_white: float


@dataclass(slots=True)
class Metrics:
    games: int = 0
    correct_predictions: int = 0
    log_loss_sum: float = 0.0
    brier_sum: float = 0.0

    def record(self, predicted_white: float, actual_white: float) -> None:
        clipped = min(max(predicted_white, 1e-12), 1.0 - 1e-12)
        self.games += 1
        self.correct_predictions += int((predicted_white >= 0.5) == (actual_white >= 0.5))
        self.log_loss_sum += -(actual_white * math.log(clipped) + (1.0 - actual_white) * math.log(1.0 - clipped))
        self.brier_sum += (predicted_white - actual_white) ** 2

    def as_dict(self) -> dict[str, float | int]:
        if self.games == 0:
            return {"games": 0, "accuracy": 0.0, "average_log_loss": 0.0, "average_brier": 0.0}
        return {
            "games": self.games,
            "accuracy": self.correct_predictions / self.games,
            "average_log_loss": self.log_loss_sum / self.games,
            "average_brier": self.brier_sum / self.games,
        }


@dataclass(slots=True)
class BayrateRunResult:
    config: dict[str, float | int | str | None | bool]
    event_count: int
    player_count: int
    pre_event_metrics: dict[str, float | int]
    post_event_fit_metrics: dict[str, float | int]
    player_results: list[EventPlayerResult]
    game_results: list[EventGameResult]


def _parse_date(value: str | None) -> date | None:
    if value is None:
        return None
    text = value.strip()
    if not text or text.upper() == "NULL":
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    raise ValueError(f"Unsupported date value: {value!r}")


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    text = value.strip()
    if not text or text.upper() == "NULL":
        return None
    return int(float(text))


def _parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = value.strip()
    if not text or text.upper() == "NULL":
        return None
    return float(text)


def _parse_csv_bool(value: str | int | None) -> bool:
    if value is None:
        raise ValueError("missing boolean value")
    if isinstance(value, int):
        return value != 0
    text = value.strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"", "0", "false", "no", "n", "null"}:
        return False
    raise ValueError(f"unsupported boolean value {value!r}")


def rank_to_seed(rank_text: str | None) -> float | None:
    if rank_text is None:
        return None
    text = rank_text.strip()
    if not text:
        return None
    suffix = text[-1].lower()
    try:
        value = int(text[:-1])
    except ValueError:
        return None
    if suffix == "k":
        return -(value + 0.5)
    if suffix == "d":
        return value + 0.5
    return None


def _cubic_spline_interp(xs: list[float], ys: list[float], x: float) -> float:
    if x <= xs[0]:
        return ys[0]
    if x >= xs[-1]:
        return ys[-1]
    n = len(xs)
    y2 = [0.0] * n
    work = [0.0] * (n - 1)
    for i in range(1, n - 1):
        sig = (xs[i] - xs[i - 1]) / (xs[i + 1] - xs[i - 1])
        p = sig * y2[i - 1] + 2.0
        y2[i] = (sig - 1.0) / p
        slope_next = (ys[i + 1] - ys[i]) / (xs[i + 1] - xs[i])
        slope_prev = (ys[i] - ys[i - 1]) / (xs[i] - xs[i - 1])
        work[i] = (6.0 * (slope_next - slope_prev) / (xs[i + 1] - xs[i - 1]) - sig * work[i - 1]) / p
    for k in range(n - 2, -1, -1):
        y2[k] = y2[k] * y2[k + 1] + work[k]

    lo = 0
    hi = n - 1
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if xs[mid] > x:
            hi = mid
        else:
            lo = mid
    h = xs[hi] - xs[lo]
    a = (xs[hi] - x) / h
    b = (x - xs[lo]) / h
    return (
        a * ys[lo]
        + b * ys[hi]
        + ((a * a * a - a) * y2[lo] + (b * b * b - b) * y2[hi]) * h * h / 6.0
    )

def calc_init_sigma(seed: float) -> float:
    if seed > 7.5:
        return 1.0
    if seed < -50.5:
        return 6.0
    adjusted = seed - 1.0 if seed > 0 else seed + 1.0
    return _cubic_spline_interp(INIT_SIGMA_R, INIT_SIGMA_S, adjusted)


def close_boundary(value: float) -> float:
    return value - 1.0 if value > 0 else value + 1.0


def open_boundary(value: float) -> float:
    return value + 1.0 if value > 0 else value - 1.0


def calc_handicap_eqv(handicap: int, komi: float) -> tuple[float, float]:
    if handicap in (0, 1):
        handicapeqv = 0.580 - 0.0757 * komi
        sigma_px = 1.0649 - 0.0021976 * komi + 0.00014984 * komi * komi
        return handicapeqv, sigma_px
    base = {
        2: 1.13672,
        3: 1.18795,
        4: 1.22841,
        5: 1.27457,
        6: 1.31978,
        7: 1.35881,
        8: 1.39782,
        9: 1.43614,
    }.get(handicap, 1.43614)
    return handicap - 0.0757 * komi, (-0.0035169 * komi) + base


def normal_win_probability(rd: float, sigma_px: float) -> float:
    p = 0.5 * math.erfc(-rd / (sigma_px * SQRT2))
    return min(max(p, MIN_PROBABILITY), 1.0 - MIN_PROBABILITY)


GAME_CSV_REQUIRED_COLUMNS = [
    "Game_ID",
    "Tournament_Code",
    "Game_Date",
    "Round",
    "Pin_Player_1",
    "Pin_Player_2",
    "Rank_1",
    "Rank_2",
    "Color_1",
    "Handicap",
    "Komi",
    "Result",
    "Rated",
    "Exclude",
    "Online",
]


def _read_required_value(
    row: dict[str, str],
    column: str,
    path: Path,
    line_number: int,
    errors: list[CsvRowError],
) -> str | None:
    value = row.get(column)
    if value is None or not value.strip():
        errors.append(CsvRowError(path, line_number, column, "required value is missing"))
        return None
    return value


def _parse_required_int(
    row: dict[str, str],
    column: str,
    path: Path,
    line_number: int,
    errors: list[CsvRowError],
) -> int | None:
    value = _read_required_value(row, column, path, line_number, errors)
    if value is None:
        return None
    try:
        parsed = _parse_int(value)
    except ValueError as exc:
        errors.append(CsvRowError(path, line_number, column, str(exc)))
        return None
    if parsed is None:
        errors.append(CsvRowError(path, line_number, column, "required value is missing"))
    return parsed


def _parse_optional_int(
    row: dict[str, str],
    column: str,
    path: Path,
    line_number: int,
    errors: list[CsvRowError],
) -> int | None:
    try:
        return _parse_int(row.get(column))
    except ValueError as exc:
        errors.append(CsvRowError(path, line_number, column, str(exc)))
        return None


def _parse_required_float(
    row: dict[str, str],
    column: str,
    path: Path,
    line_number: int,
    errors: list[CsvRowError],
) -> float | None:
    value = _read_required_value(row, column, path, line_number, errors)
    if value is None:
        return None
    try:
        parsed = _parse_float(value)
    except ValueError as exc:
        errors.append(CsvRowError(path, line_number, column, str(exc)))
        return None
    if parsed is None:
        errors.append(CsvRowError(path, line_number, column, "required value is missing"))
    return parsed


def _parse_required_date(
    row: dict[str, str],
    column: str,
    path: Path,
    line_number: int,
    errors: list[CsvRowError],
) -> date | None:
    value = _read_required_value(row, column, path, line_number, errors)
    if value is None:
        return None
    try:
        parsed = _parse_date(value)
    except ValueError as exc:
        errors.append(CsvRowError(path, line_number, column, str(exc)))
        return None
    if parsed is None:
        errors.append(CsvRowError(path, line_number, column, "required value is missing"))
    return parsed


def _parse_required_bool(
    row: dict[str, str],
    column: str,
    path: Path,
    line_number: int,
    errors: list[CsvRowError],
) -> bool | None:
    value = _read_required_value(row, column, path, line_number, errors)
    if value is None:
        return None
    try:
        return _parse_csv_bool(value)
    except ValueError as exc:
        errors.append(CsvRowError(path, line_number, column, str(exc)))
        return None


def _add_rank_error(path: Path, line_number: int, column: str, value: str | None, errors: list[CsvRowError]) -> None:
    if value is None or not value.strip():
        errors.append(CsvRowError(path, line_number, column, "required value is missing"))
    else:
        errors.append(CsvRowError(path, line_number, column, f"unsupported rank value {value!r}"))


def load_games_from_csv(path: Path, config: BayrateConfig) -> list[GameRecord]:
    games: list[GameRecord] = []
    errors: list[CsvRowError] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        missing_columns = [column for column in GAME_CSV_REQUIRED_COLUMNS if column not in (reader.fieldnames or [])]
        if missing_columns:
            raise CsvValidationError(
                [CsvRowError(path, 1, column, "required column is missing") for column in missing_columns]
            )
        for line_number, row in enumerate(reader, start=2):
            game_date = _parse_required_date(row, "Game_Date", path, line_number, errors)
            rated = _parse_required_bool(row, "Rated", path, line_number, errors)
            excluded = _parse_required_bool(row, "Exclude", path, line_number, errors)
            is_online = _parse_required_bool(row, "Online", path, line_number, errors)
            if game_date is None or rated is None or excluded is None or is_online is None:
                continue
            if config.min_game_date and game_date < config.min_game_date:
                continue
            if config.max_game_date and game_date > config.max_game_date:
                continue
            if not rated or excluded:
                continue
            if is_online and not config.allow_online_games:
                continue
            color_1 = (row.get("Color_1") or "").strip().upper()
            result = (row.get("Result") or "").strip().upper()
            if color_1 not in {"W", "B"}:
                errors.append(CsvRowError(path, line_number, "Color_1", "expected W or B"))
                continue
            if result not in {"W", "B"}:
                errors.append(CsvRowError(path, line_number, "Result", "expected W or B"))
                continue
            game_id = _parse_required_int(row, "Game_ID", path, line_number, errors)
            round_number = _parse_optional_int(row, "Round", path, line_number, errors)
            pin_1 = _parse_required_int(row, "Pin_Player_1", path, line_number, errors)
            pin_2 = _parse_required_int(row, "Pin_Player_2", path, line_number, errors)
            handicap = _parse_required_int(row, "Handicap", path, line_number, errors)
            komi = _parse_required_float(row, "Komi", path, line_number, errors)
            if pin_1 is not None and pin_2 is not None and pin_1 == pin_2:
                errors.append(CsvRowError(path, line_number, "Pin_Player_2", "players must be different"))
            if (
                game_id is None
                or pin_1 is None
                or pin_2 is None
                or pin_1 == pin_2
                or handicap is None
                or komi is None
            ):
                continue
            seed_1 = rank_to_seed(row.get("Rank_1"))
            seed_2 = rank_to_seed(row.get("Rank_2"))
            if seed_1 is None:
                _add_rank_error(path, line_number, "Rank_1", row.get("Rank_1"), errors)
            if seed_2 is None:
                _add_rank_error(path, line_number, "Rank_2", row.get("Rank_2"), errors)
            if seed_1 is None or seed_2 is None:
                continue
            if color_1 == "W":
                white_agaid = pin_1
                black_agaid = pin_2
                white_seed_rank = seed_1
                black_seed_rank = seed_2
            else:
                white_agaid = pin_2
                black_agaid = pin_1
                white_seed_rank = seed_2
                black_seed_rank = seed_1
            games.append(
                GameRecord(
                    source_game_id=game_id,
                    tournament_code=(row.get("Tournament_Code") or "").strip() or None,
                    game_date=game_date,
                    round_number=round_number,
                    white_agaid=white_agaid,
                    black_agaid=black_agaid,
                    white_seed_rank=white_seed_rank,
                    black_seed_rank=black_seed_rank,
                    handicap=handicap,
                    komi=komi,
                    white_wins=(result == "W"),
                    is_online_game=is_online,
                )
            )
    if errors:
        raise CsvValidationError(errors)
    games.sort(key=lambda g: (g.game_date, g.tournament_code or "", g.round_number or 0, g.source_game_id))
    return games


def load_official_history(path: Path) -> dict[int, list[OfficialSnapshot]]:
    history: dict[int, list[OfficialSnapshot]] = {}
    errors: list[CsvRowError] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        for line_number, row in enumerate(reader, start=1):
            if len(row) < 6:
                errors.append(CsvRowError(path, line_number, "row", "expected 6 columns"))
                continue
            try:
                elab_date = _parse_date(row[3])
            except ValueError as exc:
                errors.append(CsvRowError(path, line_number, "Elab_Date", str(exc)))
                continue
            if elab_date is None:
                errors.append(CsvRowError(path, line_number, "Elab_Date", "required value is missing"))
                continue
            try:
                snapshot = OfficialSnapshot(
                    player_id=int(row[0]),
                    rating=float(row[1]),
                    sigma=float(row[2]),
                    elab_date=elab_date,
                    tournament_code=None if not row[4].strip() or row[4].strip().upper() == "NULL" else row[4].strip(),
                    row_id=int(row[5]),
                )
            except ValueError as exc:
                errors.append(CsvRowError(path, line_number, "row", str(exc)))
                continue
            history.setdefault(snapshot.player_id, []).append(snapshot)
    if errors:
        raise CsvValidationError(errors)
    for snapshots in history.values():
        snapshots.sort(key=lambda s: (s.elab_date, s.row_id))
    return history


def latest_snapshot_before(snapshots: list[OfficialSnapshot] | None, event_date: date) -> OfficialSnapshot | None:
    if not snapshots:
        return None
    latest: OfficialSnapshot | None = None
    for snapshot in snapshots:
        if snapshot.elab_date >= event_date:
            break
        latest = snapshot
    return latest


def build_events(games: Iterable[GameRecord]) -> list[EventRecord]:
    grouped: dict[str, EventRecord] = {}
    for game in games:
        if game.tournament_code:
            key = f"code:{game.tournament_code}"
        else:
            key = f"date:{game.game_date.isoformat()}"
        event = grouped.get(key)
        if event is None:
            event = EventRecord(
                event_key=key,
                event_date=game.game_date,
                tournament_code=game.tournament_code,
                games=[],
            )
            grouped[key] = event
        event.games.append(game)
    events = sorted(grouped.values(), key=lambda e: (e.event_date, e.tournament_code or "", e.games[0].source_game_id))
    return events


def _prepare_event(
    event: EventRecord,
    td_list: dict[int, TdListEntry],
    history: dict[int, list[OfficialSnapshot]],
    config: BayrateConfig,
) -> tuple[list[PreparedPlayer], list[PreparedGame]]:
    participant_rank_seed: dict[int, float] = {}
    win_count: dict[int, int] = {}
    for game in event.games:
        participant_rank_seed.setdefault(game.white_agaid, game.white_seed_rank)
        participant_rank_seed.setdefault(game.black_agaid, game.black_seed_rank)
        win_count.setdefault(game.white_agaid, 0)
        win_count.setdefault(game.black_agaid, 0)
        winner = game.white_agaid if game.white_wins else game.black_agaid
        win_count[winner] = win_count.get(winner, 0) + 1

    for player_id in participant_rank_seed:
        if player_id not in td_list:
            snapshot = latest_snapshot_before(history.get(player_id), event.event_date)
            if snapshot is not None:
                td_list[player_id] = TdListEntry(
                    player_id=player_id,
                    rating=snapshot.rating,
                    sigma=snapshot.sigma,
                    last_rating_date=snapshot.elab_date,
                )

    players: list[PreparedPlayer] = []
    ordered_ids = sorted(participant_rank_seed)
    index_by_id = {player_id: idx for idx, player_id in enumerate(ordered_ids)}
    for player_id in ordered_ids:
        rank_seed = participant_rank_seed[player_id]
        td_entry = td_list.get(player_id)
        seed = rank_seed
        prior_sigma = calc_init_sigma(seed)
        prior_rating: float | None = None
        prior_sigma_for_update: float | None = None
        prior_date: date | None = None
        if td_entry is not None:
            prior_rating = td_entry.rating
            prior_sigma_for_update = td_entry.sigma
            prior_date = td_entry.last_rating_date
            if td_entry.rating != 0.0 and td_entry.sigma != 0.0:
                if rank_seed * td_entry.rating > 0:
                    delta_r = rank_seed - td_entry.rating
                else:
                    delta_r = rank_seed - td_entry.rating - 2.0
                if delta_r < 0.0:
                    seed = td_entry.rating
                    day_count = max(0, (event.event_date - td_entry.last_rating_date).days)
                    prior_sigma = math.sqrt(td_entry.sigma * td_entry.sigma + (config.inactivity_growth_per_day * day_count) ** 2)
                elif delta_r >= config.full_reseed_threshold and win_count.get(player_id, 0) > 0:
                    seed = rank_seed
                    prior_sigma = calc_init_sigma(seed)
                elif delta_r >= config.partial_reseed_threshold and win_count.get(player_id, 0) > 0:
                    seed = td_entry.rating + config.partial_reseed_rating_offset + config.partial_reseed_rating_factor * delta_r
                    prior_sigma = math.sqrt(
                        td_entry.sigma * td_entry.sigma
                        + config.partial_reseed_sigma_base * math.pow(delta_r, config.partial_reseed_sigma_power)
                    )
                else:
                    seed = td_entry.rating
                    day_count = max(0, (event.event_date - td_entry.last_rating_date).days)
                    prior_sigma = math.sqrt(td_entry.sigma * td_entry.sigma + (config.inactivity_growth_per_day * day_count) ** 2)
        players.append(
            PreparedPlayer(
                player_id=player_id,
                index=index_by_id[player_id],
                rank_seed=rank_seed,
                seed=close_boundary(seed),
                prior_sigma=prior_sigma,
                prior_rating_for_update=prior_rating,
                prior_sigma_for_update=prior_sigma_for_update,
                prior_date_for_update=prior_date,
            )
        )

    prepared_games: list[PreparedGame] = []
    for game in event.games:
        handicapeqv, sigma_px = calc_handicap_eqv(game.handicap, game.komi)
        prepared_games.append(
            PreparedGame(
                source_game_id=game.source_game_id,
                game_date=game.game_date,
                tournament_code=game.tournament_code,
                white_idx=index_by_id[game.white_agaid],
                black_idx=index_by_id[game.black_agaid],
                white_agaid=game.white_agaid,
                black_agaid=game.black_agaid,
                white_wins=game.white_wins,
                handicap=game.handicap,
                komi=game.komi,
                handicapeqv=handicapeqv,
                sigma_px=sigma_px,
            )
        )
    return players, prepared_games


def _calc_pt_and_gradient(players: list[PreparedPlayer], games: list[PreparedGame], ratings: list[float]) -> tuple[float, list[float]]:
    gradient = [0.0] * len(players)
    pt = 0.0
    for player in players:
        z = (ratings[player.index] - player.seed) / player.prior_sigma
        pt += -0.5 * z * z - LOG_SQRT_2PI
        gradient[player.index] = -z / player.prior_sigma
    for game in games:
        rd = ratings[game.white_idx] - ratings[game.black_idx] - game.handicapeqv
        if game.white_wins:
            p = normal_win_probability(rd, game.sigma_px)
            denom = max(math.erfc(-rd / (SQRT2 * game.sigma_px)), MIN_PROBABILITY)
            dp = SQRT2PI / game.sigma_px * math.exp(-(rd * rd) / (2.0 * game.sigma_px * game.sigma_px)) / denom
            gradient[game.white_idx] += dp
            gradient[game.black_idx] -= dp
        else:
            p = normal_win_probability(-rd, game.sigma_px)
            denom = max(math.erfc(rd / (SQRT2 * game.sigma_px)), MIN_PROBABILITY)
            dp = SQRT2PI / game.sigma_px * math.exp(-(rd * rd) / (2.0 * game.sigma_px * game.sigma_px)) / denom
            gradient[game.white_idx] -= dp
            gradient[game.black_idx] += dp
        pt += math.log(max(p, MIN_PROBABILITY))
    return pt, gradient


def _dot(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


def _norm(a: list[float]) -> float:
    return math.sqrt(_dot(a, a))


def _solve_event_ratings(
    players: list[PreparedPlayer],
    games: list[PreparedGame],
    config: BayrateConfig,
    rng: random.Random,
) -> tuple[list[float], int]:
    ratings = [player.seed + rng.uniform(0.0, config.optimizer_random_jitter) for player in players]
    best_ratings = ratings[:]
    best_pt, gradient = _calc_pt_and_gradient(players, games, ratings)
    direction = gradient[:]
    iterations = 0
    for iterations in range(1, config.optimizer_max_iterations + 1):
        grad_norm = _norm(gradient)
        if grad_norm <= config.optimizer_gradient_tolerance:
            break
        directional_gain = _dot(gradient, direction)
        if directional_gain <= 0.0:
            direction = gradient[:]
            directional_gain = _dot(gradient, direction)
            if directional_gain <= 0.0:
                break
        step = min(1.0, 1.0 / max(grad_norm, 1e-12))
        accepted = False
        next_ratings = ratings
        next_pt = best_pt
        next_gradient = gradient
        while step >= config.optimizer_min_step:
            candidate = [r + step * d for r, d in zip(ratings, direction)]
            candidate_pt, candidate_gradient = _calc_pt_and_gradient(players, games, candidate)
            if candidate_pt >= best_pt + config.optimizer_armijo * step * directional_gain:
                next_ratings = candidate
                next_pt = candidate_pt
                next_gradient = candidate_gradient
                accepted = True
                break
            step *= 0.5
        if not accepted:
            direction = gradient[:]
            continue
        beta_num = _dot(next_gradient, [ng - g for ng, g in zip(next_gradient, gradient)])
        beta_den = max(_dot(gradient, gradient), 1e-12)
        beta = max(0.0, beta_num / beta_den)
        direction = [ng + beta * d for ng, d in zip(next_gradient, direction)]
        ratings = next_ratings
        best_ratings = next_ratings
        best_pt = next_pt
        gradient = next_gradient
    return best_ratings, iterations


def _calc_sigma2(players: list[PreparedPlayer], games: list[PreparedGame], ratings: list[float]) -> list[float]:
    new_sigma = [0.0] * len(players)
    for player in players:
        sum_x2w = 0.0
        sum_w = 0.0
        sigma = player.prior_sigma
        for i in range(100):
            x = -5.0 * sigma - sigma / 20.0 + i * sigma / 10.0
            r = ratings[player.index] + x
            z = (r - player.seed) / sigma
            w = math.exp(-0.5 * z * z) / math.sqrt(2.0 * math.pi)
            for game in games:
                if game.white_idx == player.index:
                    rd = r - ratings[game.black_idx] - game.handicapeqv
                    if game.white_wins:
                        w *= math.erfc(-rd / (game.sigma_px * SQRT2))
                    else:
                        w *= math.erfc(rd / (game.sigma_px * SQRT2))
                elif game.black_idx == player.index:
                    rd = ratings[game.white_idx] - r - game.handicapeqv
                    if game.white_wins:
                        w *= math.erfc(-rd / (game.sigma_px * SQRT2))
                    else:
                        w *= math.erfc(rd / (game.sigma_px * SQRT2))
            sum_x2w += x * x * w
            sum_w += w
        new_sigma[player.index] = sigma if sum_w <= 0.0 else math.sqrt(sum_x2w / sum_w)
    return new_sigma


def _run_events(
    events: list[EventRecord],
    history: dict[int, list[OfficialSnapshot]],
    config: BayrateConfig,
    *,
    td_list: dict[int, TdListEntry],
) -> BayrateRunResult:
    rng = random.Random(config.random_seed)
    pre_metrics = Metrics()
    post_metrics = Metrics()
    player_results: list[EventPlayerResult] = []
    game_results: list[EventGameResult] = []
    processed_event_count = 0

    for event in events:
        processed_event_count += 1
        players, prepared_games = _prepare_event(
            event,
            td_list,
            history,
            config,
        )
        ratings_closed, _ = _solve_event_ratings(players, prepared_games, config, rng)
        posterior_sigma = _calc_sigma2(players, prepared_games, ratings_closed)
        ratings_open = [open_boundary(r) for r in ratings_closed]
        seeds_open = [open_boundary(player.seed) for player in players]

        for game in prepared_games:
            pre_rd = seeds_open[game.white_idx] - seeds_open[game.black_idx] - game.handicapeqv
            post_rd = ratings_open[game.white_idx] - ratings_open[game.black_idx] - game.handicapeqv
            pre_prob = normal_win_probability(pre_rd, game.sigma_px)
            post_prob = normal_win_probability(post_rd, game.sigma_px)
            actual_white = 1.0 if game.white_wins else 0.0
            pre_metrics.record(pre_prob, actual_white)
            post_metrics.record(post_prob, actual_white)
            game_results.append(
                EventGameResult(
                    source_game_id=game.source_game_id,
                    event_key=event.event_key,
                    event_date=event.event_date,
                    tournament_code=event.tournament_code,
                    white_agaid=game.white_agaid,
                    black_agaid=game.black_agaid,
                    handicap=game.handicap,
                    komi=game.komi,
                    white_wins=game.white_wins,
                    white_seed_before=seeds_open[game.white_idx],
                    black_seed_before=seeds_open[game.black_idx],
                    pre_event_expected_white=pre_prob,
                    post_event_expected_white=post_prob,
                )
            )

        for player in players:
            rating_after = ratings_open[player.index]
            sigma_after = posterior_sigma[player.index]
            player_results.append(
                EventPlayerResult(
                    player_id=player.player_id,
                    event_key=event.event_key,
                    event_date=event.event_date,
                    tournament_code=event.tournament_code,
                    rank_seed=player.rank_seed,
                    seed_before_closing_boundary=open_boundary(player.seed),
                    prior_rating=player.prior_rating_for_update,
                    prior_sigma=player.prior_sigma_for_update,
                    rating_after=rating_after,
                    sigma_after=sigma_after,
                )
            )
            td_list[player.player_id] = TdListEntry(
                player_id=player.player_id,
                rating=rating_after,
                sigma=sigma_after,
                last_rating_date=event.event_date,
            )

    return BayrateRunResult(
        config=asdict(config),
        event_count=processed_event_count,
        player_count=len(td_list),
        pre_event_metrics=pre_metrics.as_dict(),
        post_event_fit_metrics=post_metrics.as_dict(),
        player_results=player_results,
        game_results=game_results,
    )


def run_bayrate_loaded(
    games: list[GameRecord],
    history: dict[int, list[OfficialSnapshot]],
    config: BayrateConfig | None = None,
    *,
    initial_td_list: dict[int, TdListEntry] | None = None,
) -> BayrateRunResult:
    effective_config = config or BayrateConfig()
    events = build_events(games)
    if effective_config.max_events is not None:
        events = events[: effective_config.max_events]
    td_list = dict(initial_td_list or {})
    return _run_events(
        events,
        history,
        effective_config,
        td_list=td_list,
    )


def run_bayrate(
    games_path: Path,
    ratings_path: Path,
    config: BayrateConfig | None = None,
) -> BayrateRunResult:
    effective_config = config or BayrateConfig()
    games = load_games_from_csv(games_path, effective_config)
    history = load_official_history(ratings_path)
    return run_bayrate_loaded(games, history, effective_config)


def result_to_json(result: BayrateRunResult) -> str:
    return json.dumps(
        {
            "config": result.config,
            "event_count": result.event_count,
            "player_count": result.player_count,
            "pre_event_metrics": result.pre_event_metrics,
            "post_event_fit_metrics": result.post_event_fit_metrics,
            "player_results": [asdict(row) for row in result.player_results],
            "game_results": [asdict(row) for row in result.game_results],
        },
        indent=2,
        default=str,
    )
