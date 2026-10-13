import json
from datetime import date
from pathlib import Path
from typing import Any

import pyodbc

from function2.bayrate import (
    EventReplayBatch,
    GameRecord,
    BayrateConfig,
    OfficialSnapshot,
    TdListEntry,
    _parse_bool,
    _parse_date,
    _parse_float,
    _parse_int,
    rank_to_seed,
)

DEFAULT_DDL_PATH = Path(__file__).resolve().parents[1] / 'ratings-bayrate.sql'


def load_connection_string(settings_path: Path | None = None) -> str:
    import os
    conn = os.environ.get('SQL_CONNECTION_STRING') or os.environ.get('MYSQL_SYNC_SQL_CONNECTION_STRING')
    if conn:
        return conn
    candidates = []
    if settings_path is not None:
        candidates.append(settings_path)
    repo_root = Path(__file__).resolve().parents[1]
    candidates.extend([Path(__file__).resolve().parent / 'local.settings.json', repo_root / 'local.settings.json'])
    for candidate in candidates:
        if not candidate.exists():
            continue
        values = json.loads(candidate.read_text(encoding='utf-8')).get('Values', {})
        conn = values.get('SQL_CONNECTION_STRING') or values.get('MYSQL_SYNC_SQL_CONNECTION_STRING')
        if conn:
            return conn
    raise RuntimeError('SQL connection string not found')


def connect_sql(conn_str: str) -> pyodbc.Connection:
    conn = pyodbc.connect(conn_str)
    conn.autocommit = False
    return conn


def _fetch_dicts(cursor: pyodbc.Cursor) -> list[dict[str, Any]]:
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


def ensure_schema(conn: pyodbc.Connection, ddl_path: Path = DEFAULT_DDL_PATH) -> None:
    sql = ddl_path.read_text(encoding='utf-8-sig')
    cur = conn.cursor()
    for statement in [part.strip() for part in sql.split('GO') if part.strip()]:
        cur.execute(statement)
    conn.commit()


def load_games_from_sql(conn: pyodbc.Connection, config: BayrateConfig) -> list[GameRecord]:
    cur = conn.cursor()
    cur.execute('''
SELECT [Game_ID], [Tournament_Code], [Game_Date], [Round], [Pin_Player_1], [Color_1], [Rank_1],
       [Pin_Player_2], [Color_2], [Rank_2], [Handicap], [Komi], [Result], [Online], [Exclude], [Rated]
FROM [ratings].[vw_bayrate_games_source]
ORDER BY [Game_Date], [Tournament_Code], [Round], [Game_ID]
''')
    raw_rows = _fetch_dicts(cur)
    games: list[GameRecord] = []
    for row in raw_rows:
        game_date = row.get('Game_Date')
        parsed_game_date = game_date if isinstance(game_date, date) else _parse_date(None if game_date is None else str(game_date))
        if parsed_game_date is None:
            continue
        if config.min_game_date and parsed_game_date < config.min_game_date:
            continue
        if config.max_game_date and parsed_game_date > config.max_game_date:
            continue
        if not _parse_bool(row.get('Rated')) or _parse_bool(row.get('Exclude')):
            continue
        is_online = _parse_bool(row.get('Online'))
        if is_online and not config.allow_online_games:
            continue
        color_1 = str(row.get('Color_1') or '').strip().upper()
        result = str(row.get('Result') or '').strip().upper()
        if color_1 not in {'W', 'B'} or result not in {'W', 'B'}:
            continue
        pin_1 = _parse_int(None if row.get('Pin_Player_1') is None else str(row.get('Pin_Player_1')))
        pin_2 = _parse_int(None if row.get('Pin_Player_2') is None else str(row.get('Pin_Player_2')))
        if pin_1 is None or pin_2 is None or pin_1 == pin_2:
            continue
        seed_1 = rank_to_seed(None if row.get('Rank_1') is None else str(row.get('Rank_1')))
        seed_2 = rank_to_seed(None if row.get('Rank_2') is None else str(row.get('Rank_2')))
        if seed_1 is None or seed_2 is None:
            continue
        if color_1 == 'W':
            white_agaid = pin_1
            black_agaid = pin_2
            white_seed_rank = seed_1
            black_seed_rank = seed_2
        else:
            white_agaid = pin_2
            black_agaid = pin_1
            white_seed_rank = seed_2
            black_seed_rank = seed_1
        games.append(GameRecord(
            source_game_id=_parse_int(None if row.get('Game_ID') is None else str(row.get('Game_ID'))) or 0,
            tournament_code=(str(row.get('Tournament_Code') or '').strip() or None),
            game_date=parsed_game_date,
            round_number=_parse_int(None if row.get('Round') is None else str(row.get('Round'))),
            white_agaid=white_agaid,
            black_agaid=black_agaid,
            white_seed_rank=white_seed_rank,
            black_seed_rank=black_seed_rank,
            handicap=_parse_int(None if row.get('Handicap') is None else str(row.get('Handicap'))) or 0,
            komi=_parse_float(None if row.get('Komi') is None else str(row.get('Komi'))) or 0.0,
            white_wins=(result == 'W'),
            is_online_game=is_online,
        ))
    return games


def load_official_history_from_sql(conn: pyodbc.Connection) -> dict[int, list[OfficialSnapshot]]:
    cur = conn.cursor()
    cur.execute('''
SELECT [Player_ID], [Rating], [Sigma], [Elab_Date], [Tournament_Code], [Row_ID]
FROM [ratings].[vw_bayrate_snapshots_source]
ORDER BY [Player_ID], [Elab_Date], [Row_ID]
''')
    rows = _fetch_dicts(cur)
    history: dict[int, list[OfficialSnapshot]] = {}
    for row in rows:
        elab_date = row.get('Elab_Date')
        parsed_elab_date = elab_date if isinstance(elab_date, date) else _parse_date(None if elab_date is None else str(elab_date))
        if parsed_elab_date is None:
            continue
        snapshot = OfficialSnapshot(
            player_id=int(row['Player_ID']),
            rating=float(row['Rating']),
            sigma=float(row['Sigma']),
            elab_date=parsed_elab_date,
            tournament_code=(str(row.get('Tournament_Code') or '').strip() or None),
            row_id=int(row['Row_ID']),
        )
        history.setdefault(snapshot.player_id, []).append(snapshot)
    return history


def refresh_event_index(conn: pyodbc.Connection, model_label: str) -> None:
    conn.cursor().execute('EXEC [ratings].[sp_bayrate_refresh_event_index] @ModelLabel = ?', model_label)
    conn.commit()


def get_replay_boundary(conn: pyodbc.Connection, model_label: str) -> dict[str, Any] | None:
    cur = conn.cursor()
    cur.execute('EXEC [ratings].[sp_bayrate_get_replay_boundary] @ModelLabel = ?', model_label)
    rows = _fetch_dicts(cur)
    return rows[0] if rows else None


def load_event_ordinal_lookup(conn: pyodbc.Connection, model_label: str) -> dict[str, int]:
    cur = conn.cursor()
    cur.execute('SELECT [EventKey], [EventOrdinal] FROM [ratings].[bayrate_event_index] WHERE [ModelLabel] = ?', model_label)
    return {str(row['EventKey']): int(row['EventOrdinal']) for row in _fetch_dicts(cur)}



def get_cutover_control(conn: pyodbc.Connection, model_label: str) -> dict[str, Any] | None:
    cur = conn.cursor()
    cur.execute('EXEC [ratings].[sp_bayrate_get_cutover_control] @ModelLabel = ?', model_label)
    rows = _fetch_dicts(cur)
    return rows[0] if rows else None


def load_event_boundary(conn: pyodbc.Connection, model_label: str, event_ordinal: int) -> dict[str, Any] | None:
    cur = conn.cursor()
    cur.execute('SELECT [EventOrdinal], [EventKey], [EventDate], [Tournament_Code] FROM [ratings].[bayrate_event_index] WHERE [ModelLabel] = ? AND [EventOrdinal] = ?', model_label, event_ordinal)
    rows = _fetch_dicts(cur)
    return rows[0] if rows else None


def load_snapshot_seed_before_event(conn: pyodbc.Connection, model_label: str, event_ordinal: int) -> tuple[dict[int, TdListEntry], dict[int, int]]:
    cur = conn.cursor()
    cur.execute('EXEC [ratings].[sp_bayrate_get_snapshot_seed_before_event] @ModelLabel = ?, @EventOrdinal = ?', model_label, event_ordinal)
    rows = _fetch_dicts(cur)
    td_list: dict[int, TdListEntry] = {}
    momentum_streaks: dict[int, int] = {}
    for row in rows:
        player_id = int(row['AGAID'])
        td_list[player_id] = TdListEntry(
            player_id=player_id,
            rating=float(row['Rating']),
            sigma=float(row['Sigma']),
            last_rating_date=row['LastRatingDate'],
        )
        momentum_streaks[player_id] = 0
    return td_list, momentum_streaks


def start_run(conn: pyodbc.Connection, model_label: str, config_hash: bytes, source_version: str | None, boundary: dict[str, Any]) -> int:
    cur = conn.cursor()
    cur.execute('''
EXEC [ratings].[sp_bayrate_start_run]
    @ModelLabel = ?, @ConfigHash = ?, @SourceVersion = ?,
    @ReplayFromEventOrdinal = ?, @ReplayFromEventKey = ?, @ReplayFromEventDate = ?
''', model_label, config_hash, source_version, int(boundary['EventOrdinal']), str(boundary['EventKey']), boundary['EventDate'])
    run_id = int(cur.fetchone()[0])
    conn.commit()
    return run_id


def load_checkpoint_before_event(conn: pyodbc.Connection, model_label: str, event_ordinal: int) -> tuple[dict[int, TdListEntry], dict[int, int]]:
    cur = conn.cursor()
    cur.execute('EXEC [ratings].[sp_bayrate_get_checkpoint_before_event] @ModelLabel = ?, @EventOrdinal = ?', model_label, event_ordinal)
    rows = _fetch_dicts(cur)
    td_list: dict[int, TdListEntry] = {}
    momentum_streaks: dict[int, int] = {}
    for row in rows:
        player_id = int(row['AGAID'])
        td_list[player_id] = TdListEntry(player_id=player_id, rating=float(row['Rating']), sigma=float(row['Sigma']), last_rating_date=row['LastRatingDate'])
        momentum_streaks[player_id] = int(row.get('MomentumStreak') or 0)
    return td_list, momentum_streaks


def purge_from_event(conn: pyodbc.Connection, model_label: str, event_ordinal: int) -> None:
    conn.cursor().execute('EXEC [ratings].[sp_bayrate_purge_from_event] @ModelLabel = ?, @EventOrdinal = ?', model_label, event_ordinal)
    conn.commit()


def publish_event_batch(conn: pyodbc.Connection, run_id: int, model_label: str, batch: EventReplayBatch) -> None:
    cur = conn.cursor()
    cur.fast_executemany = True
    player_rows = [(model_label, run_id, batch.event_ordinal, row.event_key, row.event_date, row.tournament_code, row.player_id, row.rank_seed, row.seed_before_closing_boundary, row.prior_rating, row.prior_sigma, row.rating_after, row.sigma_after) for row in batch.player_results]
    game_rows = [(model_label, run_id, batch.event_ordinal, row.event_key, row.event_date, row.tournament_code, row.source_game_id, row.white_agaid, row.black_agaid, row.handicap, row.komi, 1 if row.white_wins else 0, row.white_seed_before, row.black_seed_before, row.pre_event_expected_white, row.post_event_expected_white) for row in batch.game_results]
    checkpoint_rows = [(model_label, row.player_id, row.event_ordinal, row.event_key, row.event_date, row.rating, row.sigma, row.last_rating_date, row.momentum_streak, run_id) for row in batch.checkpoints]
    if player_rows:
        cur.executemany('''
INSERT INTO [ratings].[bayrate_event_player_ratings]
([ModelLabel], [RunID], [EventOrdinal], [EventKey], [EventDate], [Tournament_Code], [AGAID], [RankSeed], [SeedBefore], [PriorRating], [PriorSigma], [RatingAfter], [SigmaAfter])
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
''', player_rows)
    if game_rows:
        cur.executemany('''
INSERT INTO [ratings].[bayrate_event_game_results]
([ModelLabel], [RunID], [EventOrdinal], [EventKey], [EventDate], [Tournament_Code], [SourceGameID], [WhiteAGAID], [BlackAGAID], [Handicap], [Komi], [WhiteWins], [WhiteSeedBefore], [BlackSeedBefore], [PreEventExpectedWhite], [PostEventExpectedWhite])
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
''', game_rows)
    if checkpoint_rows:
        cur.executemany('''
INSERT INTO [ratings].[bayrate_player_checkpoint]
([ModelLabel], [AGAID], [EventOrdinal], [EventKey], [EventDate], [Rating], [Sigma], [LastRatingDate], [MomentumStreak], [RunID])
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
''', checkpoint_rows)
    conn.commit()


def mark_event_computed(conn: pyodbc.Connection, model_label: str, event_ordinal: int, run_id: int) -> None:
    conn.cursor().execute('EXEC [ratings].[sp_bayrate_mark_event_computed] @ModelLabel = ?, @EventOrdinal = ?, @RunID = ?', model_label, event_ordinal, run_id)
    conn.commit()


def publish_run_metrics(conn: pyodbc.Connection, run_id: int, result) -> None:
    metric_rows: list[tuple[int, str, float, str | None]] = []
    for section_name, metrics in (('pre_event_metrics', result.pre_event_metrics), ('post_event_fit_metrics', result.post_event_fit_metrics)):
        for key, value in metrics.items():
            metric_rows.append((run_id, f'{section_name}.{key}', float(value), None))
    if not metric_rows:
        return
    cur = conn.cursor()
    cur.fast_executemany = True
    cur.executemany('INSERT INTO [ratings].[bayrate_run_metrics] ([RunID], [MetricName], [MetricValue], [MetricText]) VALUES (?, ?, ?, ?)', metric_rows)
    conn.commit()


def complete_run(conn: pyodbc.Connection, run_id: int, result) -> None:
    conn.cursor().execute('EXEC [ratings].[sp_bayrate_complete_run] @RunID = ?, @EventCount = ?, @PlayerCount = ?, @GameCount = ?', run_id, int(result.event_count), int(result.player_count), int(len(result.game_results)))
    conn.commit()


def fail_run(conn: pyodbc.Connection, run_id: int, error_message: str) -> None:
    conn.cursor().execute('EXEC [ratings].[sp_bayrate_fail_run] @RunID = ?, @ErrorMessage = ?', run_id, error_message)
    conn.commit()
