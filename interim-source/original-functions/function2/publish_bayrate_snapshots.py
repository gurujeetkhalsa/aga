import argparse
import json
from datetime import date, datetime

import pyodbc

from function2.bayrate_sql import connect_sql, load_connection_string


def _fetch_dicts(cursor: pyodbc.Cursor) -> list[dict[str, object]]:
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


def _parse_publication_date(value: str | None) -> date | None:
    if value is None:
        return None
    return datetime.strptime(value, '%Y-%m-%d').date()


def _get_cutover_control(conn: pyodbc.Connection, model_label: str) -> dict[str, object] | None:
    cur = conn.cursor()
    cur.execute('EXEC [ratings].[sp_bayrate_get_cutover_control] @ModelLabel = ?', model_label)
    rows = _fetch_dicts(cur)
    return rows[0] if rows else None


def _get_run_summary(conn: pyodbc.Connection, run_id: int) -> dict[str, object] | None:
    cur = conn.cursor()
    cur.execute('SELECT [RunID], [ModelLabel], [Status], [CompletedAt] FROM [ratings].[bayrate_runs] WHERE [RunID] = ?', run_id)
    rows = _fetch_dicts(cur)
    return rows[0] if rows else None


def _resolve_publication_date(conn: pyodbc.Connection, run_id: int, requested: date | None) -> date:
    if requested is not None:
        return requested
    cur = conn.cursor()
    cur.execute('SELECT MAX([EventDate]) AS [PublicationDate] FROM [ratings].[bayrate_event_player_ratings] WHERE [RunID] = ?', run_id)
    rows = _fetch_dicts(cur)
    publication_date = rows[0].get('PublicationDate') if rows else None
    if publication_date is None:
        raise RuntimeError(f'No Bayrate player results found for RunID {run_id}.')
    return publication_date


def _load_publish_candidates(conn: pyodbc.Connection, run_id: int, model_label: str, cutover_ordinal: int) -> list[dict[str, object]]:
    cur = conn.cursor()
    cur.execute(
        """
WITH latest_per_player AS
(
    SELECT r.[AGAID], MAX(r.[EventOrdinal]) AS [EventOrdinal]
    FROM [ratings].[bayrate_event_player_ratings] AS r
    WHERE r.[RunID] = ?
      AND r.[ModelLabel] = ?
      AND r.[EventOrdinal] >= ?
    GROUP BY r.[AGAID]
),
latest_rows AS
(
    SELECT p.[AGAID] AS [Pin_Player],
           p.[RatingAfter] AS [Rating],
           p.[SigmaAfter] AS [Sigma],
           p.[Tournament_Code],
           p.[EventDate],
           p.[EventOrdinal],
           ROW_NUMBER() OVER (
               PARTITION BY p.[AGAID]
               ORDER BY p.[EventOrdinal] DESC, p.[AGAID]
           ) AS [rn]
    FROM latest_per_player AS lp
    JOIN [ratings].[bayrate_event_player_ratings] AS p
      ON p.[AGAID] = lp.[AGAID]
     AND p.[EventOrdinal] = lp.[EventOrdinal]
     AND p.[RunID] = ?
     AND p.[ModelLabel] = ?
)
SELECT [Pin_Player],
       [Rating],
       [Sigma],
       [Tournament_Code],
       [EventDate],
       [EventOrdinal]
FROM latest_rows
WHERE [rn] = 1
ORDER BY [Pin_Player]
""",
        run_id,
        model_label,
        cutover_ordinal,
        run_id,
        model_label,
    )
    return _fetch_dicts(cur)


def _load_next_id(conn: pyodbc.Connection) -> int:
    cur = conn.cursor()
    cur.execute('SELECT ISNULL(MAX([id]), 0) + 1 AS [NextID] FROM [ratings].[ratings]')
    rows = _fetch_dicts(cur)
    return int(rows[0]['NextID'])


def _existing_snapshot_keys(conn: pyodbc.Connection, publication_date: date, players: list[int]) -> set[tuple[int, date, str | None, float, float]]:
    if not players:
        return set()
    placeholders = ','.join('?' for _ in players)
    cur = conn.cursor()
    query = f'''
SELECT [Pin_Player], [Elab_Date], [Tournament_Code], [Rating], [Sigma]
FROM [ratings].[ratings]
WHERE [Elab_Date] = ?
  AND [Pin_Player] IN ({placeholders})
'''
    cur.execute(query, publication_date, *players)
    rows = _fetch_dicts(cur)
    return {
        (
            int(row['Pin_Player']),
            row['Elab_Date'],
            None if row['Tournament_Code'] is None else str(row['Tournament_Code']),
            float(row['Rating']),
            float(row['Sigma']),
        )
        for row in rows
    }


def publish_snapshots(run_id: int, model_label: str, publication_date: date | None, dry_run: bool) -> dict[str, object]:
    conn = connect_sql(load_connection_string())
    try:
        run = _get_run_summary(conn, run_id)
        if run is None:
            raise RuntimeError(f'RunID {run_id} not found.')
        if str(run['ModelLabel']) != model_label:
            raise RuntimeError(f'RunID {run_id} belongs to model {run["ModelLabel"]!r}, not {model_label!r}.')
        if str(run['Status']).lower() != 'completed':
            raise RuntimeError(f'RunID {run_id} is not completed; current status is {run["Status"]!r}.')

        cutover = _get_cutover_control(conn, model_label)
        if not cutover or cutover.get('CutoverEventOrdinal') is None:
            raise RuntimeError(f'Cutover is not configured for model {model_label!r}.')
        cutover_ordinal = int(cutover['CutoverEventOrdinal'])

        effective_publication_date = _resolve_publication_date(conn, run_id, publication_date)
        candidates = _load_publish_candidates(conn, run_id, model_label, cutover_ordinal)
        if not candidates:
            return {
                'status': 'noop',
                'reason': 'no_post_cutover_players_in_run',
                'run_id': run_id,
                'model_label': model_label,
                'cutover_event_ordinal': cutover_ordinal,
                'publication_date': effective_publication_date.isoformat(),
            }

        existing_keys = _existing_snapshot_keys(conn, effective_publication_date, [int(row['Pin_Player']) for row in candidates])
        next_id = _load_next_id(conn)
        inserts: list[tuple[int, float, float, date, str | None, int]] = []
        skipped_existing = 0
        for row in candidates:
            key = (
                int(row['Pin_Player']),
                effective_publication_date,
                None if row['Tournament_Code'] is None else str(row['Tournament_Code']),
                float(row['Rating']),
                float(row['Sigma']),
            )
            if key in existing_keys:
                skipped_existing += 1
                continue
            inserts.append((
                int(row['Pin_Player']),
                float(row['Rating']),
                float(row['Sigma']),
                effective_publication_date,
                None if row['Tournament_Code'] is None else str(row['Tournament_Code']),
                next_id,
            ))
            next_id += 1

        if not dry_run and inserts:
            cur = conn.cursor()
            cur.fast_executemany = True
            cur.executemany(
                'INSERT INTO [ratings].[ratings] ([Pin_Player], [Rating], [Sigma], [Elab_Date], [Tournament_Code], [id]) VALUES (?, ?, ?, ?, ?, ?)',
                inserts,
            )
            conn.commit()

        return {
            'status': 'dry_run' if dry_run else 'published',
            'run_id': run_id,
            'model_label': model_label,
            'cutover_event_ordinal': cutover_ordinal,
            'publication_date': effective_publication_date.isoformat(),
            'candidate_players': len(candidates),
            'inserted_rows': len(inserts),
            'skipped_existing_rows': skipped_existing,
        }
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description='Append post-cutover Bayrate results into ratings.ratings without changing pre-cutover snapshots.')
    parser.add_argument('--run-id', type=int, required=True)
    parser.add_argument('--model-label', default='bayrate_best')
    parser.add_argument('--publication-date', default=None, help='Snapshot Elab_Date to write in YYYY-MM-DD. Defaults to the latest EventDate in the run.')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    summary = publish_snapshots(
        run_id=args.run_id,
        model_label=args.model_label,
        publication_date=_parse_publication_date(args.publication_date),
        dry_run=args.dry_run,
    )
    print(json.dumps(summary, indent=2, default=str))


if __name__ == '__main__':
    main()
