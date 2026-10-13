from pathlib import Path

path = Path(r"C:\Users\guruj\aga-functions\function2\function_app.py")
text = path.read_text(encoding="utf-8")
old = '''@lru_cache(maxsize=4)
def _load_bayrate_index(table_path_text: str, table_mtime_ns: int) -> dict[int, list[tuple[datetime, float]]]:
    table_path = Path(table_path_text)
    history: dict[int, list[tuple[datetime, float]]] = {}
    with table_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            try:
                agaid = int(row["AGAID"])
                event_date = datetime.strptime(row["EventDate"], "%Y-%m-%d")
                rating_after = float(row["RatingAfter"])
            except (KeyError, TypeError, ValueError):
                continue
            history.setdefault(agaid, []).append((event_date, rating_after))
    for points in history.values():
        points.sort(key=lambda item: item[0])
    return history


def _load_bayrate_modeled_rating_history(agaid: int) -> list[tuple[datetime, float]]:
    table_path = _resolve_data_file(BAYRATE_TABLE_FILENAME)
    if not table_path.exists():
        raise ValueError(f"Bayrate history table not found: {table_path}")
    stat = table_path.stat()
    history_index = _load_bayrate_index(str(table_path), stat.st_mtime_ns)
    return history_index.get(agaid, [])
'''
new = '''def _load_bayrate_modeled_rating_history(agaid: int) -> list[tuple[datetime, float]]:
    conn_str = _get_sql_connection_string()
    if not conn_str:
        raise ValueError("SQL connection string not configured.")
    conn = _tds_connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
SELECT [EventDate], [RatingAfter]
FROM [ratings].[bayrate_event_player_ratings]
WHERE [ModelLabel] = ? AND [AGAID] = ?
ORDER BY [EventOrdinal]
""",
            [BAYRATE_MODEL_LABEL, agaid],
        )
        rows = cursor.fetchall()
    finally:
        conn.close()
    return [
        (datetime.combine(row["EventDate"], datetime.min.time()), float(row["RatingAfter"]))
        for row in rows
    ]
'''
if old not in text:
    raise SystemExit('target block not found')
text = text.replace(old, new, 1)
text = text.replace('BAYRATE_TABLE_FILENAME = "bayrate-best-event-ratings.csv"\n', '')
path.write_text(text, encoding='utf-8')
