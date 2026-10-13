import csv
import html
import json
import math
import os
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Optional
from urllib import parse

import azure.functions as func
import certifi
import pytds

app = func.FunctionApp()
LEGACY_BAYRATE_MODEL_LABEL = "legacy_bayrate_best"
OFFICIAL_RATINGS_FILENAME = "aga-ratings.csv"
LEGACY_BAYRATE_TABLE_FILENAME = "legacy-bayrate-best-event-ratings.csv"


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _function2_root() -> Path:
    return Path(__file__).resolve().parent


def _data_root() -> Path:
    return _function2_root() / "data"


def _resolve_data_file(filename: str) -> Path:
    explicit = os.environ.get(f"LEGACY_CHART_{filename.upper().replace('-', '_').replace('.', '_')}")
    if explicit:
        return Path(explicit)
    for candidate in (_data_root() / filename, _function2_root() / filename, _repo_root() / filename):
        if candidate.exists():
            return candidate
    return _data_root() / filename


def _load_official_rating_history(agaid: int) -> list[tuple[datetime, float]]:
    ratings_path = _resolve_data_file(OFFICIAL_RATINGS_FILENAME)
    if not ratings_path.exists():
        raise ValueError(f"Official ratings file not found: {ratings_path}")
    points: list[tuple[datetime, float]] = []
    with ratings_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if len(row) < 4:
                continue
            try:
                row_agaid = int(row[0])
            except (TypeError, ValueError):
                continue
            if row_agaid != agaid:
                continue
            try:
                points.append((datetime.strptime(row[3], "%Y-%m-%d"), float(row[1])))
            except (TypeError, ValueError):
                continue
    return sorted(points, key=lambda item: item[0])


def _parse_sql_connection_string(connection_string: str) -> dict[str, object]:
    parts: dict[str, str] = {}
    for item in connection_string.split(";"):
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        parts[key.strip().lower()] = value.strip().strip("{}")
    server = parts["server"].replace("tcp:", "")
    host, port_text = (server.split(",", 1) + ["1433"])[:2]
    return {
        "server": host,
        "port": int(port_text),
        "database": parts["database"],
        "user": parts["uid"],
        "password": parts["pwd"],
    }


def _tds_connect(conn_str: str):
    sql = _parse_sql_connection_string(conn_str)
    return pytds.connect(
        server=sql["server"],
        port=sql["port"],
        database=sql["database"],
        user=sql["user"],
        password=sql["password"],
        cafile=certifi.where(),
        validate_host=True,
        enc_login_only=False,
        autocommit=True,
        timeout=60,
        as_dict=True,
    )


def _get_sql_connection_string() -> Optional[str]:
    conn = os.environ.get("SQL_CONNECTION_STRING")
    if conn:
        return conn
    for settings_path in (_function2_root() / "local.settings.json", _repo_root() / "local.settings.json"):
        if settings_path.exists():
            try:
                values = json.loads(settings_path.read_text(encoding="utf-8")).get("Values", {})
            except (OSError, json.JSONDecodeError):
                continue
            conn = values.get("SQL_CONNECTION_STRING")
            if conn:
                return conn
    return None


def _load_member_name(conn_str: str, agaid: int) -> str | None:
    conn = _tds_connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
SELECT TOP (1) [FirstName], [LastName]
FROM [membership].[members]
WHERE [AGAID] = ?
""",
            [agaid],
        )
        row = cursor.fetchone()
    finally:
        conn.close()
    if not row:
        return None
    first = (row["FirstName"] or "").strip()
    last = (row["LastName"] or "").strip()
    name = f"{first} {last}".strip()
    return name or None


@lru_cache(maxsize=4)
def _load_legacy_bayrate_index(table_path_text: str, table_mtime_ns: int) -> dict[int, list[tuple[datetime, float]]]:
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


def _load_legacy_modeled_rating_history(agaid: int) -> list[tuple[datetime, float]]:
    table_path = _resolve_data_file(LEGACY_BAYRATE_TABLE_FILENAME)
    if not table_path.exists():
        raise ValueError(f"Legacy Bayrate history table not found: {table_path}")
    stat = table_path.stat()
    history_index = _load_legacy_bayrate_index(str(table_path), stat.st_mtime_ns)
    return history_index.get(agaid, [])


def _render_rating_history_svg(agaid: int, official: list[tuple[datetime, float]], modeled: list[tuple[datetime, float]], model_label: str, member_name: str | None = None) -> str:
    all_points = official + modeled
    if not all_points:
        raise ValueError(f"No rating history found for AGAID {agaid}.")

    min_date = min(point[0] for point in all_points)
    max_date = max(point[0] for point in all_points)
    min_rating = min(point[1] for point in all_points)
    max_rating = max(point[1] for point in all_points)
    if min_rating == max_rating:
        min_rating -= 1.0
        max_rating += 1.0

    width = 1200
    height = 650
    left = 92
    right = 36
    top = 48
    bottom = 76
    plot_w = width - left - right
    plot_h = height - top - bottom

    def x_pos(dt: datetime) -> float:
        total_days = (max_date - min_date).days or 1
        return left + (((dt - min_date).days) / total_days) * plot_w

    def y_pos(rating: float) -> float:
        return top + ((max_rating - rating) / (max_rating - min_rating)) * plot_h

    def polyline(points: list[tuple[datetime, float]]) -> str:
        return " ".join(f"{x_pos(dt):.2f},{y_pos(value):.2f}" for dt, value in points)

    x_ticks = []
    for year in range(min_date.year, max_date.year + 1, 2):
        tick = datetime(year, 1, 1)
        if min_date <= tick <= max_date:
            x_ticks.append(tick)
    if not x_ticks:
        x_ticks = [min_date, max_date]

    y_start = math.floor(min_rating)
    y_end = math.ceil(max_rating)
    y_ticks = list(range(y_start, y_end + 1, 2))
    if len(y_ticks) < 2:
        y_ticks = [math.floor(min_rating), math.ceil(max_rating)]

    svg = []
    svg.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">')
    svg.append('<rect width="100%" height="100%" fill="#fffdf8"/>')
    title = html.escape(member_name) if member_name else f'AGAID {agaid}'
    svg.append(f'<text x="{width / 2}" y="28" text-anchor="middle" font-family="Georgia, Times New Roman, serif" font-size="24" fill="#1f2937">{title}</text>')
    svg.append(f'<text x="{width / 2}" y="50" text-anchor="middle" font-family="Georgia, Times New Roman, serif" font-size="13" fill="#6b7280">Official AGA numeric ratings vs modeled event ratings ({html.escape(model_label)})</text>')

    for tick in x_ticks:
        x = x_pos(tick)
        svg.append(f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{top + plot_h}" stroke="#e5e7eb" stroke-width="1"/>')
        svg.append(f'<text x="{x:.2f}" y="{height - 34}" text-anchor="middle" font-family="Georgia, Times New Roman, serif" font-size="12" fill="#4b5563">{tick.year}</text>')

    for tick in y_ticks:
        y = y_pos(float(tick))
        svg.append(f'<line x1="{left}" y1="{y:.2f}" x2="{left + plot_w}" y2="{y:.2f}" stroke="#e5e7eb" stroke-width="1"/>')
        svg.append(f'<text x="{left - 12}" y="{y + 4:.2f}" text-anchor="end" font-family="Georgia, Times New Roman, serif" font-size="12" fill="#4b5563">{tick:.0f}</text>')

    svg.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_h}" stroke="#111827" stroke-width="1.5"/>')
    svg.append(f'<line x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" y2="{top + plot_h}" stroke="#111827" stroke-width="1.5"/>')

    if official:
        svg.append(f'<polyline fill="none" stroke="#1d4ed8" stroke-width="2.75" points="{polyline(official)}"/>')
        for dt, value in official:
            svg.append(f'<circle cx="{x_pos(dt):.2f}" cy="{y_pos(value):.2f}" r="2.4" fill="#1d4ed8"/>')
    if modeled:
        svg.append(f'<polyline fill="none" stroke="#b91c1c" stroke-width="1.8" points="{polyline(modeled)}"/>')
        for dt, value in modeled:
            svg.append(f'<circle cx="{x_pos(dt):.2f}" cy="{y_pos(value):.2f}" r="1.9" fill="#b91c1c"/>')

    legend_x = left + 18
    legend_y = top + 20
    svg.append(f'<line x1="{legend_x}" y1="{legend_y}" x2="{legend_x + 30}" y2="{legend_y}" stroke="#1d4ed8" stroke-width="2.75"/>')
    svg.append(f'<text x="{legend_x + 40}" y="{legend_y + 4}" font-family="Georgia, Times New Roman, serif" font-size="13" fill="#1f2937">Official AGA snapshots</text>')
    svg.append(f'<line x1="{legend_x}" y1="{legend_y + 24}" x2="{legend_x + 30}" y2="{legend_y + 24}" stroke="#b91c1c" stroke-width="1.8"/>')
    svg.append(f'<text x="{legend_x + 40}" y="{legend_y + 28}" font-family="Georgia, Times New Roman, serif" font-size="13" fill="#1f2937">Legacy Bayrate event ratings</text>')

    svg.append(f'<text x="{width / 2}" y="{height - 8}" text-anchor="middle" font-family="Georgia, Times New Roman, serif" font-size="14" fill="#1f2937">Date</text>')
    svg.append(f'<text x="24" y="{height / 2}" transform="rotate(-90 24 {height / 2})" text-anchor="middle" font-family="Georgia, Times New Roman, serif" font-size="14" fill="#1f2937">AGA numeric rating</text>')
    svg.append('</svg>')
    return "\n".join(svg)


@app.function_name(name="LegacyRatingHistoryChart")
@app.route(route="legacy-rating-history-chart", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def legacy_rating_history_chart(req: func.HttpRequest) -> func.HttpResponse:
    agaid_text = (req.params.get("agaid") or "").strip()
    initial_agaid = html.escape(agaid_text) if agaid_text else ""
    member_name = None
    conn_str = _get_sql_connection_string()
    if agaid_text.isdigit() and conn_str:
        try:
            member_name = _load_member_name(conn_str, int(agaid_text))
        except Exception:
            member_name = None
    display_name = html.escape(member_name) if member_name else f"AGAID {initial_agaid}" if initial_agaid else "AGA Legacy Rating History"
    initial_svg = ""
    if agaid_text:
        initial_svg = f'<img id="history-chart" alt="Legacy rating history chart" src="/api/legacy-rating-history-chart.svg?agaid={parse.quote(agaid_text)}" style="width:100%;height:auto;border:1px solid #d1d5db;border-radius:12px;background:#fff"/>'
    page = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{display_name}</title>
  <style>
    :root {{
      --bg: #f4f1ea;
      --panel: #fffdf8;
      --ink: #1f2937;
      --muted: #6b7280;
      --line: #d1d5db;
      --official: #1d4ed8;
      --model: #b91c1c;
      --accent: #7c2d12;
    }}
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(124,45,18,.10), transparent 30%),
        radial-gradient(circle at bottom right, rgba(29,78,216,.10), transparent 30%),
        var(--bg);
    }}
    .wrap {{
      max-width: 1100px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid rgba(31,41,55,.12);
      border-radius: 18px;
      box-shadow: 0 18px 40px rgba(31,41,55,.08);
      padding: 24px;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 2rem;
      line-height: 1.1;
    }}
    p {{
      margin: 0;
      color: var(--muted);
    }}
    form {{
      display: flex;
      gap: 12px;
      margin: 24px 0 18px;
      flex-wrap: wrap;
    }}
    input {{
      flex: 1 1 260px;
      min-width: 0;
      font: inherit;
      font-size: 1rem;
      padding: 14px 16px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: white;
    }}
    button {{
      font: inherit;
      font-size: 1rem;
      border: 0;
      border-radius: 12px;
      padding: 14px 18px;
      background: var(--accent);
      color: white;
      cursor: pointer;
    }}
    .legend {{
      display: flex;
      gap: 18px;
      flex-wrap: wrap;
      margin: 0 0 18px;
      color: var(--muted);
      font-size: .95rem;
    }}
    .legend span::before {{
      content: "";
      display: inline-block;
      width: 18px;
      height: 3px;
      margin-right: 8px;
      vertical-align: middle;
      border-radius: 99px;
    }}
    .official::before {{ background: var(--official); }}
    .model::before {{ background: var(--model); }}
    .empty {{
      border: 1px dashed var(--line);
      border-radius: 12px;
      padding: 28px;
      color: var(--muted);
      background: rgba(255,255,255,.55);
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="panel">
      <h1>{display_name}</h1>
      <p>Enter a single AGAID to compare official AGA snapshot history against the tuned legacy Bayrate event history from the precomputed full-history table.</p>
      <form id="chart-form" method="get" action="/api/legacy-rating-history-chart">
        <label for="agaid" style="display:block;width:100%;font-size:.95rem;color:#4b5563">AGAID</label>
        <input id="agaid" name="agaid" inputmode="numeric" pattern="[0-9]+" placeholder="Enter AGAID" value="{initial_agaid}" required />
        <button type="submit">Show Chart</button>
      </form>
      <div class="legend">
        <span class="official">Official AGA snapshots</span>
        <span class="model">Legacy Bayrate event ratings</span>
      </div>
      <div id="chart-host">{initial_svg or '<div class="empty">No AGAID selected yet.</div>'}</div>
    </div>
  </div>
  <script>
    const form = document.getElementById("chart-form");
    const input = document.getElementById("agaid");
    const host = document.getElementById("chart-host");
    form.addEventListener("submit", (event) => {{
      event.preventDefault();
      const agaid = input.value.trim();
      if (!/^\\d+$/.test(agaid)) {{
        host.innerHTML = '<div class="empty">Enter a numeric AGAID.</div>';
        return;
      }}
      const url = `/api/legacy-rating-history-chart.svg?agaid=${{encodeURIComponent(agaid)}}`;
      host.innerHTML = `<img id="history-chart" alt="Legacy rating history chart" src="${{url}}" style="width:100%;height:auto;border:1px solid #d1d5db;border-radius:12px;background:#fff"/>`;
      history.replaceState(null, "", `/api/legacy-rating-history-chart?agaid=${{encodeURIComponent(agaid)}}`);
    }});
  </script>
</body>
</html>"""
    return func.HttpResponse(page, mimetype="text/html; charset=utf-8", status_code=200)


@app.function_name(name="LegacyRatingHistoryChartSvg")
@app.route(route="legacy-rating-history-chart.svg", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def legacy_rating_history_chart_svg(req: func.HttpRequest) -> func.HttpResponse:
    agaid_text = (req.params.get("agaid") or "").strip()
    if not agaid_text.isdigit():
        return func.HttpResponse("Query parameter 'agaid' must be numeric.", status_code=400)
    agaid = int(agaid_text)
    conn_str = _get_sql_connection_string()
    member_name = None
    if conn_str:
        try:
            member_name = _load_member_name(conn_str, agaid)
        except Exception:
            member_name = None
    try:
        official = _load_official_rating_history(agaid)
        modeled = _load_legacy_modeled_rating_history(agaid)
        svg = _render_rating_history_svg(agaid, official, modeled, LEGACY_BAYRATE_MODEL_LABEL, member_name)
    except ValueError as exc:
        return func.HttpResponse(str(exc), status_code=404)
    except Exception as exc:
        return func.HttpResponse(f"Chart generation failed: {exc}", status_code=500)
    return func.HttpResponse(svg, mimetype="image/svg+xml; charset=utf-8", status_code=200)
