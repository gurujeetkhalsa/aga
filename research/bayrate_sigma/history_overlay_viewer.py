"""Local interactive viewer for BayRate rating-history experiment overlays."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse

from .render_history_overlay_svg import HistoryPoint, HistorySeries, render_overlay_svg


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASELINE_RESULTS = (
    REPO_ROOT
    / "bayrate"
    / "output"
    / "experiments"
    / "baseline-full-history-20260526"
    / "player_results.csv"
)
DEFAULT_CANDIDATE_RESULTS = (
    REPO_ROOT
    / "bayrate"
    / "output"
    / "experiments"
    / "sigma-smooth-taper-detail-full-history-20260527"
    / "taper_6_8_floor_050"
    / "player_results.csv"
)


@dataclass(frozen=True)
class ViewerConfig:
    """Store viewer configuration."""
    baseline_results: Path
    candidate_results: Path
    candidate_label: str


def _parse_date(value: str) -> datetime | None:
    """Parse date."""
    text = (value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d")
    except ValueError:
        return None


def _parse_float(value: str) -> float | None:
    """Parse float."""
    text = (value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _append_point(
    index: dict[int, list[HistoryPoint]],
    player_id: int,
    event_date: datetime,
    rating: float,
    sigma: float,
    source_order: int,
) -> None:
    """Execute the append point routine."""
    index.setdefault(player_id, []).append(HistoryPoint(event_date, rating, sigma, source_order))


def load_player_results_index(path: Path) -> dict[int, list[HistoryPoint]]:
    """Load player results index."""
    index: dict[int, list[HistoryPoint]] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row_number, row in enumerate(reader):
            player_id_text = (row.get("player_id") or "").strip()
            if not player_id_text.isdigit():
                continue
            rating = _parse_float(row.get("rating_after") or "")
            sigma = _parse_float(row.get("sigma_after") or "") or 0.0
            event_date = _parse_date(row.get("event_date") or "")
            if rating is None or event_date is None:
                continue
            _append_point(index, int(player_id_text), event_date, rating, sigma, row_number)
    for points in index.values():
        points.sort(key=lambda point: (point.date, point.source_order))
    return index


class HistoryOverlayStore:
    """Represent history overlay store."""
    def __init__(self, config: ViewerConfig):
        """Initialize the history overlay store instance."""
        self.config = config
        self._baseline: dict[int, list[HistoryPoint]] | None = None
        self._candidate: dict[int, list[HistoryPoint]] | None = None

    @property
    def baseline(self) -> dict[int, list[HistoryPoint]]:
        """Execute the baseline routine."""
        if self._baseline is None:
            self._baseline = load_player_results_index(self.config.baseline_results)
        return self._baseline

    @property
    def candidate(self) -> dict[int, list[HistoryPoint]]:
        """Execute the candidate routine."""
        if self._candidate is None:
            self._candidate = load_player_results_index(self.config.candidate_results)
        return self._candidate

    def series_for(self, agaid: int) -> list[HistorySeries]:
        """Execute the series for routine."""
        return [
            HistorySeries(
                "Baseline replay",
                self.baseline.get(agaid, []),
                line_color="#5c6670",
                dasharray="8 6",
                point_radius=0.0,
            ),
            HistorySeries(
                self.config.candidate_label,
                self.candidate.get(agaid, []),
                line_color="#c85f16",
                band_color="#f2a65a",
                band_opacity=0.18,
                point_radius=2.6,
            ),
        ]

    def summary_for(self, agaid: int) -> dict[str, object]:
        """Execute the summary for routine."""
        return {
            "agaid": agaid,
            "series": [
                _series_summary("baseline", "Baseline replay", self.baseline.get(agaid, [])),
                _series_summary("candidate", self.config.candidate_label, self.candidate.get(agaid, [])),
            ],
        }


def _series_summary(key: str, label: str, points: list[HistoryPoint]) -> dict[str, object]:
    """Execute the series summary routine."""
    latest = points[-1] if points else None
    return {
        "key": key,
        "label": label,
        "count": len(points),
        "latest": None
        if latest is None
        else {
            "date": latest.date.date().isoformat(),
            "rating": latest.rating,
            "sigma": latest.sigma,
        },
    }


def _html_page(candidate_label: str) -> str:
    """Execute the html page routine."""
    quoted_candidate = quote(candidate_label)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>BayRate History Overlay</title>
  <style>
    :root {{
      color-scheme: light;
      --ink: #162736;
      --muted: #5a7082;
      --line: #d9e7f2;
      --panel: #ffffff;
      --panel-alt: #f4f9fd;
      --blue: #014a7d;
      --orange: #c85f16;
      --gray: #5c6670;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      background: #edf4f8;
      color: var(--ink);
      font-family: "Red Hat Text", "Segoe UI", Helvetica, Arial, sans-serif;
    }}
    main {{
      width: min(1320px, calc(100% - 28px));
      margin: 0 auto;
      padding: 18px 0 26px;
    }}
    .toolbar {{
      display: flex;
      align-items: end;
      justify-content: space-between;
      gap: 14px;
      margin-bottom: 14px;
    }}
    h1 {{
      margin: 0;
      font-size: 1.35rem;
      line-height: 1.2;
      letter-spacing: 0;
    }}
    form {{
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }}
    label {{
      color: var(--muted);
      font-size: 0.84rem;
      font-weight: 800;
      text-transform: uppercase;
    }}
    input {{
      width: 150px;
      height: 38px;
      border: 1px solid #b7cbda;
      border-radius: 6px;
      padding: 0 10px;
      font: inherit;
      color: var(--ink);
      background: #ffffff;
    }}
    button {{
      height: 38px;
      border: 0;
      border-radius: 6px;
      padding: 0 14px;
      font: inherit;
      font-weight: 800;
      color: #ffffff;
      background: var(--blue);
      cursor: pointer;
    }}
    button.secondary {{
      color: var(--blue);
      background: #dcebf5;
    }}
    .summary {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
      margin-bottom: 12px;
    }}
    .metric {{
      min-height: 80px;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px 12px;
      background: var(--panel);
    }}
    .metric .label {{
      color: var(--muted);
      font-size: 0.78rem;
      font-weight: 800;
      text-transform: uppercase;
      overflow-wrap: anywhere;
    }}
    .metric .value {{
      margin-top: 6px;
      font-size: 1.1rem;
      font-weight: 800;
      color: var(--ink);
    }}
    .metric .meta {{
      margin-top: 2px;
      color: var(--muted);
      font-size: 0.88rem;
    }}
    .chart-shell {{
      border: 1px solid var(--blue);
      border-radius: 8px;
      overflow: hidden;
      background: var(--panel);
      min-height: 260px;
    }}
    img {{
      display: block;
      width: 100%;
      height: auto;
    }}
    .status {{
      min-height: 22px;
      margin-top: 8px;
      color: var(--muted);
      font-size: 0.9rem;
    }}
    .error {{ color: #a33a1f; font-weight: 800; }}
    @media (max-width: 760px) {{
      main {{ width: min(100% - 18px, 1320px); padding-top: 10px; }}
      .toolbar {{ align-items: stretch; flex-direction: column; }}
      form {{ justify-content: flex-start; }}
      input {{ flex: 1 1 140px; min-width: 0; }}
      .summary {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <main>
    <div class="toolbar">
      <h1>BayRate History Overlay</h1>
      <form id="agaid-form">
        <label for="agaid">AGAID</label>
        <input id="agaid" name="agaid" inputmode="numeric" pattern="[0-9]*" value="15784" autocomplete="off">
        <button type="submit">Load</button>
        <button class="secondary" type="button" id="svg-link">SVG</button>
      </form>
    </div>
    <section class="summary" id="summary" aria-live="polite"></section>
    <section class="chart-shell">
      <img id="chart" alt="Rating history overlay chart">
    </section>
    <div class="status" id="status"></div>
  </main>
  <script>
    const candidateLabel = decodeURIComponent("{quoted_candidate}");
    const form = document.getElementById("agaid-form");
    const input = document.getElementById("agaid");
    const chart = document.getElementById("chart");
    const summary = document.getElementById("summary");
    const status = document.getElementById("status");
    const svgLink = document.getElementById("svg-link");

    function formatNumber(value) {{
      const numeric = Number(value);
      return Number.isFinite(numeric) ? numeric.toFixed(3) : "n/a";
    }}

    function card(series) {{
      const latest = series.latest;
      const value = latest ? `${{formatNumber(latest.rating)}} / ${{formatNumber(latest.sigma)}}` : "n/a";
      const meta = latest ? `${{latest.date}} - ${{series.count}} points` : `${{series.count}} points`;
      return `<article class="metric"><div class="label">${{series.label}}</div><div class="value">${{value}}</div><div class="meta">${{meta}}</div></article>`;
    }}

    function svgUrl(agaid) {{
      return `/history.svg?agaid=${{encodeURIComponent(agaid)}}`;
    }}

    async function loadAgaid(agaid) {{
      const clean = String(agaid || "").trim();
      if (!/^\\d+$/.test(clean)) {{
        status.innerHTML = '<span class="error">Enter a numeric AGAID.</span>';
        return;
      }}
      status.textContent = "Loading...";
      summary.innerHTML = "";
      const response = await fetch(`/api/summary?agaid=${{encodeURIComponent(clean)}}`);
      if (!response.ok) {{
        const text = await response.text();
        throw new Error(text || `Request failed with ${{response.status}}`);
      }}
      const payload = await response.json();
      const hasPoints = payload.series.some((item) => item.count > 0);
      if (!hasPoints) throw new Error(`No history found for AGAID ${{clean}}.`);
      summary.innerHTML = payload.series.map(card).join("");
      chart.src = `${{svgUrl(clean)}}&v=${{Date.now()}}`;
      chart.alt = `Rating history overlay chart for AGAID ${{clean}}`;
      const url = new URL(window.location.href);
      url.searchParams.set("agaid", clean);
      window.history.replaceState(null, "", url);
      status.textContent = `Loaded AGAID ${{clean}} against ${{candidateLabel}}.`;
    }}

    form.addEventListener("submit", async (event) => {{
      event.preventDefault();
      try {{
        await loadAgaid(input.value);
      }} catch (error) {{
        status.innerHTML = `<span class="error">${{error.message}}</span>`;
      }}
    }});

    svgLink.addEventListener("click", () => {{
      const clean = String(input.value || "").trim();
      if (/^\\d+$/.test(clean)) window.open(svgUrl(clean), "_blank", "noopener");
    }});

    const initial = new URLSearchParams(window.location.search).get("agaid") || input.value;
    input.value = initial;
    loadAgaid(initial).catch((error) => {{
      status.innerHTML = `<span class="error">${{error.message}}</span>`;
    }});
  </script>
</body>
</html>
"""


class HistoryOverlayHandler(BaseHTTPRequestHandler):
    """Represent history overlay handler."""
    store: HistoryOverlayStore

    def log_message(self, format: str, *args: object) -> None:
        """Execute the log message routine."""
        return

    def _send_bytes(self, status: HTTPStatus, content_type: str, payload: bytes) -> None:
        """Send bytes."""
        self.send_response(status.value)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def _send_text(self, status: HTTPStatus, content_type: str, text: str) -> None:
        """Send text."""
        self._send_bytes(status, f"{content_type}; charset=utf-8", text.encode("utf-8"))

    def _agaid_from_query(self, query: dict[str, list[str]]) -> int:
        """Execute the agaid from query routine."""
        value = (query.get("agaid") or [""])[0].strip()
        if not value.isdigit():
            raise ValueError("Query parameter 'agaid' must be numeric.")
        return int(value)

    def do_GET(self) -> None:
        """Execute the do GET routine."""
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        try:
            if parsed.path in {"/", "/index.html"}:
                self._send_text(HTTPStatus.OK, "text/html", _html_page(self.store.config.candidate_label))
                return
            if parsed.path == "/api/summary":
                agaid = self._agaid_from_query(query)
                payload = json.dumps(self.store.summary_for(agaid), separators=(",", ":"))
                self._send_text(HTTPStatus.OK, "application/json", payload)
                return
            if parsed.path == "/history.svg":
                agaid = self._agaid_from_query(query)
                series = self.store.series_for(agaid)
                svg = render_overlay_svg(agaid, series, title=f"AGAID {agaid} Rating History")
                self._send_text(HTTPStatus.OK, "image/svg+xml", svg)
                return
            if parsed.path == "/favicon.ico":
                self._send_bytes(HTTPStatus.NO_CONTENT, "image/x-icon", b"")
                return
            self._send_text(HTTPStatus.NOT_FOUND, "text/plain", "Not found.")
        except ValueError as exc:
            self._send_text(HTTPStatus.BAD_REQUEST, "text/plain", str(exc))
        except FileNotFoundError as exc:
            self._send_text(HTTPStatus.INTERNAL_SERVER_ERROR, "text/plain", f"Input file not found: {exc.filename}")
        except Exception as exc:
            self._send_text(HTTPStatus.INTERNAL_SERVER_ERROR, "text/plain", f"History viewer failed: {exc}")


def build_arg_parser() -> argparse.ArgumentParser:
    """Build arg parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind.")
    parser.add_argument("--port", type=int, default=8765, help="Port to bind.")
    parser.add_argument("--baseline-results", type=Path, default=DEFAULT_BASELINE_RESULTS)
    parser.add_argument("--candidate-results", type=Path, default=DEFAULT_CANDIDATE_RESULTS)
    parser.add_argument("--candidate-label", default="Smooth taper floor 0.50")
    return parser


def main() -> int:
    """Run the command-line entry point for this module."""
    args = build_arg_parser().parse_args()
    config = ViewerConfig(
        baseline_results=args.baseline_results.resolve(),
        candidate_results=args.candidate_results.resolve(),
        candidate_label=args.candidate_label,
    )
    handler = type("ConfiguredHistoryOverlayHandler", (HistoryOverlayHandler,), {})
    handler.store = HistoryOverlayStore(config)
    server = ThreadingHTTPServer((args.host, args.port), handler)
    print(f"BayRate history overlay viewer: http://{args.host}:{args.port}/?agaid=15784")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
