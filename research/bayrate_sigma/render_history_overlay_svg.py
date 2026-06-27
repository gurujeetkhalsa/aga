"""Render a Ratings Explorer style history SVG with experiment overlays."""

from __future__ import annotations

import argparse
import csv
import html
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class HistoryPoint:
    date: datetime
    rating: float
    sigma: float
    source_order: int


@dataclass(frozen=True)
class HistorySeries:
    label: str
    points: list[HistoryPoint]
    line_color: str
    band_color: str | None = None
    band_opacity: float = 0.0
    dasharray: str | None = None
    point_radius: float = 2.9


def _parse_date(value: str) -> datetime | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d")
    except ValueError:
        return None


def _parse_float(value: str) -> float | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def read_official_ratings(path: Path, agaid: int) -> list[HistoryPoint]:
    points: list[HistoryPoint] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        for index, row in enumerate(reader):
            if len(row) < 4 or row[0].strip() != str(agaid):
                continue
            rating = _parse_float(row[1])
            sigma = _parse_float(row[2]) or 0.0
            event_date = _parse_date(row[3])
            if rating is None or event_date is None:
                continue
            points.append(HistoryPoint(event_date, rating, sigma, index))
    return sorted(points, key=lambda point: (point.date, point.source_order))


def read_player_results(path: Path, agaid: int) -> list[HistoryPoint]:
    points: list[HistoryPoint] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for index, row in enumerate(reader):
            if (row.get("player_id") or "").strip() != str(agaid):
                continue
            rating = _parse_float(row.get("rating_after") or "")
            sigma = _parse_float(row.get("sigma_after") or "") or 0.0
            event_date = _parse_date(row.get("event_date") or "")
            if rating is None or event_date is None:
                continue
            points.append(HistoryPoint(event_date, rating, sigma, index))
    return sorted(points, key=lambda point: (point.date, point.source_order))


def chart_rating(value: float) -> float:
    if value <= -1.0:
        return value
    if value >= 1.0:
        return value - 2.0
    return -1.0


def chart_tick_label(tick: int) -> str:
    if tick == -1:
        return "1/-1"
    return f"{tick + 2:d}" if tick >= 0 else f"{tick:d}"


def _points_extent(series: Iterable[HistorySeries]) -> tuple[datetime, datetime, float, float]:
    all_points = [point for item in series for point in item.points]
    if not all_points:
        raise ValueError("No history points were found for the requested AGAID.")

    min_date = min(point.date for point in all_points)
    max_date = max(point.date for point in all_points)
    min_chart_rating = min(chart_rating(point.rating) - point.sigma for point in all_points)
    max_chart_rating = max(chart_rating(point.rating) + point.sigma for point in all_points)
    if min_chart_rating == max_chart_rating:
        min_chart_rating -= 1.0
        max_chart_rating += 1.0
    chart_padding = max((max_chart_rating - min_chart_rating) * 0.05, 0.25)
    return min_date, max_date, min_chart_rating - chart_padding, max_chart_rating + chart_padding


def render_overlay_svg(agaid: int, series: list[HistorySeries], title: str | None = None) -> str:
    non_empty_series = [item for item in series if item.points]
    if not non_empty_series:
        raise ValueError(f"No history points found for AGAID {agaid}.")

    min_date, max_date, min_chart_rating, max_chart_rating = _points_extent(non_empty_series)
    width = 1200
    height = 560
    left = 88
    right = 30
    top = 78
    bottom = 66
    plot_w = width - left - right
    plot_h = height - top - bottom

    def x_pos(dt: datetime) -> float:
        total_days = (max_date - min_date).days or 1
        return left + (((dt - min_date).days) / total_days) * plot_w

    def y_pos(rating: float) -> float:
        return top + ((max_chart_rating - chart_rating(rating)) / (max_chart_rating - min_chart_rating)) * plot_h

    def y_pos_chart(chart_value: float) -> float:
        return top + ((max_chart_rating - chart_value) / (max_chart_rating - min_chart_rating)) * plot_h

    def polyline(points: list[HistoryPoint]) -> str:
        return " ".join(f"{x_pos(point.date):.2f},{y_pos(point.rating):.2f}" for point in points)

    def sigma_band(points: list[HistoryPoint]) -> str:
        upper = [
            f"{x_pos(point.date):.2f},{y_pos_chart(chart_rating(point.rating) + point.sigma):.2f}"
            for point in points
        ]
        lower = [
            f"{x_pos(point.date):.2f},{y_pos_chart(chart_rating(point.rating) - point.sigma):.2f}"
            for point in reversed(points)
        ]
        return " ".join(upper + lower)

    x_ticks = []
    for year in range(min_date.year, max_date.year + 1, 2):
        tick = datetime(year, 1, 1)
        if min_date <= tick <= max_date:
            x_ticks.append(tick)
    if not x_ticks:
        x_ticks = [min_date, max_date]

    y_start = math.floor(min_chart_rating)
    y_end = math.ceil(max_chart_rating)
    y_ticks = list(range(y_start, y_end + 1))
    if y_start <= -1 <= y_end and -1 not in y_ticks:
        y_ticks.append(-1)
    if len(y_ticks) < 2:
        y_ticks = sorted({math.floor(min_chart_rating), math.ceil(max_chart_rating)})
    else:
        y_ticks = sorted(set(y_ticks))

    chart_title = html.escape(title or f"AGAID {agaid} Rating History")
    subtitle = "BayRate replay comparison"
    font_stack = "'Red Hat Text', 'Segoe UI', Helvetica, Arial, sans-serif"

    svg: list[str] = []
    svg.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">')
    svg.append(f"<style>text{{font-family:{font_stack};}}</style>")
    svg.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    svg.append('<rect x="0" y="0" width="100%" height="100%" fill="url(#chartBg)"/>')
    svg.append(
        "<defs>"
        '<linearGradient id="chartBg" x1="0%" y1="0%" x2="100%" y2="100%">'
        '<stop offset="0%" stop-color="#ffffff"/>'
        '<stop offset="100%" stop-color="#f4f9fd"/>'
        "</linearGradient>"
        "</defs>"
    )
    svg.append(f'<text x="{width / 2}" y="28" text-anchor="middle" font-size="24" font-weight="700" fill="#163248">{chart_title}</text>')
    svg.append(f'<text x="{width / 2}" y="50" text-anchor="middle" font-size="13" font-weight="500" fill="#5a7082">{subtitle}</text>')

    for tick in x_ticks:
        x = x_pos(tick)
        svg.append(f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{top + plot_h}" stroke="#d9e7f2" stroke-width="1"/>')
        svg.append(f'<text x="{x:.2f}" y="{height - 26}" text-anchor="middle" font-size="13" font-weight="700" fill="#4a6477">{tick.year}</text>')
    for tick in y_ticks:
        y = top + ((max_chart_rating - float(tick)) / (max_chart_rating - min_chart_rating)) * plot_h
        svg.append(f'<line x1="{left}" y1="{y:.2f}" x2="{left + plot_w}" y2="{y:.2f}" stroke="#d9e7f2" stroke-width="1"/>')
        svg.append(f'<text x="{left - 10}" y="{y + 4:.2f}" text-anchor="end" font-size="13" font-weight="700" fill="#4a6477">{chart_tick_label(tick)}</text>')
    svg.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_h}" stroke="#014a7d" stroke-width="1.5"/>')
    svg.append(f'<line x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" y2="{top + plot_h}" stroke="#014a7d" stroke-width="1.5"/>')

    for item in non_empty_series:
        if item.band_color and item.band_opacity > 0:
            svg.append(
                f'<polygon fill="{item.band_color}" fill-opacity="{item.band_opacity:.2f}" '
                f'points="{sigma_band(item.points)}"/>'
            )

    for item in non_empty_series:
        dash = f' stroke-dasharray="{html.escape(item.dasharray)}"' if item.dasharray else ""
        svg.append(
            f'<polyline fill="none" stroke="{item.line_color}" stroke-width="3" '
            f'stroke-linejoin="round" stroke-linecap="round"{dash} points="{polyline(item.points)}"/>'
        )
        if item.point_radius > 0:
            for point in item.points:
                svg.append(
                    f'<circle cx="{x_pos(point.date):.2f}" cy="{y_pos(point.rating):.2f}" '
                    f'r="{item.point_radius:.1f}" fill="{item.line_color}" stroke="#ffffff" stroke-width="1.1"/>'
                )

    legend_x = left + 10
    legend_y = top - 16
    for index, item in enumerate(non_empty_series):
        x = legend_x + index * 260
        dash = f' stroke-dasharray="{html.escape(item.dasharray)}"' if item.dasharray else ""
        svg.append(f'<line x1="{x}" y1="{legend_y}" x2="{x + 34}" y2="{legend_y}" stroke="{item.line_color}" stroke-width="4" stroke-linecap="round"{dash}/>')
        svg.append(f'<text x="{x + 44}" y="{legend_y + 4}" font-size="13" font-weight="700" fill="#163248">{html.escape(item.label)}</text>')

    svg.append(f'<text x="{width / 2}" y="{height - 5}" text-anchor="middle" font-size="14" font-weight="700" fill="#163248">Date</text>')
    svg.append(f'<text x="24" y="{height / 2}" transform="rotate(-90 24 {height / 2})" text-anchor="middle" font-size="14" font-weight="700" fill="#163248">AGA numeric rating</text>')
    svg.append("</svg>")
    return "\n".join(svg)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agaid", type=int, required=True, help="AGA player id to render.")
    parser.add_argument("--official-ratings", type=Path, help="Headerless official ratings CSV.")
    parser.add_argument("--baseline-results", type=Path, help="Optional baseline player_results.csv.")
    parser.add_argument("--candidate-results", type=Path, required=True, help="Candidate player_results.csv.")
    parser.add_argument("--candidate-label", default="Candidate", help="Label for the candidate overlay.")
    parser.add_argument("--output", type=Path, required=True, help="Output SVG path.")
    parser.add_argument("--title", help="Optional chart title.")
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    series: list[HistorySeries] = []
    if args.official_ratings:
        series.append(
            HistorySeries(
                "Official history",
                read_official_ratings(args.official_ratings, args.agaid),
                line_color="#3a73b2",
                band_color="#aed0e8",
                band_opacity=0.38,
                point_radius=2.3,
            )
        )
    if args.baseline_results:
        series.append(
            HistorySeries(
                "Baseline replay",
                read_player_results(args.baseline_results, args.agaid),
                line_color="#5c6670",
                dasharray="8 6",
                point_radius=0.0,
            )
        )
    series.append(
        HistorySeries(
            args.candidate_label,
            read_player_results(args.candidate_results, args.agaid),
            line_color="#c85f16",
            band_color="#f2a65a",
            band_opacity=0.18,
            point_radius=2.6,
        )
    )

    svg = render_overlay_svg(args.agaid, series, title=args.title)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(svg, encoding="utf-8")
    print(f"Wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
