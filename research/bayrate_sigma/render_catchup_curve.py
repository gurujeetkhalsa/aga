"""Render cumulative catch-up curves from an improver simulation run."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass(frozen=True)
class AlgorithmStyle:
    """Represent algorithm style."""
    key: str
    label: str
    color: str
    dasharray: str = ""


ALGORITHMS = [
    AlgorithmStyle("baseline", "Baseline", "#275DAD"),
    AlgorithmStyle("surprise_taper_floor_050", "Surprise/taper", "#C44900"),
]


def parse_args() -> argparse.Namespace:
    """Parse args."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("simulation_dir", type=Path, help="Directory containing metadata.json and player_trajectories.csv.")
    parser.add_argument("--name", default="catchup_cumulative_reached", help="Output filename stem.")
    parser.add_argument("--split-self-promote", action="store_true", help="Draw separate lines for self-promoted and non-self-promoted players.")
    return parser.parse_args()


def main() -> None:
    """Run the command-line entry point for this module."""
    args = parse_args()
    simulation_dir = args.simulation_dir
    metadata = json.loads((simulation_dir / "metadata.json").read_text(encoding="utf-8"))
    rows = read_csv(simulation_dir / "player_trajectories.csv")
    player_ids = sorted({row["simulation_player_id"] for row in rows})
    total_players = len(player_ids)
    start_date = date.fromisoformat(metadata["start_date"])
    total_days = round(float(metadata["years"]) * 365)
    player_groups = None
    subtitle = (
        f"Share of {total_players} simulated players whose replayed rating reached "
        "their hidden one-rank-stronger level"
    )

    if args.split_self_promote:
        player_groups = self_promote_groups(read_csv(simulation_dir / "simulation_players.csv"))
        group_totals = count_groups(player_groups)
        first_reach = first_reach_days(rows, start_date=start_date, total_days=total_days, player_groups=player_groups)
        series = []
        for algorithm in ALGORITHMS:
            for group_value, group_label, dasharray in (
                ("False", "no self-promotion", "7 5"),
                ("True", "self-promoted", ""),
            ):
                key = f"{algorithm.key}|{group_value}"
                series.append(
                    build_series(
                        AlgorithmStyle(key, f"{algorithm.label}: {group_label}", algorithm.color, dasharray),
                        first_reach.get(key, []),
                        total_players=group_totals[group_value],
                        total_days=total_days,
                    )
                )
        subtitle = "Percent within each self-promotion subgroup reaching the hidden one-rank-stronger level"
    else:
        first_reach = first_reach_days(rows, start_date=start_date, total_days=total_days)
        series = [
            build_series(algorithm, first_reach.get(algorithm.key, []), total_players=total_players, total_days=total_days)
            for algorithm in ALGORITHMS
        ]

    csv_path = simulation_dir / f"{args.name}.csv"
    svg_path = simulation_dir / f"{args.name}.svg"
    write_chart_data(csv_path, series=series, total_days=total_days)
    write_svg(
        svg_path,
        series=series,
        total_players=total_players,
        total_days=total_days,
        metadata=metadata,
        subtitle=subtitle,
    )

    print(f"Chart written to {svg_path}")
    print(f"Data written to {csv_path}")
    for item in series:
        print(f"{item['label']}: {item['reached']}/{item['total_players']} ({item['final_pct']:.1f}%)")


def read_csv(path: Path) -> list[dict[str, str]]:
    """Read csv."""
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def self_promote_groups(player_rows: list[dict[str, str]]) -> dict[str, str]:
    """Execute the self promote groups routine."""
    return {row["simulation_player_id"]: row["self_promote"] for row in player_rows}


def count_groups(player_groups: dict[str, str]) -> dict[str, int]:
    """Execute the count groups routine."""
    counts = {"False": 0, "True": 0}
    for value in player_groups.values():
        counts[value] += 1
    return counts


def first_reach_days(
    rows: list[dict[str, str]],
    *,
    start_date: date,
    total_days: int,
    player_groups: dict[str, str] | None = None,
) -> dict[str, list[int]]:
    """Execute the first reach days routine."""
    reached_by_algorithm: dict[str, dict[str, int]] = {}
    for row in rows:
        if row["reached"] != "True":
            continue
        event_date = date.fromisoformat(row["event_date"])
        day = max(0, min(total_days, (event_date - start_date).days))
        player_id = row["simulation_player_id"]
        key = row["algorithm"]
        if player_groups is not None:
            group = player_groups.get(player_id)
            if group is None:
                continue
            key = f"{key}|{group}"
        player_reaches = reached_by_algorithm.setdefault(key, {})
        if player_id not in player_reaches or day < player_reaches[player_id]:
            player_reaches[player_id] = day
    return {algorithm: sorted(days.values()) for algorithm, days in reached_by_algorithm.items()}


def build_series(
    algorithm: AlgorithmStyle,
    reach_days: list[int],
    *,
    total_players: int,
    total_days: int,
) -> dict[str, object]:
    """Build series."""
    points = [{"day": 0, "pct": 0.0}]
    reached = 0
    index = 0
    while index < len(reach_days):
        day = reach_days[index]
        if points[-1]["day"] != day:
            points.append({"day": day, "pct": reached / total_players * 100})
        while index < len(reach_days) and reach_days[index] == day:
            reached += 1
            index += 1
        points.append({"day": day, "pct": reached / total_players * 100})
    points.append({"day": total_days, "pct": reached / total_players * 100})
    return {
        "key": algorithm.key,
        "label": algorithm.label,
        "color": algorithm.color,
        "dasharray": algorithm.dasharray,
        "reach_days": reach_days,
        "reached": reached,
        "total_players": total_players,
        "final_pct": reached / total_players * 100,
        "points": points,
    }


def write_chart_data(
    path: Path,
    *,
    series: list[dict[str, object]],
    total_days: int,
) -> None:
    """Write chart data."""
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["algorithm", "day", "month", "reached", "total", "percent"])
        writer.writeheader()
        for item in series:
            days = list(item["reach_days"])
            total_players = int(item["total_players"])
            reached = 0
            index = 0
            for day in list(range(0, total_days + 1, 30)) + [total_days]:
                while index < len(days) and days[index] <= day:
                    reached += 1
                    index += 1
                writer.writerow(
                    {
                        "algorithm": item["label"],
                        "day": day,
                        "month": round(day / 30.4375, 2),
                        "reached": reached,
                        "total": total_players,
                        "percent": round(reached / total_players * 100, 2),
                    }
                )


def write_svg(
    path: Path,
    *,
    series: list[dict[str, object]],
    total_players: int,
    total_days: int,
    metadata: dict[str, object],
    subtitle: str,
) -> None:
    """Write svg."""
    width = 1100
    height = 700
    margin_left = 82
    margin_right = 46
    margin_top = 78
    margin_bottom = 92
    inner_width = width - margin_left - margin_right
    inner_height = height - margin_top - margin_bottom

    def x(day: float) -> float:
        """Execute the x routine."""
        return margin_left + day / total_days * inner_width

    def y(percent: float) -> float:
        """Execute the y routine."""
        return margin_top + (100 - percent) / 100 * inner_height

    def path_for(points: list[dict[str, float]]) -> str:
        """Execute the path for routine."""
        parts = []
        for index, point in enumerate(points):
            command = "M" if index == 0 else "L"
            parts.append(f"{command}{x(float(point['day'])):.2f},{y(float(point['pct'])):.2f}")
        return " ".join(parts)

    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-labelledby="title desc">',
        '<title id="title">Cumulative percent of simulated improvers reaching true strength</title>',
        '<desc id="desc">Baseline and surprise/taper cumulative catch-up over a three year simulation.</desc>',
        "<style>",
        "text { font-family: Segoe UI, Arial, sans-serif; fill: #1f2933; }",
        ".title { font-size: 27px; font-weight: 700; }",
        ".subtitle { font-size: 14px; fill: #5b6472; }",
        ".axis { stroke: #28313f; stroke-width: 1.4; }",
        ".grid { stroke: #d8dee8; stroke-width: 1; }",
        ".tick { font-size: 12px; fill: #4c5664; }",
        ".label { font-size: 14px; font-weight: 600; fill: #364152; }",
        ".legend { font-size: 14px; font-weight: 600; }",
        ".note { font-size: 12px; fill: #5b6472; }",
        "</style>",
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="{margin_left}" y="35" class="title">One-rank improver simulation: catch-up over time</text>',
        f'<text x="{margin_left}" y="58" class="subtitle">{escape_xml(subtitle)}</text>',
    ]

    for percent in (0, 20, 40, 60, 80, 100):
        y_pos = y(percent)
        lines.append(f'<line x1="{margin_left}" y1="{y_pos:.2f}" x2="{margin_left + inner_width:.2f}" y2="{y_pos:.2f}" class="grid"/>')
        lines.append(f'<text x="{margin_left - 12}" y="{y_pos + 4:.2f}" text-anchor="end" class="tick">{percent}%</text>')

    for month in (0, 6, 12, 18, 24, 30, 36):
        day = month / 36 * total_days
        x_pos = x(day)
        lines.append(f'<line x1="{x_pos:.2f}" y1="{margin_top}" x2="{x_pos:.2f}" y2="{margin_top + inner_height}" class="grid"/>')
        lines.append(f'<text x="{x_pos:.2f}" y="{margin_top + inner_height + 24}" text-anchor="middle" class="tick">{month}</text>')

    lines.extend(
        [
            f'<line x1="{margin_left}" y1="{margin_top + inner_height}" x2="{margin_left + inner_width}" y2="{margin_top + inner_height}" class="axis"/>',
            f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + inner_height}" class="axis"/>',
        ]
    )

    for item in series:
        points = item["points"]
        dasharray = f' stroke-dasharray="{item["dasharray"]}"' if item["dasharray"] else ""
        lines.append(
            f'<path d="{path_for(points)}" fill="none" stroke="{item["color"]}" stroke-width="4" stroke-linejoin="round" stroke-linecap="round"{dasharray}/>'
        )
        last = points[-1]
        lines.append(f'<circle cx="{x(float(last["day"])):.2f}" cy="{y(float(last["pct"])):.2f}" r="5" fill="{item["color"]}"/>')

    lines.append(f'<text x="{margin_left + inner_width / 2:.2f}" y="{height - 34}" text-anchor="middle" class="label">Months since simulation start</text>')
    lines.append(f'<text transform="translate(24 {margin_top + inner_height / 2:.2f}) rotate(-90)" text-anchor="middle" class="label">Cumulative percent reached</text>')

    legend_x = margin_left + 12
    legend_y = margin_top + 20
    for item in series:
        dasharray = f' stroke-dasharray="{item["dasharray"]}"' if item["dasharray"] else ""
        lines.append(f'<line x1="{legend_x}" y1="{legend_y}" x2="{legend_x + 34}" y2="{legend_y}" stroke="{item["color"]}" stroke-width="4" stroke-linecap="round"{dasharray}/>')
        lines.append(
            f'<text x="{legend_x + 44}" y="{legend_y + 5}" class="legend">{escape_xml(item["label"])}: {item["reached"]}/{item["total_players"]} ({item["final_pct"]:.1f}%)</text>'
        )
        legend_y += 26

    callout_x = margin_left + inner_width - 330
    callout_y = margin_top + 16
    callout_height = 34 + len(series) * 20
    lines.extend(
        [
            f'<rect x="{callout_x}" y="{callout_y}" width="312" height="{callout_height}" rx="6" fill="#f8fafc" stroke="#d8dee8"/>',
            f'<text x="{callout_x + 14}" y="{callout_y + 24}" class="note">Final at 36 months</text>',
        ]
    )
    for index, item in enumerate(series):
        lines.append(
            f'<text x="{callout_x + 14}" y="{callout_y + 47 + index * 20}" class="legend" fill="{item["color"]}">{escape_xml(item["label"])}: {item["final_pct"]:.1f}%</text>'
        )

    lines.append(
        f'<text x="{margin_left}" y="{height - 12}" class="note">Simulation: {escape_xml(metadata["name"])}; start {metadata["start_date"]}; {metadata["years"]} years; cumulative by first reached event date.</text>'
    )
    lines.append("</svg>")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def escape_xml(value: object) -> str:
    """Execute the escape xml routine."""
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


if __name__ == "__main__":
    main()
