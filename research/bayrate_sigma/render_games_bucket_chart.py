"""Render reached-rate bars by simulated game-count bucket."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AlgorithmStyle:
    key: str
    label: str
    color: str


ALGORITHMS = [
    AlgorithmStyle("baseline", "Baseline", "#275DAD"),
    AlgorithmStyle("surprise_taper_floor_050", "Surprise/taper", "#C44900"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("simulation_dir", type=Path, help="Directory containing metadata.json and player_outcomes.csv.")
    parser.add_argument("--name", default="games_bucket_reached", help="Output filename stem.")
    parser.add_argument(
        "--bucket-size",
        type=int,
        help="Optional game-count bucket width. When omitted, exact game counts become buckets.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.bucket_size is not None and args.bucket_size <= 0:
        raise SystemExit("--bucket-size must be positive")

    simulation_dir = args.simulation_dir
    metadata = json.loads((simulation_dir / "metadata.json").read_text(encoding="utf-8"))
    rows = read_csv(simulation_dir / "player_outcomes.csv")
    bucket_rows = build_bucket_rows(rows, bucket_size=args.bucket_size)

    csv_path = simulation_dir / f"{args.name}.csv"
    svg_path = simulation_dir / f"{args.name}.svg"
    write_csv(csv_path, bucket_rows)
    write_svg(svg_path, bucket_rows, metadata=metadata)

    print(f"Chart written to {svg_path}")
    print(f"Data written to {csv_path}")
    for row in bucket_rows:
        if row["algorithm"] == "baseline":
            continue
        baseline = next(
            other
            for other in bucket_rows
            if other["bucket_index"] == row["bucket_index"] and other["algorithm"] == "baseline"
        )
        print(
            f"{row['bucket_label']}: baseline {baseline['percent']:.1f}% "
            f"vs surprise/taper {row['percent']:.1f}% (n={row['players']})"
        )


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def build_bucket_rows(rows: list[dict[str, str]], *, bucket_size: int | None) -> list[dict[str, object]]:
    games_by_player: dict[str, int] = {}
    for row in rows:
        games_by_player.setdefault(row["simulation_player_id"], int(row["simulated_games"]))
    if not games_by_player:
        return []

    bucket_by_player = {
        player_id: game_bucket(games, bucket_size=bucket_size)
        for player_id, games in games_by_player.items()
    }
    bucket_order = sorted(set(bucket_by_player.values()), key=lambda bucket: bucket[0])
    rows_by_algorithm: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        rows_by_algorithm.setdefault(row["algorithm"], []).append(row)

    output: list[dict[str, object]] = []
    for bucket_index, bucket in enumerate(bucket_order, start=1):
        bucket_min, bucket_max, bucket_label = bucket
        bucket_player_ids = {
            player_id
            for player_id, player_bucket in bucket_by_player.items()
            if player_bucket == bucket
        }
        for algorithm in ALGORITHMS:
            algorithm_rows = [
                row
                for row in rows_by_algorithm.get(algorithm.key, [])
                if row["simulation_player_id"] in bucket_player_ids
            ]
            reached = sum(1 for row in algorithm_rows if row["reached"] == "True")
            players = len(algorithm_rows)
            output.append(
                {
                    "bucket_index": bucket_index,
                    "bucket_label": bucket_label,
                    "bucket_min_games": bucket_min,
                    "bucket_max_games": bucket_max,
                    "algorithm": algorithm.key,
                    "algorithm_label": algorithm.label,
                    "players": players,
                    "reached": reached,
                    "percent": reached / players * 100 if players else 0.0,
                }
            )
    return output


def game_bucket(games: int, *, bucket_size: int | None) -> tuple[int, int, str]:
    if bucket_size is None:
        return games, games, f"{games} games"
    bucket_min = (games // bucket_size) * bucket_size
    bucket_max = bucket_min + bucket_size - 1
    return bucket_min, bucket_max, f"{bucket_min}-{bucket_max}"


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    fieldnames = [
        "bucket_index",
        "bucket_label",
        "bucket_min_games",
        "bucket_max_games",
        "algorithm",
        "algorithm_label",
        "players",
        "reached",
        "percent",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_svg(path: Path, rows: list[dict[str, object]], *, metadata: dict[str, object]) -> None:
    width = 980
    height = 680
    margin_left = 82
    margin_right = 44
    margin_top = 84
    margin_bottom = 116
    inner_width = width - margin_left - margin_right
    inner_height = height - margin_top - margin_bottom
    bucket_count = len({int(row["bucket_index"]) for row in rows})
    bucket_width = inner_width / bucket_count if bucket_count else inner_width
    group_gap = 46
    bar_gap = 8
    bar_width = max(24, min(95, (bucket_width - group_gap - bar_gap) / 2))

    def x_bucket(ordinal: int) -> float:
        return margin_left + ordinal * bucket_width

    def y(percent: float) -> float:
        return margin_top + (100 - percent) / 100 * inner_height

    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-labelledby="title desc">',
        '<title id="title">Reached true strength by simulated game count</title>',
        '<desc id="desc">Grouped bars compare baseline and surprise/taper reached rates by number of simulated games over three years.</desc>',
        "<style>",
        "text { font-family: Segoe UI, Arial, sans-serif; fill: #1f2933; }",
        ".title { font-size: 27px; font-weight: 700; }",
        ".subtitle { font-size: 14px; fill: #5b6472; }",
        ".axis { stroke: #28313f; stroke-width: 1.4; }",
        ".grid { stroke: #d8dee8; stroke-width: 1; }",
        ".tick { font-size: 12px; fill: #4c5664; }",
        ".barlabel { font-size: 12px; fill: #253041; font-weight: 700; }",
        ".label { font-size: 14px; font-weight: 600; fill: #364152; }",
        ".legend { font-size: 14px; font-weight: 600; }",
        ".note { font-size: 12px; fill: #5b6472; }",
        "</style>",
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="{margin_left}" y="35" class="title">Catch-up rate by games played</text>',
        f'<text x="{margin_left}" y="58" class="subtitle">Percent reaching hidden one-rank-stronger level by total rated games over the 3-year simulation</text>',
    ]

    for percent in (0, 20, 40, 60, 80, 100):
        y_pos = y(percent)
        lines.append(f'<line x1="{margin_left}" y1="{y_pos:.2f}" x2="{margin_left + inner_width:.2f}" y2="{y_pos:.2f}" class="grid"/>')
        lines.append(f'<text x="{margin_left - 12}" y="{y_pos + 4:.2f}" text-anchor="end" class="tick">{percent}%</text>')

    lines.extend(
        [
            f'<line x1="{margin_left}" y1="{margin_top + inner_height}" x2="{margin_left + inner_width}" y2="{margin_top + inner_height}" class="axis"/>',
            f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + inner_height}" class="axis"/>',
        ]
    )

    rows_by_bucket: dict[int, list[dict[str, object]]] = {}
    for row in rows:
        rows_by_bucket.setdefault(int(row["bucket_index"]), []).append(row)
    color_by_algorithm = {algorithm.key: algorithm.color for algorithm in ALGORITHMS}

    for ordinal, bucket_index in enumerate(sorted(rows_by_bucket)):
        bucket_rows = sorted(rows_by_bucket[bucket_index], key=lambda row: row["algorithm"])
        bucket_left = x_bucket(ordinal)
        first_row = bucket_rows[0]
        group_left = bucket_left + (bucket_width - (2 * bar_width + bar_gap)) / 2
        for algorithm_index, row in enumerate(bucket_rows):
            percent = float(row["percent"])
            bar_x = group_left + algorithm_index * (bar_width + bar_gap)
            bar_y = y(percent)
            bar_height = margin_top + inner_height - bar_y
            color = color_by_algorithm[str(row["algorithm"])]
            lines.append(
                f'<rect x="{bar_x:.2f}" y="{bar_y:.2f}" width="{bar_width:.2f}" height="{bar_height:.2f}" fill="{color}"/>'
            )
            lines.append(
                f'<text x="{bar_x + bar_width / 2:.2f}" y="{bar_y - 6:.2f}" text-anchor="middle" class="barlabel">{percent:.0f}%</text>'
            )
        center = bucket_left + bucket_width / 2
        lines.append(f'<text x="{center:.2f}" y="{margin_top + inner_height + 24}" text-anchor="middle" class="tick">{escape_xml(first_row["bucket_label"])}</text>')
        lines.append(f'<text x="{center:.2f}" y="{margin_top + inner_height + 41}" text-anchor="middle" class="note">n={first_row["players"]}</text>')

    lines.append(f'<text x="{margin_left + inner_width / 2:.2f}" y="{height - 42}" text-anchor="middle" class="label">Simulated rated games over 3 years</text>')
    lines.append(f'<text transform="translate(24 {margin_top + inner_height / 2:.2f}) rotate(-90)" text-anchor="middle" class="label">Percent reached</text>')

    legend_x = margin_left + inner_width - 310
    legend_y = margin_top - 10
    for index, algorithm in enumerate(ALGORITHMS):
        x_pos = legend_x + index * 150
        lines.append(f'<rect x="{x_pos}" y="{legend_y - 12}" width="18" height="18" fill="{algorithm.color}"/>')
        lines.append(f'<text x="{x_pos + 26}" y="{legend_y + 2}" class="legend">{escape_xml(algorithm.label)}</text>')

    lines.append(
        f'<text x="{margin_left}" y="{height - 12}" class="note">Simulation: {escape_xml(metadata["name"])}; game-count buckets use simulated_games from player outcomes.</text>'
    )
    lines.append("</svg>")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def escape_xml(value: object) -> str:
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
