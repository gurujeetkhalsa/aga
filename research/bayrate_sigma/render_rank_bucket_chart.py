"""Render reached-rate bars by starting rank bucket for an improver simulation."""

from __future__ import annotations

import argparse
import csv
import json
import math
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AlgorithmStyle:
    """Represent algorithm style."""
    key: str
    label: str
    color: str


@dataclass(frozen=True)
class RankBucket:
    """Represent rank bucket."""
    index: int
    label: str
    min_rating: float
    max_rating: float


ALGORITHMS = [
    AlgorithmStyle("baseline", "Baseline", "#275DAD"),
    AlgorithmStyle("surprise_taper_floor_050", "Surprise/taper", "#C44900"),
]


def parse_args() -> argparse.Namespace:
    """Parse args."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("simulation_dir", type=Path, help="Directory containing metadata.json and player_outcomes.csv.")
    parser.add_argument("--name", default="rank_bucket_reached", help="Output filename stem.")
    parser.add_argument("--min-players", type=int, default=1, help="Exclude rank buckets with fewer than this many simulated players.")
    return parser.parse_args()


def main() -> None:
    """Run the command-line entry point for this module."""
    args = parse_args()
    simulation_dir = args.simulation_dir
    metadata = json.loads((simulation_dir / "metadata.json").read_text(encoding="utf-8"))
    rows = read_csv(simulation_dir / "player_outcomes.csv")
    bucket_rows = build_bucket_rows(rows, min_players=args.min_players)

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
    """Read csv."""
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def build_bucket_rows(rows: list[dict[str, str]], *, min_players: int = 1) -> list[dict[str, object]]:
    """Build bucket rows."""
    start_ratings_by_player: dict[str, float] = {}
    for row in rows:
        start_ratings_by_player.setdefault(row["simulation_player_id"], float(row["start_rating"]))
    if not start_ratings_by_player:
        return []

    buckets = rank_buckets(start_ratings_by_player.values())
    rows_by_algorithm: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        rows_by_algorithm.setdefault(row["algorithm"], []).append(row)

    output: list[dict[str, object]] = []
    for bucket in buckets:
        bucket_player_ids = {
            player_id
            for player_id, rating in start_ratings_by_player.items()
            if bucket.min_rating <= rating < bucket.max_rating
        }
        if len(bucket_player_ids) < min_players:
            continue
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
                    "bucket_index": bucket.index,
                    "bucket_label": bucket.label,
                    "bucket_min_rating": bucket.min_rating,
                    "bucket_max_rating": bucket.max_rating,
                    "algorithm": algorithm.key,
                    "algorithm_label": algorithm.label,
                    "players": players,
                    "reached": reached,
                    "percent": reached / players * 100 if players else 0.0,
                }
            )
    return output


def rank_buckets(ratings: object) -> list[RankBucket]:
    """Execute the rank buckets routine."""
    values = list(ratings)
    min_rating = min(values)
    max_rating = max(values)
    buckets: list[RankBucket] = []
    index = 1

    if min_rating < 1.0:
        weakest_kyu = max(4, int(math.ceil(max(0.0, -min_rating - 1.0) / 4.0) * 4))
        for high_kyu in range(weakest_kyu, 0, -4):
            low_kyu = high_kyu - 3
            bucket_min = -(high_kyu + 1)
            bucket_max = 1.0 if low_kyu == 1 else -low_kyu
            buckets.append(
                RankBucket(
                    index=index,
                    label=f"{high_kyu}k-{low_kyu}k",
                    min_rating=bucket_min,
                    max_rating=bucket_max,
                )
            )
            index += 1

    if max_rating >= 1.0:
        strongest_dan = int(math.ceil((max_rating - 1.0) / 4.0) * 4)
        strongest_dan = max(4, strongest_dan)
        for low_dan in range(1, strongest_dan + 1, 4):
            high_dan = low_dan + 3
            buckets.append(
                RankBucket(
                    index=index,
                    label=f"{low_dan}d-{high_dan}d",
                    min_rating=float(low_dan),
                    max_rating=float(high_dan + 1),
                )
            )
            index += 1

    return buckets


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    """Write csv."""
    fieldnames = [
        "bucket_index",
        "bucket_label",
        "bucket_min_rating",
        "bucket_max_rating",
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
    """Write svg."""
    width = 1180
    height = 720
    margin_left = 82
    margin_right = 44
    margin_top = 84
    margin_bottom = 126
    inner_width = width - margin_left - margin_right
    inner_height = height - margin_top - margin_bottom
    bucket_count = len({int(row["bucket_index"]) for row in rows})
    bucket_width = inner_width / bucket_count if bucket_count else inner_width
    group_gap = 14
    bar_gap = 5
    bar_width = max(7, (bucket_width - group_gap - bar_gap) / 2)

    def x_bucket(ordinal: int) -> float:
        """Execute the x bucket routine."""
        return margin_left + ordinal * bucket_width

    def y(percent: float) -> float:
        """Execute the y routine."""
        return margin_top + (100 - percent) / 100 * inner_height

    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-labelledby="title desc">',
        '<title id="title">Reached true strength by starting rank bucket</title>',
        '<desc id="desc">Grouped bars compare baseline and surprise/taper reached rates by starting rank bucket.</desc>',
        "<style>",
        "text { font-family: Segoe UI, Arial, sans-serif; fill: #1f2933; }",
        ".title { font-size: 27px; font-weight: 700; }",
        ".subtitle { font-size: 14px; fill: #5b6472; }",
        ".axis { stroke: #28313f; stroke-width: 1.4; }",
        ".grid { stroke: #d8dee8; stroke-width: 1; }",
        ".tick { font-size: 12px; fill: #4c5664; }",
        ".barlabel { font-size: 10px; fill: #253041; font-weight: 600; }",
        ".label { font-size: 14px; font-weight: 600; fill: #364152; }",
        ".legend { font-size: 14px; font-weight: 600; }",
        ".note { font-size: 12px; fill: #5b6472; }",
        "</style>",
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="{margin_left}" y="35" class="title">Catch-up rate by starting rank</text>',
        f'<text x="{margin_left}" y="58" class="subtitle">Four-rank starting buckets; percent of simulated players reaching their hidden one-rank-stronger level</text>',
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
        group_left = bucket_left + group_gap / 2
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
                f'<text x="{bar_x + bar_width / 2:.2f}" y="{bar_y - 5:.2f}" text-anchor="middle" class="barlabel">{percent:.0f}%</text>'
            )
        center = bucket_left + bucket_width / 2
        lines.append(f'<text x="{center:.2f}" y="{margin_top + inner_height + 24}" text-anchor="middle" class="tick">{escape_xml(first_row["bucket_label"])}</text>')
        lines.append(f'<text x="{center:.2f}" y="{margin_top + inner_height + 41}" text-anchor="middle" class="note">n={first_row["players"]}</text>')

    lines.append(f'<text x="{margin_left + inner_width / 2:.2f}" y="{height - 42}" text-anchor="middle" class="label">Starting rank bucket</text>')
    lines.append(f'<text transform="translate(24 {margin_top + inner_height / 2:.2f}) rotate(-90)" text-anchor="middle" class="label">Percent reached</text>')

    legend_x = margin_left + inner_width - 310
    legend_y = margin_top - 10
    for index, algorithm in enumerate(ALGORITHMS):
        x_pos = legend_x + index * 150
        lines.append(f'<rect x="{x_pos}" y="{legend_y - 12}" width="18" height="18" fill="{algorithm.color}"/>')
        lines.append(f'<text x="{x_pos + 26}" y="{legend_y + 2}" class="legend">{escape_xml(algorithm.label)}</text>')

    lines.append(
        f'<text x="{margin_left}" y="{height - 12}" class="note">Simulation: {escape_xml(metadata["name"])}; rank buckets use starting rating ranges centered on normal rank labels.</text>'
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
