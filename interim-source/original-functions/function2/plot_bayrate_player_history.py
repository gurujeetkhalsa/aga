import argparse
import json
from datetime import datetime
from pathlib import Path


def load_points(path: Path):
    payload = json.loads(path.read_text(encoding="utf-8"))
    official = [
        (datetime.strptime(row["date"], "%Y-%m-%d"), float(row["rating"]))
        for row in payload.get("official_history", [])
    ]
    model = [
        (datetime.strptime(row["date"], "%Y-%m-%d"), float(row["rating_after"]))
        for row in payload.get("model_history", [])
    ]
    agaid = payload.get("summary", {}).get("agaid")
    return agaid, official, model


def polyline(points, x_pos, y_pos):
    return " ".join(f"{x_pos(d):.2f},{y_pos(r):.2f}" for d, r in points)


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot official vs Bayrate history for one AGAID.")
    parser.add_argument("history_json", type=Path)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    agaid, official, model = load_points(args.history_json)
    all_points = official + model
    if not all_points:
        raise SystemExit("No points to plot")

    min_date = min(d for d, _ in all_points)
    max_date = max(d for d, _ in all_points)
    min_rating = min(r for _, r in all_points)
    max_rating = max(r for _, r in all_points)
    if min_rating == max_rating:
        min_rating -= 1.0
        max_rating += 1.0

    width = 1200
    height = 650
    left = 80
    right = 30
    top = 40
    bottom = 70
    plot_w = width - left - right
    plot_h = height - top - bottom

    def x_pos(dt):
        total = (max_date - min_date).days or 1
        return left + ((dt - min_date).days / total) * plot_w

    def y_pos(rating):
        return top + (max_rating - rating) / (max_rating - min_rating) * plot_h

    x_ticks = []
    for year in range(min_date.year, max_date.year + 1, 2):
        tick_date = datetime(year, 1, 1)
        if min_date <= tick_date <= max_date:
            x_ticks.append(tick_date)

    y_ticks = list(range(int(min_rating) - 1, int(max_rating) + 2, 2))

    svg = []
    svg.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">')
    svg.append('<rect width="100%" height="100%" fill="white"/>')
    svg.append(f'<text x="{width/2}" y="24" text-anchor="middle" font-family="Arial" font-size="24">AGAID {agaid} Rating History</text>')

    for tick in x_ticks:
        x = x_pos(tick)
        svg.append(f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{top+plot_h}" stroke="#e5e7eb" stroke-width="1"/>')
        svg.append(f'<text x="{x:.2f}" y="{height-30}" text-anchor="middle" font-family="Arial" font-size="12" fill="#444">{tick.year}</text>')

    for tick in y_ticks:
        y = y_pos(tick)
        svg.append(f'<line x1="{left}" y1="{y:.2f}" x2="{left+plot_w}" y2="{y:.2f}" stroke="#e5e7eb" stroke-width="1"/>')
        svg.append(f'<text x="{left-10}" y="{y+4:.2f}" text-anchor="end" font-family="Arial" font-size="12" fill="#444">{tick}</text>')

    svg.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top+plot_h}" stroke="#111" stroke-width="1.5"/>')
    svg.append(f'<line x1="{left}" y1="{top+plot_h}" x2="{left+plot_w}" y2="{top+plot_h}" stroke="#111" stroke-width="1.5"/>')

    if official:
        svg.append(f'<polyline fill="none" stroke="#1f77b4" stroke-width="2.5" points="{polyline(official, x_pos, y_pos)}"/>')
    if model:
        svg.append(f'<polyline fill="none" stroke="#d62728" stroke-width="1.5" points="{polyline(model, x_pos, y_pos)}"/>')

    legend_x = left + 20
    legend_y = top + 20
    svg.append(f'<line x1="{legend_x}" y1="{legend_y}" x2="{legend_x+30}" y2="{legend_y}" stroke="#1f77b4" stroke-width="2.5"/>')
    svg.append(f'<text x="{legend_x+40}" y="{legend_y+4}" font-family="Arial" font-size="13">Official AGA snapshots</text>')
    svg.append(f'<line x1="{legend_x}" y1="{legend_y+24}" x2="{legend_x+30}" y2="{legend_y+24}" stroke="#d62728" stroke-width="1.5"/>')
    svg.append(f'<text x="{legend_x+40}" y="{legend_y+28}" font-family="Arial" font-size="13">Modeled Bayrate events</text>')

    svg.append(f'<text x="{width/2}" y="{height-8}" text-anchor="middle" font-family="Arial" font-size="14">Date</text>')
    svg.append(f'<text x="20" y="{height/2}" transform="rotate(-90 20 {height/2})" text-anchor="middle" font-family="Arial" font-size="14">Rating</text>')
    svg.append('</svg>')

    out_path = args.output or args.history_json.with_suffix('.svg')
    out_path.write_text("\n".join(svg), encoding="utf-8")
    print(out_path)


if __name__ == "__main__":
    main()
