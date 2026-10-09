import argparse
import csv
import html
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Chart measured load generator time series")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--deletion-second", type=float)
    parser.add_argument("--title", default="Measured cache stampede experiment")
    args = parser.parse_args()
    with (args.input / "series.csv").open() as source:
        rows = list(csv.DictReader(source))
    if not rows:
        raise SystemExit("No observations")
    width, height = 1100, 1100
    left, right, top, panel_height = 95, 1040, 85, 205
    xmax = max(float(row["second"]) for row in rows) + 1
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">', '<rect width="100%" height="100%" fill="white"/>', '<g font-family="sans-serif" font-size="13" fill="#243247">', f'<text x="45" y="32" font-size="23">{html.escape(args.title)}</text>', '<text x="45" y="56">Actual measurements · 1-second buckets · HTTP latency includes error responses</text>']
    panels = [
        ("PostgreSQL query starts / second", [("db_qps", "DB QPS", "#b33e30")]),
        ("Endpoint p99 latency (ms)", [("p99_ms", "HTTP p99", "#6247aa")]),
        ("Request failures / second", [("errors", "HTTP / transport errors", "#b33e30"), ("timeouts", "Timeouts (subset)", "#e39122"), ("dropped", "Generator drops", "#111827")]),
        ("Request rate / second", [("offered", "Offered", "#2167a5"), ("dispatched", "Dispatched", "#dc8f28"), ("completed", "Completed", "#29946b")]),
    ]
    for index, (title, lines) in enumerate(panels):
        ytop = top + index * 245
        bottom = ytop + panel_height - 45
        values = [float(row[key]) for key, _, _ in lines for row in rows if row[key] != ""]
        ymax = max(values + [1]) * 1.1
        parts.append(f'<text x="{left}" y="{ytop - 10}" font-size="16">{title}</text>')
        for fraction in [0, .25, .5, .75, 1]:
            y = bottom - fraction * (panel_height - 45)
            parts.append(f'<path d="M {left} {y} H {right}" stroke="#e2e7ed"/><text x="{left - 10}" y="{y + 4}" text-anchor="end">{ymax * fraction:.1f}</text>')
        for tick in range(0, int(xmax) + 1, max(1, int(xmax / 10))):
            x = left + tick / xmax * (right - left)
            parts.append(f'<text x="{x}" y="{bottom + 19}" text-anchor="middle">{tick}</text>')
        for line_index, (key, label, color) in enumerate(lines):
            segments, points = [], []
            for row in rows:
                if row[key] == "":
                    if points:
                        segments.append(points)
                    points = []
                    continue
                x = left + (float(row["second"]) + .5) / xmax * (right - left)
                y = bottom - float(row[key]) / ymax * (panel_height - 45)
                points.append(f"{x:.2f},{y:.2f}")
            if points:
                segments.append(points)
            for segment in segments:
                parts.append(f'<polyline points="{" ".join(segment)}" stroke="{color}" stroke-width="2" fill="none"/>')
            parts.append(f'<text x="{left + line_index * 255}" y="{bottom + 40}" fill="{color}">{label}</text>')
        if args.deletion_second is not None:
            x = left + args.deletion_second / xmax * (right - left)
            parts.append(f'<path d="M {x} {ytop} V {bottom}" stroke="#111827" stroke-dasharray="5 4"/><text x="{x + 5}" y="{ytop + 15}">Cache deletion</text>')
    parts.extend(['<text x="550" y="1080" text-anchor="middle">Seconds since load generator start · Missing DB samples are gaps</text>', '</g></svg>'])
    destination = args.input / "charts.svg"
    destination.write_text("\n".join(parts))
    print(destination)


if __name__ == "__main__":
    main()
