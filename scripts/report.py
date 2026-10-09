import argparse
import csv
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Summarize measured benchmark results")
    parser.add_argument("--input", required=True, type=Path)
    args = parser.parse_args()
    rows = []
    for path in sorted(args.input.glob("*/summary.json")):
        summary = json.loads(path.read_text())
        with (path.parent / "series.csv").open() as source:
            series = list(csv.DictReader(source))
        complete_metrics = all("# scrape_error" not in p.read_text() for p in path.parent.glob("metrics-*.prom"))
        rows.append(dict(run=path.parent.name, offered_rps=summary["offered_rps"], completed_rps=round(summary["completed_rps_including_drain"], 2), successful_rps=round((summary["completed"] - summary["errors"]) / summary["elapsed_seconds"], 2), completed=summary["completed"], errors=summary["errors"], error_percent=round(100 * summary["errors"] / max(summary["completed"], 1), 3), timeouts=summary["timeouts"], generator_drops=summary["dropped"], db_starts_observed=sum(float(r["db_query_starts"]) for r in series if r["db_query_starts"]), db_metrics_complete=complete_metrics, p50_ms=round(summary["p50_ms"], 3), p95_ms=round(summary["p95_ms"], 3), p99_ms=round(summary["p99_ms"], 3), peak_second_p99_ms=max(float(r["p99_ms"]) for r in series), schedule_p99_ms=round(summary["scheduling_lag_p99_ms"], 3)))
    if not rows:
        raise SystemExit("No completed runs")
    with (args.input / "comparison.csv").open("w") as destination:
        writer = csv.DictWriter(destination, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print("| Run | Completed RPS | Errors | Observed DB starts | Overall p99 ms | Peak 1s p99 ms |")
    print("| --- | ---: | ---: | ---: | ---: | ---: |")
    for row in rows:
        query_count = str(int(row["db_starts_observed"])) + (" (partial)" if not row["db_metrics_complete"] else "")
        print(f'| {row["run"]} | {row["completed_rps"]} | {row["errors"]} | {query_count} | {row["p99_ms"]} | {row["peak_second_p99_ms"]} |')


if __name__ == "__main__":
    main()
