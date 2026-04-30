"""Weekly event-study rerun, with history snapshots.

Runs `run_event_study.py` for WTI and Brent at the standard horizons,
keeps a timestamped JSON snapshot under
`data/processed/research/event_study_history/`, and appends a one-row-
per-(symbol, bucket) summary to `event_study_history.csv` so the
stats can be plotted over time.

Run weekly via cron, or ad hoc when you want a fresh read.
"""

from __future__ import annotations

import csv
import json
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
RESEARCH_DIR = BASE_DIR / "data" / "processed" / "research"
HISTORY_DIR = RESEARCH_DIR / "event_study_history"
HISTORY_CSV = RESEARCH_DIR / "event_study_history.csv"
PYTHON = sys.executable

SYMBOLS = ["WTI", "Brent"]
HORIZONS = "1,3,5,10"
SUMMARY_COLS = [
    "run_date", "symbol", "bucket", "count",
    "avg_fwd_ret_1d", "hit_rate_1d",
    "avg_fwd_ret_3d", "hit_rate_3d",
    "avg_fwd_ret_5d", "hit_rate_5d",
    "avg_fwd_ret_10d", "hit_rate_10d",
    "sample_size",
]


def run_one(symbol: str) -> dict:
    cmd = [
        PYTHON, str(BASE_DIR / "scripts" / "run_event_study.py"),
        "--symbol", symbol, "--horizons", HORIZONS,
    ]
    subprocess.run(cmd, check=True, cwd=BASE_DIR)
    out_path = RESEARCH_DIR / f"event_study_crude_oil_{symbol}.json"
    return json.loads(out_path.read_text(encoding="utf-8"))


def snapshot(symbol: str, run_day: str) -> None:
    src = RESEARCH_DIR / f"event_study_crude_oil_{symbol}.json"
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    dst = HISTORY_DIR / f"{run_day}_{symbol}.json"
    shutil.copy2(src, dst)


def append_summary(symbol: str, result: dict, run_day: str) -> int:
    is_new = not HISTORY_CSV.exists()
    rows_written = 0
    with HISTORY_CSV.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SUMMARY_COLS)
        if is_new:
            w.writeheader()
        sample_size = result.get("sample_size", 0)
        for bucket, stats in (result.get("bucket_summary") or {}).items():
            row = {"run_date": run_day, "symbol": symbol, "bucket": bucket,
                   "count": stats.get("count", 0), "sample_size": sample_size}
            for h in (1, 3, 5, 10):
                row[f"avg_fwd_ret_{h}d"] = stats.get(f"avg_fwd_ret_{h}d")
                row[f"hit_rate_{h}d"] = stats.get(f"hit_rate_{h}d")
            w.writerow(row)
            rows_written += 1
    return rows_written


def main() -> None:
    run_day = date.today().isoformat()
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Weekly event-study run for {run_day}")
    for symbol in SYMBOLS:
        result = run_one(symbol)
        snapshot(symbol, run_day)
        n = append_summary(symbol, result, run_day)
        print(f"  {symbol}: sample_size={result.get('sample_size')}, "
              f"buckets={n}, snapshot=event_study_history/{run_day}_{symbol}.json")

    print(f"\nAppended to {HISTORY_CSV}")


if __name__ == "__main__":
    main()
