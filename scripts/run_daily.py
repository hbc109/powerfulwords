"""One-command morning routine.

Runs, in order:
  1. fetch_sources    - pull fresh narratives into the inbox
  2. fetch_prices     - update WTI / Brent / RBOB / ULSD from Yahoo
  3. ingest_folder    - chunk + store new files
  4. extract_narratives - rule mode (or LLM if ANTHROPIC_API_KEY is set)
  5. score_narratives - daily subtheme + theme rollup
  6. run_multi_backtest - per-book P&L incl. spreads + cracks
  7. morning_digest   - markdown report (optionally email if SMTP_* set)

Each step prints a one-line status. A failure in one step does not
prevent later steps from running where it makes sense (e.g. fetcher
failure should not stop scoring of what's already in the DB).

Usage:
  python scripts/run_daily.py
  python scripts/run_daily.py --skip fetch_sources,fetch_prices  # use what's in DB
  python scripts/run_daily.py --dashboard                          # also launch dashboard at end
"""

from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import argparse
import os
import subprocess
import time
from datetime import datetime


PYTHON = sys.executable

STEPS = [
    ("fetch_sources",      [PYTHON, "scripts/fetch_sources.py"]),
    ("fetch_prices",       [PYTHON, "scripts/fetch_prices.py"]),
    ("ingest_folder",      [PYTHON, "scripts/ingest_folder.py"]),
    ("extract_narratives", [PYTHON, "scripts/extract_narratives.py", "--mode", "auto"]),
    ("score_narratives",   [PYTHON, "scripts/score_narratives.py"]),
    ("run_multi_backtest", [PYTHON, "scripts/run_multi_backtest.py"]),
    ("morning_digest",     [PYTHON, "scripts/morning_digest.py"]),
]


def run_step(name: str, cmd: list[str], env: dict) -> tuple[bool, float, str]:
    start = time.monotonic()
    try:
        proc = subprocess.run(
            cmd, cwd=str(BASE_DIR), env=env,
            capture_output=True, text=True, check=False, timeout=600,
        )
    except subprocess.TimeoutExpired:
        return False, time.monotonic() - start, "timeout after 600s"
    dur = time.monotonic() - start
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout or "").strip().splitlines()[-3:]
        return False, dur, "  | ".join(tail) or f"exit {proc.returncode}"
    last_line = (proc.stdout or "").strip().splitlines()
    summary = last_line[-1] if last_line else "ok"
    return True, dur, summary[:200]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip", default="", help="Comma-separated step names to skip")
    parser.add_argument("--only", default="", help="Comma-separated step names to run (others skipped)")
    parser.add_argument("--dashboard", action="store_true", help="Launch the Streamlit dashboard after the pipeline")
    args = parser.parse_args()

    skip = {s.strip() for s in args.skip.split(",") if s.strip()}
    only = {s.strip() for s in args.only.split(",") if s.strip()}

    env = os.environ.copy()
    env["PYTHONPATH"] = str(BASE_DIR) + ":" + env.get("PYTHONPATH", "")

    print(f"=== Daily run @ {datetime.now().isoformat(timespec='seconds')} ===")
    has_anth = bool(env.get("ANTHROPIC_API_KEY"))
    has_oai = bool(env.get("OPENAI_API_KEY"))
    print(f"LLM credentials: anthropic={'yes' if has_anth else 'no'}  openai={'yes' if has_oai else 'no'}")
    if not has_anth and not has_oai:
        print("(no LLM key found — extraction will use rule-based fallback)")
    print()

    failures = []
    for name, cmd in STEPS:
        if only and name not in only:
            print(f"  SKIP   {name:>22}  (not in --only)")
            continue
        if name in skip:
            print(f"  SKIP   {name:>22}  (in --skip)")
            continue
        ok, dur, summary = run_step(name, cmd, env)
        marker = "OK    " if ok else "FAIL  "
        print(f"  {marker} {name:>22}  ({dur:5.1f}s)  {summary}")
        if not ok:
            failures.append(name)

    print()
    if failures:
        print(f"Completed with {len(failures)} failure(s): {', '.join(failures)}")
    else:
        print("All steps completed.")

    digest_dir = BASE_DIR / "data" / "processed" / "digests"
    if digest_dir.exists():
        latest = sorted(digest_dir.glob("morning_*.md"))
        if latest:
            print(f"Digest: {latest[-1]}")

    if args.dashboard:
        print()
        print("Launching dashboard at http://localhost:8501 ...")
        subprocess.run(
            [PYTHON, "-m", "streamlit", "run", "app/dashboard/streamlit_app.py",
             "--server.headless=true", "--browser.gatherUsageStats=false"],
            cwd=str(BASE_DIR), env=env,
        )


if __name__ == "__main__":
    main()
