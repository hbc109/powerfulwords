"""Generate an AI judgment review for a given date and store it.

Reads today's paper-trade snapshot + recent themes + recent titles +
recent closed trades, calls the configured LLM provider (DeepSeek by
default — see app/config/llm_config.json) for a short prose review,
writes to the `ai_reviews` table.

Skips gracefully if the provider's API key isn't set or if no snapshot
exists yet for the date.

Run:
    python scripts/generate_ai_review.py
    python scripts/generate_ai_review.py --date 2026-05-15
    python scripts/generate_ai_review.py --model deepseek-chat

Designed to be cron'd nightly at 07:15 (after the paper snapshot at 07:00).
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.scoring.ai_reviewer import generate_review, save_review


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="Review date (YYYY-MM-DD). Defaults to today.")
    ap.add_argument("--model", default=None,
                    help="Override LLM model (default: provider's model from llm_config.json).")
    args = ap.parse_args()
    review_date = date.fromisoformat(args.date) if args.date else date.today()

    print(f"Generating AI review for {review_date}...")
    result = generate_review(review_date, model=args.model)

    if result["status"] != "ok":
        print(f"  status={result['status']}  reason={result['reason']}")
        return

    rid = save_review(review_date, result["model"], result["context"], result["review_text"])
    print(f"  saved as review_id={rid}")
    print()
    print("--- review text ---")
    print(result["review_text"])


if __name__ == "__main__":
    main()
