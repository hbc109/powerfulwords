#!/usr/bin/env bash
# Wrapper for the dashboard's "after upload" pipeline. Run via:
#   flock /tmp/oil_pipeline.lock /home/hongbingchen/powerfulwords/scripts/upload_pipeline.sh
# Blocking flock (no -n) so an upload waits for the hourly cron to
# finish before starting. The hourly cron uses `flock -n` and skips
# its run if an upload is in progress — so the two never race on the
# SQLite write lock. Without this serialization init_sources.py in
# the cron's wrapper used to die with "database is locked".

set -e
cd /home/hongbingchen/powerfulwords
PY=/usr/bin/python3

# While a long one-time DeepSeek backfill is running, defer this pipeline so its
# inbox re-scan + writes don't contend on the single SQLite writer and stall the
# backfill. Self-expiring (ignored if the sentinel is >4h old) so a crashed
# backfill can't pause uploads forever. Files just wait in the inbox; the next
# run after the sentinel clears ingests them.
SENTINEL=/tmp/pw_backfill.active
if [ -f "$SENTINEL" ] && [ $(( $(date +%s) - $(stat -c %Y "$SENTINEL") )) -lt 14400 ]; then
  echo "[skip] DeepSeek backfill active — deferring upload pipeline ($(date))"
  exit 0
fi
$PY scripts/ingest_folder.py
# --incremental: only extract NEW chunks. CRITICAL now that extraction is
# DeepSeek-LLM (mode_default=llm): without it, every upload re-LLM-reads the
# entire ~64k-chunk corpus (hours, holds the DB lock, piles up dashboard-
# triggered runs). The hourly pipeline already uses --incremental.
$PY scripts/extract_narratives.py --mode auto --incremental
$PY scripts/score_narratives.py
