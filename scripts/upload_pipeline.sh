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
$PY scripts/ingest_folder.py
$PY scripts/extract_narratives.py --mode auto
$PY scripts/score_narratives.py
