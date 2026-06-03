#!/usr/bin/env bash
# Wrapper for the hourly pipeline. Run via:
#   flock -n /tmp/oil_pipeline.lock /home/hongbingchen/powerfulwords/scripts/hourly_pipeline.sh
# `flock -n` causes the cron job to exit silently when a previous run
# is still going (e.g. extract_narratives.py is still processing a
# large upload batch). The next hour's tick takes over naturally —
# fetchers pull a 14-day rolling window and dedup is checksum-based,
# so no data is lost.

set -e  # exit on first failure, same as `&&` chain semantics

cd /home/hongbingchen/powerfulwords

PY=/usr/bin/python3

$PY scripts/init_sources.py
$PY scripts/fetch_sources.py
$PY scripts/fetch_prices.py --period 1mo
$PY scripts/compute_regimes.py
$PY scripts/ingest_folder.py
$PY scripts/extract_narratives.py
$PY scripts/score_narratives.py
$PY scripts/test_strategy_hypotheses.py
