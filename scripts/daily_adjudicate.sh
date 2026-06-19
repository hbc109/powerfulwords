#!/usr/bin/env bash
# Daily LLM direction adjudication (keyless Claude CLI, capped) + re-score.
# Keeps the free rule extractor for topic/structure; uses Claude only for the
# bull/bear call on recent risk-topic chunks with de-escalation/diplomacy
# language. Resumable + capped, so it chips away at any backlog over nights and
# then keeps up with the daily trickle. Schedule once daily (cron).
set -e
cd /home/hongbingchen/powerfulwords
PY=/usr/bin/python3
SINCE=$(date -d '12 days ago' +%F)
# --cap bounds per-night CLI calls (~15s each). Raise if backlog is large and
# your Claude Code quota allows; lower to spend less.
$PY scripts/llm_adjudicate_direction.py --since "$SINCE" --cap 250
$PY scripts/score_narratives.py
