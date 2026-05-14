# ops/

Reproducible-on-another-machine operations config.

## crontab

The cron schedule the project runs on. Three jobs:

| When | What |
|---|---|
| Hourly at `:05` | Full pipeline: `init_sources → fetch_sources → fetch_prices → compute_regimes → ingest_folder → extract_narratives → score_narratives → test_strategy_hypotheses` |
| Nightly at `03:15` | `run_composite_backtest.py` — refreshes `data/processed/backtests/composite_pnl_*.json` so the dashboard's Composite Backtest tab shows current results each morning |
| Sundays at `02:30` | `run_event_study_weekly.py` — weekly conditional event study |

All output appended to `/tmp/oil_pipeline.log`.

### Install

```bash
# 1. Edit ops/crontab and replace __SET_ME__ with your real EIA_API_KEY
#    (free at https://www.eia.gov/opendata/register.php)

# 2. Install (REPLACES your existing crontab)
crontab ops/crontab

# 3. Verify
crontab -l
```

### EIA_API_KEY

Cron does not source `~/.bashrc`, so `EIA_API_KEY` must be exported inside the
crontab itself (top of file) — without it, `fetch_prices.py` skips the EIA
inventory fetch and the inventory factor falls behind weekly.

**Do not commit your real key.** The repo carries `EIA_API_KEY=__SET_ME__` as a
placeholder; replace it locally before `crontab ops/crontab`. If you need the
file to stay untouched between pulls, keep a private copy at `~/ops_crontab`
with your real key and install from there instead.

### Edit live

```bash
crontab -e          # opens your live crontab in $EDITOR
```

Edits to `ops/crontab` in the repo do **not** automatically update the live
crontab — re-run `crontab ops/crontab` to apply.
