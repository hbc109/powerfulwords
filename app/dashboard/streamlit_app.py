import json
import sqlite3
import sys
from datetime import date
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.strategy.recommendations import compute_recommendations as _compute_recs_core

DB_PATH = BASE_DIR / "data" / "oil_narrative.db"
STRATEGY_CFG_PATH = BASE_DIR / "app" / "config" / "strategy_config.json"
MULTI_CFG_PATH = BASE_DIR / "app" / "config" / "multi_strategy_config.json"


def _load_thresholds() -> dict:
    with open(STRATEGY_CFG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return {
        "long": float(cfg["entry_threshold_long"]),
        "short": float(cfg["entry_threshold_short"]),
        "strong_long": float(cfg["strong_entry_threshold_long"]),
        "strong_short": float(cfg["strong_entry_threshold_short"]),
    }


_THRESHOLDS = _load_thresholds()


def load_df(query: str, params: tuple = ()) -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


def load_scores() -> pd.DataFrame:
    return load_df("""
        SELECT score_date, commodity, theme, topic, narrative_score,
               official_confirmation_score, news_breadth_score,
               chatter_score, crowding_score,
               breadth, persistence, source_divergence
        FROM daily_narrative_scores
        ORDER BY score_date DESC, ABS(narrative_score) DESC
    """)


def load_theme_scores() -> pd.DataFrame:
    return load_df("""
        SELECT score_date, commodity, theme, narrative_score,
               raw_score, event_count, subtheme_count,
               breadth, persistence, source_divergence
        FROM daily_theme_scores
        ORDER BY score_date DESC, ABS(narrative_score) DESC
    """)


def load_events() -> pd.DataFrame:
    return load_df("""
        SELECT event_time, theme, topic, direction, source_bucket, source_name,
               credibility, novelty, verification_status, horizon,
               rumor_flag, confidence, evidence_text
        FROM narrative_events
        ORDER BY event_time DESC
    """)


def load_prices() -> pd.DataFrame:
    return load_df("""
        SELECT price_time, symbol, close
        FROM market_prices
        ORDER BY price_time
    """)


def load_regimes() -> pd.DataFrame:
    return load_df("""
        SELECT regime_date, symbol, primary_regime, regime_tags, regime_streak,
               close, rsi14, adx14, bb_pctb, atr_ratio
        FROM daily_regimes
        ORDER BY regime_date DESC, symbol
    """)


def load_research_payload():
    research_dir = BASE_DIR / "data" / "processed" / "research"
    if not research_dir.exists():
        return None
    files = sorted(research_dir.glob("event_study_*.json"))
    if not files:
        return None
    try:
        return json.loads(files[-1].read_text(encoding="utf-8"))
    except Exception:
        return None


def load_backtest_payload():
    backtest_dir = BASE_DIR / "data" / "processed" / "backtests"
    if not backtest_dir.exists():
        return None
    files = sorted(backtest_dir.glob("backtest_*.json"))
    if not files:
        return None
    try:
        return json.loads(files[-1].read_text(encoding="utf-8"))
    except Exception:
        return None


def load_multi_backtest_payload():
    backtest_dir = BASE_DIR / "data" / "processed" / "backtests"
    if not backtest_dir.exists():
        return None
    files = sorted(backtest_dir.glob("multi_backtest_*.json"))
    if not files:
        return None
    try:
        return json.loads(files[-1].read_text(encoding="utf-8"))
    except Exception:
        return None


def load_multi_cfg():
    if not MULTI_CFG_PATH.exists():
        return None
    return json.loads(MULTI_CFG_PATH.read_text(encoding="utf-8"))


def compute_recommendations(theme_scores_df: pd.DataFrame, score_date: str) -> list[dict]:
    cfg = load_multi_cfg()
    if cfg is None:
        return []
    day_df = theme_scores_df[theme_scores_df["score_date"] == score_date]
    if day_df.empty:
        return []
    score_rows = [
        {"score_date": r["score_date"], "theme": r["theme"], "narrative_score": float(r["narrative_score"])}
        for _, r in day_df.iterrows()
    ]
    return _compute_recs_core(score_rows, cfg)


def instrument_label(inst: dict) -> str:
    t = inst.get("type")
    if t == "outright":
        return inst.get("symbol", "?")
    if t == "spread":
        return f"{inst.get('long_symbol', '?')}-{inst.get('short_symbol', '?')} spread"
    if t == "crack":
        return f"{inst.get('product_symbol', '?')}-{inst.get('crude_symbol', '?')} crack"
    return str(inst)


def topic_label(x: str) -> str:
    return str(x).replace("_", " ").title()


def bias_label(score: float) -> str:
    if score >= _THRESHOLDS["strong_long"]:
        return "Strong Bullish"
    if score >= _THRESHOLDS["long"]:
        return "Bullish"
    if score <= _THRESHOLDS["strong_short"]:
        return "Strong Bearish"
    if score <= _THRESHOLDS["short"]:
        return "Bearish"
    return "Neutral"


def tilt_label(direction: str) -> str:
    """Map engine direction (LONG/SHORT/FLAT) to display-friendly tilt
    wording. The score is a narrative bias, not a trade signal."""
    return {
        "LONG": "Bullish lean",
        "SHORT": "Bearish lean",
        "FLAT": "Neutral",
    }.get(direction, direction)


st.set_page_config(page_title="Oil Narrative Dashboard", layout="wide")
st.title("Oil Narrative Dashboard")

if not DB_PATH.exists():
    st.error(f"Database not found: {DB_PATH}")
    st.stop()

scores = load_scores()
theme_scores = load_theme_scores()
events = load_events()
regimes = load_regimes()

if scores.empty:
    st.warning("No scores found in database.")
    st.stop()

scores["score_date"] = scores["score_date"].astype(str)
if not theme_scores.empty:
    theme_scores["score_date"] = theme_scores["score_date"].astype(str)
if not events.empty:
    events["event_date"] = events["event_time"].astype(str).str[:10]
else:
    events["event_date"] = ""

available_dates = sorted(scores["score_date"].unique(), reverse=True)
latest_with_data = date.fromisoformat(available_dates[0]) if available_dates else date.today()
picked = st.date_input(
    "Select date",
    value=latest_with_data,
    min_value=date(2010, 1, 1),
    max_value=date.today(),
    help="Pick any date — dates without scored narratives will show an empty view.",
)
selected_date = picked.isoformat()
if selected_date not in available_dates:
    st.caption(f"No scored narratives for {selected_date} yet. "
               f"Latest with data: {available_dates[0] if available_dates else '—'}.")

day_scores = scores[scores["score_date"] == selected_date].copy()
day_events = events[events["event_date"] == selected_date].copy()

primary_narrative = "-"
market_bias = "-"
avg_conf = "-"
main_sources = "-"

if not day_scores.empty:
    day_scores = day_scores.sort_values("narrative_score", ascending=False)
    primary_narrative = topic_label(day_scores.iloc[0]["topic"])
    market_bias = bias_label(day_scores["narrative_score"].sum())

if not day_events.empty:
    conf_series = pd.to_numeric(day_events["confidence"], errors="coerce").dropna()
    if not conf_series.empty:
        avg_conf = round(conf_series.mean(), 3)
    top_sources = day_events["source_name"].dropna().value_counts().head(3).index.tolist()
    if top_sources:
        main_sources = ", ".join(top_sources)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Selected Date", selected_date)
c2.metric("Primary Narrative", primary_narrative)
c3.metric("Market Bias", market_bias)
c4.metric("Score Rows", len(day_scores))
c5.metric("Average Event Confidence", avg_conf)

st.info(f"Main Sources: {main_sources}")

tab_recs, tab_upload, tab1, tab_trends, tab2, tab3, tab_multi, tab_method = st.tabs(
    ["Narrative Tilt", "Upload", "Overview", "Trends", "Research", "Backtest", "Multi-book", "Methodology"]
)

def _book_history_score(book_cfg, theme_scores_df, score_date_str):
    """Re-run aggregation across history to get this book's daily scores
    so we can compute a rolling z-score for context."""
    if theme_scores_df.empty:
        return None, None
    from app.strategy.backtest_engine import aggregate_score_by_date
    weights = (book_cfg.get("scoring") or {}).get("theme_weights")
    rows = [
        {"score_date": str(r["score_date"]), "theme": r["theme"],
         "narrative_score": float(r["narrative_score"])}
        for _, r in theme_scores_df.iterrows()
    ]
    agg = aggregate_score_by_date(rows, weights=weights, group_field="theme")
    if not agg:
        return None, None
    df = pd.DataFrame(agg)
    df = df.sort_values("score_date")
    today_val = df.loc[df["score_date"] == score_date_str, "aggregate_score"]
    if today_val.empty:
        return None, None
    today_val = float(today_val.iloc[0])
    history = df[df["score_date"] < score_date_str]["aggregate_score"].tail(30)
    if len(history) < 5:
        return today_val, None  # not enough history for a meaningful z
    mean, std = history.mean(), history.std()
    if std == 0:
        return today_val, None
    return today_val, (today_val - mean) / std


with tab_recs:
    st.subheader(f"Narrative tilt for {selected_date}")

    st.info(
        "These are **narrative-derived biases, not calibrated trade signals.** "
        "Event-study work shows the score behaves as a *regime tracker* — "
        "directionally right when the market trends with consensus, near-random "
        "or wrong in sideways and shock regimes. Treat as an input, not an order."
    )

    with st.expander("📖 How to read these scores", expanded=False):
        st.markdown(f"""
**Tilt** — what the book's narrative reads today: `Bullish lean`, `Bearish lean`, or `Neutral`.

**Tilt strength** — `+2.0` strong bullish lean, `+1.0` mild bullish lean, `0.0` neutral. Same scale on the bearish side.

**Weighted score** — sum of (theme score × this book's theme weight) for today.
This is what the tilt thresholds compare against:

| Threshold | Strength | Meaning |
|---|---|---|
| ≥ {_THRESHOLDS['strong_long']:+.2f} | `+2.0` strong bullish lean | high-conviction bullish narrative |
| ≥ {_THRESHOLDS['long']:+.2f} | `+1.0` mild bullish lean | mild bullish narrative |
| between | `0.0` neutral | nothing decisive |
| ≤ {_THRESHOLDS['short']:+.2f} | `−1.0` mild bearish lean | mild bearish narrative |
| ≤ {_THRESHOLDS['strong_short']:+.2f} | `−2.0` strong bearish lean | high-conviction bearish narrative |

**Z-score (σ)** — today's weighted score expressed as standard deviations above /
below this book's recent (≤30-day) mean. `+0σ` = average day; `+2σ` =
unusually bullish; `+3σ` = extreme. Z is a better gauge than the raw weighted
score when raw values balloon (a +60 raw is very different on a quiet day vs
in the middle of a multi-week conflict).

**Theme drivers** — the three themes contributing the most to the weighted
score for this book. Each book applies its own weights so the same theme can
contribute differently across books (e.g. the spread book weights geopolitics
1.5× and demand only 0.3×).

### Vetoes — when the strategy refuses to enter

A veto fires when a single theme score crosses its threshold. Multiple
vetoes per book are evaluated independently — **any one** firing forces
flat. They guard against trading into an obviously hostile regime even
when the aggregate score looks decisive.

The currently-configured vetoes per book:
""")

        # Build the per-book veto table directly from the live config so
        # it always matches what the engine will do.
        cfg = load_multi_cfg() or {"books": []}
        veto_rows = []
        for book in cfg.get("books", []):
            vetoes = (book.get("scoring") or {}).get("theme_vetoes", []) or []
            if not vetoes:
                veto_rows.append({
                    "book": book["name"],
                    "if_theme": "—",
                    "condition": "—",
                    "blocks": "—",
                    "rationale": "(no vetoes — book always trades on score)",
                })
                continue
            for v in vetoes:
                op = v.get("is", "above")
                val = float(v.get("value", 0))
                arrow = "≥" if op == "above" else "≤"
                veto_rows.append({
                    "book": book["name"],
                    "if_theme": v.get("if_theme", "?"),
                    "condition": f"{arrow} {val:+.1f}",
                    "blocks": v.get("blocks", "?"),
                    "rationale": v.get("note") or "",
                })
        st.dataframe(pd.DataFrame(veto_rows), width="stretch", hide_index=True)

        st.markdown(f"""
**Are vetoes mutual / symmetric?**  Yes — each veto is independent and any one
firing flattens the position. Multiple themes (macro, geopolitics, inventories…)
can all be set up to veto, in either direction (`blocks: long` or `blocks: short`).

**Suggested veto set in this config:**
- **Macro bullish-lean veto** (WTI): macro ≤ −1.0 → no bullish lean. Don't lean
  into a recession-fear / dollar-strength regime even with bullish supply news.
- **Macro bearish-lean veto** (WTI): macro ≥ +1.0 → no bearish lean. Mirror image.
- **Macro veto on Brent**: same idea but threshold widened to ±2.0 — Brent is
  more globally driven and less rate-sensitive than WTI, so only block on
  *extreme* macro readings.
- **Geopolitics symmetric veto** (WTI, Brent, Brent-WTI spread): geopolitics
  ≥ +1.5 → no bearish lean; ≤ −1.5 → no bullish lean. Don't fade a strong
  geopolitical risk premium build, and don't chase one as it collapses
  (e.g. on a ceasefire or sanctions easing).
- **Inventories veto** (gasoline + diesel cracks only): inventories ≥ +1.0
  → no bullish crack lean; ≤ −1.0 → no bearish crack lean. The crack lives or
  dies on product–crude balance, so a clear product build/draw should override
  the underlying narrative tilt.

**Other vetoes worth considering** (not currently active):
- **Source-divergence veto**: if chatter is wildly out of line with officials
  on the dominant theme, sit out. Needs an engine tweak — vetoes look at
  theme scores today; divergence is per-subtheme.
- **Rumor-only veto**: if the bullish/bearish lean comes >75% from
  `social_*` buckets and nothing official, refuse to size up.
- **Crowding veto**: if `crowding_score` for the dominant theme is extreme,
  the trade is consensus and edge is mostly gone.

**Why books differ** — each of the {len(cfg.get('books', []))} books has its own
theme weights, thresholds, and veto rules. The same data can leave WTI flat
(macro vetoed) while Brent is long and the Brent-WTI spread is strong long.
That's the multi-book design at work.

Edit any of this in `app/config/multi_strategy_config.json` — changes take
effect on the next `score_narratives.py` / `run_multi_backtest.py` run, and
the dashboard re-reads the config on each refresh.
""")

    if theme_scores.empty:
        st.write("No theme scores yet. Run scripts/score_narratives.py.")
    else:
        recs = compute_recommendations(theme_scores, selected_date)
        if not recs:
            st.write("No theme rows for the selected date, or no books configured.")
        else:
            multi_cfg_for_z = load_multi_cfg() or {"books": []}
            book_cfg_by_name = {b["name"]: b for b in multi_cfg_for_z.get("books", [])}

            cols = st.columns(len(recs))
            for col, r in zip(cols, recs):
                with col:
                    color = {"LONG": "#1f77b4", "SHORT": "#d62728", "FLAT": "#888888"}[r["direction"]]
                    st.markdown(
                        f"<div style='border-left: 6px solid {color}; padding-left: 8px;'>"
                        f"<b>{r['book']}</b><br/>"
                        f"<small>{instrument_label(r['instrument'])}</small></div>",
                        unsafe_allow_html=True,
                    )
                    st.metric("Tilt", tilt_label(r["direction"]), delta=f"{r['target_position']:+.1f}")

                    # Raw weighted score + rolling z-score for context.
                    book_cfg = book_cfg_by_name.get(r["book"])
                    z_str = ""
                    if book_cfg is not None:
                        _, z = _book_history_score(book_cfg, theme_scores, selected_date)
                        if z is not None:
                            z_str = f"  ({z:+.2f}σ vs 30d)"
                        else:
                            z_str = "  (n/a — <5d history)"
                    st.write(f"Weighted score: **{r['weighted_score']:+.3f}**{z_str}")

                    if r["proposed_position"] != r["target_position"]:
                        st.warning(
                            f"Vetoed (proposed {r['proposed_position']:+.1f} → forced flat)"
                        )
                    if r["top_themes"]:
                        st.caption("Top theme drivers")
                        for t in r["top_themes"]:
                            st.write(f"- {topic_label(t['theme'])}: {t['weighted_score']:+.3f}")
                    if r["vetoes"]:
                        with st.expander("Vetoes"):
                            for v in r["vetoes"]:
                                st.write(
                                    f"`{v['if_theme']}` {v['is']} {v['value']} "
                                    f"(score {v['theme_score']:+.3f}) blocks {v['blocks']}"
                                )

# --- Upload tab: drag-and-drop reports straight into the inbox ---
with tab_upload:
    import re
    import subprocess
    from datetime import date as _date

    st.subheader("Upload an analyst report or any narrative document")
    st.caption(
        "Drop a PDF / DOCX / TXT here, pick its source and date. The file is "
        "saved into data/inbox/<bucket>/<source_id>/<date>_<title>.<ext>, and "
        "you can run the ingest pipeline immediately afterwards."
    )

    INBOX_ROOT = BASE_DIR / "data" / "inbox"

    @st.cache_data(ttl=60)
    def _load_source_choices():
        """Returns [(label_for_dropdown, source_bucket, source_id, source_name)]."""
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT source_bucket, source_id, source_name FROM sources ORDER BY source_bucket, source_id"
        ).fetchall()
        conn.close()
        return [
            (f"{bucket}  /  {sid}  ({name})", bucket, sid, name)
            for bucket, sid, name in rows
        ]

    sources = _load_source_choices()
    if not sources:
        st.warning("No sources in DB. Run `python scripts/init_sources.py` first.")
    else:
        labels = [s[0] for s in sources]
        # Default to the sell-side bucket when present (most common for analyst reports).
        default_idx = next(
            (i for i, s in enumerate(sources) if s[2] == "sellside_manual_upload"), 0
        )

        col_a, col_b = st.columns([2, 1])
        with col_a:
            picked = st.selectbox("Source", labels, index=default_idx)
            picked_idx = labels.index(picked)
            _, sel_bucket, sel_source_id, sel_source_name = sources[picked_idx]
        with col_b:
            picked_date = st.date_input(
                "Report publication date  ⚠ set this to when the report was *written*, not today",
                value=None,
                min_value=_date(2010, 1, 1),
                max_value=_date.today(),
            )

        st.caption(
            "📅 The date you pick becomes `published_at` in the DB and drives "
            "which day the narrative scores into. A Monday-morning upload of "
            "last Wednesday's GS report should still be dated **Wednesday** "
            "or its theme contribution will land on Monday."
        )

        title_hint = st.text_input(
            "Short title / slug (optional — used in the filename)",
            placeholder="e.g. gs_oil_balance_apr",
        )

        uploaded = st.file_uploader(
            "Drop file here",
            type=["pdf", "docx", "txt", "xlsx", "xls"],
            accept_multiple_files=True,
        )

        run_pipeline = st.checkbox(
            "Run ingest + extract + score immediately after saving",
            value=True,
        )

        if uploaded and picked_date is None:
            st.warning("Pick a publication date before saving.")
        save_clicked = st.button(
            "Save to inbox",
            type="primary",
            disabled=not uploaded or picked_date is None,
        )

        def _slugify(s: str) -> str:
            s = (s or "").strip().lower().replace(" ", "_")
            return re.sub(r"[^a-z0-9_]+", "", s)[:60].strip("_") or "report"

        if save_clicked and uploaded:
            target_folder = INBOX_ROOT / sel_bucket / sel_source_id
            target_folder.mkdir(parents=True, exist_ok=True)
            saved = []
            for i, up in enumerate(uploaded):
                stem_hint = title_hint if title_hint else (up.name.rsplit(".", 1)[0] if "." in up.name else up.name)
                # If multiple files share the title, suffix to keep unique
                if len(uploaded) > 1 and not title_hint:
                    stem_hint = up.name.rsplit(".", 1)[0] if "." in up.name else up.name
                slug = _slugify(stem_hint)
                ext = up.name.rsplit(".", 1)[-1].lower() if "." in up.name else "txt"
                fname = f"{picked_date.isoformat()}_{slug}.{ext}"
                # Avoid silently overwriting an existing file with the same name
                target_path = target_folder / fname
                n = 1
                while target_path.exists():
                    n += 1
                    target_path = target_folder / f"{picked_date.isoformat()}_{slug}_{n}.{ext}"
                target_path.write_bytes(up.getvalue())
                saved.append(target_path)
                st.success(f"Saved {target_path.relative_to(BASE_DIR)}")

            if run_pipeline and saved:
                with st.spinner("Running ingest → extract → score …"):
                    cmds = [
                        [sys.executable, "scripts/ingest_folder.py"],
                        [sys.executable, "scripts/extract_narratives.py", "--mode", "auto"],
                        [sys.executable, "scripts/score_narratives.py"],
                    ]
                    for cmd in cmds:
                        result = subprocess.run(
                            cmd, cwd=str(BASE_DIR), capture_output=True, text=True
                        )
                        if result.returncode != 0:
                            st.error(f"`{cmd[1]}` failed:\n{result.stderr.strip() or result.stdout.strip()}")
                            break
                        last = (result.stdout.strip().splitlines() or [""])[-1]
                        st.info(f"`{Path(cmd[1]).name}`: {last[:200]}")
                    else:
                        st.success("Pipeline complete. Switch tabs to see updated scores.")

    st.divider()
    st.markdown(
        "**Filename convention.** Files are saved as "
        "`<YYYY-MM-DD>_<slug>.<ext>`. The date prefix tells the ingester "
        "what `published_at` to record, which drives the daily score date."
    )

    # Quick listing of what's already in this source folder so the user
    # can see what's been uploaded without leaving the dashboard.
    if sources:
        target_folder = INBOX_ROOT / sel_bucket / sel_source_id
        if target_folder.exists():
            files = sorted(p for p in target_folder.iterdir() if p.is_file() and not p.name.startswith("."))
            if files:
                st.markdown(f"**Already in `{sel_bucket}/{sel_source_id}/`:**")
                for f in files[-15:]:
                    size_kb = f.stat().st_size / 1024
                    st.write(f"- `{f.name}` ({size_kb:,.1f} KB)")

with tab1:
    with st.expander("📖 What this tab shows / how to read the scores", expanded=False):
        st.markdown("""
The **diagnostic view** for the picked date. Three sections, drilling from
roll-up to raw events. Bullish/bearish here means *narrative direction*, not a
trade — see the Narrative Tilt tab for the bias call.

### The five header metrics (visible on every tab)

| Metric | What it is |
|---|---|
| **Selected Date** | The date picked from the calendar. |
| **Primary Narrative** | The topic with the largest `narrative_score` on that day. |
| **Market Bias** | One-word label for the **sum of all narrative_scores across topics**: Strong Bullish / Bullish / Neutral / Bearish / Strong Bearish. A net read across the day's chatter. Note: a single very loud topic can outweigh many small opposite ones, so use it as an eyeball gauge, not a ground truth. |
| **Score Rows** | How many topic-level rows exist for the day. Coverage proxy. |
| **Avg Event Confidence** | Mean of the extractor's confidence (0–1) across the day's events. |

### Themes (top-level rollup)
Five themes — supply, demand, geopolitics, policy, macro. Sorted by
`|narrative_score|` (loudest first).
- **narrative_score**: signed (positive = bullish, negative = bearish).
- **bias**: word label for the score.
- **event_count / subtheme_count**: how much fed this theme.
- **breadth** (0–1): source diversity, capped at 5 distinct sources.
- **persistence** (0–1): trend continuity (5-day half-life weight on prior days).
- **source_divergence** (0–1): gap between official-bucket direction and
  chatter-bucket direction. High value = "the chatter is talking about
  something officials are not yet confirming." Worth flagging.

### Subthemes (topic level)
Same idea, more granular: opec_policy, shipping_disruption,
refining_margin_shift, etc. Extra columns:
- **official_confirmation_score** (0–1): fraction of events officially confirmed.
- **news_breadth_score** (0–1): fraction from authoritative news.
- **chatter_score** (0–1): fraction from social buckets.
- **crowding_score**: penalty (high = many events on one topic — edge usually
  fades when a story is everywhere).

### Events (raw narrative events)
One row per discrete narrative the extractor read. Drillable via the topic
filter. Use this to **audit** the extractor — `evidence_text` is the exact
passage that produced the event, so you can spot-check whether the
classification matches the source.

| Column | Meaning |
|---|---|
| `event_time` | Source publication time. |
| `topic` | Subtheme. |
| `direction` | bullish / bearish / mixed / neutral. |
| `source_bucket` | official_data / official_reports / authoritative_news / sellside_private / social_open / … |
| `source_name` | The actual source (e.g. "Goldman Sachs", "Reddit", "WhiteHouse.gov"). |
| `verification_status` | officially_confirmed / partially_confirmed / unverified / refuted. |
| `confidence` | Extractor's confidence (0–1). |
| `evidence_text` | The exact passage — your audit trail. |
""")

    day_themes = (
        theme_scores[theme_scores["score_date"] == selected_date].copy()
        if not theme_scores.empty
        else pd.DataFrame()
    )

    st.subheader(f"Price regime on {selected_date}")
    if regimes.empty:
        st.write("No regime data yet — run `python scripts/compute_regimes.py`.")
    else:
        regimes["regime_date"] = regimes["regime_date"].astype(str)
        day_regimes = regimes[regimes["regime_date"] == selected_date].copy()
        if day_regimes.empty:
            # fall back to the closest prior date so the panel is rarely empty
            prior = regimes[regimes["regime_date"] <= selected_date]
            if not prior.empty:
                latest_avail = prior["regime_date"].iloc[0]
                day_regimes = regimes[regimes["regime_date"] == latest_avail].copy()
                st.caption(f"No regime row for {selected_date}; showing nearest prior {latest_avail}.")
        if day_regimes.empty:
            st.write("No prior regime data available.")
        else:
            cols = st.columns(len(day_regimes))
            color = {
                "shock":          "#d62728",
                "stretched_up":   "#ff7f0e",
                "stretched_down": "#ff7f0e",
                "trend_up":       "#2ca02c",
                "trend_down":     "#9467bd",
                "range":          "#7f7f7f",
            }
            for col, (_, r) in zip(cols, day_regimes.iterrows()):
                with col:
                    c = color.get(r["primary_regime"], "#7f7f7f")
                    st.markdown(
                        f"<div style='border-left: 6px solid {c}; padding-left: 8px;'>"
                        f"<b>{r['symbol']}</b><br/>"
                        f"<span style='color:{c}'><b>{r['primary_regime']}</b></span> "
                        f"<small>(streak {int(r['regime_streak'])}d)</small></div>",
                        unsafe_allow_html=True,
                    )
                    st.caption(
                        f"close {r['close']:.2f} · RSI {r['rsi14']:.1f} · "
                        f"ADX {r['adx14']:.1f} · %B {r['bb_pctb']:.2f} · "
                        f"ATRr {r['atr_ratio']:.2f}"
                    )
                    if r["regime_tags"] and r["regime_tags"] != r["primary_regime"]:
                        st.caption(f"all tags: {r['regime_tags']}")

    st.subheader("Themes")
    if day_themes.empty:
        st.write("No theme rollup for selected date.")
    else:
        day_themes = day_themes.sort_values("narrative_score", key=lambda s: s.abs(), ascending=False)
        show_themes = day_themes.copy()
        show_themes["theme"] = show_themes["theme"].apply(topic_label)
        show_themes["bias"] = show_themes["narrative_score"].apply(bias_label)
        st.dataframe(
            show_themes[
                [
                    "theme",
                    "narrative_score",
                    "bias",
                    "event_count",
                    "subtheme_count",
                    "breadth",
                    "persistence",
                    "source_divergence",
                ]
            ],
            width="stretch",
            hide_index=True,
        )
        st.bar_chart(show_themes[["theme", "narrative_score"]].set_index("theme"))

    st.subheader("Subthemes")
    if day_scores.empty:
        st.write("No scores found for selected date.")
    else:
        show_scores = day_scores.copy()
        show_scores["topic"] = show_scores["topic"].apply(topic_label)
        show_scores["bias"] = show_scores["narrative_score"].apply(bias_label)

        st.dataframe(
            show_scores[
                [
                    "score_date",
                    "topic",
                    "narrative_score",
                    "bias",
                    "official_confirmation_score",
                    "news_breadth_score",
                    "chatter_score",
                    "crowding_score",
                ]
            ],
            width="stretch",
            hide_index=True,
        )

        chart_df = show_scores[["topic", "narrative_score"]].set_index("topic")
        st.bar_chart(chart_df)

    st.subheader("Events")
    if day_events.empty:
        st.write("No events found for selected date.")
    else:
        topic_options = ["ALL"] + sorted(day_events["topic"].dropna().unique().tolist())
        selected_topic = st.selectbox("Filter topic", topic_options, index=0)

        filtered_events = day_events.copy()
        if selected_topic != "ALL":
            filtered_events = filtered_events[filtered_events["topic"] == selected_topic]

        show_events = filtered_events.copy()
        show_events["topic"] = show_events["topic"].apply(topic_label)

        st.dataframe(
            show_events[
                [
                    "event_time",
                    "topic",
                    "direction",
                    "source_bucket",
                    "source_name",
                    "verification_status",
                    "confidence",
                    "evidence_text",
                ]
            ],
            width="stretch",
            hide_index=True,
        )

with tab_trends:
    with st.expander("📖 What this tab shows / how to read the charts", expanded=False):
        st.markdown("""
The **time-and-attribution view**. Overview tells you what's loud *today*;
Trends tells you *when* themes were loud across the timeline, *how* the
score related to price, and *who* is pushing each theme today.

### Theme heatmap (top)
- **x**: date · **y**: theme (supply / demand / geopolitics / macro / inventories / other) · **color**: theme-level `narrative_score` on a red-blue diverging scale (blue bullish, red bearish, white ≈ neutral).
- Spot theme rotations at a glance — a long horizontal blue band = a persistent bullish theme.
- Uses the **theme-level** rollup, not subtopics.

### Narrative vs price (middle)
Independent-axis overlay:
- **Bars** = daily *net* narrative score (sum across themes). Blue = bullish day, red = bearish.
- **Line** = close price for the symbol you pick (WTI / Brent / RBOB / ULSD).
- Qualitative co-movement check — are bars and price aligned (narratives confirm trend) or fighting each other (regime mismatch)? Not a backtest; for that see the Research tab and the event study.

### Source-bucket contribution (selected date) (bottom)
Stacked bar for the picked date only:
- **x**: theme · **y**: `direction × confidence` summed across events · **color**: source_bucket (official_data / official_reports / sellside_private / authoritative_news / social_open / ...)
- For each theme, see which source types are pushing it. A theme tilted bullish but stacked entirely with `social_open` and no officials → speculative; the same tilt with `official_reports` + `authoritative_news` aligned → confirmed consensus.
- **Caveat**: this chart uses *raw* `direction × confidence`, not the full `narrative_score` formula (no bucket weights / persistence / breadth / crowding). Intentional — it shows you the unfiltered source picture before the scorer's adjustments.
""")

    st.subheader("Theme heatmap")
    if theme_scores.empty:
        st.write("No theme scores yet.")
    else:
        heat_df = theme_scores[["score_date", "theme", "narrative_score"]].copy()
        heat_df["theme"] = heat_df["theme"].apply(topic_label)
        heat_chart = (
            alt.Chart(heat_df)
            .mark_rect()
            .encode(
                x=alt.X("score_date:O", title="Date", sort="ascending"),
                y=alt.Y("theme:N", title="Theme"),
                color=alt.Color(
                    "narrative_score:Q",
                    scale=alt.Scale(scheme="redblue", domainMid=0),
                    title="Score",
                ),
                tooltip=["score_date", "theme", "narrative_score"],
            )
            .properties(height=alt.Step(28))
        )
        st.altair_chart(heat_chart, use_container_width=True)

    st.subheader("Narrative vs price")
    prices = load_prices()
    if theme_scores.empty or prices.empty:
        st.write("Need both theme scores and prices loaded to show this chart.")
    else:
        daily_total = (
            theme_scores.groupby("score_date", as_index=False)["narrative_score"].sum()
            .rename(columns={"narrative_score": "net_narrative"})
        )
        daily_total["score_date"] = pd.to_datetime(daily_total["score_date"])
        px = prices.copy()
        px["price_time"] = pd.to_datetime(px["price_time"])
        symbol_opts = sorted(px["symbol"].unique().tolist())
        symbol_choice = st.selectbox("Price symbol", symbol_opts, index=0)
        px_one = px[px["symbol"] == symbol_choice][["price_time", "close"]].rename(columns={"price_time": "date"})
        narr = daily_total.rename(columns={"score_date": "date"})
        overlay_price = (
            alt.Chart(px_one)
            .mark_line(color="#444")
            .encode(x="date:T", y=alt.Y("close:Q", title=f"{symbol_choice} close"))
        )
        overlay_score = (
            alt.Chart(narr)
            .mark_bar(opacity=0.45)
            .encode(
                x="date:T",
                y=alt.Y("net_narrative:Q", title="Net narrative score", axis=alt.Axis(titleColor="#1f77b4")),
                color=alt.condition(
                    "datum.net_narrative > 0",
                    alt.value("#1f77b4"),
                    alt.value("#d62728"),
                ),
            )
        )
        st.altair_chart(
            alt.layer(overlay_score, overlay_price).resolve_scale(y="independent"),
            use_container_width=True,
        )

    st.subheader("Source-bucket contribution (selected date)")
    if day_events.empty:
        st.write("No events on selected date.")
    else:
        direction_sign = {"bullish": 1, "bearish": -1, "mixed": 0.25, "neutral": 0}
        contrib = day_events.copy()
        contrib["signed"] = contrib["direction"].map(direction_sign).fillna(0) * pd.to_numeric(
            contrib["confidence"], errors="coerce"
        ).fillna(0)
        contrib["theme"] = contrib["theme"].fillna("other").apply(topic_label)
        stack = (
            alt.Chart(contrib)
            .mark_bar()
            .encode(
                x=alt.X("theme:N", title="Theme"),
                y=alt.Y("sum(signed):Q", title="Signed confidence contribution"),
                color=alt.Color("source_bucket:N", title="Source bucket"),
                tooltip=["source_bucket", "source_name", "theme", "topic", "direction", "confidence"],
            )
        )
        st.altair_chart(stack, use_container_width=True)

with tab2:
    st.subheader("Research Snapshot")
    research_payload = load_research_payload()

    if not research_payload:
        st.write("No research payload found.")
    else:
        st.write("Latest event study file loaded.")

        if "symbol" in research_payload:
            st.write(f"Symbol: {research_payload['symbol']}")
        if "commodity" in research_payload:
            st.write(f"Commodity: {research_payload['commodity']}")
        if "num_samples" in research_payload:
            st.write(f"Samples: {research_payload['num_samples']}")

        bucket_summary = research_payload.get("bucket_summary", {})
        if bucket_summary:
            rows = []
            for bucket, stats in bucket_summary.items():
                rows.append(
                    {
                        "bucket": bucket,
                        "count": stats.get("count"),
                        "avg_fwd_ret_1d": stats.get("avg_fwd_ret_1d"),
                        "hit_rate_1d": stats.get("hit_rate_1d"),
                        "avg_fwd_ret_3d": stats.get("avg_fwd_ret_3d"),
                        "hit_rate_3d": stats.get("hit_rate_3d"),
                        "avg_fwd_ret_5d": stats.get("avg_fwd_ret_5d"),
                        "hit_rate_5d": stats.get("hit_rate_5d"),
                        "avg_fwd_ret_10d": stats.get("avg_fwd_ret_10d"),
                        "hit_rate_10d": stats.get("hit_rate_10d"),
                    }
                )
            st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
        else:
            st.write("No bucket summary available.")

with tab3:
    st.subheader("Backtest Snapshot")
    backtest_payload = load_backtest_payload()

    if not backtest_payload:
        st.write("No backtest payload found.")
    else:
        summary = backtest_payload.get("summary", {})

        b1, b2, b3, b4 = st.columns(4)
        b1.metric("Initial Capital", summary.get("initial_capital", "-"))
        b2.metric("Final Equity", summary.get("final_equity", "-"))
        b3.metric("Total Return", summary.get("total_return", "-"))
        b4.metric("Trades", summary.get("num_trades", "-"))

        equity_curve = backtest_payload.get("equity_curve", [])
        if equity_curve:
            eq_df = pd.DataFrame(equity_curve)
            if "date" in eq_df.columns and "equity" in eq_df.columns:
                eq_df["date"] = pd.to_datetime(eq_df["date"])
                eq_df = eq_df.sort_values("date")
                st.line_chart(eq_df.set_index("date")[["equity"]])

        trades = backtest_payload.get("trades", [])
        if trades:
            trades_df = pd.DataFrame(trades)
            st.subheader("Recent Trades")
            st.dataframe(trades_df.tail(20), width="stretch", hide_index=True)
        else:
            st.write("No trades recorded in backtest.")

with tab_multi:
    st.subheader("Multi-book backtest")
    multi = load_multi_backtest_payload()
    if not multi:
        st.write(
            "No multi-book backtest output yet. Run "
            "`python scripts/run_multi_backtest.py` to generate one."
        )
    else:
        portfolio = multi.get("portfolio", {})
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Initial Capital", f"{portfolio.get('initial_capital', 0):,.0f}")
        m2.metric("Final Equity", f"{portfolio.get('final_equity', 0):,.0f}")
        m3.metric("Total Return", f"{(portfolio.get('total_return') or 0)*100:+.2f}%")
        m4.metric("Books / Trades", f"{portfolio.get('num_books', 0)} / {portfolio.get('num_trades', 0)}")

        # Per-book equity overlay.
        rows = []
        for b in multi.get("books", []):
            for ec in b.get("equity_curve", []):
                rows.append({
                    "book": b["name"],
                    "date": ec["date"],
                    "equity": ec["equity"],
                })
        port_curve = portfolio.get("portfolio_curve", [])
        for row in port_curve:
            rows.append({"book": "PORTFOLIO", "date": row["date"], "equity": row["equity"]})
        if rows:
            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"])
            chart = (
                alt.Chart(df)
                .mark_line()
                .encode(
                    x="date:T",
                    y=alt.Y("equity:Q", title="Equity"),
                    color=alt.Color("book:N", title="Book"),
                    strokeWidth=alt.condition(
                        "datum.book == 'PORTFOLIO'",
                        alt.value(3.0),
                        alt.value(1.5),
                    ),
                    tooltip=["book", "date", "equity"],
                )
            )
            st.altair_chart(chart, use_container_width=True)

        st.subheader("Per-book summary")
        summary_rows = [
            {
                "book": b["name"],
                "instrument": instrument_label(b.get("instrument", {})),
                **{k: v for k, v in b.get("summary", {}).items() if k != "scoring_mode"},
            }
            for b in multi.get("books", [])
        ]
        if summary_rows:
            st.dataframe(pd.DataFrame(summary_rows), width="stretch", hide_index=True)

        for b in multi.get("books", []):
            with st.expander(f"{b['name']} — {len(b.get('trades', []))} trades"):
                trades = b.get("trades", [])
                if not trades:
                    st.write("No trades.")
                    continue
                st.dataframe(pd.DataFrame(trades), width="stretch", hide_index=True)

with tab_method:
    method_path = BASE_DIR / "docs" / "methodology.md"
    if method_path.exists():
        st.markdown(method_path.read_text(encoding="utf-8"))
    else:
        st.warning(f"Methodology doc not found at {method_path}")
        st.caption(
            "Expected file: docs/methodology.md at the repo root. "
            "If you cloned the repo without it, pull the latest from main."
        )