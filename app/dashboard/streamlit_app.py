import json
import os
import sqlite3
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.strategy.recommendations import compute_recommendations as _compute_recs_core
from app.scoring.factors import term_structure_factor, positioning_factor, inventory_factor
from app.scoring.composite import composite_score

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
               close, rsi14, adx14, bb_pctb, atr_ratio,
               macd_hist, volume_ratio, cross_product_agreement
        FROM daily_regimes
        ORDER BY regime_date DESC, symbol
    """)


def load_research_payload(symbol: str = "WTI", commodity: str = "crude_oil"):
    """Load the event-study JSON for a specific symbol; fall back to any
    available file if the requested one is missing."""
    research_dir = BASE_DIR / "data" / "processed" / "research"
    if not research_dir.exists():
        return None, None
    target = research_dir / f"event_study_{commodity}_{symbol}.json"
    if target.exists():
        try:
            data = json.loads(target.read_text(encoding="utf-8"))
            mtime = target.stat().st_mtime
            return data, mtime
        except Exception:
            return None, None
    files = sorted(research_dir.glob("event_study_*.json"))
    if not files:
        return None, None
    try:
        data = json.loads(files[-1].read_text(encoding="utf-8"))
        return data, files[-1].stat().st_mtime
    except Exception:
        return None, None


def list_research_symbols(commodity: str = "crude_oil") -> list[str]:
    research_dir = BASE_DIR / "data" / "processed" / "research"
    if not research_dir.exists():
        return []
    prefix = f"event_study_{commodity}_"
    return sorted(
        f.name.replace(prefix, "").replace(".json", "")
        for f in research_dir.glob(f"{prefix}*.json")
    )


def load_event_study_history():
    csv_path = BASE_DIR / "data" / "processed" / "research" / "event_study_history.csv"
    if not csv_path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(csv_path)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=120)
def load_documents_index() -> pd.DataFrame:
    """Document inventory — light listing, no raw_text blob.

    Joins pre-aggregated chunk/event counts instead of correlated
    subqueries (~2400x faster on 4k+ docs). Cached for 2 minutes;
    cron updates roll in automatically.
    """
    return load_df("""
        SELECT d.document_id, d.source_id, d.source_bucket, d.title,
               d.published_at, d.file_path, d.ingested_at,
               COALESCE(c.n_chunks, 0) AS n_chunks,
               COALESCE(e.n_events, 0) AS n_events
        FROM documents d
        LEFT JOIN (SELECT document_id, COUNT(*) AS n_chunks
                   FROM chunks GROUP BY document_id) c USING (document_id)
        LEFT JOIN (SELECT document_id, COUNT(*) AS n_events
                   FROM narrative_events GROUP BY document_id) e USING (document_id)
        ORDER BY d.published_at DESC, d.ingested_at DESC
    """)


def load_document_text(doc_id: str) -> tuple[str, dict]:
    """Fetch raw_text + metadata for one document on demand."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.execute(
            "SELECT raw_text, source_id, source_bucket, title, "
            "published_at, file_path, source_name "
            "FROM documents WHERE document_id = ?",
            (doc_id,),
        )
        row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return "", {}
    raw_text, source_id, bucket, title, pub, fpath, sname = row
    return (raw_text or ""), {
        "source_id": source_id, "source_bucket": bucket,
        "title": title, "published_at": pub,
        "file_path": fpath, "source_name": sname,
    }


def load_document_events(doc_id: str) -> pd.DataFrame:
    return load_df(
        "SELECT event_time, theme, topic, direction, source_name, "
        "credibility, confidence, evidence_text "
        "FROM narrative_events WHERE document_id = ? "
        "ORDER BY event_time, topic",
        params=(doc_id,),
    )


def load_hypotheses_payload():
    json_path = BASE_DIR / "data" / "processed" / "research" / "strategy_hypotheses.json"
    if not json_path.exists():
        return None
    try:
        return json.loads(json_path.read_text(encoding="utf-8"))
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

tab_recs, tab_upload, tab_library, tab1, tab_trends, tab2, tab3, tab_multi, tab_composite_bt, tab_paper, tab_ai, tab_daily, tab_method = st.tabs(
    ["Signal", "Upload", "Library", "Overview", "Trends", "Research",
     "Baseline Backtest", "Baseline Multi-book", "Composite Backtest", "Paper Trading", "AI Judgment", "Daily Report", "Methodology"]
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
    st.subheader(f"Composite signal — WTI ({selected_date})")
    st.caption(
        "Regime-conditional weighted blend of narrative + factor scores. "
        "All inputs are **z-scores** (today vs its own recent mean), so "
        "weights across factors are comparable. Missing factors (momentum, "
        "positioning, inventory) are renormalized out — to be added next."
    )

    with st.expander("📖 How positioning (COT) works", expanded=False):
        st.markdown(
            """
**What it is.** Weekly Money-Manager net length from the CFTC
Commitments of Traders report, expressed as `(MM long − MM short) / OI %`,
then z-scored over the trailing 52 weeks.

**Source.** CFTC Disaggregated Futures-and-Options Combined report
(public Socrata API at `publicreporting.cftc.gov`, dataset `kh3c-gbw2`).
Updated every Friday afternoon US time with Tuesday-close data.

**Why contrarian.** Money managers are momentum followers — they pile in
near tops and capitulate near bottoms. Extreme MM net length therefore
fades on average. Trend-following exposure is already captured by term
structure (and later, momentum factors); doubling down on it via
positioning would just add correlation, not signal.

**Why a threshold.** Below ~1σ from the trailing mean, MM positioning
is essentially noise — the contrarian edge only shows up at extremes.
The factor uses a soft gate: within ±1σ it contributes **0**; past it
the magnitude grows linearly with distance past the gate.

| raw z-score | factor value |
|---|---|
| ±0.5σ | 0.0 (gated out) |
| ±0.8σ | 0.0 (gated out) |
| ±1.5σ | ∓0.5 |
| ±2.0σ | ∓1.0 |

**Sign.** Positive factor → MMs are *less* long than usual → bullish (room to add).
Negative factor → MMs are *more* long than usual → bearish (crowded, fade).

**Coverage.** WTI uses the `CRUDE OIL, LIGHT SWEET-WTI - ICE FUTURES EUROPE`
entry (the legacy NYMEX identifier stopped reporting). Brent uses
`BRENT LAST DAY - NEW YORK MERCANTILE EXCHANGE` — the financially-settled
NYMEX-listed Brent contract.
"""
        )

    with st.expander("📖 How inventory (EIA) works", expanded=False):
        st.markdown(
            """
**What it is.** Seasonal-deviation z-score across four EIA weekly stock
series, sign-flipped so high stocks vs seasonal = bearish.

**Source.** EIA Weekly Petroleum Status Report via the Open Data API
v2 (free, requires API key from `eia.gov/opendata`). Updated every
Wednesday ~10:30am ET (Thursday after holidays).

**Series included** (equal-weight average):

| Series | Cadence | Why |
|---|---|---|
| EIA US crude stocks (excl SPR) | Weekly | Headline crude balance |
| EIA Cushing OK crude | Weekly | WTI delivery point — drives front spread |
| EIA Total motor gasoline | Weekly | End-demand pull (refinery throughput) |
| EIA Total distillate | Weekly | End-demand pull (diesel + heating oil) |
| JODI OECD crude stocks | Monthly (lag ~6-8 weeks) | International (Europe + Asia) context EIA misses |

**Why seasonal baseline.** Raw inventory levels follow a strong annual
cycle (refinery maintenance, summer driving, winter heating). What
matters is whether stocks are *unusually* high or low **for this time
of year**, not vs an absolute mean. So we compare today's reading to
the same week-of-year (±7 days) average over the trailing 5 years and
z-score against that seasonal std.

**Sign.** Positive factor → stocks below seasonal → tight market → bullish.
Negative factor → stocks above seasonal → oversupplied → bearish.

**Coverage.** Same factor used for WTI and Brent. EIA US data is the
global leading indicator (Brent–WTI correlation ~80% on weekly
balances). JODI adds international crude context (sum of US, JP, DE,
FR, GB, IT, ES, NL, KR, CA, AU closing stocks) but is monthly and
lagged, so its same-week-of-year peer count is much lower than EIA's
(~5 vs ~11) — it nudges the factor without dominating it.

Fujairah (FOIZ weekly) and Singapore (EnterpriseSG weekly) are
candidates for a future iteration when scraping is built.
"""
        )

    with st.expander("📖 How to read the composite", expanded=False):
        st.markdown(
            """
**What it is.** One number combining narrative + factor scores using the
current regime's weights.

**Sign = direction, magnitude = conviction.** Positive → bullish lean.
Inputs are z-scored so values mostly land in roughly `[-1.5, +1.5]`;
`±0.5` is a clear lean, `±1.0+` is strong. The LONG/SHORT/FLAT label
uses a soft threshold of `|composite| > 0.1`.

**z-score = how unusual is today vs its own recent mean.**
`z = (today − 30d mean) / 30d std`. So:
- `z = 0` → today equals the recent average.
- `z = +1` → 1σ above. Mildly elevated.
- `z = +2` → 2σ above. Unusually high (~5% of days).

**Why this can disagree with the Narrative Tilt panel below.** The tilt
uses raw weighted scores ("bullish vs. zero"); the composite uses z-scores
("bullish vs. recent mean"). So narrative can be solidly positive in
absolute terms while its z is negative — the market is bullish, but
*less bullish than the recent average*, which the composite reads as
cooling momentum.

**Breakdown table.** Each row shows one input's z-score, its renormalized
weight in this regime, and its contribution (z × weight) to the total.
Lets you see at a glance which factor is driving today's signal and
whether factors agree or pull against each other.
"""
        )

    if not regimes.empty:
        regimes["regime_date"] = regimes["regime_date"].astype(str)

    _multi_cfg = load_multi_cfg() or {"books": []}
    _book_by_name = {b.get("name"): b for b in _multi_cfg.get("books", [])}

    # Backtest hit-rate uplift of composite vs narrative-only baseline
    # (250 trading days, 2023-05-08 → 2026-05-11, fwd 5-day return).
    # Source: scripts/backtest_composite.py — re-run after weight changes.
    _VALIDATION = {
        "WTI": {
            "level": "success",
            "msg": ("**Backtest-validated.** Over 251 trading days (2023-05 → 2026-05), the composite "
                    "beats narrative-only by **+1.5pp** on 5-day hit-rate (56.5% vs 55.0%) and **+4.1pp** "
                    "on 10-day. Strongest uplift in `shock` (+6.1pp) and `trend_*` (+3.6 to +4.3pp). "
                    "Effectively neutral in `range` and `stretched_down` after weight tuning. "
                    "Treat as a decision input."),
        },
        "Brent": {
            "level": "warning",
            "msg": ("**Cautionary — still under narrative-only on aggregate, but the gap has closed.** "
                    "Composite 5-day hit-rate 53.4% vs narrative-only 55.8% (−2.4pp; was −5.3pp before "
                    "per-symbol Brent tuning). The previously catastrophic `trend_up` and `stretched_down` "
                    "regimes are much improved (−17.9pp → −6.8pp; −28pp → −10pp). Non-narrative factors "
                    "still cost Brent slightly on average — US inventory and COT are less leading for "
                    "Brent than for WTI. Use composite for factor transparency; defer to narrative tilt "
                    "below when in doubt."),
        },
    }

    def _render_composite(symbol: str, book_name: str) -> None:
        st.markdown(f"#### {symbol}")
        v = _VALIDATION.get(symbol)
        if v:
            (st.success if v["level"] == "success" else st.warning)(v["msg"])

        regime = None
        if not regimes.empty:
            today = regimes[(regimes["regime_date"] <= selected_date) & (regimes["symbol"] == symbol)]
            if not today.empty:
                regime = today.iloc[0]["primary_regime"]

        book_cfg = _book_by_name.get(book_name)
        narr_z = None
        if book_cfg is not None and not theme_scores.empty:
            _, narr_z = _book_history_score(book_cfg, theme_scores, selected_date)

        try:
            ts = term_structure_factor(symbol, date.fromisoformat(selected_date))
        except Exception as e:
            ts = None
            st.caption(f"term_structure_factor unavailable: {e}")

        try:
            pos = positioning_factor(symbol, date.fromisoformat(selected_date))
        except Exception as e:
            pos = None
            st.caption(f"positioning_factor unavailable: {e}")

        try:
            inv = inventory_factor(symbol, date.fromisoformat(selected_date))
        except Exception as e:
            inv = None
            st.caption(f"inventory_factor unavailable: {e}")

        if regime is None:
            st.info(f"No {symbol} regime row available — run `python scripts/compute_regimes.py`.")
            return
        try:
            comp = composite_score(
                symbol, regime, narr_z,
                {"term_structure": ts, "positioning": pos, "inventory": inv},
            )
        except KeyError as e:
            st.warning(f"No regime weights configured for ({symbol}, {regime}): {e}")
            return

        direction = "LONG" if comp["total"] > 0.1 else ("SHORT" if comp["total"] < -0.1 else "FLAT")
        color = {"LONG": "#1f77b4", "SHORT": "#d62728", "FLAT": "#888888"}[direction]
        cA, cB, cC = st.columns([1, 1, 2])
        cA.markdown(
            f"<div style='border-left:6px solid {color};padding-left:8px'>"
            f"<b>Regime</b><br/><span style='color:{color}'>{regime}</span></div>",
            unsafe_allow_html=True,
        )
        cB.metric("Composite", f"{comp['total']:+.3f}", delta=direction)
        cC.write(f"narrative z = {narr_z:+.2f}σ" if narr_z is not None else "narrative z = n/a")
        cC.write(
            f"term_structure z = {ts:+.2f}σ" if ts is not None
            else "term_structure z = n/a (need ≥30d of M1/M2 data)"
        )
        cC.write(
            f"positioning = {pos:+.2f} (contrarian, gated past 1σ extreme)" if pos is not None
            else "positioning = n/a (need ≥26 weeks of COT data)"
        )
        cC.write(
            f"inventory z = {inv:+.2f}σ (high stocks vs seasonal = bearish)" if inv is not None
            else "inventory z = n/a (set EIA_API_KEY and run scripts/fetch_prices.py)"
        )

        if comp["breakdown"]:
            st.dataframe(
                pd.DataFrame([
                    {
                        "factor": r["factor"],
                        "value (z)": round(r["value"], 3),
                        "weight (renorm)": round(r["weight"], 3),
                        "contribution": round(r["contribution"], 3),
                    }
                    for r in comp["breakdown"]
                ]),
                width="stretch",
                hide_index=True,
            )

    _render_composite("WTI", "wti_outright")
    _render_composite("Brent", "brent_outright")

    st.divider()
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
                # Fire-and-forget: chain the three pipeline steps via shell &&
                # so they run sequentially in the background, but don't block
                # the dashboard. Logs append to /tmp/upload_pipeline.log so we
                # can surface a tail if the user wants to debug.
                log_path = Path("/tmp/upload_pipeline.log")
                cmd = (
                    f"{sys.executable} scripts/ingest_folder.py >> {log_path} 2>&1 && "
                    f"{sys.executable} scripts/extract_narratives.py --mode auto >> {log_path} 2>&1 && "
                    f"{sys.executable} scripts/score_narratives.py >> {log_path} 2>&1"
                )
                subprocess.Popen(
                    cmd, shell=True, cwd=str(BASE_DIR),
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    start_new_session=True,
                )
                st.success(
                    "Pipeline kicked off in background — refresh in ~1-2 minutes "
                    "to see updated scores. Logs at `/tmp/upload_pipeline.log`."
                )

        st.markdown("---")
        st.subheader("Or paste an email / text body directly")
        st.caption(
            "Browsers can't accept Outlook drag-and-drop (proprietary "
            "clipboard format), so paste the body here instead. Reuses the "
            "**source** and **date** picked above. Saved as a `.txt` file "
            "into the same inbox folder, then run through the same ingest pipeline."
        )

        # Clear the paste fields on the run AFTER a successful save (Streamlit
        # only lets us mutate widget-bound session_state before the widget renders).
        if st.session_state.pop("_clear_paste_next_run", False):
            st.session_state["paste_title_input"] = ""
            st.session_state["paste_body_input"] = ""
        for _msg in st.session_state.pop("_paste_success_msgs", []):
            st.success(_msg)

        col_p1, col_p2 = st.columns([2, 1])
        with col_p1:
            paste_title = st.text_input(
                "Subject / short title (required — used in the filename)",
                placeholder="e.g. gs_morning_note_apr18",
                key="paste_title_input",
            )
        with col_p2:
            paste_pub_date = st.date_input(
                "Publication date  ⚠ when the email was *written*",
                value=picked_date,  # defaults to whatever's set above; can be overridden here
                min_value=_date(2010, 1, 1),
                max_value=_date.today(),
                key="paste_pub_date",
            )
        paste_body = st.text_area(
            "Paste email / text body",
            height=240,
            placeholder="Paste the full email body here. Include any header lines "
                        "(From / Subject / Date) you want preserved — the ingester will see all of it.",
            key="paste_body_input",
        )

        paste_blocked = (
            not paste_body.strip()
            or not paste_title.strip()
            or paste_pub_date is None
        )
        if paste_body.strip() and paste_pub_date is None:
            st.warning("Pick a publication date before saving pasted text.")
        if paste_body.strip() and not paste_title.strip():
            st.warning("Enter a subject / short title before saving pasted text.")

        save_paste = st.button(
            "Save pasted text to inbox",
            type="primary",
            disabled=paste_blocked,
            key="save_paste_btn",
        )

        if save_paste and not paste_blocked:
            target_folder = INBOX_ROOT / sel_bucket / sel_source_id
            target_folder.mkdir(parents=True, exist_ok=True)
            slug = _slugify(paste_title)
            target_path = target_folder / f"{paste_pub_date.isoformat()}_{slug}.txt"
            n = 1
            while target_path.exists():
                n += 1
                target_path = target_folder / f"{paste_pub_date.isoformat()}_{slug}_{n}.txt"
            target_path.write_text(paste_body, encoding="utf-8")
            msgs = [f"Saved {target_path.relative_to(BASE_DIR)} ({len(paste_body):,} chars)"]

            if run_pipeline:
                log_path = Path("/tmp/upload_pipeline.log")
                cmd = (
                    f"{sys.executable} scripts/ingest_folder.py >> {log_path} 2>&1 && "
                    f"{sys.executable} scripts/extract_narratives.py --mode auto >> {log_path} 2>&1 && "
                    f"{sys.executable} scripts/score_narratives.py >> {log_path} 2>&1"
                )
                subprocess.Popen(
                    cmd, shell=True, cwd=str(BASE_DIR),
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    start_new_session=True,
                )
                msgs.append(
                    "Pipeline kicked off in background — refresh in ~1-2 minutes "
                    "to see updated scores. Logs at `/tmp/upload_pipeline.log`."
                )

            st.session_state["_paste_success_msgs"] = msgs
            st.session_state["_clear_paste_next_run"] = True
            st.rerun()

    st.divider()
    st.subheader("Manual pipeline trigger")
    st.caption(
        "Run ingest → extract → score on whatever is currently in `data/inbox/`. "
        "Useful when you saved with the auto-checkbox off, or dropped files "
        "into the inbox folder from outside the dashboard. Logs append to "
        "`/tmp/upload_pipeline.log`."
    )
    col_t1, col_t2 = st.columns([1, 3])
    with col_t1:
        run_now = st.button("Run ingest pipeline now", type="secondary", key="manual_pipeline_btn")
    with col_t2:
        show_log = st.checkbox("Show last 30 lines of pipeline log", value=False, key="show_log_chk")
    if run_now:
        log_path = Path("/tmp/upload_pipeline.log")
        cmd = (
            f"{sys.executable} scripts/ingest_folder.py >> {log_path} 2>&1 && "
            f"{sys.executable} scripts/extract_narratives.py --mode auto >> {log_path} 2>&1 && "
            f"{sys.executable} scripts/score_narratives.py >> {log_path} 2>&1"
        )
        subprocess.Popen(
            cmd, shell=True, cwd=str(BASE_DIR),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        st.success(
            "Pipeline kicked off in background — refresh in ~1-2 minutes to see "
            "updated scores. Tick **Show last 30 lines** above to inspect progress."
        )
    if show_log:
        log_path = Path("/tmp/upload_pipeline.log")
        if log_path.exists():
            tail = log_path.read_text(errors="replace").splitlines()[-30:]
            st.code("\n".join(tail) or "(empty)", language="text")
        else:
            st.caption("(log file not created yet — run the pipeline at least once)")

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

# --- Library: browse / search / open every ingested document ---
with tab_library:
    with st.expander("📖 What this tab shows", expanded=False):
        st.markdown("""
Every document in the database — uploaded reports, fetched RSS items,
chatter — searchable and filterable. Use this to:
- Find specific reports by source, date, or keyword
- Audit what the extractor saw vs. the original source
- Pull up the original PDF/DOCX/TXT file when you need it

**Two layers of storage** behind this view:
1. **Original files on disk** at `data/inbox/<bucket>/<source_id>/` — preserved untouched.
2. **Database** (`data/oil_narrative.db`) — parsed text + metadata + extracted events.

Click a row in the table to expand the detail panel below: metadata,
events the extractor pulled, full extracted text, and a download
button for the original file when it's still on disk.
""")

    docs_idx = load_documents_index()
    if docs_idx.empty:
        st.info("No documents in the library yet.")
    else:
        docs_idx["published_at"] = pd.to_datetime(docs_idx["published_at"], errors="coerce")
        docs_idx["pub_date"] = docs_idx["published_at"].dt.strftime("%Y-%m-%d")

        # ---- Filter row ----
        f1, f2, f3, f4 = st.columns([2, 2, 3, 1])
        bucket_options = ["(all)"] + sorted(docs_idx["source_bucket"].dropna().unique().tolist())
        with f1:
            sel_bucket = st.selectbox("Source bucket", bucket_options, index=0, key="lib_bucket")

        sub_df = docs_idx if sel_bucket == "(all)" else docs_idx[docs_idx["source_bucket"] == sel_bucket]
        source_options = ["(all)"] + sorted(sub_df["source_id"].dropna().unique().tolist())
        with f2:
            sel_source = st.selectbox("Source ID", source_options, index=0, key="lib_source")

        with f3:
            kw = st.text_input(
                "Keyword (case-insensitive)", value="",
                placeholder="goldman, brent, tariff…",
                key="lib_kw",
            )
        with f4:
            search_in = st.selectbox(
                "Search in", ["Title", "Title+Content"], index=0, key="lib_search_in",
                help="Title is fast; Title+Content scans the full extracted text (slower, ~2-3s for 4k docs)",
            )

        d1, d2, d3 = st.columns([2, 2, 1])
        date_min_data = (docs_idx["published_at"].min().date()
                         if pd.notna(docs_idx["published_at"].min()) else date(2010, 1, 1))
        date_max_data = (docs_idx["published_at"].max().date()
                         if pd.notna(docs_idx["published_at"].max()) else date.today())
        # Default to last 90 days so first impression isn't 4k rows
        default_from = max(date_min_data, date.today() - pd.Timedelta(days=90).to_pytimedelta())
        with d1:
            d_from = st.date_input("From", value=default_from, min_value=date(2010, 1, 1),
                                   max_value=date.today(), key="lib_from")
        with d2:
            d_to = st.date_input("To", value=date_max_data, min_value=date(2010, 1, 1),
                                 max_value=date.today(), key="lib_to")
        with d3:
            st.caption("Shortcuts:")
            cs1, cs2, cs3 = st.columns(3)
            if cs1.button("7d", use_container_width=True):
                st.session_state["lib_from"] = date.today() - pd.Timedelta(days=7).to_pytimedelta()
                st.rerun()
            if cs2.button("All", use_container_width=True):
                st.session_state["lib_from"] = date_min_data
                st.rerun()

        # ---- Apply filters ----
        view = docs_idx.copy()
        if sel_bucket != "(all)":
            view = view[view["source_bucket"] == sel_bucket]
        if sel_source != "(all)":
            view = view[view["source_id"] == sel_source]
        if kw.strip():
            kw_lower = kw.strip().lower()
            if search_in == "Title":
                view = view[view["title"].fillna("").str.lower().str.contains(kw_lower, na=False)]
            else:
                # Title+Content: hit raw_text per matching doc
                # Apply other filters first to keep this set small
                view_f = view[(view["published_at"] >= pd.Timestamp(d_from))
                              & (view["published_at"] <= pd.Timestamp(d_to) + pd.Timedelta(days=1))]
                ids = tuple(view_f["document_id"].tolist())
                if ids:
                    placeholder = ",".join("?" * len(ids))
                    matched = load_df(
                        f"SELECT document_id FROM documents "
                        f"WHERE document_id IN ({placeholder}) "
                        f"AND (LOWER(title) LIKE ? OR LOWER(raw_text) LIKE ?)",
                        params=(*ids, f"%{kw_lower}%", f"%{kw_lower}%"),
                    )
                    matched_ids = set(matched["document_id"].tolist())
                    view = view[view["document_id"].isin(matched_ids)]
                else:
                    view = view.iloc[0:0]
        view = view[(view["published_at"] >= pd.Timestamp(d_from))
                    & (view["published_at"] <= pd.Timestamp(d_to) + pd.Timedelta(days=1))]

        st.caption(
            f"**{len(view)}** documents match (out of {len(docs_idx)} total). "
            f"Default window is last 90 days; click **All** above to broaden."
        )

        if view.empty:
            st.write("No matches.")
        else:
            display_cols = ["pub_date", "source_bucket", "source_id", "title", "n_chunks", "n_events"]
            st.dataframe(
                view[display_cols].rename(columns={"pub_date": "published"}),
                width="stretch", hide_index=True, height=380,
            )

            # ---- Pick one to inspect ----
            doc_choices = view.head(500).copy()
            doc_choices["label"] = (
                doc_choices["pub_date"] + "  ·  "
                + doc_choices["source_id"].fillna("?") + "  ·  "
                + doc_choices["title"].fillna("?").str[:80]
            )
            picked_label = st.selectbox(
                "Inspect a document",
                ["—"] + doc_choices["label"].tolist(),
                index=0, key="lib_pick",
            )
            if picked_label != "—":
                picked_row = doc_choices[doc_choices["label"] == picked_label].iloc[0]
                doc_id = picked_row["document_id"]
                raw_text, meta = load_document_text(doc_id)
                events_df = load_document_events(doc_id)

                st.markdown(f"### {meta.get('title') or doc_id}")
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Source", meta.get("source_id", "—"))
                m2.metric("Bucket", meta.get("source_bucket", "—"))
                m3.metric("Published", str(meta.get("published_at", "—"))[:10])
                m4.metric("Events extracted", len(events_df))

                # ---- Fix wrong publication date ----
                with st.expander("⚙ Edit publication date (use only if wrong)", expanded=False):
                    cur_pub = str(meta.get("published_at") or "")[:10]
                    try:
                        cur_pub_date = (date.fromisoformat(cur_pub)
                                        if cur_pub else date.today())
                    except ValueError:
                        cur_pub_date = date.today()
                    st.caption(
                        "Updates `documents.published_at`, every related "
                        "`narrative_events.event_time`, and renames the inbox file "
                        "to match. Re-runs `score_narratives.py` in the background "
                        "so the new date flows through to the daily scores."
                    )
                    new_date = st.date_input(
                        "New publication date",
                        value=cur_pub_date,
                        min_value=date(2010, 1, 1),
                        max_value=date.today(),
                        key=f"edit_date_{doc_id}",
                    )
                    if st.button("Apply new date", key=f"apply_{doc_id}"):
                        if new_date.isoformat() == cur_pub:
                            st.info("Date unchanged — nothing to do.")
                        else:
                            try:
                                from app.db.repository import repoint_document_date
                                conn = sqlite3.connect(DB_PATH)
                                try:
                                    res = repoint_document_date(conn, doc_id, new_date.isoformat())
                                    conn.commit()
                                finally:
                                    conn.close()
                                # Trigger background re-score so daily aggregates pick up the new date
                                subprocess.Popen(
                                    [sys.executable, "scripts/score_narratives.py"],
                                    cwd=str(BASE_DIR),
                                    stdout=subprocess.DEVNULL,
                                    stderr=subprocess.DEVNULL,
                                    start_new_session=True,
                                )
                                msg = (
                                    f"✓ Updated. Events repointed: {res['events_updated']}.  "
                                    + (f"File renamed to `{Path(res['file_renamed_to']).name}`."
                                       if res.get("file_renamed_to") else "(file not on disk to rename)")
                                    + " Re-scoring in background — refresh in ~30s."
                                )
                                st.success(msg)
                            except Exception as e:
                                st.error(f"Failed: {type(e).__name__}: {e}")

                fpath = meta.get("file_path")
                if fpath and Path(fpath).exists():
                    p = Path(fpath)
                    with open(p, "rb") as fh:
                        st.download_button(
                            f"Download original ({p.suffix} · {p.stat().st_size/1024:,.0f} KB)",
                            data=fh.read(),
                            file_name=p.name,
                            mime="application/octet-stream",
                        )
                elif fpath:
                    st.caption(f"Original file path on record but missing on disk: `{fpath}`")
                else:
                    st.caption("No original file path stored (this is normal for RSS-fetched docs).")

                if not events_df.empty:
                    with st.expander(f"Events extracted ({len(events_df)})", expanded=False):
                        st.dataframe(events_df, width="stretch", hide_index=True)

                with st.expander(f"Extracted text ({len(raw_text):,} chars)", expanded=False):
                    if raw_text:
                        max_chars = 20000
                        if len(raw_text) > max_chars:
                            st.caption(f"Showing first {max_chars:,} characters of {len(raw_text):,}.")
                            st.text(raw_text[:max_chars])
                        else:
                            st.text(raw_text)
                    else:
                        st.write("(no text stored)")

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
                    macd_h = r.get("macd_hist")
                    vol_r = r.get("volume_ratio")
                    xprod = r.get("cross_product_agreement")
                    extras = []
                    if pd.notna(macd_h):
                        extras.append(f"MACD_h {macd_h:+.2f}")
                    if pd.notna(vol_r):
                        extras.append(f"vol×{vol_r:.2f}")
                    if pd.notna(xprod):
                        extras.append(f"xprod {xprod:.0%}")
                    if extras:
                        st.caption(" · ".join(extras))
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
    with st.expander("📖 What this tab shows / how to read the numbers", expanded=False):
        st.markdown("""
This is the **empirical validation layer** — does the narrative score
actually predict price moves? Two studies live here:

1. **Unconditional event study** — group all score-dates by the
   narrative bucket they fall into (`strong_bullish`, `bullish`,
   `neutral`, `bearish`, `strong_bearish`) and measure forward returns
   over horizons (1, 3, 5, 10 trading days). The headline metrics:
   - **count** — how many score-dates landed in this bucket. N < 30
     is suggestive; N > 100 is robust.
   - **hit_rate_<H>d** — fraction of cases where price moved the
     direction the narrative implied (up for bullish, down for bearish).
     **50% is random.** Above 50% = signal in the predicted direction.
     Far below 50% = systematically wrong (and itself a fade signal).
   - **avg_fwd_ret_<H>d** — mean forward return across the bucket
     samples (positive number = price up on average).

2. **Conditional event study (regime × narrative bucket)** — the same
   buckets, but split by the *price regime* on the score-date
   (trend_up / trend_down / range / stretched_up / stretched_down /
   shock). This is where the **narrative-as-regime-tracker** finding
   gets quantified: bullish chatter in a clean uptrend ≠ bullish
   chatter at a stretched top.

3. **Hypotheses** — falsifiable trading rules combining narrative
   tilt + regime + cross-product breadth. Each rule fires on
   specific (date, symbol) combinations; we measure hit rate at 5d
   on the **per-date** dedup count (one trade per day, not per topic).
   Low-N findings (⚠ N<30) are flagged as suggestive only.

4. **History panel** (when ≥2 weekly snapshots exist): how each
   bucket's hit rate has evolved across `event_study_history.csv`.
   A pattern stable across 6+ snapshots is more credible than one
   that just appeared this week.

**Key cautions:**
- The `bucket` is set by the narrative score thresholds in
  `strategy_config.json` — not regime-aware.
- A score date with no regime data (early history before regime
  computation) is dropped from the conditional study only.
- This is **not** a trading strategy — it's a diagnostic on the
  signal. The trading layer (Backtest / Multi-book tabs) applies
  thresholds, position sizing, and vetoes on top.
""")

    symbols = list_research_symbols() or ["WTI"]
    sym_default = symbols.index("WTI") if "WTI" in symbols else 0
    chosen_symbol = st.selectbox("Symbol", symbols, index=sym_default, key="research_symbol")
    research_payload, payload_mtime = load_research_payload(chosen_symbol)

    if not research_payload:
        st.write(f"No research payload for {chosen_symbol}. "
                 f"Run `python scripts/run_event_study.py --symbol {chosen_symbol}`.")
    else:
        bucket_summary = research_payload.get("bucket_summary", {}) or {}
        sample_size = research_payload.get("sample_size") or sum(
            int(s.get("count") or 0) for s in bucket_summary.values()
        )
        last_updated = (
            datetime.fromtimestamp(payload_mtime).strftime("%Y-%m-%d %H:%M")
            if payload_mtime else "—"
        )
        m1, m2, m3 = st.columns(3)
        m1.metric("Samples", sample_size)
        m2.metric("Buckets", len(bucket_summary))
        m3.metric("Last updated", last_updated)

        # ---- Unconditional bucket summary: chart + table ----
        st.subheader("Unconditional bucket summary")
        if not bucket_summary:
            st.write("No bucket summary in payload.")
        else:
            order = ["strong_bullish", "bullish", "neutral", "bearish", "strong_bearish"]
            rows = []
            for bucket in order:
                stats = bucket_summary.get(bucket)
                if not stats:
                    continue
                row = {"bucket": bucket, "count": stats.get("count")}
                for h in (1, 3, 5, 10):
                    row[f"hit_rate_{h}d"] = stats.get(f"hit_rate_{h}d")
                    row[f"avg_fwd_ret_{h}d"] = stats.get(f"avg_fwd_ret_{h}d")
                rows.append(row)
            df = pd.DataFrame(rows)

            chart_df = df[["bucket", "hit_rate_5d", "count"]].copy()
            chart_df["bucket"] = pd.Categorical(chart_df["bucket"], categories=order, ordered=True)
            chart_df["low_n"] = chart_df["count"] < 30
            bars = (
                alt.Chart(chart_df)
                .mark_bar()
                .encode(
                    x=alt.X("bucket:N", sort=order, title=None),
                    y=alt.Y("hit_rate_5d:Q", title="Hit rate at 5d", scale=alt.Scale(domain=[0, 1])),
                    color=alt.Color(
                        "hit_rate_5d:Q",
                        scale=alt.Scale(scheme="redblue", domain=[0, 1], domainMid=0.5),
                        legend=None,
                    ),
                    tooltip=["bucket", "hit_rate_5d", "count"],
                )
            )
            ref_line = alt.Chart(pd.DataFrame({"y": [0.5]})).mark_rule(
                strokeDash=[4, 4], color="#888"
            ).encode(y="y:Q")
            n_text = (
                alt.Chart(chart_df)
                .mark_text(dy=-6, color="#444", fontSize=11)
                .encode(
                    x=alt.X("bucket:N", sort=order),
                    y="hit_rate_5d:Q",
                    text=alt.Text("count:Q"),
                )
            )
            st.altair_chart(bars + ref_line + n_text, use_container_width=True)
            st.caption("Dashed line = 50% (random). Numbers above bars = N. Buckets with N<30 are suggestive only.")
            st.dataframe(df, width="stretch", hide_index=True)

        # ---- Conditional study: regime × bucket ----
        cond = research_payload.get("conditional")
        if cond and cond.get("by_regime"):
            st.subheader("Conditional study — regime × narrative bucket")
            regime_order = ["trend_up", "trend_down", "range", "stretched_up", "stretched_down", "shock"]
            bucket_order = ["strong_bullish", "bullish", "neutral", "bearish", "strong_bearish"]
            cells = []
            for regime, by_bucket in cond["by_regime"].items():
                for bucket, stats in by_bucket.items():
                    cells.append({
                        "regime": regime,
                        "bucket": bucket,
                        "count": stats.get("count"),
                        "hit_rate_5d": stats.get("hit_rate_5d"),
                        "avg_fwd_ret_5d": stats.get("avg_fwd_ret_5d"),
                    })
            cond_df = pd.DataFrame(cells)
            heat = (
                alt.Chart(cond_df)
                .mark_rect()
                .encode(
                    x=alt.X("bucket:N", sort=bucket_order, title="Narrative bucket"),
                    y=alt.Y("regime:N", sort=regime_order, title="Price regime"),
                    color=alt.Color(
                        "hit_rate_5d:Q",
                        scale=alt.Scale(scheme="redblue", domain=[0, 1], domainMid=0.5),
                        title="Hit rate 5d",
                    ),
                    tooltip=["regime", "bucket", "count", "hit_rate_5d", "avg_fwd_ret_5d"],
                )
                .properties(height=alt.Step(40))
            )
            text = (
                alt.Chart(cond_df)
                .mark_text(fontSize=11, color="#000")
                .encode(
                    x=alt.X("bucket:N", sort=bucket_order),
                    y=alt.Y("regime:N", sort=regime_order),
                    text=alt.Text("count:Q"),
                )
            )
            st.altair_chart(heat + text, use_container_width=True)
            st.caption(
                "Cells show **count** of score-dates falling in that (regime, bucket). "
                "Color = hit rate at 5d (blue = predictive, red = inverse). "
                f"Total samples: {sum(c['count'] or 0 for c in cells)}. "
                f"Skipped (no regime data): {cond.get('skipped_no_regime', 0)}."
            )
            with st.expander("See full conditional table"):
                st.dataframe(cond_df.sort_values(["regime", "bucket"]),
                             width="stretch", hide_index=True)
        else:
            st.info(
                "Conditional study not available in this payload. "
                "Re-run `python scripts/run_event_study.py --symbol "
                f"{chosen_symbol}` to regenerate."
            )

        # ---- Hypotheses ----
        hypotheses_payload = load_hypotheses_payload()
        if hypotheses_payload:
            st.subheader("Hypotheses")
            st.caption(
                "Falsifiable rules combining narrative tilt, price regime, and "
                "cross-product breadth. Hit rates use **per-date** dedup — one "
                "trade per day, not per topic. ⚠ N<30 = treat as suggestive."
            )
            # Group by hypothesis name; rows from each symbol stacked beneath
            from collections import defaultdict
            grouped: dict = defaultdict(list)
            for r in hypotheses_payload:
                grouped[r["name"]].append(r)
            for h_name, rows in grouped.items():
                first = rows[0]
                with st.container():
                    direction = first["direction"].upper()
                    color = {"LONG": "#2ca02c", "SHORT": "#d62728"}.get(direction, "#7f7f7f")
                    st.markdown(
                        f"<div style='border-left: 6px solid {color}; "
                        f"padding-left: 8px; margin-top: 8px;'>"
                        f"<b>{h_name}</b> — direction: <span style='color:{color}'>"
                        f"<b>{direction}</b></span><br/>"
                        f"<small>{first.get('description','')}</small></div>",
                        unsafe_allow_html=True,
                    )
                    sym_rows = []
                    for r in rows:
                        h5 = (r.get("by_horizon") or {}).get("5", {}) or \
                             (r.get("by_horizon") or {}).get(5, {}) or {}
                        sym_rows.append({
                            "symbol": r.get("symbol"),
                            "unique_dates": r.get("unique_dates"),
                            "5d_count": h5.get("count"),
                            "5d_hit_rate": h5.get("hit_rate"),
                            "5d_avg_ret": h5.get("avg_fwd_ret"),
                            "low_N_flag": "⚠ N<30" if (h5.get("count") or 0) < 30 else "",
                        })
                    sym_df = pd.DataFrame(sym_rows)
                    st.dataframe(sym_df, width="stretch", hide_index=True)
        else:
            st.info(
                "No hypothesis-test payload yet. "
                "Run `python scripts/test_strategy_hypotheses.py` to generate one."
            )

        # ---- History panel ----
        history_df = load_event_study_history()
        if not history_df.empty:
            st.subheader("Hit-rate evolution across weekly snapshots")
            sym_hist = history_df[history_df["symbol"] == chosen_symbol].copy()
            if sym_hist.empty:
                st.write(f"No history rows for {chosen_symbol} yet.")
            elif sym_hist["run_date"].nunique() < 2:
                st.write(
                    f"Only {sym_hist['run_date'].nunique()} weekly snapshot so far. "
                    "More snapshots accumulate every Sunday at 02:30."
                )
            else:
                line = (
                    alt.Chart(sym_hist)
                    .mark_line(point=True)
                    .encode(
                        x=alt.X(
                            "run_date:T",
                            title="Analysis run date",
                            axis=alt.Axis(
                                format="%Y-%m-%d",
                                labelAngle=-30,
                                tickCount="day",
                            ),
                        ),
                        y=alt.Y("hit_rate_5d:Q",
                                scale=alt.Scale(domain=[0, 1]),
                                title="Hit rate 5d"),
                        color=alt.Color("bucket:N", title="Bucket"),
                        tooltip=["run_date", "bucket", "hit_rate_5d", "count"],
                    )
                )
                ref = alt.Chart(pd.DataFrame({"y": [0.5]})).mark_rule(
                    strokeDash=[4, 4], color="#888"
                ).encode(y="y:Q")
                st.altair_chart(line + ref, use_container_width=True)
                st.caption(
                    "Each line = one bucket's hit rate over weekly reruns. "
                    "Patterns stable across many snapshots are more credible than "
                    "ones that just appeared in the latest run."
                )

with tab3:
    st.subheader("Baseline Backtest Snapshot")
    st.warning(
        "**This is a baseline, not a recommended strategy.** It runs the naive "
        "approach — turn narrative tilt directly into LONG/SHORT via the per-book "
        "thresholds, simulate P&L. The conditional event-study work in the "
        "Research tab shows narrative tilt is **regime-dependent**, so this "
        "P&L curve is a **comparator** for any hypothesis-driven strategy we "
        "build later, not a live trading signal."
    )
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
    st.subheader("Baseline Multi-book Backtest")
    st.warning(
        "**Baseline, not a recommended strategy.** Multi-book version of the "
        "naive narrative-tilt-as-trade-signal approach. Useful as a comparator "
        "once we plug hypothesis-driven entries into the same engine. See "
        "Research → Hypotheses for the strategy work."
    )
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

with tab_composite_bt:
    st.subheader("Composite Backtest — regime-conditional multi-factor signal")
    st.caption(
        "Same PnL machinery as the Baseline Backtest (close-to-close return × position, "
        "5bps transaction cost), but the signal is `composite_score()` — narrative + "
        "positioning + inventory blended by regime-conditional weights, instead of the "
        "raw narrative-weighted theme score. Per-symbol output read from "
        "`data/processed/backtests/composite_pnl_<sym>.json`. "
        "Re-run with `python scripts/run_composite_backtest.py`."
    )

    cb_dir = BASE_DIR / "data" / "processed" / "backtests"

    for cb_sym in ["WTI", "Brent"]:
        cb_path = cb_dir / f"composite_pnl_{cb_sym}.json"
        st.markdown(f"### {cb_sym}")
        if not cb_path.exists():
            st.info(f"No composite backtest for {cb_sym}. Run "
                    f"`python scripts/run_composite_backtest.py` to generate it.")
            continue
        cb_data = json.loads(cb_path.read_text())
        cb_summary = cb_data.get("summary", {})
        cb_regimes = cb_data.get("by_regime", {})

        cb1, cb2, cb3, cb4 = st.columns(4)
        cb1.metric("Final Equity",
                   f"${cb_summary.get('final_equity', 0):,.0f}",
                   delta=f"{cb_summary.get('total_return', 0):+.1%} total return")
        sh = cb_summary.get("annualized_sharpe")
        cb2.metric("Annualized Sharpe", f"{sh:.2f}" if sh is not None else "n/a")
        dd = cb_summary.get("max_drawdown")
        cb3.metric("Max Drawdown", f"{dd:+.1%}" if dd is not None else "n/a")
        cb4.metric("Trades / Days",
                   f"{cb_summary.get('num_trades', 0)} / {cb_summary.get('num_days', 0)}")

        cb_eq = cb_data.get("equity_curve", [])
        if cb_eq:
            cb_eq_df = pd.DataFrame(cb_eq)
            cb_eq_df["date"] = pd.to_datetime(cb_eq_df["date"])
            cb_eq_df = cb_eq_df.sort_values("date")
            st.line_chart(cb_eq_df.set_index("date")[["equity"]])

        if cb_regimes:
            st.markdown("**Per-regime contribution** (active days = days with non-flat position):")
            rows = []
            for regime, agg in sorted(cb_regimes.items(), key=lambda kv: -(kv[1].get("pnl_sum") or 0)):
                rows.append({
                    "regime": regime,
                    "trades": agg.get("n_trades"),
                    "active days": agg.get("non_flat_days"),
                    "pnl total ($)": agg.get("pnl_sum"),
                    "pnl per active day ($)": agg.get("pnl_per_day"),
                    "active-day hit rate": agg.get("day_hit_rate"),
                })
            st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

        cb_trades = cb_data.get("trades", [])
        if cb_trades:
            n_show = st.slider(
                f"Show last N trades ({cb_sym})",
                min_value=5, max_value=min(50, len(cb_trades)), value=min(15, len(cb_trades)),
                key=f"cb_trade_n_{cb_sym}",
            )
            st.markdown("**Trade-by-trade explanation** — newest first. Each row expands to show the factor "
                        "breakdown that drove the position and the realized PnL until the next trade flipped it.")
            for tr in reversed(cb_trades[-n_show:]):
                pos = tr.get("target_position", 0.0)
                direction = "LONG" if pos > 0 else ("SHORT" if pos < 0 else "FLAT")
                color = {"LONG": "🟢", "SHORT": "🔴", "FLAT": "⚪"}[direction]
                realized = tr.get("realized_pnl_pct")
                holding = tr.get("holding_days")
                # Headline includes outcome up-front
                if realized is not None:
                    outcome_str = f"{realized:+.2%}" + (f" over {holding}d" if holding else "")
                    outcome_emoji = "✅" if realized > 0 else ("❌" if realized < 0 else "·")
                else:
                    outcome_str = "open"
                    outcome_emoji = "·"
                comp = tr.get("composite")
                regime = tr.get("regime", "?")
                header = (f"{color} **{tr['date']}** · `{regime}` · {direction} {abs(pos):.0f}x "
                          f"· composite={comp:+.2f}" if comp is not None else
                          f"{color} **{tr['date']}** · `{regime}` · {direction} {abs(pos):.0f}x")
                header += f" · {outcome_emoji} **{outcome_str}**"

                with st.expander(header):
                    cA, cB, cC = st.columns(3)
                    cA.metric("Entry close", f"{tr.get('entry_close', 0):,.2f}")
                    cB.metric("Exit close", f"{tr.get('exit_close', 0):,.2f}" if tr.get("exit_close") else "open")
                    cC.metric("Holding", f"{holding}d" if holding else "—")

                    bd = tr.get("breakdown") or []
                    if bd:
                        st.markdown("**Composite breakdown** — what drove this trade:")
                        st.dataframe(pd.DataFrame([
                            {
                                "factor": r["factor"],
                                "value (z)": round(r["value"], 3),
                                "weight (renorm)": round(r["weight"], 3),
                                "contribution": round(r["contribution"], 3),
                            }
                            for r in bd
                        ]), width="stretch", hide_index=True)
                    else:
                        st.caption("No factor breakdown stored for this trade.")

                    if tr.get("transaction_cost") is not None:
                        st.caption(
                            f"prev_position {tr.get('prev_position', 0):+.1f} → target {pos:+.1f}  ·  "
                            f"turnover {tr.get('turnover', 0):.2f}  ·  "
                            f"transaction cost ${tr.get('transaction_cost', 0):.2f}"
                        )

        st.divider()

with tab_paper:
    st.subheader("Paper Trading — daily auto-snapshot of the composite signal")
    st.caption(
        "Each night, `scripts/snapshot_paper_trades.py` records the composite signal "
        "for WTI and Brent into the `paper_trades` table, with an auto-generated "
        "reasoning line from the factor breakdown. When the next snapshot flips "
        "direction, the previous position auto-resolves and the realized PnL is "
        "computed from close-to-close. This is a true forward-test (out-of-sample) "
        "of the system since v1 lock on 2026-05-14."
    )

    with st.expander("📖 Trading rules & execution semantics", expanded=False):
        st.markdown(
            """
**Signal → position** (`score_to_target_position`):

| Composite | Position |
|---|---|
| `> +0.40` | **+2x LONG** (strong) |
| `> +0.10` | **+1x LONG** |
| `−0.10 ≤ x ≤ +0.10` | **FLAT** (no position, no trade) |
| `< −0.10` | **−1x SHORT** |
| `< −0.40` | **−2x SHORT** (strong) |

`max_abs_position = 2.0`. The dead-band ±0.10 is the *"odds aren't good
enough to act"* zone — recorded as `direction=FLAT` in the ledger so
you see the model considered the day and chose not to trade.

**Execution price** — close-to-close:
- `entry_close` = the most recent `market_prices.close` ≤ `plan_date`
- `exit_close`  = the next snapshot's `entry_close` when direction flips
- `realized_pnl_pct = (exit_close / entry_close − 1) × target_position`

No bid/ask, no slippage, no partial fills — close prints assumed
executable. For real-world comparison, subtract roughly 5–10bps per
turnover (the Composite Backtest applies 5bps; this paper ledger does
**not** deduct cost yet — realized PnL here is gross of fees).

**Auto-resolution.** When tonight's snapshot direction differs from
the open position's direction, the previous trade closes at tonight's
entry close. Same direction (e.g., LONG → LONG, possibly different
size) does **not** close the position — sizing changes accumulate as
turnover but stay in the same trade record.

**Cron schedule.** Snapshot runs nightly at **07:00 local (UTC+8)** —
that's after NYMEX WTI's 17:00 ET = 05:00 UTC+8 settlement print, with
a margin for the 06:05 hourly `fetch_prices` to publish the official
daily close. Earlier than 06:00 UTC+8 risks using yesterday's pre-
settlement quote and getting the wrong entry price.

**De-dup.** `(symbol, plan_date)` is unique — re-running the snapshot
script for the same day is a no-op (won't double-record).

**What this is and isn't.**
- ✅ A truthful, ongoing scorecard of the model's signal vs. realized
  market moves, marked-to-market each time direction flips.
- ✅ A way to detect model drift (cumulative hit-rate divergence from
  backtest expectations).
- ❌ Real PnL — no fees, no slippage, no risk management overlay.
- ❌ A tradeable system without a sizing/risk layer above this signal.
"""
        )

    from app.scoring.paper_trading import load_trades, ensure_table
    ensure_table()  # make sure the table exists even before first snapshot

    pt_all = load_trades(limit=400)
    if not pt_all:
        st.info("No paper trades yet. Run `python scripts/snapshot_paper_trades.py` (or wait for tonight's 03:30 cron).")
    else:
        for pt_sym in ["WTI", "Brent"]:
            sub = [t for t in pt_all if t["symbol"] == pt_sym]
            st.markdown(f"### {pt_sym}")
            if not sub:
                st.caption(f"No trades recorded for {pt_sym} yet.")
                continue

            open_pos = next((t for t in sub if t["exit_date"] is None), None)
            closed = [t for t in sub if t["exit_date"] is not None]

            cP1, cP2, cP3, cP4 = st.columns(4)
            cP1.metric("Open position",
                       f"{open_pos['direction']} {abs(open_pos['target_position']):.0f}x" if open_pos else "—",
                       delta=f"since {open_pos['plan_date']}" if open_pos else None)
            n_closed = len(closed)
            wins = sum(1 for t in closed if (t.get("realized_pnl_pct") or 0) > 0)
            losses = sum(1 for t in closed if (t.get("realized_pnl_pct") or 0) < 0)
            hr = (wins / (wins + losses)) if (wins + losses) else None
            cP2.metric("Closed trades", n_closed)
            cP3.metric("Hit rate", f"{hr:.1%}" if hr is not None else "n/a")
            # Cumulative paper PnL (sum of realized_pnl_pct, no compounding for now)
            cum = sum((t.get("realized_pnl_pct") or 0) for t in closed)
            cP4.metric("Cumulative realized %", f"{cum:+.2%}")

            if open_pos:
                with st.expander(f"📌 Open position — {open_pos['direction']} {abs(open_pos['target_position']):.0f}x since {open_pos['plan_date']}", expanded=True):
                    st.write(f"**Reasoning**: {open_pos.get('reasoning', '—')}")
                    st.caption(
                        f"Composite {open_pos.get('composite_score'):+.3f} · regime `{open_pos.get('regime')}` · "
                        f"entry close {open_pos.get('entry_close'):,.2f}" if open_pos.get('composite_score') is not None
                        else f"regime `{open_pos.get('regime')}`"
                    )
                    if open_pos.get("notes"):
                        st.write(f"📝 Notes: {open_pos['notes']}")
                    bd = open_pos.get("breakdown") or []
                    if bd:
                        st.dataframe(pd.DataFrame([
                            {"factor": r["factor"], "value (z)": round(r["value"], 3),
                             "weight": round(r["weight"], 3), "contribution": round(r["contribution"], 3)}
                            for r in bd
                        ]), width="stretch", hide_index=True)

            if closed:
                st.markdown("**Closed trades** — newest first. Header shows `entry → exit · direction · regime · composite · realized PnL · hold`.")
                show_n = min(20, len(closed))
                for tr in closed[:show_n]:
                    pos = tr.get("target_position", 0.0)
                    realized = tr.get("realized_pnl_pct")
                    holding = tr.get("holding_days")
                    color = {"LONG": "🟢", "SHORT": "🔴", "FLAT": "⚪"}.get(tr.get("direction"), "·")
                    if realized is not None:
                        outcome = f"{realized:+.2%}"
                        emoji = "✅" if realized > 0 else ("❌" if realized < 0 else "·")
                    else:
                        outcome = "—"
                        emoji = "·"
                    entry_d = tr.get("plan_date", "?")
                    exit_d = tr.get("exit_date") or "open"
                    hold_str = f" · {holding}d" if holding else ""
                    comp_str = (f" · composite {tr.get('composite_score'):+.2f}"
                                if tr.get('composite_score') is not None else "")
                    head = (f"{color} **{entry_d} → {exit_d}** · `{tr.get('regime', '?')}` · "
                            f"{tr.get('direction')} {abs(pos):.0f}x{comp_str} · "
                            f"{emoji} **{outcome}**{hold_str}")
                    with st.expander(head):
                        st.write(f"**Reasoning**: {tr.get('reasoning', '—')}")
                        if tr.get("notes"):
                            st.write(f"📝 Notes: {tr['notes']}")
                        cT1, cT2, cT3 = st.columns(3)
                        if tr.get("entry_close"):
                            cT1.metric(f"Entry close · {entry_d}", f"{tr['entry_close']:,.2f}")
                        if tr.get("exit_close"):
                            cT2.metric(f"Exit close · {exit_d}", f"{tr['exit_close']:,.2f}")
                        cT3.metric("Holding", f"{holding}d" if holding else "—")
                        bd = tr.get("breakdown") or []
                        if bd:
                            st.dataframe(pd.DataFrame([
                                {"factor": r["factor"], "value (z)": round(r["value"], 3),
                                 "weight": round(r["weight"], 3), "contribution": round(r["contribution"], 3)}
                                for r in bd
                            ]), width="stretch", hide_index=True)
                if len(closed) > show_n:
                    st.caption(f"... showing {show_n} of {len(closed)} closed trades.")
            st.divider()

with tab_ai:
    st.subheader("AI Judgment — parallel advisory overlay (does NOT touch trades)")
    st.caption(
        "Each evening, `scripts/generate_ai_review.py` calls Claude with "
        "today's composite signal, factor breakdown, regime, recent narrative "
        "themes, recent document titles, and last 8 closed paper trades. "
        "Claude writes a short prose review: signal coherence, factor "
        "disagreements, cross-symbol observations, tail-risk flags. "
        "**The trade-decision logic stays 100% rule-based; this layer is "
        "advisory only.** Stored in the `ai_reviews` table."
    )

    from app.scoring.ai_reviewer import load_reviews, prepare_prompt, save_review
    ai_reviews = load_reviews(limit=60)

    # --- Manual paste-flow for users on a claude.ai subscription (no API key) ---
    with st.expander("✍️ Generate review manually via claude.ai (no API key needed)", expanded=True):
        ml1, ml2 = st.columns([3, 1])
        with ml1:
            st.caption(
                "Your claude.ai subscription doesn't expose an API. Use this paste-flow "
                "instead: copy the prompt → paste into claude.ai → paste Claude's "
                "response back here → save. The saved review lives in the same table "
                "as auto-generated ones and shows up below."
            )
        with ml2:
            st.link_button("Open claude.ai ↗", "https://claude.ai/new", use_container_width=True)

        with st.expander("📖 How to use this — step by step", expanded=False):
            st.markdown("""
**Setup (one time)**: nothing. Just be logged in to [claude.ai](https://claude.ai) in another browser tab.

**Each day** (takes about 60 seconds):

1. **Pick the date** in the box below (defaults to today). For yesterday's review pick yesterday, etc.
2. **Click `📋 Build prompt for this date`** — the dashboard assembles the context (today's signal, factor breakdown, recent themes, recent titles, recent paper trades) into two text blocks: a **system prompt** and a **user prompt**.
3. **Open a new chat at claude.ai** (Sonnet 4.6 recommended for the right cost/quality balance, but Opus 4.7 works too).
4. **Paste the system prompt first**, on its own line, then a blank line, then **paste the user prompt** under it. Submit. *(claude.ai web UI doesn't have a separate system field, so prepending it works fine.)*
5. **Wait for Claude's reply** — a single paragraph, 150-200 words.
6. **Copy Claude's full reply** and **paste it into the box** at the bottom of this section.
7. **Click `💾 Save as today's review`** — written to `ai_reviews` table, dashboard reruns, the new review appears below as the "Latest review" with `model: claude.ai-manual` to distinguish from API-generated ones.

**Tips:**
- If Claude refuses or gives a wildly off-shape reply, just regenerate in claude.ai and paste the better version. Save overwrites the existing review for that date (unique on `(review_date)`).
- The system prompt is the same every day; the user prompt is what changes. If you do this enough that pasting the system prompt feels redundant, just paste the user prompt — Claude will infer most of the right behavior, just with slightly less consistent output style.
- You can backfill: pick an older date, build the prompt, generate, save. As long as a paper-trade snapshot exists for that date, this works.
""")

        mc1, mc2 = st.columns([1, 3])
        with mc1:
            paste_review_date = st.date_input(
                "Review date",
                value=date.today(),
                min_value=date(2024, 1, 1),
                max_value=date.today(),
                key="paste_review_date",
            )
        with mc2:
            if st.button("📋 Build prompt for this date", key="build_prompt_btn"):
                st.session_state["_paste_prompt_payload"] = prepare_prompt(paste_review_date)

        payload = st.session_state.get("_paste_prompt_payload")
        if payload and not payload.get("ready"):
            st.warning(payload.get("reason") or "No data for this date.")
        elif payload:
            user_len = len(payload.get("user") or "")
            sys_len = len(payload.get("system") or "")
            st.caption(
                f"📝 Prompt built — **system: {sys_len:,} chars · user: {user_len:,} chars** "
                f"(~{(sys_len + user_len) // 4:,} tokens). Paste into a fresh claude.ai chat; "
                f"Sonnet 4.6 is the recommended model."
            )
            st.markdown("**System prompt** (copy first into claude.ai's *Style*/system-prompt field if available, "
                        "or just paste it as a prefix):")
            st.code(payload["system"], language="text")
            st.markdown("**User prompt** (paste as your message to claude.ai):")
            st.code(payload["user"], language="text")
            st.caption(
                "Claude.ai will write a 150-200 word review. Paste it back in the box "
                "below and save."
            )

            pasted = st.text_area(
                "Paste Claude's response here",
                height=200,
                key="paste_review_response_input",
                placeholder="Paste Claude.ai's review text...",
            )
            save_disabled = not pasted.strip()
            if st.button("💾 Save as today's review", type="primary",
                         disabled=save_disabled, key="save_pasted_review_btn"):
                rid = save_review(
                    paste_review_date,
                    model="claude.ai-manual",
                    context=payload["context"],
                    review_text=pasted.strip(),
                )
                st.success(f"Saved review_id={rid} for {paste_review_date}.")
                st.session_state.pop("_paste_prompt_payload", None)
                st.session_state["paste_review_response_input"] = ""
                st.rerun()

    if not os.environ.get("ANTHROPIC_API_KEY") and not ai_reviews:
        st.info(
            "**ANTHROPIC_API_KEY not set on the dashboard process.** Set it "
            "in `~/.bashrc` and add it to the cron environment "
            "(see `ops/crontab`), then run `python scripts/generate_ai_review.py` "
            "to produce the first review. Get a key at https://console.anthropic.com/."
        )
    elif not ai_reviews:
        st.info("No AI reviews yet. Run `python scripts/generate_ai_review.py` "
                "(or wait for tonight's 07:15 cron).")
    else:
        st.markdown(f"### Latest review — {ai_reviews[0]['review_date']}")
        latest = ai_reviews[0]
        st.markdown(latest["review_text"])
        st.caption(f"model: `{latest['model']}` · generated {latest['created_at']}")

        ctx = latest.get("context") or {}
        if ctx.get("signals_today"):
            with st.expander("Show context fed to Claude (what it 'saw')"):
                st.markdown("**Signals today:**")
                for s in ctx["signals_today"]:
                    comp = s.get("composite")
                    comp_str = f"{comp:+.3f}" if isinstance(comp, (int, float)) else "n/a"
                    st.write(f"- {s['symbol']}: {s.get('direction', '?')} "
                             f"{abs(s.get('target_position') or 0):.0f}x "
                             f"· regime `{s.get('regime', '?')}` · composite {comp_str}")
                if ctx.get("recent_themes_7d"):
                    st.markdown("**Top recent themes (7d):**")
                    for t in ctx["recent_themes_7d"][:5]:
                        st.write(f"- {t['theme']}: total {t['total_score']:+.2f} "
                                 f"({t['n_days_present']}d)")
                if ctx.get("recent_closed_trades"):
                    st.markdown("**Recent closed paper trades:**")
                    for c in ctx["recent_closed_trades"][:5]:
                        pnl = c.get("realized_pnl_pct")
                        pnl_str = f"{pnl:+.2%}" if isinstance(pnl, (int, float)) else "n/a"
                        st.write(f"- {c['plan_date']} → {c['exit_date']}  "
                                 f"{c['symbol']} {c['direction']} · realized {pnl_str}")

        if len(ai_reviews) > 1:
            st.divider()
            st.markdown(f"### Earlier reviews ({len(ai_reviews) - 1})")
            for r in ai_reviews[1:]:
                with st.expander(f"{r['review_date']} — `{r['model']}`"):
                    st.markdown(r["review_text"])

with tab_daily:
    st.subheader("Daily Oil Report")
    st.caption(
        "Two flavors per day: a quick **raw** report (headlines + inventory, "
        "auto-generated, no LLM) and a polished **prose** report in sell-side "
        "Chinese style (built via claude.ai paste-flow). The prose version "
        "displays in preference when it exists for the selected date."
    )

    from app.scoring.daily_report import (
        prepare_daily_report_prompt, save_llm_report, load_llm_report, llm_report_path,
    )

    digest_dir = BASE_DIR / "data" / "processed" / "digests"

    # --- Date picker + raw rebuild button ---
    available = sorted(
        list(digest_dir.glob("daily_news_*.md")) + list(digest_dir.glob("daily_llm_*.md")),
        reverse=True,
    ) if digest_dir.exists() else []
    available_dates = sorted({p.stem.split("_")[-1] for p in available}, reverse=True)
    if not available_dates:
        available_dates = [date.today().isoformat()]

    dc1, dc2 = st.columns([2, 1])
    with dc1:
        picked_str = st.selectbox(
            "Report date",
            available_dates,
            index=0,
            key="daily_report_pick",
        )
    with dc2:
        if st.button("🔄 Rebuild raw report for today", key="rebuild_daily_btn"):
            try:
                subprocess.run(
                    [sys.executable, str(BASE_DIR / "scripts" / "daily_news_report.py")],
                    check=True, cwd=str(BASE_DIR), capture_output=True, timeout=30,
                )
                st.success("Rebuilt — refresh to see the latest.")
                st.rerun()
            except subprocess.TimeoutExpired:
                st.error("Build timed out after 30s.")
            except subprocess.CalledProcessError as e:
                st.error(f"Build failed: {e.stderr.decode()[-300:]}")

    # --- Paste-flow for prose report ---
    with st.expander("📰 Generate prose report via claude.ai (recommended for presenting)", expanded=True):
        ml1, ml2 = st.columns([3, 1])
        with ml1:
            st.caption(
                "Builds a prompt with today's EIA inventory, prices, and top oil-relevant "
                "documents; Claude writes a sell-side-style Chinese numbered-prose report "
                "(同卖方周报示例). Saved as `daily_llm_<date>.md` — shows below in preference."
            )
        with ml2:
            st.link_button("Open claude.ai ↗", "https://claude.ai/new", use_container_width=True)

        with st.expander("📖 How to use this — step by step", expanded=False):
            st.markdown("""
1. **Pick the report date** above (defaults to today's selection).
2. Click **`📋 Build prose-report prompt`** below.
3. **Open claude.ai** (button top right), start a new chat. Sonnet 4.6 recommended.
4. **Paste the system prompt** first, blank line, then **paste the user prompt** under it. Submit.
5. Claude writes the daily report (in Chinese, sell-side style — see your example).
6. **Copy Claude's full reply** → paste into the response box below → click save.
7. The dashboard reloads and shows the prose report in place of the raw one.
""")

        rp_date = date.fromisoformat(picked_str) if picked_str else date.today()
        if st.button("📋 Build prose-report prompt", key="build_dr_prompt_btn"):
            st.session_state["_dr_prompt_payload"] = prepare_daily_report_prompt(rp_date)

        dr_payload = st.session_state.get("_dr_prompt_payload")
        if dr_payload and not dr_payload.get("ready"):
            st.warning(dr_payload.get("reason") or "Insufficient data for this date.")
        elif dr_payload:
            combined = (dr_payload.get("system") or "") + "\n\n---\n\n" + (dr_payload.get("user") or "")
            st.caption(
                f"📝 Prompt built — **{len(combined):,} chars** "
                f"(~{len(combined) // 4:,} tokens). Hover the top-right of the box to copy."
            )
            st.code(combined, language="text")

            pasted = st.text_area(
                "Paste Claude's response here",
                height=240,
                key="paste_daily_report_input",
                placeholder="Paste the Chinese daily report Claude wrote...",
            )
            if st.button("💾 Save prose report", type="primary",
                         disabled=not pasted.strip(), key="save_dr_btn"):
                p = save_llm_report(rp_date, pasted.strip())
                st.success(f"Saved to {p.relative_to(BASE_DIR)}")
                st.session_state.pop("_dr_prompt_payload", None)
                st.session_state["paste_daily_report_input"] = ""
                st.rerun()

    # --- Render: prefer LLM prose if exists, else raw ---
    st.divider()
    llm_text = load_llm_report(date.fromisoformat(picked_str))
    raw_path = digest_dir / f"daily_news_{picked_str}.md"
    if llm_text:
        st.caption(f"_Showing prose (LLM) report for {picked_str}._")
        st.markdown(llm_text)
        if raw_path.exists():
            with st.expander(f"Show raw (template) report for {picked_str} too"):
                st.markdown(raw_path.read_text(encoding="utf-8"))
    elif raw_path.exists():
        st.caption(f"_Showing raw (template) report for {picked_str}. No prose version yet — use the paste-flow above to generate one._")
        st.markdown(raw_path.read_text(encoding="utf-8"))
    else:
        st.info(f"No report for {picked_str} yet.")

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