from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "oil_narrative.db"


@st.cache_data(ttl=10)
def load_df(query: str, params: tuple = ()) -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


def load_scores() -> pd.DataFrame:
    return load_df(
        '''
        SELECT score_date, commodity, topic, narrative_score,
               official_confirmation_score, news_breadth_score,
               chatter_score, crowding_score
        FROM daily_narrative_scores
        ORDER BY score_date DESC, ABS(narrative_score) DESC
        '''
    )


def load_events() -> pd.DataFrame:
    return load_df(
        '''
        SELECT event_time, topic, direction, source_bucket, source_name,
               credibility, novelty, verification_status, horizon,
               rumor_flag, confidence, evidence_text, notes
        FROM narrative_events
        ORDER BY event_time DESC
        '''
    )


def load_research_payload() -> dict | None:
    research_dir = BASE_DIR / "data" / "processed" / "research"
    if not research_dir.exists():
        return None
    files = sorted(research_dir.glob("event_study_*.json"))
    if not files:
        return None
    return json.loads(files[-1].read_text(encoding="utf-8"))


def load_backtest_payload() -> dict | None:
    backtest_dir = BASE_DIR / "data" / "processed" / "backtests"
    if not backtest_dir.exists():
        return None
    files = sorted(backtest_dir.glob("backtest_*.json"))
    if not files:
        return None
    return json.loads(files[-1].read_text(encoding="utf-8"))


def safe_topic_label(topic: str) -> str:
    return str(topic).replace("_", " ").title()


def topic_direction(score: float) -> str:
    if score >= 0.6:
        return "Strong Bullish"
    if score > 0:
        return "Bullish"
    if score <= -0.6:
        return "Strong Bearish"
    if score < 0:
        return "Bearish"
    return "Neutral"


def build_summary(score_df: pd.DataFrame, event_df: pd.DataFrame) -> dict:
    if score_df.empty:
        return {
            "latest_date": "-",
            "primary_narrative": "-",
            "secondary_narrative": "-",
            "market_bias": "-",
            "confidence": "-",
            "main_sources": "-",
        }

    latest_date = str(score_df["score_date"].astype(str).max())
    latest_scores = score_df[score_df["score_date"].astype(str) == latest_date].copy()
    latest_scores = latest_scores.sort_values("narrative_score", ascending=False)

    primary = latest_scores.iloc[0]["topic"] if not latest_scores.empty else "-"
    secondary = latest_scores.iloc[1]["topic"] if len(latest_scores) > 1 else "-"

    total_score = latest_scores["narrative_score"].sum()
    bias = topic_direction(total_score)

    latest_events = event_df[event_df["event_time"].astype(str).str.startswith(latest_date)].copy()
    if latest_events.empty:
        confidence = "Low"
        sources = "-"
    else:
        avg_conf = latest_events["confidence"].fillna(0).mean()
        if avg_conf >= 0.75:
            confidence = "High"
        elif avg_conf >= 0.5:
            confidence = "Medium"
        else:
            confidence = "Low"
        top_sources = latest_events["source_name"].dropna().value_counts().head(3).index.tolist()
        sources = ", ".join(top_sources) if top_sources else "-"

    return {
        "latest_date": latest_date,
        "primary_narrative": safe_topic_label(primary),
        "secondary_narrative": safe_topic_label(secondary),
        "market_bias": bias,
        "confidence": confidence,
        "main_sources": sources,
    }


def render_overview(score_df: pd.DataFrame, event_df: pd.DataFrame):
    summary = build_summary(score_df, event_df)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Latest Date", summary["latest_date"])
    c2.metric("Primary Narrative", summary["primary_narrative"])
    c3.metric("Market Bias", summary["market_bias"])
    c4.metric("Confidence", summary["confidence"])

    st.markdown("### Main Narrative Summary")
    st.info(
        f"""
**Primary Narrative:** {summary["primary_narrative"]}

**Secondary Narrative:** {summary["secondary_narrative"]}

**Main Sources:** {summary["main_sources"]}
"""
    )


def render_topic_scores(score_df: pd.DataFrame):
    st.markdown("### Topic Scores")

    if score_df.empty:
        st.warning("No daily scores found.")
        return

    dates = sorted(score_df["score_date"].astype(str).unique(), reverse=True)
    selected_date = st.selectbox("Select score date", dates, index=0, key="score_date_select")

    day_df = score_df[score_df["score_date"].astype(str) == selected_date].copy()
    day_df["topic_label"] = day_df["topic"].apply(safe_topic_label)
    day_df["bias"] = day_df["narrative_score"].apply(topic_direction)
    day_df = day_df.sort_values("narrative_score", ascending=False)

    st.dataframe(
        day_df[[
            "topic_label", "narrative_score", "bias",
            "official_confirmation_score", "news_breadth_score",
            "chatter_score", "crowding_score"
        ]],
        width="stretch",
        hide_index=True,
    )

    chart_df = day_df[["topic_label", "narrative_score"]].set_index("topic_label")
    st.bar_chart(chart_df)


def render_event_feed(event_df: pd.DataFrame):
    st.markdown("### Recent Evidence / Event Feed")

    if event_df.empty:
        st.warning("No narrative events found.")
        return

    event_df = event_df.copy()
    event_df["event_date"] = event_df["event_time"].astype(str).str[:10]
    dates = sorted(event_df["event_date"].unique(), reverse=True)
    selected_date = st.selectbox("Select event date", dates, index=0, key="event_date_select")

    topic_options = ["ALL"] + sorted(event_df["topic"].dropna().unique().tolist())
    selected_topic = st.selectbox("Filter topic", topic_options, index=0)

    filtered = event_df[event_df["event_date"] == selected_date].copy()
    if selected_topic != "ALL":
        filtered = filtered[filtered["topic"] == selected_topic]

    if filtered.empty:
        st.info("No events for the selected filters.")
        return

    pretty = filtered.copy()
    pretty["topic"] = pretty["topic"].apply(safe_topic_label)
    pretty["preview"] = pretty["evidence_text"].fillna("").astype(str).str.slice(0, 220)

    st.dataframe(
        pretty[[
            "event_time", "topic", "direction", "source_bucket", "source_name",
            "verification_status", "confidence", "preview"
        ]],
        width="stretch",
        hide_index=True,
    )

    st.markdown("#### Detailed Evidence")
    for _, row in pretty.head(8).iterrows():
        with st.expander(
            f'{row["event_time"]} | {row["topic"]} | {row["direction"]} | {row["source_name"]}',
            expanded=False
        ):
            st.write(row["evidence_text"])


def render_research_snapshot():
    st.markdown("### Research Snapshot")
    payload = load_research_payload()
    if not payload:
        st.info("No event study result found yet.")
        return

    bucket_summary = payload.get("bucket_summary", {})
    if bucket_summary:
        rows = []
        for bucket, stats in bucket_summary.items():
            rows.append({
                "bucket": bucket,
                "count": stats.get("count"),
                "avg_fwd_ret_1d": stats.get("avg_fwd_ret_1d"),
                "avg_fwd_ret_3d": stats.get("avg_fwd_ret_3d"),
                "hit_rate_1d": stats.get("hit_rate_1d"),
                "hit_rate_3d": stats.get("hit_rate_3d"),
            })
        st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
    else:
        st.info("Research output exists, but bucket summary is empty.")


def render_backtest_snapshot():
    st.markdown("### Backtest Snapshot")
    payload = load_backtest_payload()
    if not payload:
        st.info("No backtest result found yet.")
        return

    summary = payload.get("summary", {})
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Initial Capital", summary.get("initial_capital", "-"))
    c2.metric("Final Equity", summary.get("final_equity", "-"))
    c3.metric("Total Return", summary.get("total_return", "-"))
    c4.metric("Trades", summary.get("num_trades", "-"))

    equity_curve = payload.get("equity_curve", [])
    if equity_curve:
        eq_df = pd.DataFrame(equity_curve)
        eq_df["date"] = pd.to_datetime(eq_df["date"])
        st.line_chart(eq_df.set_index("date")[["equity"]])

    trades = payload.get("trades", [])
    if trades:
        st.dataframe(pd.DataFrame(trades).tail(10), width="stretch", hide_index=True)


st.set_page_config(page_title="Oil Narrative Dashboard", layout="wide")
st.title("Oil Narrative Dashboard")

if not DB_PATH.exists():
    st.error(f"Database not found: {DB_PATH}")
    st.stop()

score_df = load_scores()
event_df = load_events()

render_overview(score_df, event_df)

tab1, tab2, tab3 = st.tabs(["Overview", "Research", "Backtest"])

with tab1:
    render_topic_scores(score_df)
    render_event_feed(event_df)

with tab2:
    render_research_snapshot()

with tab3:
    render_backtest_snapshot()
