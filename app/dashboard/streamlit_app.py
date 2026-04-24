import json
import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "oil_narrative.db"
STRATEGY_CFG_PATH = BASE_DIR / "app" / "config" / "strategy_config.json"


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
        SELECT score_date, commodity, topic, narrative_score,
               official_confirmation_score, news_breadth_score,
               chatter_score, crowding_score
        FROM daily_narrative_scores
        ORDER BY score_date DESC, ABS(narrative_score) DESC
    """)


def load_events() -> pd.DataFrame:
    return load_df("""
        SELECT event_time, topic, direction, source_bucket, source_name,
               credibility, novelty, verification_status, horizon,
               rumor_flag, confidence, evidence_text
        FROM narrative_events
        ORDER BY event_time DESC
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


st.set_page_config(page_title="Oil Narrative Dashboard", layout="wide")
st.title("Oil Narrative Dashboard")

if not DB_PATH.exists():
    st.error(f"Database not found: {DB_PATH}")
    st.stop()

scores = load_scores()
events = load_events()

if scores.empty:
    st.warning("No scores found in database.")
    st.stop()

scores["score_date"] = scores["score_date"].astype(str)
if not events.empty:
    events["event_date"] = events["event_time"].astype(str).str[:10]
else:
    events["event_date"] = ""

available_dates = sorted(scores["score_date"].unique(), reverse=True)
selected_date = st.selectbox("Select date", available_dates, index=0)

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

tab1, tab2, tab3 = st.tabs(["Overview", "Research", "Backtest"])

with tab1:
    st.subheader("Scores")
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