from __future__ import annotations
import json, sqlite3
from pathlib import Path
import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "oil_narrative.db"

@st.cache_data
def load_table(query: str, params: tuple = ()) -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

def latest_scores_df() -> pd.DataFrame:
    return load_table("""
        SELECT score_date, commodity, topic, narrative_score,
               official_confirmation_score, news_breadth_score,
               chatter_score, crowding_score
        FROM daily_narrative_scores
        ORDER BY score_date DESC, ABS(narrative_score) DESC
    """)

def events_df() -> pd.DataFrame:
    return load_table("""
        SELECT event_time, topic, direction, source_bucket, source_name,
               credibility, novelty, verification_status, horizon,
               rumor_flag, confidence, evidence_text, document_id, chunk_id
        FROM narrative_events
        ORDER BY event_time DESC
    """)

def prices_df(symbol: str) -> pd.DataFrame:
    return load_table("""
        SELECT price_time, symbol, close
        FROM market_prices
        WHERE symbol = ?
        ORDER BY price_time
    """, (symbol,))

def latest_research_summary() -> dict | None:
    research_dir = BASE_DIR / "data" / "processed" / "research"
    files = sorted(research_dir.glob("event_study_*.json")) if research_dir.exists() else []
    return json.loads(files[-1].read_text(encoding="utf-8")) if files else None

@st.cache_data
def latest_backtest_summary() -> dict | None:
    out_dir = BASE_DIR / "data" / "processed" / "backtests"
    files = sorted(out_dir.glob("backtest_*.json")) if out_dir.exists() else []
    return json.loads(files[-1].read_text(encoding="utf-8")) if files else None

def render_kpis(score_df: pd.DataFrame, event_df: pd.DataFrame):
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Score rows", len(score_df))
    c2.metric("Events", len(event_df))
    if not score_df.empty:
        c3.metric("Latest score date", str(score_df.iloc[0]["score_date"]))
        c4.metric("Max |score|", round(score_df["narrative_score"].abs().max(), 4))
    else:
        c3.metric("Latest score date", "-")
        c4.metric("Max |score|", "-")

def render_score_section(score_df: pd.DataFrame):
    st.subheader("Latest topic scores")
    if score_df.empty:
        st.info("No scores found. Run score_narratives.py first.")
        return
    available_dates = sorted(score_df["score_date"].astype(str).unique(), reverse=True)
    selected_date = st.selectbox("Score date", available_dates, index=0)
    day_df = score_df[score_df["score_date"].astype(str) == selected_date].copy().sort_values("narrative_score", ascending=False)
    st.dataframe(day_df, use_container_width=True)
    st.bar_chart(day_df[["topic", "narrative_score"]].set_index("topic"))

def render_event_section(event_df: pd.DataFrame):
    st.subheader("Narrative events")
    if event_df.empty:
        st.info("No narrative events found. Run extract_narratives.py first.")
        return
    topics = ["ALL"] + sorted(event_df["topic"].dropna().unique().tolist())
    buckets = ["ALL"] + sorted(event_df["source_bucket"].dropna().unique().tolist())
    c1, c2 = st.columns(2)
    selected_topic = c1.selectbox("Filter topic", topics, index=0)
    selected_bucket = c2.selectbox("Filter source bucket", buckets, index=0)
    filtered = event_df.copy()
    if selected_topic != "ALL":
        filtered = filtered[filtered["topic"] == selected_topic]
    if selected_bucket != "ALL":
        filtered = filtered[filtered["source_bucket"] == selected_bucket]
    st.dataframe(filtered, use_container_width=True)
    if not filtered.empty:
        st.bar_chart(filtered["topic"].value_counts().rename_axis("topic").to_frame("count"))

def render_price_section():
    st.subheader("Price series")
    symbol = st.selectbox("Symbol", ["WTI", "Brent", "XLE"], index=0)
    pdf = prices_df(symbol)
    if pdf.empty:
        st.info("No market prices found. Run load_prices_csv.py first.")
        return
    pdf["price_time"] = pd.to_datetime(pdf["price_time"])
    st.line_chart(pdf.set_index("price_time")[["close"]])

def render_research_section():
    st.subheader("Latest research summary")
    payload = latest_research_summary()
    if not payload:
        st.info("No event study result found. Run run_event_study.py first.")
        return
    st.write(f"Sample size: {payload.get('sample_size')}")
    if payload.get("bucket_summary"):
        st.json(payload["bucket_summary"])
    if payload.get("topic_summary"):
        st.json(payload["topic_summary"])

def render_backtest_section():
    st.subheader("Latest backtest summary")
    payload = latest_backtest_summary()
    if not payload:
        st.info("No backtest result found. Run run_backtest.py first.")
        return
    st.json(payload.get("summary", {}))
    eq = payload.get("equity_curve", [])
    if eq:
        df = pd.DataFrame(eq)
        df["date"] = pd.to_datetime(df["date"])
        st.line_chart(df.set_index("date")[["equity"]])
    trades = payload.get("trades", [])
    if trades:
        st.dataframe(pd.DataFrame(trades), use_container_width=True)

def main():
    st.set_page_config(page_title="Oil Narrative Dashboard", layout="wide")
    st.title("Oil Narrative Dashboard")
    if not DB_PATH.exists():
        st.error(f"Database not found: {DB_PATH}")
        return
    score_df = latest_scores_df()
    event_df = events_df()
    render_kpis(score_df, event_df)
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Scores", "Events", "Prices", "Research", "Backtest"])
    with tab1:
        render_score_section(score_df)
    with tab2:
        render_event_section(event_df)
    with tab3:
        render_price_section()
    with tab4:
        render_research_section()
    with tab5:
        render_backtest_section()

if __name__ == "__main__":
    main()
