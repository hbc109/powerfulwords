"""RV dashboard — self-contained Streamlit view for the relative-value module.

Imported by the main dashboard, which just adds one tab calling `render()`.
All RV UI lives here so the module stays independent. v1 = crude differentials:
the curves per spread (sources overlaid), the inter-broker dislocations, and a
raw quote table. Z-scores / opportunity ranking land here once history banks.
"""

from __future__ import annotations

import re

import pandas as pd
import streamlit as st

from app.db.database import get_connection
from app.rv import db as rvdb
from app.rv import dislocations as dx

_CRUDE_DIFFS = ["WTI-Brent", "Brent-Dubai(EFS)", "WTI-Dubai", "Dated-Dubai", "Brent DFL"]
_CRUDE_OUTRIGHTS = ["WTI", "Brent", "Dubai", "Dated Brent"]
_M_RE = re.compile(r"^M\d+$")

# Per desk: PVM is the source of truth for crude. SC quotes Dubai on a different
# forward convention (~$2.5 off), so for crude we lead with PVM and treat SC as
# overlay only. (SC becomes primary later for products / cracks.)
_CRUDE_PRIMARY = "PVM"


def _order_sources(pivot):
    """Put the crude primary source first so it leads the chart/legend."""
    ordered = [c for c in (_CRUDE_PRIMARY, "SC", "MITSUI") if c in pivot.columns]
    ordered += [c for c in pivot.columns if c not in ordered]
    return pivot[ordered] if ordered else pivot


def render() -> None:
    st.header("🛢️ Relative Value — crude differentials")
    st.caption("Broker spread sheets (SC, PVM) ingested daily → constant-maturity "
               "curves. Z-score rich/cheap and trade ideas activate as history banks.")

    conn = get_connection()
    rvdb.ensure_schema(conn)
    obs = rvdb.latest_obs_date(conn)
    if not obs:
        st.info("No RV quotes yet. Drop broker sheets in "
                "`data/inbox/sellside_private/sellside_manual_upload/` and run "
                "`python scripts/rv_ingest.py` (or the daily pipeline).")
        conn.close()
        return

    dates = [r[0] for r in conn.execute(
        "SELECT DISTINCT obs_date FROM rv_quotes ORDER BY obs_date DESC")]
    sel = st.selectbox("As of", dates, index=0)
    df = pd.read_sql_query(
        "SELECT source, category, spread, tenor, contract, value, unit "
        "FROM rv_quotes WHERE obs_date=?", conn, params=(sel,))
    hist_days = conn.execute("SELECT COUNT(DISTINCT obs_date) FROM rv_quotes").fetchone()[0]
    board = dx.build_board(conn, obs_date=sel)
    conn.close()

    if df.empty:
        st.info("No quotes for this date.")
        return

    srcs = sorted(df["source"].unique())
    st.caption(f"Sources: {', '.join(srcs)}  ·  history: {hist_days} day(s)")
    if hist_days < 10:
        st.info(f"📈 {hist_days} day(s) of history. Rich/cheap z-scores and the full "
                "trade-idea ranking need ~2–3 weeks of daily uploads — keep dropping "
                "the sheets. Curve-shape reads below are live now.")

    # --- dislocation board (the trade-idea layer) ---
    st.subheader("🎯 Dislocation board")
    st.caption("Crude-diff dislocations as {angle · evidence · idea}. Structure reads "
               f"are live; rich/cheap & clean-kink ranking sharpen with history ({hist_days}/8+ days).")
    angles = board.get("angles", {})
    for title in ("Curve kinks (butterflies)", "Inter-broker gaps", "Rich / cheap"):
        rows = angles.get(title) or []
        if not rows:
            note = (f"forming ({hist_days}/8 days)" if title == "Rich / cheap" else "none today")
            st.caption(f"• **{title}** — {note}")
            continue
        st.markdown(f"**{title}**")
        st.dataframe(pd.DataFrame([
            {"spread": r["spread"], "tenor": r["tenor"], "value": r["value"],
             "evidence": r["evidence"], "idea": r.get("direction", "")} for r in rows[:8]
        ]), hide_index=True, use_container_width=True)
    cs = angles.get("Curve shape") or []
    if cs:
        st.markdown("**Curve shape — structure / carry read (live)**")
        st.dataframe(pd.DataFrame([
            {"spread": r["spread"], "tenor": r["tenor"], "slope": r["value"], "read": r["evidence"]}
            for r in cs
        ]), hide_index=True, use_container_width=True)
    st.divider()

    mser = df[df["tenor"].map(lambda t: bool(_M_RE.match(str(t))))].copy()
    mser["m"] = mser["tenor"].str[1:].astype(int)

    # --- differential curves, primary source leads ---
    st.subheader("Crude differential curves (constant maturity)")
    st.caption(f"Crude reference: **{_CRUDE_PRIMARY}**. Other sources shown as overlay "
               "where they quote the same line (SC uses a different Dubai convention).")
    cols = st.columns(2)
    for i, sp in enumerate(_CRUDE_DIFFS):
        sub = mser[mser["spread"] == sp]
        if sub.empty:
            continue
        pivot = _order_sources(sub.pivot_table(index="m", columns="source", values="value"))
        pivot.index = [f"M{m}" for m in pivot.index]
        with cols[i % 2]:
            st.markdown(f"**{sp}**  _( {sub['unit'].iloc[0]} )_")
            st.line_chart(pivot, height=200)

    # --- outright curves ---
    with st.expander("Outright curves"):
        ocols = st.columns(2)
        for i, sp in enumerate(_CRUDE_OUTRIGHTS):
            sub = mser[mser["spread"] == sp]
            if sub.empty:
                continue
            pivot = _order_sources(sub.pivot_table(index="m", columns="source", values="value"))
            pivot.index = [f"M{m}" for m in pivot.index]
            with ocols[i % 2]:
                st.markdown(f"**{sp}**")
                st.line_chart(pivot, height=180)

    # --- raw table ---
    with st.expander("All quotes (raw)"):
        show = df.sort_values(["category", "spread", "source", "tenor"])
        st.dataframe(show, hide_index=True, use_container_width=True)
