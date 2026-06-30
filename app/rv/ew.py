"""East-West (Brent/Dubai) RV model — dedicated dashboard tab.

The Brent/Dubai complex is ONE East-West spread, expressed at three points on
the Brent ladder (all share the same Dubai leg):

  - EFS              ICE Brent **futures**[contract month] − Dubai swap[same month].
                     Contract-month indexed; the Brent contract is cal **+2** (it
                     expires the last business day of the 2nd month prior). The
                     prompt = the front *live* Brent contract.
  - swap-swap (BOD)  Brent 1st-line **swap** − Dubai swap, calendar-month indexed.
  - Dated-Dubai(PDD) Dated Brent − Dubai (physical / cash EW).

They differ only on the Brent side:
  Dated-Dubai = swap-swap + DFL            (DFL = Dated − front Brent; N.Sea physical)
  EFS         = swap-swap + (EFP + the contract-vs-calendar/Dubai-time-spread shift)

This v1 tab shows the three cleaned curves + that decomposition. Data comes from
`rv_quotes` (PVM), so it reuses the existing RV ingest/parser untouched; the
contract-month cleaning (dropping expired-Brent rows like the stale prompt) is
applied here at read time.
"""

from __future__ import annotations

import calendar
import re
from datetime import date

import pandas as pd
import streamlit as st

from app.db.database import get_connection
from app.rv import db as rvdb

_MON = {m.upper(): i for i, m in enumerate(calendar.month_abbr) if m}  # JAN->1 ...
_CONTRACT_RE = re.compile(r"([A-Za-z]{3})[-']?(\d{2})$")  # "Jun-26", "AUG'26", "MAR'27"

# Spreads/outrights we pull from rv_quotes, mapped to our field names.
_WANT = {
    "Brent-Dubai(EFS)": "efs",
    "Dated-Dubai": "dated_dubai",
    "Brent DFL": "dfl",
    "Brent": "brent",   # swap outright (parser uses the swap column)
    "Dubai": "dubai",   # swap outright
}


def _parse_contract(s) -> tuple[int, int] | None:
    """('Jun-26' | "AUG'26" | "MAR'27") -> (year, month), else None."""
    m = _CONTRACT_RE.match(str(s).strip())
    if not m:
        return None
    mon = _MON.get(m.group(1).upper())
    if not mon:
        return None
    return (2000 + int(m.group(2)), mon)


def _brent_expiry(ym: tuple[int, int]) -> date:
    """ICE Brent contract expires the last business day of the 2nd month prior.
    We approximate with the last *calendar* day of that month (a daily live/dead
    filter doesn't need business-day precision)."""
    yr, mon = ym
    em, ey = mon - 2, yr
    if em <= 0:
        em += 12
        ey -= 1
    return date(ey, em, calendar.monthrange(ey, em)[1])


def _label(ym: tuple[int, int]) -> str:
    return f"{calendar.month_abbr[ym[1]]}-{ym[0] % 100:02d}"


def load_ew(conn, obs_date: str) -> pd.DataFrame | None:
    """Assemble the EW curve for one obs_date, indexed by Brent contract month.

    Returns a DataFrame ordered by contract with columns: year, month, label,
    live, efs, dated_dubai, dfl, brent, dubai, swapswap, dubai_ts. `live` flags
    contracts whose Brent leg has not yet expired as of obs_date (the prompt is
    the first live row; expired rows carry the stale roll print).
    """
    df = pd.read_sql_query(
        "SELECT spread, contract, value FROM rv_quotes "
        "WHERE source='PVM' AND obs_date=?",
        conn, params=(obs_date,),
    )
    if df.empty:
        return None
    obs = date.fromisoformat(obs_date)
    rec: dict[tuple[int, int], dict] = {}
    for _, row in df.iterrows():
        field = _WANT.get(row["spread"])
        if not field:
            continue
        ym = _parse_contract(row["contract"])
        if not ym:
            continue
        rec.setdefault(ym, {})[field] = row["value"]
    if not rec:
        return None

    rows = []
    for ym in rec:
        rows.append({"year": ym[0], "month": ym[1], "label": _label(ym),
                     "live": _brent_expiry(ym) >= obs, **rec[ym]})
    cur = (pd.DataFrame(rows)
           .sort_values(["year", "month"])
           .reset_index(drop=True))
    for col in ("efs", "dated_dubai", "dfl", "brent", "dubai"):
        if col not in cur:
            cur[col] = pd.NA
    cur["swapswap"] = cur["brent"] - cur["dubai"]           # BOD ≈ Brent swap − Dubai
    cur["dubai_ts"] = cur["dubai"] - cur["dubai"].shift(-1)  # Dubai M − M+1 time spread
    return cur


def _prompt_delta(cur: pd.DataFrame, prev: pd.DataFrame | None, ym, field):
    """Δ of `field` at contract `ym` vs the same contract on the prior date."""
    if prev is None:
        return None
    match = prev[(prev["year"] == ym[0]) & (prev["month"] == ym[1])]
    if match.empty or pd.isna(match.iloc[0].get(field)):
        return None
    now = cur[(cur["year"] == ym[0]) & (cur["month"] == ym[1])].iloc[0].get(field)
    if pd.isna(now):
        return None
    return float(now) - float(match.iloc[0][field])


def render() -> None:
    st.header("🌍 East-West — Brent / Dubai")
    st.caption(
        "One East-West spread at three points on the Brent ladder: **EFS** "
        "(Brent futures vs Dubai, contract-month indexed, cal +2), **swap-swap** "
        "(Brent 1st-line swap vs Dubai, calendar-month), and **Dated-Dubai** "
        "(physical). They differ only on the Brent leg — DFL and the EFP/Dubai-"
        "time-spread basis."
    )

    conn = get_connection()
    rvdb.ensure_schema(conn)
    obs = rvdb.latest_obs_date(conn)
    if not obs:
        st.info("No RV quotes yet. Drop PVM sheets in the manual-upload inbox and "
                "run `python scripts/rv_ingest.py`.")
        conn.close()
        return

    dates = [r[0] for r in conn.execute(
        "SELECT DISTINCT obs_date FROM rv_quotes ORDER BY obs_date DESC")]
    sel = st.selectbox("As of", dates, index=0, key="ew_as_of")
    cur = load_ew(conn, sel)
    prev = None
    if cur is not None:
        idx = dates.index(sel)
        if idx + 1 < len(dates):
            prev = load_ew(conn, dates[idx + 1])
    hist_days = len(dates)
    conn.close()

    if cur is None or cur.empty:
        st.info("No PVM crude quotes for this date.")
        return

    live = cur[cur["live"]].reset_index(drop=True)
    dropped = cur[~cur["live"]]["label"].tolist()
    if live.empty:
        st.warning("Every contract row reads as expired for this date — check the sheet.")
        return

    if dropped:
        st.caption(f"🧹 Dropped {len(dropped)} expired-Brent-contract row(s) "
                   f"(stale roll prints): {', '.join(dropped)}. Prompt = "
                   f"**{live.iloc[0]['label']}** (front live Brent contract).")
    if hist_days < 15:
        st.info(f"📈 {hist_days} day(s) of history. Levels / curve shape / daily change "
                "are live; rich-cheap z-scores need ~3 weeks of uploads.")

    # --- Prompt panel ---
    p = live.iloc[0]
    pym = (int(p["year"]), int(p["month"]))
    st.subheader(f"Prompt ({p['label']})")
    c1, c2, c3 = st.columns(3)
    for col, fld, name in ((c1, "efs", "EFS"), (c2, "swapswap", "Swap-swap"),
                           (c3, "dated_dubai", "Dated-Dubai")):
        val = p.get(fld)
        d = _prompt_delta(live, prev[prev["live"]] if prev is not None else None, pym, fld)
        with col:
            st.metric(f"{name}  ($/bbl)",
                      f"{float(val):+.2f}" if pd.notna(val) else "—",
                      f"{d:+.2f} d/d" if d is not None else None)

    # --- Three EW curves overlaid (live strip) ---
    st.subheader("East-West curves")
    curve = live.set_index("label")[["efs", "swapswap", "dated_dubai"]].rename(
        columns={"efs": "EFS", "swapswap": "Swap-swap", "dated_dubai": "Dated-Dubai"})
    st.line_chart(curve.dropna(how="all"), height=260)
    st.caption("All three are East-West (Brent rich vs Dubai). Backwardated = prompt "
               "premium eases out the curve. Gaps between the lines are the Brent-side basis →")

    # --- Decomposition: the two legs that separate the three curves ---
    st.subheader("Basis decomposition")
    d1, d2 = st.columns(2)
    with d1:
        st.markdown("**DFL — Dated vs front Brent** _(separates Dated-Dubai from swap-swap)_")
        st.line_chart(live.set_index("label")[["dfl"]].rename(columns={"dfl": "DFL"}),
                      height=200)
        st.caption("North Sea physical. DFL ↑ → Dated-Dubai richer than the swap-swap. "
                   "Check: Dated-Dubai − swap-swap ≈ DFL.")
    with d2:
        st.markdown("**Dubai time-spread (M−M+1)** _(behind EFS vs swap-swap)_")
        st.line_chart(live.set_index("label")[["dubai_ts"]].rename(
            columns={"dubai_ts": "Dubai M−M+1"}), height=200)
        st.caption("EFS indexes the Dubai of the Brent *contract* month (cal +2), so the "
                   "EFS-vs-swap-swap gap rides the Dubai curve shape.")

    # --- Drivers: arb-economics gate (v1, manual freight/quality) ---
    st.subheader("Drivers — arb-economics gate (West→East)")
    st.caption(
        "The EW is the gate for moving Atlantic (Brent-linked) crude East. Arb is "
        "**open** when delivering to Asia beats the local Dubai-linked barrel: "
        "`Brent + freight ≤ Dubai + quality`  ⟺  `EW ≤ gate`, where "
        "**gate = quality − freight**. Freight/quality are your inputs for now — a "
        "VLCC freight fetcher is the next data task."
    )
    g1, g2, g3 = st.columns(3)
    with g1:
        freight = st.number_input("W→E freight ($/bbl)", min_value=0.0, value=3.50,
                                  step=0.25, key="ew_freight",
                                  help="Net VLCC cost to land a Brent-linked barrel in Asia.")
    with g2:
        quality = st.number_input("Asian sweet quality premium ($/bbl)", value=1.00,
                                  step=0.25, key="ew_quality",
                                  help="Value uplift of the lighter/sweeter Atlantic grade vs the "
                                       "Dubai-linked alternative in Asia.")
    with g3:
        basis = st.selectbox("EW basis", ["Dated-Dubai", "EFS", "Swap-swap"], index=0,
                             key="ew_gate_basis",
                             help="Physical flows arb off Dated-Dubai; EFS/swap-swap are the financial EWs.")
    gate = quality - freight
    fld = {"Dated-Dubai": "dated_dubai", "EFS": "efs", "Swap-swap": "swapswap"}[basis]
    ew_now = float(p.get(fld)) if pd.notna(p.get(fld)) else None
    m1, m2, m3 = st.columns(3)
    m1.metric("Arb gate (fair-value EW)", f"{gate:+.2f}")
    m2.metric(f"Live EW — {basis} ({p['label']})",
              f"{ew_now:+.2f}" if ew_now is not None else "—")
    if ew_now is not None:
        resid = ew_now - gate
        m3.metric("Residual (EW − gate)", f"{resid:+.2f}")
        if resid > 0.25:
            st.warning(f"EW **${resid:.2f} above** the gate → W→E arb **shut**, Atlantic stays "
                       "West. EW rich vs flow economics → mean-reversion pressure **down**, "
                       "unless the Atlantic is structurally tight (check DFL above).")
        elif resid < -0.25:
            st.success(f"EW **${abs(resid):.2f} below** the gate → arb **open**, barrels move "
                       "East → firms Dubai / lifts the West price → pressure **up** on EW.")
        else:
            st.info(f"EW within ${abs(resid):.2f} of the gate → roughly balanced (marginal arb).")

    # --- Reconciliation + raw ---
    with st.expander("Reconciliation & raw curve"):
        chk = live.assign(
            **{"Dated-Dubai − swap-swap": (live["dated_dubai"] - live["swapswap"]).round(2),
               "DFL": live["dfl"].round(2),
               "EFS − swap-swap": (live["efs"] - live["swapswap"]).round(2)})
        show = chk[["label", "efs", "swapswap", "dated_dubai", "dfl",
                    "Dated-Dubai − swap-swap", "EFS − swap-swap", "dubai_ts"]].round(2)
        show.columns = ["contract", "EFS", "swap-swap", "Dated-Dubai", "DFL",
                        "DtdDub−SS (≈DFL)", "EFS−SS", "Dubai M−M+1"]
        st.dataframe(show, hide_index=True, use_container_width=True)
