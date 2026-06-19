"""Crude-diff dislocation detectors — the RV "trade ideas" layer.

Turns the raw differential curves into a ranked list of dislocations, each as
{angle, what, evidence, direction}. v1 covers the angles that work on a single
daily snapshot (no history needed):

  - butterflies  : a kink in a curve — one maturity rich/cheap vs its neighbours
  - inter_broker : the same spread quoted differently across brokers (stale mark)
  - curve_shape  : term structure of each diff (context + carry read)
  - provisional z: rich/cheap vs the (short) history we have so far — sharpens
                   toward a real seasonal z-score as daily uploads bank.

PVM is the crude reference (per desk); SC/Mitsui are overlay/cross-check.
"""

from __future__ import annotations

CRUDE_DIFFS = ["WTI-Brent", "Brent-Dubai(EFS)", "WTI-Dubai", "Dated-Dubai", "Brent DFL"]
CRUDE_OUTRIGHTS = ["WTI", "Brent", "Dubai"]
PRIMARY = "PVM"


def latest_obs_date(conn) -> str | None:
    r = conn.execute("SELECT MAX(obs_date) FROM rv_quotes").fetchone()
    return r[0] if r else None


def _m_curve(conn, obs_date, source, spread) -> dict[int, float]:
    """{m_index: value} for the M-tenors of one spread on one day/source."""
    rows = conn.execute(
        "SELECT tenor, value FROM rv_quotes WHERE obs_date=? AND source=? AND spread=? "
        "AND tenor LIKE 'M%'", (obs_date, source, spread)).fetchall()
    out = {}
    for t, v in rows:
        if v is not None and t[1:].isdigit():
            out[int(t[1:])] = v
    return out


# --- angle: butterflies (curve kinks) ---------------------------------------

def butterflies(conn, obs_date, source=PRIMARY, min_abs=0.30, min_n=3) -> list[dict]:
    # Skip the front (M1/M2): on a steeply-backwardated curve the front fly is
    # natural curvature, not a kink. Deferred flies above the threshold are more
    # likely a genuine local dislocation. (Real kink-vs-curvature separation
    # wants a history baseline — coming as data banks.)
    out = []
    for spread in CRUDE_DIFFS + CRUDE_OUTRIGHTS:
        d = _m_curve(conn, obs_date, source, spread)
        for n in sorted(d):
            if n < min_n:
                continue
            if (n - 1) in d and (n + 1) in d:
                fly = d[n - 1] - 2 * d[n] + d[n + 1]
                if abs(fly) >= min_abs:
                    rich = fly < 0  # body above the wings' midpoint => body rich
                    out.append({
                        "angle": "butterfly", "spread": spread, "source": source,
                        "tenor": f"M{n}", "value": round(fly, 3),
                        "evidence": f"M{n} {'rich' if rich else 'cheap'} vs M{n-1}/M{n+1} fly {fly:+.2f}",
                        "direction": f"{'sell' if rich else 'buy'} M{n} fly ({spread})",
                        "severity": abs(fly),
                    })
    return out


# --- angle: inter-broker gaps (contract-aligned) -----------------------------

# Inter-broker is only meaningful on a tradeable SPREAD with a shared convention.
# Outright mark gaps are just snapshot-timing noise (not RV), and Dubai-leg
# spreads carry the known convention offset (resolved: use PVM). So: WTI-Brent.
_INTERBROKER_SPREADS = ["WTI-Brent"]


def inter_broker(conn, obs_date, min_abs=0.20) -> list[dict]:
    """Same spread + same contract month quoted by >1 source — gap = stale mark
    or a genuine dislocation. Aligned by `contract`. Restricted to spreads where
    the brokers share a quoting convention (no Dubai leg)."""
    out = []
    for spread in _INTERBROKER_SPREADS:
        rows = conn.execute(
            "SELECT source, contract, value FROM rv_quotes "
            "WHERE obs_date=? AND spread=? AND value IS NOT NULL", (obs_date, spread)).fetchall()
        by_contract: dict[str, dict[str, float]] = {}
        for src, contract, val in rows:
            if contract:
                by_contract.setdefault(contract, {})[src] = val
        for contract, srcs in by_contract.items():
            if PRIMARY in srcs and len(srcs) > 1:
                ref = srcs[PRIMARY]
                for other, v in srcs.items():
                    if other == PRIMARY:
                        continue
                    gap = ref - v
                    if abs(gap) >= min_abs:
                        out.append({
                            "angle": "inter-broker", "spread": spread, "source": f"{PRIMARY} vs {other}",
                            "tenor": contract, "value": round(gap, 3),
                            "evidence": f"{PRIMARY} {ref:.2f} vs {other} {v:.2f} ({gap:+.2f})",
                            "direction": f"{other} mark likely stale/reverts toward {PRIMARY}",
                            "severity": abs(gap),
                        })
    return out


# --- angle: curve shape (context) -------------------------------------------

def curve_shape(conn, obs_date, source=PRIMARY) -> list[dict]:
    out = []
    for spread in CRUDE_DIFFS:
        d = _m_curve(conn, obs_date, source, spread)
        if len(d) < 2:
            continue
        ms = sorted(d)
        front, back = d[ms[0]], d[ms[-1]]
        slope = back - front
        shape = "widening" if slope > 0 else ("narrowing" if slope < 0 else "flat")
        out.append({
            "angle": "curve-shape", "spread": spread, "source": source,
            "tenor": f"M{ms[0]}-M{ms[-1]}", "value": round(slope, 3),
            "evidence": f"front {front:+.2f} -> back {back:+.2f} ({shape} {slope:+.2f})",
            "direction": "context",
            "severity": abs(slope),
        })
    return out


# --- angle: provisional z (rich/cheap vs short history) ----------------------

def history_days(conn, source=PRIMARY) -> int:
    return conn.execute("SELECT COUNT(DISTINCT obs_date) FROM rv_quotes WHERE source=?",
                        (source,)).fetchone()[0]


def provisional_z(conn, obs_date, source=PRIMARY, tenor="M1", min_history=8) -> list[dict]:
    """z of today's value vs prior obs_dates for the same spread/tenor. Needs a
    real sample — a 3-day std is ~0 and makes z explode, so we gate on
    `min_history` and stay silent (showing 'forming') until then."""
    out = []
    for spread in CRUDE_DIFFS:
        rows = conn.execute(
            "SELECT obs_date, value FROM rv_quotes WHERE source=? AND spread=? AND tenor=? "
            "AND value IS NOT NULL ORDER BY obs_date", (source, spread, tenor)).fetchall()
        vals = [v for _, v in rows]
        if len(vals) < min_history:
            continue
        today = vals[-1]
        hist = vals[:-1]
        mean = sum(hist) / len(hist)
        var = sum((x - mean) ** 2 for x in hist) / len(hist)
        std = var ** 0.5
        if std < 1e-6:
            continue
        z = (today - mean) / std
        if abs(z) >= 1.0:
            out.append({
                "angle": "rich/cheap (provisional)", "spread": spread, "source": source,
                "tenor": tenor, "value": round(z, 2),
                "evidence": f"{today:+.2f} vs {mean:+.2f}±{std:.2f} ({len(hist)}d) z={z:+.1f}",
                "direction": f"{'fade rich: sell' if z > 0 else 'fade cheap: buy'} {spread} {tenor}",
                "severity": abs(z),
            })
    return out


def build_board(conn, obs_date=None) -> dict:
    obs_date = obs_date or latest_obs_date(conn)
    if not obs_date:
        return {"obs_date": None, "angles": {}}
    return {
        "obs_date": obs_date,
        "angles": {
            "Curve kinks (butterflies)": sorted(butterflies(conn, obs_date),
                                                key=lambda r: -r["severity"]),
            "Inter-broker gaps": sorted(inter_broker(conn, obs_date),
                                        key=lambda r: -r["severity"]),
            "Rich / cheap": sorted(provisional_z(conn, obs_date),
                                   key=lambda r: -r["severity"]),
            "Curve shape": sorted(curve_shape(conn, obs_date),
                                  key=lambda r: -r["severity"]),
        },
    }
