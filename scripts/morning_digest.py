"""Generate a morning digest of today's recommendations.

Output: data/processed/digests/morning_<YYYY-MM-DD>.md

Optional email delivery: if ANY of SMTP_HOST is set in the environment,
the digest is emailed using stdlib smtplib. Required env vars for email:

  SMTP_HOST, SMTP_PORT (default 587), SMTP_USER, SMTP_PASS,
  SMTP_FROM, SMTP_TO  (comma-separated for multiple recipients)
  SMTP_SSL=1          (optional, use SMTPS instead of STARTTLS)

Without SMTP_HOST, the script just writes the markdown file and exits.
"""

from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import argparse
import json
import os
import smtplib
from datetime import date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from app.db.database import get_connection
from app.strategy.multi_book_backtest import load_multi_strategy_config
from app.strategy.recommendations import compute_recommendations

DIGEST_DIR = BASE_DIR / "data" / "processed" / "digests"


def fetch_theme_scores_for_date(conn, score_date: str) -> list[dict]:
    cur = conn.execute(
        '''
        SELECT score_date, theme, narrative_score, event_count, breadth, persistence, source_divergence
        FROM daily_theme_scores
        WHERE score_date = ?
        ''',
        (score_date,),
    )
    return [
        {
            "score_date": r[0], "theme": r[1], "narrative_score": float(r[2]),
            "event_count": r[3], "breadth": r[4], "persistence": r[5], "source_divergence": r[6],
        }
        for r in cur.fetchall()
    ]


def latest_score_date(conn) -> str | None:
    cur = conn.execute("SELECT MAX(score_date) FROM daily_theme_scores")
    row = cur.fetchone()
    return row[0] if row and row[0] else None


def fetch_top_evidence_for_date(conn, score_date: str, limit: int = 6) -> list[dict]:
    cur = conn.execute(
        '''
        SELECT date(event_time), theme, topic, direction, source_bucket, source_name, evidence_text
        FROM narrative_events
        WHERE date(event_time) = ?
        ORDER BY confidence DESC NULLS LAST
        LIMIT ?
        ''',
        (score_date, limit),
    )
    return [
        {
            "date": r[0], "theme": r[1], "topic": r[2], "direction": r[3],
            "source_bucket": r[4], "source_name": r[5], "evidence_text": r[6],
        }
        for r in cur.fetchall()
    ]


def instrument_label(inst: dict) -> str:
    t = inst.get("type", "outright")
    if t == "outright":
        return inst.get("symbol", "?")
    if t == "spread":
        return f"{inst.get('long_symbol')} - {inst.get('short_symbol')} spread"
    if t == "crack":
        return f"{inst.get('product_symbol')} - {inst.get('crude_symbol')} crack"
    return str(inst)


def render_markdown(score_date: str, recs: list[dict], theme_scores: list[dict], evidence: list[dict]) -> str:
    lines = [f"# Morning narrative digest — {score_date}", ""]

    if not recs:
        lines.append("_No recommendations: no theme scores for this date._")
        return "\n".join(lines)

    lines.append("## Recommendations")
    lines.append("")
    lines.append("| Book | Instrument | Direction | Position | Weighted score | Veto |")
    lines.append("|---|---|---|---|---|---|")
    for r in recs:
        veto_marker = "⚠ vetoed" if r["proposed_position"] != r["target_position"] else ""
        lines.append(
            f"| {r['book']} | {instrument_label(r['instrument'])} | "
            f"**{r['direction']}** | {r['target_position']:+.1f} | "
            f"{r['weighted_score']:+.3f} | {veto_marker} |"
        )
    lines.append("")

    lines.append("### Per-book detail")
    lines.append("")
    for r in recs:
        lines.append(f"#### {r['book']} ({r['direction']})")
        lines.append(f"- Instrument: `{instrument_label(r['instrument'])}`")
        lines.append(f"- Target position: **{r['target_position']:+.1f}** (proposed pre-veto: {r['proposed_position']:+.1f})")
        lines.append(f"- Weighted score: {r['weighted_score']:+.3f}")
        if r["top_themes"]:
            tops = ", ".join(f"{t['theme']}: {t['weighted_score']:+.3f}" for t in r["top_themes"])
            lines.append(f"- Top themes: {tops}")
        if r["vetoes"]:
            lines.append("- Vetoes triggered:")
            for v in r["vetoes"]:
                note = f" — {v['note']}" if v.get("note") else ""
                lines.append(
                    f"  - `{v['if_theme']}` {v['is']} {v['value']} "
                    f"(score {v['theme_score']:+.3f}) blocks {v['blocks']}{note}"
                )
        lines.append("")

    lines.append("## Theme tape")
    lines.append("")
    lines.append("| Theme | Score | Events | Breadth | Persistence | Divergence |")
    lines.append("|---|---|---|---|---|---|")
    for t in sorted(theme_scores, key=lambda x: abs(x["narrative_score"]), reverse=True):
        lines.append(
            f"| {t['theme']} | {t['narrative_score']:+.3f} | {t['event_count']} | "
            f"{t.get('breadth') or 0:.2f} | {t.get('persistence') or 0:.2f} | "
            f"{t.get('source_divergence') or 0:.2f} |"
        )
    lines.append("")

    if evidence:
        lines.append("## Top evidence")
        lines.append("")
        for e in evidence:
            txt = (e.get("evidence_text") or "").strip().replace("\n", " ")
            if len(txt) > 280:
                txt = txt[:280] + " …"
            lines.append(
                f"- **{e['theme']}/{e['topic']}** ({e['direction']}) "
                f"[{e['source_bucket']}/{e['source_name']}]: {txt}"
            )
        lines.append("")

    return "\n".join(lines)


def maybe_email(subject: str, body_md: str) -> bool:
    """Returns True if an email was sent, False if SMTP not configured."""
    host = os.environ.get("SMTP_HOST")
    if not host:
        return False
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER")
    password = os.environ.get("SMTP_PASS")
    sender = os.environ.get("SMTP_FROM") or user
    recipients = [x.strip() for x in os.environ.get("SMTP_TO", "").split(",") if x.strip()]
    use_ssl = os.environ.get("SMTP_SSL") == "1"

    if not (sender and recipients and user and password):
        print("[email] SMTP_HOST is set but SMTP_USER / SMTP_PASS / SMTP_FROM / SMTP_TO missing — skipping send.")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(body_md, "plain", "utf-8"))

    if use_ssl:
        smtp = smtplib.SMTP_SSL(host, port, timeout=30)
    else:
        smtp = smtplib.SMTP(host, port, timeout=30)
        smtp.starttls()
    try:
        smtp.login(user, password)
        smtp.sendmail(sender, recipients, msg.as_string())
    finally:
        smtp.quit()
    print(f"[email] Sent to {', '.join(recipients)} via {host}:{port}")
    return True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Score date to render (default: latest in DB)")
    parser.add_argument("--no-email", action="store_true", help="Skip email even if SMTP_HOST is set")
    args = parser.parse_args()

    conn = get_connection()
    score_date = args.date or latest_score_date(conn)
    if not score_date:
        print("No theme scores in DB — run scripts/score_narratives.py first.")
        return
    theme_scores = fetch_theme_scores_for_date(conn, score_date)
    evidence = fetch_top_evidence_for_date(conn, score_date)
    conn.close()

    multi_cfg = load_multi_strategy_config()
    score_rows_for_recs = [
        {"score_date": t["score_date"], "theme": t["theme"], "narrative_score": t["narrative_score"]}
        for t in theme_scores
    ]
    recs = compute_recommendations(score_rows_for_recs, multi_cfg)

    body = render_markdown(score_date, recs, theme_scores, evidence)

    DIGEST_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DIGEST_DIR / f"morning_{score_date}.md"
    out_path.write_text(body, encoding="utf-8")
    print(f"Wrote digest to {out_path}")

    if args.no_email:
        return
    sent = maybe_email(f"Oil narrative digest — {score_date}", body)
    if not sent:
        print("[email] SMTP not configured; digest written to file only.")


if __name__ == "__main__":
    main()
