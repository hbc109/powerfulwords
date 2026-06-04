"""Daily report prompt builder for claude.ai paste-flow.

Assembles the day's EIA inventory + prices + oil-relevant document
bodies into a prompt that asks Claude to write a sell-side-style
Chinese numbered-prose daily report. Same paste-flow pattern as
ai_reviewer.py — works free with a claude.ai subscription, no API
key needed.
"""

from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path
from typing import Optional

from datetime import timedelta as _timedelta

from app.db.database import get_connection

BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = BASE_DIR / "data" / "processed" / "digests"

# Skip inventory rows older than this in the prompt — same logic as
# scripts/daily_news_report.py — so the LLM doesn't keep writing up
# last week's EIA print as if it were today's news.
FRESH_INVENTORY_WINDOW_DAYS = 3

HEADLINE_BUCKETS = ("authoritative_news", "sellside_private",
                    "sellside_public", "official_reports")

EXCLUDE_SOURCE_IDS = (
    "zerohedge_energy", "tass_economy", "aljazeera_all",
    "scmp_china", "cnbc_top_news", "cnbc_economy",
)


SYSTEM_PROMPT = """你是一名资深原油市场分析师, 每天根据当日的市场数据和新闻撰写卖方风格的中文原油市场日报。

【格式要求】
- 标题: "原油市场日报 YYYY年M月D日"
- 一、价格走势: 一段简短的中文散文 (2-4 句), 总结今天 WTI 和布伦特的盘面变动, 指出方向和驱动因素。如果有期限结构(M1-M2 spread)变化也简短提及。
- 二、市场因素: 编号列表, 用 "1)", "2)", "3)" 等半角数字加右括号, 不要用 markdown bullet 符号 (-, *)。每项 1-3 句话, 独立成段。覆盖 6-10 个项目, 按重要性排序:
  · **EIA 库存项目规则** (用户消息会预先给出"状态"标签作为判断依据, 你不需要自己做日期判断):
    – 如果状态 = "本期刚发布 (current release)": 第 1 项必须是 EIA 库存详细数字汇总, 把所有提供的数字 (商业原油、库欣、汽油、馏分油及变化幅度) 用一段话写完, 风格同卖方周报示例。
    – 如果状态 = "上一期已过去 (no new release)": **默认不要在报告中提及库存数字**, 不要复述上周或上次的数据。直接按地缘政治、政策、市场新闻等其他类别排序。
    – **例外 A — EIA WPSR**: 即使状态是 "上一期已过去", 如果新闻正文 (下方文档摘录) 出现明确标注为本周 (week ending YYYY-MM-DD) 的 WPSR 引用, 且 week ending 日期与上方结构化库存的"week ending"日期相匹配, 那么可以在某一项中简短转述这条引用 (但不要再当作"今日新发布的数据")。如果不能 100% 确认日期匹配, 仍然完全跳过。
    – **例外 B — API 周报**: API (American Petroleum Institute) 每周二盘后发布的库存预览, 早 EIA 一日。如果新闻正文出现明确标注为本周的 API 库存数据 (通常带"API"或"美国石油协会"字样, 并附有原油/汽油/馏分油的变化数字), 且发布日期是今日或前一日, 可以单独列为一项 (例如 "API 周报: 原油 -300 万桶, 汽油 +100 万桶..."), 明确写出来源是 API 而不是 EIA, 并提示 EIA 数据待 周三 10:30 ET 公布。如果同时有 WPSR (EIA) 和 API 数据, 优先转述 EIA, API 作为对比补充。
    – 两个例外都要求"本周"数据 — 如果新闻只是回顾上一周的库存, 不要写。
  · 其他项目按重要性涵盖: 地缘政治 (伊朗、霍尔木兹、俄乌等)、OPEC/OPEC+ 政策、主要国家政策动向 (中、美、印、欧)、机构观点 (IEA、OPEC 月报、银行)、特殊事件。
  · 每项需引用具体的数字、人名、时间, 避免空泛。如有官员或机构原话, 用中文双引号 ""...""  转写关键句即可, 不需要冗长引用。

【硬性约束】
- 全篇使用中文, 不要混用英文段落。
- 数字保留原始单位 (万桶/百万桶/美元/桶等)。
- 不要发明事实。如果某个事件没有在上下文中出现, 不要写。
- 不要包含信号、评级、看多/看空标签 — 只写客观新闻和数据。
- 不要在文末加任何"投资建议"、"风险提示"或"AI 生成"声明。
- 不要使用 markdown 标题符号 (#, ##) — 一级标题只用 "原油市场日报 YYYY年M月D日" 这一行, 二级标题用 "一、" "二、" 中文序号。
"""


def _latest_two(conn: sqlite3.Connection, symbol: str, asof: date) -> list:
    rows = conn.execute(
        "SELECT price_time, close FROM market_prices WHERE symbol=? AND price_time<=? "
        "AND close IS NOT NULL ORDER BY price_time DESC LIMIT 2",
        (symbol, asof.isoformat()),
    ).fetchall()
    return rows


def _latest_two_settled(conn: sqlite3.Connection, symbol: str, asof: date) -> list:
    """Settled-bars version of _latest_two — two filters:

      1. price_time <= asof — respect the report's effective date so
         a "report for 6/3" doesn't show 6/4's bar.
      2. price_time < today (wall-clock) — exclude today's in-progress
         bar, which during the session is intraday/partial. yfinance's
         daily Close on a *past* trade-day IS the official exchange
         settlement (Yahoo aligns the daily candle close to NYMEX /
         ICE settle once the session has closed), so for any past
         date the close field is settle-grade.

    Combined: when asof == today, we show the most recent settled
    bar (= yesterday). When asof < today, we show asof's own bar
    (the trade-day's settle). This is what the user expects: "the
    report dated 6/3 cites 6/3's settle".
    """
    today = date.today()
    rows = conn.execute(
        "SELECT price_time, close FROM market_prices WHERE symbol=? "
        "AND price_time<=? AND price_time<? "
        "AND close IS NOT NULL ORDER BY price_time DESC LIMIT 2",
        (symbol, asof.isoformat(), today.isoformat()),
    ).fetchall()
    return rows


def _gather(asof: date) -> dict:
    conn = get_connection()
    ctx: dict = {"asof": asof.isoformat(), "inventory": [], "prices": [], "docs": []}

    inv_series = [
        ("EIA_CRUDE_STOCKS",      "美国商业原油 (不含SPR)"),
        ("EIA_CUSHING_STOCKS",    "库欣原油"),
        ("EIA_GASOLINE_STOCKS",   "美国汽油"),
        ("EIA_DISTILLATE_STOCKS", "美国馏分油"),
        ("JODI_OECD_CRUDE_STOCKS","OECD 主要国家原油 (JODI, 月度)"),
    ]
    for sym, label in inv_series:
        rows = _latest_two(conn, sym, asof)
        if not rows:
            continue
        latest_d, latest_v = rows[0]
        try:
            latest_date = date.fromisoformat(latest_d[:10])
        except ValueError:
            continue
        # Option 1 fix (2026-05-29): always include the latest reading with
        # an explicit "days_old" tag, so the SYSTEM_PROMPT decides whether to
        # feature or skip based on freshness. Removes the prior ambiguity
        # where the structured section was empty but news bodies still cited
        # current-week numbers.
        days_old = (asof - latest_date).days
        chg = (latest_v - rows[1][1]) if (len(rows) > 1 and rows[1][1] is not None) else None
        ctx["inventory"].append({"label": label, "date": latest_d[:10],
                                 "level_kbbl": latest_v, "change_kbbl": chg,
                                 "days_old": days_old})

    # Front-month settlement prices. For each commodity we look in two
    # places, in order of preference:
    #   1. {sym}_BROKER_SETTLE — value parsed from a 港联 / Macquarie
    #      morning brief. This is the official ICE / NYMEX pit-settle
    #      that broker statements cite, populated by app.fetchers.
    #      broker_settle. Trusted as authoritative.
    #   2. {sym} — yfinance CL=F / BZ=F daily close. Reliable on T-2 and
    #      older, but the most recent day can lag by $1-$2 on Yahoo
    #      because the daily Close is end-of-Globex, not the pit-window
    #      settle. Falls back to this when no broker doc has been
    #      uploaded yet for the date.
    # _latest_two_settled excludes today's in-progress bar in both cases.
    # EIA spot cross-check is appended inline when available.
    for sym, label_base, eia_sym, eia_label in [
        ("WTI",   "WTI 主力",   "WTI_EIA_SPOT",   "EIA Cushing 现货"),
        ("Brent", "布伦特主力", "BRENT_EIA_SPOT", "EIA Brent Europe 现货"),
    ]:
        rows = _latest_two_settled(conn, f"{sym}_BROKER_SETTLE", asof)
        source_label = "港联/Macquarie 报价"
        if not rows:
            rows = _latest_two_settled(conn, sym, asof)
            source_label = "Yahoo 收盘"
        if not rows:
            continue
        latest_d, latest_v = rows[0]
        chg = (latest_v - rows[1][1]) if (len(rows) > 1 and rows[1][1] is not None) else None
        pct = (chg / rows[1][1] * 100) if (chg is not None and rows[1][1]) else None
        cross = conn.execute(
            "SELECT close FROM market_prices WHERE symbol=? AND price_time=? AND close IS NOT NULL",
            (eia_sym, latest_d),
        ).fetchone()
        cross_note = None
        if cross and cross[0] is not None:
            diff = latest_v - float(cross[0])
            cross_note = {"label": eia_label, "value": float(cross[0]), "diff": diff}
        ctx["prices"].append({"label": f"{label_base} ({source_label})",
                              "date": latest_d[:10],
                              "close": latest_v, "change": chg, "pct_change": pct,
                              "cross": cross_note})

    for sym, label in [("WTI", "WTI"), ("Brent", "布伦特")]:
        m1 = _latest_two_settled(conn, f"{sym}_M1", asof)
        m2 = _latest_two_settled(conn, f"{sym}_M2", asof)
        if m1 and m2:
            spread = m1[0][1] - m2[0][1]
            ctx["prices"].append({"label": f"{label} M1-M2 价差", "date": m1[0][0][:10],
                                  "close": spread, "change": None, "pct_change": None,
                                  "is_spread": True})

    placeholders_b = ",".join("?" * len(HEADLINE_BUCKETS))
    placeholders_x = ",".join("?" * len(EXCLUDE_SOURCE_IDS))
    docs = conn.execute(
        f"""
        SELECT d.source_id, d.title, d.raw_text, COUNT(e.event_id) AS n_events
        FROM documents d
        JOIN narrative_events e ON e.document_id = d.document_id
        WHERE date(d.published_at) = ?
          AND d.source_bucket IN ({placeholders_b})
          AND d.source_id NOT IN ({placeholders_x})
          AND d.raw_text IS NOT NULL
        GROUP BY d.document_id
        ORDER BY d.quality_tier DESC, n_events DESC, length(d.raw_text) DESC
        LIMIT 18
        """,
        (asof.isoformat(), *HEADLINE_BUCKETS, *EXCLUDE_SOURCE_IDS),
    ).fetchall()
    for sid, title, raw, n_events in docs:
        snippet = " ".join((raw or "").split())[:900]
        ctx["docs"].append({"source": sid, "title": title, "n_events": n_events,
                            "body": snippet})

    conn.close()
    return ctx


def _format_user_prompt(ctx: dict) -> str:
    L = [f"日期: {ctx['asof']}", ""]

    # Inventory section — always shows latest available reading with
    # an explicit pre-computed status (本期刚发布 / 上一期已过去) so the
    # LLM doesn't have to do threshold math. EIA week-ending date is
    # Friday; report releases the following Wednesday (5-day lag).
    # So a week-ending date that's 5-8 days old means "current release",
    # >8 days means "stale". We compute status in Python and tell Claude
    # the binary outcome.
    inv = ctx.get("inventory") or []
    # Use MIN days_old across EIA-grade weekly series (skip the slow
    # monthly JODI which is always old) to pick the freshness status.
    eia_ages = [r.get("days_old") for r in inv
                if r.get("days_old") is not None and "JODI" not in r.get("label", "")]
    min_age = min(eia_ages) if eia_ages else None
    is_fresh = (min_age is not None and min_age <= 8)
    status = "本期刚发布 (current release)" if is_fresh else "上一期已过去 (no new release)"
    L.append(f"【最新 EIA / JODI 库存读数 — 状态: {status}】")
    if not inv:
        L.append("(完全无可用数据 — 跳过库存项)")
    for r in inv:
        chg = r["change_kbbl"]
        chg_str = f"  环比变化 {'+' if (chg or 0) >= 0 else ''}{int(round(chg)):,}kbbl" if chg is not None else ""
        age_str = f"  [week-ending 距今 {r['days_old']} 天]" if r.get("days_old") is not None else ""
        L.append(f"- {r['label']} (week ending {r['date']}): "
                 f"{int(round(r['level_kbbl'])):,} kbbl{chg_str}{age_str}")
    L.append("")

    L.append("【今日价格】")
    if not ctx["prices"]:
        L.append("(暂无可用价格数据)")
    for r in ctx["prices"]:
        if r.get("is_spread"):
            struct = "back" if r["close"] > 0 else "contango"
            L.append(f"- {r['label']} ({r['date']}): {'+' if r['close'] >= 0 else ''}{r['close']:.2f} ({struct})")
        else:
            chg = r.get("change")
            pct = r.get("pct_change")
            chg_str = ""
            if chg is not None and pct is not None:
                chg_str = f"  环比 {'+' if chg >= 0 else ''}${chg:.2f} ({'+' if pct >= 0 else ''}{pct:.2f}%)"
            cross = r.get("cross")
            cross_str = ""
            if cross is not None:
                d = cross.get("diff")
                d_tag = ""
                if d is not None:
                    if abs(d) > 1.0:
                        d_tag = f"  ⚠ 与结算价偏差 {'+' if d >= 0 else ''}${d:.2f}"
                cross_str = (f"  [{cross['label']} 同日 ${cross['value']:.2f}{d_tag}]")
            L.append(f"- {r['label']} ({r['date']}): ${r['close']:.2f}{chg_str}{cross_str}")
    L.append("")

    L.append("【今日新闻与分析师文章 (排名依据: 高质量来源 + 多个抽取事件 + 内容深度, 上限18条)】")
    if not ctx["docs"]:
        L.append("(今日无可用新闻文档)")
    for i, d in enumerate(ctx["docs"], 1):
        L.append(f"\n[{i}] 来源: {d['source']}  (extracted_events={d['n_events']})")
        if d.get("title"):
            L.append(f"标题: {d['title']}")
        if d.get("body"):
            L.append(f"正文摘录: {d['body']}")
    L.append("")
    L.append("现在请按 SYSTEM 指定的格式撰写今日原油市场日报。注意: 第一项编号项目必须是 EIA 库存详细数字汇总。")
    return "\n".join(L)


def prepare_daily_report_prompt(asof: date) -> dict:
    ctx = _gather(asof)
    user = _format_user_prompt(ctx)
    return {"system": SYSTEM_PROMPT, "user": user, "context": ctx,
            "ready": bool(ctx["docs"] or ctx["inventory"] or ctx["prices"]),
            "reason": None if (ctx["docs"] or ctx["inventory"] or ctx["prices"])
                      else "No documents, inventory, or prices available for this date."}


def llm_report_path(asof: date) -> Path:
    return OUT_DIR / f"daily_llm_{asof.isoformat()}.md"


def save_llm_report(asof: date, text: str) -> Path:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    p = llm_report_path(asof)
    p.write_text(text, encoding="utf-8")
    return p


def load_llm_report(asof: date) -> Optional[str]:
    p = llm_report_path(asof)
    return p.read_text(encoding="utf-8") if p.exists() else None


DEFAULT_API_MODEL = "claude-sonnet-4-6"


def generate_daily_report_via_api(
    asof: date,
    *,
    model: str = DEFAULT_API_MODEL,
    max_tokens: int = 2200,
) -> dict:
    """Call the Anthropic SDK to generate the prose daily report directly.

    Same prompt the paste-flow uses — just routes through the API for
    users who prefer not to copy/paste. Returns:
      {"status": "ok"|"skipped"|"error", "text": str|None,
       "model": str, "reason": str|None}
    """
    import os as _os
    if not _os.environ.get("ANTHROPIC_API_KEY"):
        return {"status": "skipped", "reason": "ANTHROPIC_API_KEY not set",
                "text": None, "model": model}

    payload = prepare_daily_report_prompt(asof)
    if not payload.get("ready"):
        return {"status": "skipped", "reason": payload.get("reason"),
                "text": None, "model": model}

    try:
        import anthropic
    except ImportError:
        return {"status": "error", "reason": "anthropic SDK not installed",
                "text": None, "model": model}

    try:
        client = anthropic.Anthropic(timeout=90)
        resp = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            temperature=0.3,
            system=payload["system"],
            messages=[{"role": "user", "content": payload["user"]}],
        )
        text = "".join(
            getattr(b, "text", "") for b in resp.content
            if getattr(b, "type", None) == "text"
        ).strip()
        if not text:
            return {"status": "error", "reason": "empty response",
                    "text": None, "model": model}
        return {"status": "ok", "reason": None, "text": text, "model": model}
    except Exception as e:
        return {"status": "error", "reason": f"{type(e).__name__}: {e}",
                "text": None, "model": model}
