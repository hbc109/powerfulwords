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

from app.db.database import get_connection

BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = BASE_DIR / "data" / "processed" / "digests"

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
  · 第 1 项通常是 EIA 周度库存详情, 把所有提供的数字 (商业原油、SPR、库欣、汽油、馏分油以及变化幅度) 用一段话写完, 风格同卖方周报示例。
  · 后续项目按重要性涵盖: 地缘政治 (伊朗、霍尔木兹、俄乌等)、OPEC/OPEC+ 政策、主要国家政策动向 (中、美、印、欧)、机构观点 (IEA、OPEC 月报、银行)、特殊事件。
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
        chg = (latest_v - rows[1][1]) if (len(rows) > 1 and rows[1][1] is not None) else None
        ctx["inventory"].append({"label": label, "date": latest_d[:10],
                                 "level_kbbl": latest_v, "change_kbbl": chg})

    for sym, label in [("WTI", "WTI 主力"), ("Brent", "布伦特主力")]:
        rows = _latest_two(conn, sym, asof)
        if not rows:
            continue
        latest_d, latest_v = rows[0]
        chg = (latest_v - rows[1][1]) if (len(rows) > 1 and rows[1][1] is not None) else None
        pct = (chg / rows[1][1] * 100) if (chg is not None and rows[1][1]) else None
        ctx["prices"].append({"label": label, "date": latest_d[:10],
                              "close": latest_v, "change": chg, "pct_change": pct})

    for sym, label in [("WTI", "WTI"), ("Brent", "布伦特")]:
        m1 = _latest_two(conn, f"{sym}_M1", asof)
        m2 = _latest_two(conn, f"{sym}_M2", asof)
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

    L.append("【今日 EIA 库存数据 (最新一期 vs 上一期, 单位: 千桶 kbbl)】")
    if not ctx["inventory"]:
        L.append("(暂无可用库存数据)")
    for r in ctx["inventory"]:
        chg = r["change_kbbl"]
        chg_str = f"  环比变化 {'+' if (chg or 0) >= 0 else ''}{int(round(chg)):,}kbbl" if chg is not None else ""
        L.append(f"- {r['label']} ({r['date']}): {int(round(r['level_kbbl'])):,} kbbl{chg_str}")
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
            L.append(f"- {r['label']} ({r['date']}): ${r['close']:.2f}{chg_str}")
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
