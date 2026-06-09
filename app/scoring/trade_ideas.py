"""Intraday trade-ideas prompt builder for claude.ai paste-flow.

Same shape as app/scoring/daily_report.py but framed for on-demand use:
"given the data right now, what's the model saying + what trade ideas
make sense?" The LLM is asked to write 2-3 specific trade ideas with
rationale and risks, using the composite signal as a *reference*
(not a constraint) and adding qualitative judgment the rules can't see.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from app.db.database import get_connection
from app.scoring.composite import composite_score
from app.scoring.factors import positioning_factor, inventory_factor, term_structure_factor
from app.strategy.backtest_engine import aggregate_score_by_date

BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = BASE_DIR / "data" / "processed" / "digests"

SYMBOLS = [("WTI", "wti_outright"), ("Brent", "brent_outright")]


SYSTEM_PROMPT = """你是一名资深原油交易员, 收到一份基于规则的模型当前快照和最近的市场资讯, 你的任务是给出 2-3 个具体可执行的交易想法 (trade ideas)。

【输出格式】

第一段: 简短市场色调 (3-5 句中文散文), 概括当前 WTI / Brent 的盘面和最近 24-48 小时的主要驱动事件。

然后用编号列表 1)、2)、3) 给出 2-3 个交易想法, 每个想法包括:
- 标的: 如 WTI 平盘 / Brent 平盘 / Brent-WTI 价差 / WTI M1-M2 月差 / 等等
- 方向与规模: LONG / SHORT, 推荐 1x 或 2x 头寸
- 持有窗口: intraday / 1-3 天 / 1-2 周
- 主要论据: 引用具体的因子读数 (narrative z=X, inventory z=Y...) 或新闻事件
- 主要风险: 哪些条件出现时该想法失效

最后一段, 简短的 "模型未涵盖的尾部风险" (Tail risks the model can't see) — 用 2-3 句指出当前规则信号无法捕捉但可能影响价格的事件 (Fed 突发声明、OPEC+ 紧急会议、地缘冲突升级等)。

【硬性约束】
- 交易想法可以与模型的 composite signal 一致, 也可以不同 (例如模型说 SHORT WTI, 你可以建议做多 Brent-WTI 价差扩大, 如果你的论据支持)。
- 全篇中文, 不要混用英文段落。
- 不要发明事实。如果某个事件没有在上下文中出现, 不要写。
- 不要使用 markdown 标题符号 (#, ##), 也不要加 "投资建议" 或 "AI 生成" 声明。
- 每个想法都要给出具体的论据和具体的风险, 避免空泛。
"""


def _latest_two(conn: sqlite3.Connection, symbol: str, asof: date) -> list:
    # released_at <= asof keeps EIA / JODI / COT rows hidden until
    # their actual publication date. Price symbols (lag=0) unaffected.
    return conn.execute(
        "SELECT price_time, close FROM market_prices WHERE symbol=? AND released_at<=? "
        "AND close IS NOT NULL ORDER BY price_time DESC LIMIT 2",
        (symbol, asof.isoformat()),
    ).fetchall()


def _load_book_cfg(name: str) -> dict:
    cfg_path = BASE_DIR / "app" / "config" / "multi_strategy_config.json"
    cfg = json.loads(cfg_path.read_text())
    for b in cfg.get("books", []):
        if b.get("name") == name:
            return b
    raise KeyError(f"Book {name!r} not in multi_strategy_config")


def _narrative_z(book_cfg: dict, theme_scores: pd.DataFrame, asof: date) -> Optional[float]:
    weights = (book_cfg.get("scoring") or {}).get("theme_weights")
    rows = [{"score_date": str(r["score_date"]), "theme": r["theme"],
             "narrative_score": float(r["narrative_score"])}
            for _, r in theme_scores.iterrows()]
    agg = aggregate_score_by_date(rows, weights=weights, group_field="theme")
    if not agg:
        return None
    df = pd.DataFrame(agg).sort_values("score_date").reset_index(drop=True)
    df["score_date"] = df["score_date"].astype(str)
    df["aggregate_score"] = df["aggregate_score"].astype(float)
    asof_iso = asof.isoformat()
    before = df[df["score_date"] <= asof_iso].tail(31)
    if len(before) < 6:
        return None
    today_val = float(before.iloc[-1]["aggregate_score"])
    prior = before.iloc[:-1]["aggregate_score"]
    mean, std = prior.mean(), prior.std()
    if std == 0 or pd.isna(std):
        return None
    return (today_val - mean) / std


def _gather(asof_dt: datetime, recent_hours: int = 24) -> dict:
    asof = asof_dt.date()
    conn = get_connection()
    ctx: dict = {"asof": asof_dt.isoformat(timespec="minutes"),
                 "signals": [], "prices": [], "inventory": [], "recent_docs": []}

    # Per-symbol live signal
    theme_scores = pd.read_sql(
        "SELECT score_date, theme, narrative_score FROM daily_theme_scores WHERE commodity='crude_oil'",
        conn,
    )
    for sym, book_name in SYMBOLS:
        regime_row = conn.execute(
            "SELECT primary_regime FROM daily_regimes WHERE symbol=? AND regime_date<=? "
            "ORDER BY regime_date DESC LIMIT 1",
            (sym, asof.isoformat()),
        ).fetchone()
        regime = regime_row[0] if regime_row else None
        try:
            book_cfg = _load_book_cfg(book_name)
        except KeyError:
            book_cfg = {}
        nz = _narrative_z(book_cfg, theme_scores, asof) if book_cfg else None
        try:
            ts = term_structure_factor(sym, asof)
        except Exception:
            ts = None
        try:
            pos = positioning_factor(sym, asof)
        except Exception:
            pos = None
        try:
            inv = inventory_factor(sym, asof)
        except Exception:
            inv = None
        composite = None
        breakdown = []
        if regime and nz is not None:
            try:
                out = composite_score(sym, regime, nz,
                                      {"term_structure": ts, "positioning": pos, "inventory": inv})
                composite = float(out["total"])
                breakdown = out.get("breakdown", [])
            except KeyError:
                composite = None
        ctx["signals"].append({
            "symbol": sym, "regime": regime, "composite": composite,
            "narrative_z": nz, "term_structure": ts, "positioning": pos, "inventory": inv,
            "breakdown": breakdown,
        })

    # Prices + curve spreads
    for sym, label in [("WTI", "WTI 平盘"), ("Brent", "布伦特平盘")]:
        rows = _latest_two(conn, sym, asof)
        if not rows:
            continue
        latest_d, latest_v = rows[0]
        chg = (latest_v - rows[1][1]) if (len(rows) > 1 and rows[1][1] is not None) else None
        pct = (chg / rows[1][1] * 100) if (chg is not None and rows[1][1]) else None
        ctx["prices"].append({"label": label, "date": latest_d[:10], "close": latest_v,
                              "change": chg, "pct_change": pct})
    for sym, label in [("WTI", "WTI"), ("Brent", "布伦特")]:
        m1 = _latest_two(conn, f"{sym}_M1", asof)
        m2 = _latest_two(conn, f"{sym}_M2", asof)
        if m1 and m2:
            spread = m1[0][1] - m2[0][1]
            ctx["prices"].append({"label": f"{label} M1-M2 月差", "date": m1[0][0][:10],
                                  "close": spread, "is_spread": True})

    # Inventory: include latest as "current state of the world", with publication date.
    # No freshness filter here — for trade-idea context, the most recent reading IS
    # the current state, even if a few days old.
    for sym, label in [
        ("EIA_CRUDE_STOCKS",      "美国商业原油 (不含SPR)"),
        ("EIA_CUSHING_STOCKS",    "库欣原油"),
        ("EIA_GASOLINE_STOCKS",   "美国汽油"),
        ("EIA_DISTILLATE_STOCKS", "美国馏分油"),
    ]:
        rows = _latest_two(conn, sym, asof)
        if not rows:
            continue
        latest_d, latest_v = rows[0]
        chg = (latest_v - rows[1][1]) if (len(rows) > 1 and rows[1][1] is not None) else None
        days_old = (asof - date.fromisoformat(latest_d[:10])).days
        ctx["inventory"].append({"label": label, "date": latest_d[:10], "days_old": days_old,
                                 "level_kbbl": latest_v, "change_kbbl": chg})

    # Recent oil-relevant docs from quality buckets
    cutoff = (asof_dt - timedelta(hours=recent_hours)).isoformat()
    docs = conn.execute(
        """
        SELECT d.source_id, d.title, d.raw_text, d.published_at
        FROM documents d
        JOIN narrative_events e ON e.document_id = d.document_id
        WHERE d.published_at >= ?
          AND d.source_bucket IN ('authoritative_news','sellside_private','sellside_public','official_reports')
          AND d.source_id NOT IN ('zerohedge_energy','tass_economy','aljazeera_all','scmp_china','cnbc_top_news','cnbc_economy')
          AND d.raw_text IS NOT NULL
        GROUP BY d.document_id
        ORDER BY d.quality_tier DESC, COUNT(e.event_id) DESC, length(d.raw_text) DESC
        LIMIT 14
        """,
        (cutoff,),
    ).fetchall()
    for sid, title, raw, pub in docs:
        ctx["recent_docs"].append({
            "source": sid, "title": title, "published_at": (pub or "")[:16],
            "body": " ".join((raw or "").split())[:700],
        })
    conn.close()
    return ctx


def _format_user_prompt(ctx: dict, recent_hours: int) -> str:
    L = [f"当前时间: {ctx['asof']}", f"最近文档窗口: {recent_hours} 小时", ""]

    L.append("【当前模型信号 (composite + 因子读数)】")
    for s in ctx["signals"]:
        comp = s.get("composite")
        comp_str = f"{comp:+.3f}" if isinstance(comp, (int, float)) else "n/a"
        L.append(f"- {s['symbol']}: 制度 `{s.get('regime', '?')}`, composite={comp_str}")
        for f, label in [("narrative_z", "narrative_z"), ("term_structure", "term_structure"),
                         ("positioning", "positioning"), ("inventory", "inventory")]:
            v = s.get(f)
            if isinstance(v, (int, float)):
                L.append(f"    {label}: {v:+.3f}")
        if s.get("breakdown"):
            L.append("    factor contributions:")
            for r in s["breakdown"]:
                L.append(f"      - {r['factor']}: z={r['value']:+.3f}, w={r['weight']:.2f}, contrib={r['contribution']:+.3f}")
    L.append("")

    L.append("【最新价格】")
    for p in ctx["prices"]:
        if p.get("is_spread"):
            struct = "back" if p["close"] > 0 else "contango"
            L.append(f"- {p['label']} ({p['date']}): {'+' if p['close'] >= 0 else ''}{p['close']:.2f} ({struct})")
        else:
            chg = p.get("change"); pct = p.get("pct_change")
            chg_str = (f"  环比 {'+' if chg >= 0 else ''}${chg:.2f} ({'+' if pct >= 0 else ''}{pct:.2f}%)"
                       if (chg is not None and pct is not None) else "")
            L.append(f"- {p['label']} ({p['date']}): ${p['close']:.2f}{chg_str}")
    L.append("")

    L.append("【最新库存读数 (作为当前世界状态, 不作为'今日新闻')】")
    if not ctx["inventory"]:
        L.append("(无库存数据)")
    for r in ctx["inventory"]:
        chg = r["change_kbbl"]
        chg_str = (f"  环比 {'+' if (chg or 0) >= 0 else ''}{int(round(chg)):,}kbbl"
                   if chg is not None else "")
        age = f" [发布于 {r['days_old']} 天前]" if r['days_old'] > 1 else ""
        L.append(f"- {r['label']} ({r['date']}): {int(round(r['level_kbbl'])):,} kbbl{chg_str}{age}")
    L.append("")

    L.append(f"【最近 {recent_hours} 小时主要文档 (高质量来源, 按相关性排序, 上限 14 条)】")
    if not ctx["recent_docs"]:
        L.append("(暂无文档)")
    for i, d in enumerate(ctx["recent_docs"], 1):
        L.append(f"\n[{i}] 来源: {d['source']}, 发布: {d['published_at']}")
        if d.get("title"):
            L.append(f"标题: {d['title']}")
        if d.get("body"):
            L.append(f"正文摘录: {d['body']}")
    L.append("")
    L.append("现在请按 SYSTEM 指定的格式给出 2-3 个具体交易想法。可以与模型 composite 一致或不一致, 但每个想法都要有具体论据和具体风险。")
    return "\n".join(L)


def prepare_trade_ideas_prompt(asof_dt: Optional[datetime] = None, recent_hours: int = 24) -> dict:
    asof_dt = asof_dt or datetime.now()
    ctx = _gather(asof_dt, recent_hours=recent_hours)
    has_signal = any(s.get("composite") is not None for s in ctx["signals"])
    if not has_signal and not ctx["recent_docs"]:
        return {"system": SYSTEM_PROMPT, "user": "", "context": ctx,
                "ready": False, "reason": "No composite signal and no recent documents — nothing to reason over."}
    user = _format_user_prompt(ctx, recent_hours)
    return {"system": SYSTEM_PROMPT, "user": user, "context": ctx,
            "ready": True, "reason": None}


def _ideas_path(asof_dt: datetime) -> Path:
    return OUT_DIR / f"trade_ideas_{asof_dt.strftime('%Y-%m-%d_%H-%M')}.md"


def save_trade_ideas(asof_dt: datetime, text: str) -> Path:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    p = _ideas_path(asof_dt)
    p.write_text(text, encoding="utf-8")
    return p


def list_trade_ideas(limit: int = 20) -> list:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(OUT_DIR.glob("trade_ideas_*.md"), reverse=True)[:limit]
    return [{"path": p, "stamp": p.stem.replace("trade_ideas_", ""),
             "text": p.read_text(encoding="utf-8")} for p in files]
