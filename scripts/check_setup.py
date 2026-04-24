"""Verify the local environment is ready to run the pipeline.

Reports OK / WARN / FAIL for each check. Exits 0 if no FAIL.

  - all required Python packages importable
  - sources table populated (init_sources has been run)
  - inbox folder tree present (setup_inbox has been run)
  - LLM credentials configured for the configured provider (warning only)
  - SMTP credentials configured for digest email (warning only)
"""

from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import importlib
import json
import os
import sqlite3


REQUIRED_PACKAGES = [
    "pydantic", "yaml", "pypdf", "docx", "streamlit", "pandas",
    "altair", "anthropic", "requests", "feedparser", "bs4", "yfinance",
]

DB_PATH = BASE_DIR / "data" / "oil_narrative.db"
INBOX_DIR = BASE_DIR / "data" / "inbox"
LLM_CONFIG_PATH = BASE_DIR / "app" / "config" / "llm_config.json"


def check_packages() -> list[tuple[str, str, str]]:
    out = []
    for m in REQUIRED_PACKAGES:
        try:
            importlib.import_module(m)
            out.append(("OK  ", f"package:{m}", "importable"))
        except Exception as e:
            out.append(("FAIL", f"package:{m}", f"missing — pip install -r requirements.txt  ({e})"))
    return out


def check_db_sources() -> tuple[str, str, str]:
    if not DB_PATH.exists():
        return ("WARN", "db:sources", "no DB yet — run scripts/init_sources.py")
    try:
        conn = sqlite3.connect(DB_PATH)
        n = conn.execute("SELECT COUNT(*) FROM sources").fetchone()[0]
        conn.close()
    except sqlite3.OperationalError:
        return ("FAIL", "db:sources", "sources table missing — run scripts/init_sources.py")
    if n == 0:
        return ("WARN", "db:sources", "sources table empty — run scripts/init_sources.py")
    return ("OK  ", "db:sources", f"{n} sources in DB")


def check_inbox() -> tuple[str, str, str]:
    if not INBOX_DIR.exists():
        return ("WARN", "inbox", "not created — run scripts/setup_inbox.py")
    folders = sum(1 for p in INBOX_DIR.rglob("*") if p.is_dir())
    if folders < 5:
        return ("WARN", "inbox", f"only {folders} folders — try scripts/setup_inbox.py")
    return ("OK  ", "inbox", f"{folders} source folders")


def check_llm_creds() -> tuple[str, str, str]:
    cfg = json.loads(LLM_CONFIG_PATH.read_text(encoding="utf-8"))
    provider = cfg.get("provider", "anthropic")
    var = "ANTHROPIC_API_KEY" if provider == "anthropic" else "OPENAI_API_KEY"
    if os.environ.get(var):
        return ("OK  ", f"llm:{provider}", f"{var} is set")
    return ("WARN", f"llm:{provider}", f"{var} not set — extraction will fall back to rule mode")


def check_smtp() -> tuple[str, str, str]:
    needed = ["SMTP_HOST", "SMTP_USER", "SMTP_PASS", "SMTP_FROM", "SMTP_TO"]
    missing = [v for v in needed if not os.environ.get(v)]
    if not missing:
        return ("OK  ", "smtp", f"configured (host={os.environ['SMTP_HOST']})")
    return ("WARN", "smtp", f"missing {','.join(missing)} — digest will be file-only")


def main() -> None:
    checks = []
    checks.extend(check_packages())
    checks.append(check_db_sources())
    checks.append(check_inbox())
    checks.append(check_llm_creds())
    checks.append(check_smtp())

    fails = sum(1 for s, *_ in checks if s == "FAIL")
    warns = sum(1 for s, *_ in checks if s == "WARN")

    width = max(len(name) for _, name, _ in checks)
    for status, name, msg in checks:
        print(f"  [{status}] {name:<{width}}  {msg}")

    print()
    print(f"Result: {fails} FAIL, {warns} WARN")
    if fails:
        print("Fix the FAILs before running scripts/run_daily.py.")
        sys.exit(1)


if __name__ == "__main__":
    main()
