"""Interactively review proposed themes and write accepted ones into the
live taxonomy files (oil_topic_rules.json + theme_hierarchy.json).

Usage:
  python scripts/approve_themes.py                 # picks the most recent proposal
  python scripts/approve_themes.py --file path     # specific proposal file
  python scripts/approve_themes.py --yes           # accept everything (no prompts)

For each proposed subtheme:
  - prompts y/n
  - if y, appends a topic_rules entry (label, keywords, direction)
    to oil_topic_rules.json
  - and adds the label under the chosen parent theme in theme_hierarchy.json

For each proposed brand-new main theme:
  - prompts y/n
  - if y, adds it to theme_hierarchy.json as an empty theme entry
"""

from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import argparse
import json
from datetime import datetime

RULES_PATH = BASE_DIR / "app" / "config" / "oil_topic_rules.json"
HIERARCHY_PATH = BASE_DIR / "app" / "config" / "theme_hierarchy.json"
PROPOSALS_DIR = BASE_DIR / "data" / "processed" / "themes"

VALID_DIRECTIONS = {"bullish", "bearish", "mixed", "neutral"}


def latest_proposal() -> Path | None:
    if not PROPOSALS_DIR.exists():
        return None
    candidates = sorted(PROPOSALS_DIR.glob("proposed_*.json"))
    return candidates[-1] if candidates else None


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def confirm(prompt: str, default_yes: bool = False, auto_yes: bool = False) -> bool:
    if auto_yes:
        return True
    suffix = " [Y/n] " if default_yes else " [y/N] "
    while True:
        raw = input(prompt + suffix).strip().lower()
        if not raw:
            return default_yes
        if raw in ("y", "yes"):
            return True
        if raw in ("n", "no"):
            return False
        print("  please type y or n")


def add_subtheme_to_rules(rules: dict, label: str, keywords: list[str], direction: str) -> bool:
    """Append a new topic to oil_topic_rules.json. Returns True if added."""
    topic_rules = rules.setdefault("topic_rules", {})
    if label in topic_rules:
        print(f"  skip — {label} already exists in oil_topic_rules.json")
        return False
    topic_rules[label] = {
        "keywords": [k.lower() for k in keywords if k],
        "direction": direction if direction in VALID_DIRECTIONS else "neutral",
    }
    return True


def add_subtheme_to_hierarchy(hierarchy: dict, parent_theme: str, label: str) -> bool:
    """Add label under the parent theme; create the theme if missing. Returns True if changed."""
    themes = hierarchy.setdefault("themes", {})
    if parent_theme not in themes:
        themes[parent_theme] = {"label": parent_theme.title(), "subthemes": []}
        print(f"  created new parent theme: {parent_theme}")
    bucket = themes[parent_theme].setdefault("subthemes", [])
    if label in bucket:
        return False
    bucket.append(label)
    return True


def add_new_theme(hierarchy: dict, label: str, description: str, suggested_subthemes: list[str]) -> bool:
    themes = hierarchy.setdefault("themes", {})
    if label in themes:
        print(f"  skip — theme {label} already exists.")
        return False
    themes[label] = {
        "label": label.replace("_", " ").title(),
        "subthemes": list(suggested_subthemes or []),
        "description": description,
    }
    return True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", help="Path to a specific proposed_*.json (default: latest)")
    parser.add_argument("--yes", action="store_true", help="Accept every proposal without prompting")
    args = parser.parse_args()

    proposal_path = Path(args.file) if args.file else latest_proposal()
    if proposal_path is None or not proposal_path.exists():
        print(f"No proposal file found. Looked in: {PROPOSALS_DIR}")
        sys.exit(1)

    proposal = load_json(proposal_path)
    rules = load_json(RULES_PATH)
    hierarchy = load_json(HIERARCHY_PATH)

    print(f"Reviewing {proposal_path.name}")
    if proposal.get("summary"):
        print(f"Summary: {proposal['summary']}")
    if proposal.get("coverage_note"):
        print(f"Coverage note: {proposal['coverage_note']}")
    print()

    # --- New main themes first (so subthemes can attach to them) ---
    accepted_themes = 0
    for t in proposal.get("new_themes", []) or []:
        label = (t.get("label") or "").strip()
        if not label:
            continue
        print(f"--- proposed THEME: {label}")
        print(f"    description: {t.get('description', '')}")
        if t.get("suggested_subthemes"):
            print(f"    suggested subthemes: {', '.join(t['suggested_subthemes'])}")
        if confirm("  accept this new main theme?", auto_yes=args.yes):
            if add_new_theme(hierarchy, label, t.get("description", ""), t.get("suggested_subthemes", [])):
                accepted_themes += 1

    # --- Subthemes ---
    accepted_subs = 0
    for s in proposal.get("new_subthemes", []) or []:
        label = (s.get("label") or "").strip()
        parent = (s.get("parent_theme") or "").strip()
        if not label or not parent:
            continue
        print()
        print(f"--- proposed SUBTHEME: {label}  (parent: {parent})")
        print(f"    description: {s.get('description', '')}")
        if s.get("suggested_keywords"):
            print(f"    keywords: {', '.join(s['suggested_keywords'])}")
        if s.get("direction_bias"):
            print(f"    direction_bias: {s['direction_bias']}")
        if s.get("example_evidence"):
            for ex in s["example_evidence"][:2]:
                print(f"    e.g.: {ex[:160]}")
        if confirm("  accept this subtheme?", auto_yes=args.yes):
            added_rules = add_subtheme_to_rules(
                rules, label, s.get("suggested_keywords", []), s.get("direction_bias") or "neutral"
            )
            added_hier = add_subtheme_to_hierarchy(hierarchy, parent, label)
            if added_rules or added_hier:
                accepted_subs += 1

    if accepted_subs == 0 and accepted_themes == 0:
        print("\nNothing accepted. No files modified.")
        return

    write_json(RULES_PATH, rules)
    write_json(HIERARCHY_PATH, hierarchy)

    # Audit trail: rename proposal so it doesn't get re-reviewed.
    audited = proposal_path.with_name(proposal_path.stem + ".reviewed.json")
    proposal_path.rename(audited)

    print()
    print(f"Accepted {accepted_subs} subtheme(s) and {accepted_themes} new theme(s).")
    print(f"Updated: {RULES_PATH.relative_to(BASE_DIR)}")
    print(f"Updated: {HIERARCHY_PATH.relative_to(BASE_DIR)}")
    print(f"Marked proposal as reviewed: {audited.name}")
    print()
    print("Re-run extraction to pick up the new taxonomy:")
    print("  python scripts/extract_narratives.py --mode rule")
    print("  python scripts/score_narratives.py")


if __name__ == "__main__":
    main()
