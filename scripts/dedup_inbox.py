"""Delete inbox files whose content is already represented by another
file on disk (same SHA256). Keeps the version referenced by the
documents table when possible; otherwise keeps the oldest mtime.

Default is --dry-run (lists what would be deleted, deletes nothing).
Pass --apply to actually delete.

Why we end up with disk duplicates:
  The upload widget renames colliding filenames to <stem>_2.<ext>,
  <stem>_3.<ext>, etc., to avoid overwriting. The ingester is checksum-
  based, so the duplicate content is correctly skipped at the DB layer
  but the redundant file stays on disk. This script trims the redundant
  files; the DB is unchanged.
"""

from __future__ import annotations

import argparse
import hashlib
import sqlite3
from collections import defaultdict
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
INBOX = BASE_DIR / "data" / "inbox"
DB_PATH = BASE_DIR / "data" / "oil_narrative.db"


def sha256_of(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="Actually delete (default is dry-run).")
    args = ap.parse_args()

    conn = sqlite3.connect(DB_PATH)
    db_path_for_checksum: dict[str, str] = {}
    for fp, csum in conn.execute(
        "SELECT file_path, checksum FROM documents WHERE file_path IS NOT NULL AND checksum IS NOT NULL"
    ):
        if csum and fp:
            db_path_for_checksum[csum] = fp
    conn.close()

    files = [p for p in INBOX.rglob("*")
             if p.is_file() and p.name != ".gitkeep" and "README" not in p.name.upper()]
    print(f"Scanned {len(files)} inbox files.")

    groups: dict[str, list[Path]] = defaultdict(list)
    for p in files:
        try:
            groups[sha256_of(p)].append(p)
        except Exception as e:
            print(f"  [skip] {p}: {e}")

    dups = {h: ps for h, ps in groups.items() if len(ps) > 1}
    n_dup_files = sum(len(ps) - 1 for ps in dups.values())
    total_bytes = 0
    to_delete: list[tuple[Path, Path, str]] = []  # (delete, keep, reason)

    for h, ps in dups.items():
        db_keeper_path = db_path_for_checksum.get(h)
        keeper: Path | None = None
        if db_keeper_path:
            keeper = next((p for p in ps if str(p.resolve()) == str(Path(db_keeper_path).resolve())), None)
            keeper_reason = "DB reference"
        if keeper is None:
            ps.sort(key=lambda p: p.stat().st_mtime)
            keeper = ps[0]
            keeper_reason = "oldest mtime"
        for p in ps:
            if p == keeper:
                continue
            total_bytes += p.stat().st_size
            to_delete.append((p, keeper, keeper_reason))

    print(f"Duplicate groups: {len(dups)}")
    print(f"Files to delete:  {n_dup_files}  ({total_bytes / (1024*1024):.1f} MB)")
    print()
    if not to_delete:
        print("Nothing to delete.")
        return

    for d, k, reason in to_delete[:25]:
        print(f"  DEL  {d.relative_to(INBOX)}")
        print(f"  KEEP {k.relative_to(INBOX)}  ({reason})")
        print()
    if len(to_delete) > 25:
        print(f"  ... and {len(to_delete) - 25} more")
    print()

    if args.apply:
        deleted = 0
        for p, _, _ in to_delete:
            try:
                p.unlink()
                deleted += 1
            except Exception as e:
                print(f"  [error] {p}: {e}")
        print(f"Deleted {deleted} files.")
    else:
        print("Dry-run only. Re-run with --apply to actually delete.")


if __name__ == "__main__":
    main()
