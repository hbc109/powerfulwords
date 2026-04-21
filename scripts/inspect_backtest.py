from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import argparse
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    args = parser.parse_args()

    path = (BASE_DIR / args.file).resolve() if not Path(args.file).is_absolute() else Path(args.file)
    payload = json.loads(path.read_text(encoding="utf-8"))

    print("Summary:")
    print(json.dumps(payload.get("summary", {}), ensure_ascii=False, indent=2))

    print("\nLast 10 equity points:")
    for row in payload.get("equity_curve", [])[-10:]:
        print(row)

    print("\nLast 10 trades:")
    for row in payload.get("trades", [])[-10:]:
        print(row)

if __name__ == "__main__":
    main()
