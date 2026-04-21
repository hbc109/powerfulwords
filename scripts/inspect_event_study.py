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

    print("sample_size:", payload.get("sample_size"))
    print("\nBucket summary:")
    for bucket, stats in payload.get("bucket_summary", {}).items():
        print(bucket, stats)

    print("\nTopic summary:")
    for topic, stats in payload.get("topic_summary", {}).items():
        print(topic, stats)


if __name__ == "__main__":
    main()
