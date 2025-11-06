#!/usr/bin/env python3
"""
Compute simple dataset size stats (bytes) for raw input files.

Writes results to results/metrics/dataset_sizes.json
"""
from __future__ import annotations

import json
from pathlib import Path

DATASETS = [
    "email-EuAll",
    "web-BerkStan",
    "soc-LiveJournal1",
]

ROOT = Path(".")
RAW = ROOT / "data" / "raw"
OUT_DIR = ROOT / "results" / "metrics"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT_DIR / "dataset_sizes.json"


def main() -> int:
    sizes = {}
    for d in DATASETS:
        path = RAW / f"{d}.txt"
        if not path.exists():
            sizes[d] = None
            continue
        sizes[d] = Path(path).stat().st_size
    OUT_FILE.write_text(json.dumps(sizes, indent=2), encoding="utf-8")
    print(f"Wrote {OUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
