#!/usr/bin/env python3
"""
Local fallback to mimic Hadoop Streaming pipeline when Hadoop is not available.
Computes per-node indegree and degree distribution from an edge list.

Usage:
  python scripts/hadoop/local_hadoop_fallback.py --input data/raw/email-EuAll.txt --out results/hadoop/email-EuAll
"""
from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="Edge list input path")
    p.add_argument("--out", required=True, help="Output root (will write indegree and distribution subfolders)")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    indeg = Counter()
    input_path = Path(args.input)
    with input_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith('#'):
                continue
            parts = s.split()
            if len(parts) < 2:
                continue
            dst = parts[1]
            indeg[dst] += 1

    out_root = Path(args.out)
    out_indeg = out_root / "indegree"
    out_dist = out_root / "distribution"
    out_indeg.mkdir(parents=True, exist_ok=True)
    out_dist.mkdir(parents=True, exist_ok=True)

    # Write single part file to mimic Hadoop
    with (out_indeg / "part-r-00000").open("w", encoding="utf-8") as w:
        for node, d in sorted(indeg.items(), key=lambda kv: kv[0]):
            w.write(f"{node}\t{d}\n")

    hist = Counter()
    for d in indeg.values():
        hist[d] += 1
    with (out_dist / "part-r-00000").open("w", encoding="utf-8") as w:
        for d, c in sorted(hist.items(), key=lambda kv: kv[0]):
            w.write(f"{d}\t{c}\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
