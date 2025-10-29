#!/usr/bin/env python3
"""
Clean SNAP raw datasets from /data/raw and write cleaned edge lists to /data/cleaned.

Rules:
- Skip empty lines and lines starting with common comment prefixes (#, %, ");
- Split on any whitespace; take the first two tokens as src and dst;
- Write as "src dst" (space separated) per line.

Outputs one .edges file per input file using the same basename, e.g. foo.txt -> foo.edges
"""
from __future__ import annotations

import sys
from pathlib import Path


COMMENT_PREFIXES = ("#", "%", ";")


def clean_file(src_path: Path, dst_path: Path) -> tuple[int, int]:
    """Return (written_lines, skipped_lines)."""
    written = 0
    skipped = 0
    with src_path.open("r", encoding="utf-8", errors="ignore") as fin, dst_path.open(
        "w", encoding="utf-8"
    ) as fout:
        for line in fin:
            s = line.strip()
            if not s:
                skipped += 1
                continue
            if s.startswith(COMMENT_PREFIXES):
                skipped += 1
                continue
            parts = s.split()
            if len(parts) < 2:
                skipped += 1
                continue
            src, dst = parts[0], parts[1]
            fout.write(f"{src} {dst}\n")
            written += 1
    return written, skipped


def main() -> int:
    raw_dir = Path("/data/raw")
    out_dir = Path("/data/cleaned")
    out_dir.mkdir(parents=True, exist_ok=True)

    if not raw_dir.exists():
        print("/data/raw does not exist. Did you run fetch_snap.sh?", file=sys.stderr)
        return 1

    inputs = sorted([p for p in raw_dir.iterdir() if p.is_file()])
    if not inputs:
        print("No input files found in /data/raw", file=sys.stderr)
        return 1

    total_written = 0
    total_skipped = 0
    for p in inputs:
        out_name = p.with_suffix("").name + ".edges"
        out_path = out_dir / out_name
        written, skipped = clean_file(p, out_path)
        total_written += written
        total_skipped += skipped
        print(f"Cleaned {p.name}: wrote {written:,} edges (skipped {skipped:,}) -> {out_path.name}")

    print(f"Done. Total edges written: {total_written:,}. Total skipped lines: {total_skipped:,}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
