#!/usr/bin/env python3
import sys


def main():
    for line in sys.stdin:
        s = line.strip()
        if not s or s.startswith('#'):
            continue
        toks = s.split()
        if len(toks) < 2:
            continue
        dst = toks[1]
        # Emit destination node with count 1
        sys.stdout.write(f"{dst}\t1\n")


if __name__ == "__main__":
    main()
