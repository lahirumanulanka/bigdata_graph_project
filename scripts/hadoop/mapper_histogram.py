#!/usr/bin/env python3
import sys


def main():
    for line in sys.stdin:
        s = line.strip()
        if not s:
            continue
        parts = s.split('\t')
        if len(parts) != 2:
            parts = s.split()
            if len(parts) != 2:
                continue
        # parts: node    indegree
        try:
            indeg = int(parts[1])
        except ValueError:
            continue
        sys.stdout.write(f"{indeg}\t1\n")


if __name__ == "__main__":
    main()
