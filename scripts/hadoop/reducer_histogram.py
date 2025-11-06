#!/usr/bin/env python3
import sys


def emit(k, total):
    if k is None:
        return
    sys.stdout.write(f"{k}\t{total}\n")


def main():
    current_k = None
    current_total = 0
    for line in sys.stdin:
        s = line.strip()
        if not s:
            continue
        parts = s.split('\t')
        if len(parts) != 2:
            parts = s.split()
            if len(parts) != 2:
                continue
        k, v = parts[0], parts[1]
        try:
            cnt = int(v)
        except ValueError:
            cnt = 0
        if current_k is None:
            current_k = k
            current_total = cnt
            continue
        if k == current_k:
            current_total += cnt
        else:
            emit(current_k, current_total)
            current_k = k
            current_total = cnt
    emit(current_k, current_total)


if __name__ == "__main__":
    main()
