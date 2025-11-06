#!/usr/bin/env python3
import sys


def emit(key, total):
    if key is None:
        return
    sys.stdout.write(f"{key}\t{total}\n")


def main():
    current_key = None
    current_total = 0
    for line in sys.stdin:
        s = line.strip()
        if not s:
            continue
        parts = s.split('\t')
        if len(parts) != 2:
            # try space split
            parts = s.split()
            if len(parts) != 2:
                continue
        key, val = parts[0], parts[1]
        try:
            cnt = int(val)
        except ValueError:
            cnt = 0
        if current_key is None:
            current_key = key
            current_total = cnt
            continue
        if key == current_key:
            current_total += cnt
        else:
            emit(current_key, current_total)
            current_key = key
            current_total = cnt
    emit(current_key, current_total)


if __name__ == "__main__":
    main()
