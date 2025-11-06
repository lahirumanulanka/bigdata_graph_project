#!/usr/bin/env python3
"""
Run a command and record system performance metrics (CPU, memory, disk, network) at 1 Hz.

Outputs go to results/metrics/<system>/<dataset>/:
 - timeseries.csv
 - summary.json

Usage:
  python scripts/metrics/runner.py --system spark --dataset email-EuAll -- \
    powershell.exe -ExecutionPolicy Bypass -File scripts/spark/run_spark_job.ps1 -Dataset email-EuAll
"""
from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
from pathlib import Path

import psutil


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--system", required=True, choices=["spark", "hadoop"], help="System label")
    p.add_argument("--dataset", required=True, help="Dataset name")
    p.add_argument("--out-root", default="results/metrics", help="Root output directory")
    p.add_argument("--interval", type=float, default=1.0, help="Sampling interval seconds")
    p.add_argument("cmd", nargs=argparse.REMAINDER, help="Command to run (prefix with --)")
    args = p.parse_args()
    if not args.cmd:
        print("Missing command after --", file=sys.stderr)
        sys.exit(2)
    return args


def now_monotonic() -> float:
    return time.perf_counter()


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_root) / args.system / args.dataset
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "timeseries.csv"
    summary_path = out_dir / "summary.json"

    # Initialize psutil metrics
    psutil.cpu_percent(None)  # prime
    psutil.virtual_memory()
    io0 = psutil.disk_io_counters()
    net0 = psutil.net_io_counters()
    base_read = io0.read_bytes if io0 else 0
    base_write = io0.write_bytes if io0 else 0
    base_sent = net0.bytes_sent if net0 else 0
    base_recv = net0.bytes_recv if net0 else 0

    start_wall = now_monotonic()
    start_time = time.time()
    cmd = args.cmd[1:] if args.cmd and args.cmd[0] == "--" else args.cmd
    proc = subprocess.Popen(cmd)

    fields = [
        "t_sec",
        "cpu_percent",
        "mem_used_mb",
        "mem_percent",
        "disk_read_bytes",
        "disk_write_bytes",
        "net_sent_bytes",
        "net_recv_bytes",
    ]
    samples = 0
    max_mem_used_mb = 0.0
    peak_cpu = 0.0

    with csv_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(fields)
        while True:
            ret = proc.poll()
            t = now_monotonic() - start_wall
            cpu = psutil.cpu_percent(interval=None)
            mem = psutil.virtual_memory()
            io = psutil.disk_io_counters()
            net = psutil.net_io_counters()
            read_bytes = io.read_bytes if io else base_read
            write_bytes = io.write_bytes if io else base_write
            sent_bytes = net.bytes_sent if net else base_sent
            recv_bytes = net.bytes_recv if net else base_recv
            used_mb = (mem.total - mem.available) / (1024 * 1024)
            w.writerow([
                f"{t:.3f}",
                f"{cpu:.2f}",
                f"{used_mb:.2f}",
                f"{mem.percent:.2f}",
                read_bytes,
                write_bytes,
                sent_bytes,
                recv_bytes,
            ])
            samples += 1
            max_mem_used_mb = max(max_mem_used_mb, used_mb)
            peak_cpu = max(peak_cpu, cpu)
            if ret is not None:
                break
            time.sleep(max(args.interval, 0.2))

    end_wall = now_monotonic()
    end_time = time.time()
    io1 = psutil.disk_io_counters()
    net1 = psutil.net_io_counters()
    end_read = io1.read_bytes if io1 else base_read
    end_write = io1.write_bytes if io1 else base_write
    end_sent = net1.bytes_sent if net1 else base_sent
    end_recv = net1.bytes_recv if net1 else base_recv
    elapsed = end_wall - start_wall

    summary = {
        "system": args.system,
        "dataset": args.dataset,
        "start_epoch": start_time,
        "end_epoch": end_time,
        "elapsed_sec": elapsed,
        "avg_cpu_percent": None,  # compute in plotting or post if desired
        "peak_cpu_percent": peak_cpu,
        "max_mem_used_mb": max_mem_used_mb,
    "disk_read_delta_bytes": int(end_read - base_read),
    "disk_write_delta_bytes": int(end_write - base_write),
    "net_sent_delta_bytes": int(end_sent - base_sent),
    "net_recv_delta_bytes": int(end_recv - base_recv),
        "samples": samples,
    "cmd": cmd,
    }
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
