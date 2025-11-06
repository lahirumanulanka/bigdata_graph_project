#!/usr/bin/env python3
"""
Plot performance metrics for Spark vs Hadoop per dataset.

Reads metrics from results/metrics/<system>/<dataset>/{timeseries.csv,summary.json}
Produces per-dataset figures under results/metrics/plots/:
  - <dataset>_cpu_mem.png (CPU% and Memory MB over time)
  - <dataset>_io_net.png (Disk and Network bytes/sec over time)
  - <dataset>_summary.png (bar chart of elapsed time, total disk, total network)
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path("results/metrics")
PLOTS = ROOT / "plots"
SYSTEMS = ["spark", "hadoop"]
DATASETS = [
    "email-EuAll",
    "web-BerkStan",
    "soc-LiveJournal1",
]


def load_timeseries(system: str, dataset: str) -> pd.DataFrame | None:
    csv_path = ROOT / system / dataset / "timeseries.csv"
    if not csv_path.exists():
        return None
    df = pd.read_csv(csv_path)
    # Compute rates from cumulative bytes
    df = df.sort_values("t_sec")
    for col in ["disk_read_bytes", "disk_write_bytes", "net_sent_bytes", "net_recv_bytes"]:
        if col not in df.columns:
            df[col] = 0
    df["disk_bytes_total"] = df["disk_read_bytes"] + df["disk_write_bytes"]
    df["net_bytes_total"] = df["net_sent_bytes"] + df["net_recv_bytes"]
    for col in ["disk_bytes_total", "net_bytes_total"]:
        df[col + "_rate"] = df[col].diff().fillna(0) / df["t_sec"].diff().fillna(1)
        df[col + "_rate"] = df[col + "_rate"].clip(lower=0)
    return df


def load_summary(system: str, dataset: str) -> dict:
    path = ROOT / system / dataset / "summary.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def plot_dataset(dataset: str) -> None:
    PLOTS.mkdir(parents=True, exist_ok=True)
    dfs: Dict[str, pd.DataFrame] = {}
    sums: Dict[str, dict] = {}
    for sysname in SYSTEMS:
        dfs[sysname] = load_timeseries(sysname, dataset)
        sums[sysname] = load_summary(sysname, dataset)

    # CPU + Memory time series
    plt.figure(figsize=(10, 5))
    for sysname, color in [("spark", "tab:blue"), ("hadoop", "tab:orange")]:
        df = dfs.get(sysname)
        if df is None:
            continue
        plt.plot(df["t_sec"], df["cpu_percent"], label=f"{sysname} CPU%", color=color, alpha=0.8)
    ax = plt.gca()
    ax2 = ax.twinx()
    for sysname, color in [("spark", "tab:blue"), ("hadoop", "tab:orange")]:
        df = dfs.get(sysname)
        if df is None:
            continue
        ax2.plot(df["t_sec"], df["mem_used_mb"], label=f"{sysname} Mem MB", color=color, linestyle="--", alpha=0.7)
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("CPU (%)")
    ax2.set_ylabel("Memory (MB)")
    plt.title(f"CPU and Memory over time: {dataset}")
    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines + lines2, labels + labels2, loc="upper right")
    plt.tight_layout()
    out = PLOTS / f"{dataset}_cpu_mem.png"
    plt.savefig(out)
    plt.close()

    # Disk and Network rates
    plt.figure(figsize=(10, 5))
    for sysname, color in [("spark", "tab:blue"), ("hadoop", "tab:orange")]:
        df = dfs.get(sysname)
        if df is None:
            continue
        plt.plot(df["t_sec"], df["disk_bytes_total_rate"] / 1e6, label=f"{sysname} Disk MB/s", color=color, alpha=0.8)
    for sysname, color in [("spark", "tab:blue"), ("hadoop", "tab:orange")]:
        df = dfs.get(sysname)
        if df is None:
            continue
        plt.plot(df["t_sec"], df["net_bytes_total_rate"] / 1e6, label=f"{sysname} Net MB/s", color=color, linestyle=":", alpha=0.8)
    plt.xlabel("Time (s)")
    plt.ylabel("MB/s (Disk / Net)")
    plt.title(f"I/O rates over time: {dataset}")
    plt.legend()
    plt.tight_layout()
    out = PLOTS / f"{dataset}_io_net.png"
    plt.savefig(out)
    plt.close()

    # Summary bars
    systems_avail = [s for s in SYSTEMS if sums.get(s)]
    if not systems_avail:
        return
    elapsed = [sums[s]["elapsed_sec"] for s in systems_avail]
    disk_total = [(sums[s]["disk_read_delta_bytes"] + sums[s]["disk_write_delta_bytes"]) / 1e9 for s in systems_avail]
    net_total = [(sums[s]["net_sent_delta_bytes"] + sums[s]["net_recv_delta_bytes"]) / 1e6 for s in systems_avail]

    fig, axes = plt.subplots(1, 3, figsize=(12, 4))
    axes[0].bar(systems_avail, elapsed, color=["tab:blue", "tab:orange"])
    axes[0].set_title("Elapsed (s)")
    axes[1].bar(systems_avail, disk_total, color=["tab:blue", "tab:orange"])
    axes[1].set_title("Total Disk (GB)")
    axes[2].bar(systems_avail, net_total, color=["tab:blue", "tab:orange"])
    axes[2].set_title("Total Net (MB)")
    fig.suptitle(f"Performance summary: {dataset}")
    plt.tight_layout()
    out = PLOTS / f"{dataset}_summary.png"
    plt.savefig(out)
    plt.close()


def main() -> int:
    for d in DATASETS:
        plot_dataset(d)
    print(f"Wrote plots to {PLOTS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
