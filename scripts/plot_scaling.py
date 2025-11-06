#!/usr/bin/env python3
"""
Generate scaling plots comparing Spark vs Hadoop metrics as dataset size increases.

Reads:
  - results/metrics/dataset_sizes.json
  - results/metrics/<system>/<dataset>/summary.json

Writes plots to results/metrics/plots/:
  - scaling_elapsed.png
  - scaling_disk.png
  - scaling_network.png
  - scaling_memory.png
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt


ROOT = Path("results/metrics")
PLOTS = ROOT / "plots"
SYSTEMS = ["spark", "hadoop"]
DATASETS = [
    "email-EuAll",
    "web-BerkStan",
    "soc-LiveJournal1",
]


def load_sizes() -> Dict[str, int]:
    path = ROOT / "dataset_sizes.json"
    if not path.exists():
        raise SystemExit(f"Missing {path}. Run scripts/dataset_stats.py first.")
    return json.loads(path.read_text(encoding="utf-8"))


def load_summary(system: str, dataset: str) -> dict | None:
    path = ROOT / system / dataset / "summary.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    PLOTS.mkdir(parents=True, exist_ok=True)
    sizes_bytes = load_sizes()

    # Prepare series per system
    x_mb: Dict[str, List[float]] = {s: [] for s in SYSTEMS}
    elapsed: Dict[str, List[float]] = {s: [] for s in SYSTEMS}
    disk_gb: Dict[str, List[float]] = {s: [] for s in SYSTEMS}
    net_mb: Dict[str, List[float]] = {s: [] for s in SYSTEMS}
    mem_mb: Dict[str, List[float]] = {s: [] for s in SYSTEMS}

    # Use same dataset order for both systems
    for d in DATASETS:
        size_b = sizes_bytes.get(d)
        size_mb = (size_b / (1024 * 1024)) if size_b is not None else None
        for sysname in SYSTEMS:
            s = load_summary(sysname, d)
            if s is None or size_mb is None:
                continue
            x_mb[sysname].append(size_mb)
            elapsed[sysname].append(float(s.get("elapsed_sec", 0.0)))
            disk_total = float(s.get("disk_read_delta_bytes", 0.0)) + float(s.get("disk_write_delta_bytes", 0.0))
            disk_gb[sysname].append(disk_total / (1024 ** 3))
            net_total = float(s.get("net_sent_delta_bytes", 0.0)) + float(s.get("net_recv_delta_bytes", 0.0))
            net_mb[sysname].append(net_total / (1024 ** 2))
            mem_mb[sysname].append(float(s.get("max_mem_used_mb", 0.0)))

    # Helper to plot one metric vs size
    def plot_metric(name: str, ylabel: str, series: Dict[str, List[float]], logx: bool = True):
        plt.figure(figsize=(7, 5))
        for sysname, color, marker in [("spark", "tab:blue", "o"), ("hadoop", "tab:orange", "s")]:
            if not x_mb[sysname]:
                continue
            plt.plot(x_mb[sysname], series[sysname], label=sysname, color=color, marker=marker)
        plt.xlabel("Dataset size (MB)")
        plt.ylabel(ylabel)
        if logx:
            plt.xscale("log")
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()
        out = PLOTS / f"scaling_{name}.png"
        plt.savefig(out)
        plt.close()

    plot_metric("elapsed", "Elapsed time (s)", elapsed)
    plot_metric("disk", "Total disk I/O (GB)", disk_gb)
    plot_metric("network", "Total network (MB)", net_mb)
    plot_metric("memory", "Max memory (MB)", mem_mb, logx=False)

    print(f"Wrote scaling plots to {PLOTS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
