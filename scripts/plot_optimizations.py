#!/usr/bin/env python3
"""
Plot before/after (baseline vs optimized) metrics for Spark and Hadoop.

Reads:
  - results/metrics/<system>/<dataset>/summary.json for system in {spark,hadoop}
  - results/metrics/<system>_opt/<dataset>/summary.json for optimized runs

Outputs:
  - results/metrics/plots/optimizations_elapsed.png
  - results/metrics/plots/optimizations_disk.png
  - results/metrics/plots/optimizations_network.png
"""
from __future__ import annotations

import json
from pathlib import Path
import matplotlib.pyplot as plt


ROOT = Path("results/metrics")
PLOTS = ROOT / "plots"
DATASETS = ["email-EuAll", "web-BerkStan", "soc-LiveJournal1"]


def load_summary(system: str, dataset: str) -> dict | None:
    path = ROOT / system / dataset / "summary.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def series_for(metric_key: str):
    # Returns values by dataset for: spark, spark_opt, hadoop, hadoop_opt
    values = {k: [] for k in ["spark", "spark_opt", "hadoop", "hadoop_opt"]}
    for d in DATASETS:
        for sys in values.keys():
            s = load_summary(sys, d)
            if not s:
                values[sys].append(None)
                continue
            if metric_key == "elapsed_sec":
                values[sys].append(float(s.get("elapsed_sec", 0.0)))
            elif metric_key == "disk_total_gb":
                v = float(s.get("disk_read_delta_bytes", 0.0)) + float(s.get("disk_write_delta_bytes", 0.0))
                values[sys].append(v / (1024 ** 3))
            elif metric_key == "network_total_mb":
                v = float(s.get("net_sent_delta_bytes", 0.0)) + float(s.get("net_recv_delta_bytes", 0.0))
                values[sys].append(v / (1024 ** 2))
            else:
                values[sys].append(None)
    return values


def plot_elapsed():
    vals = series_for("elapsed_sec")
    PLOTS.mkdir(parents=True, exist_ok=True)
    for d_idx, d in enumerate(DATASETS):
        plt.figure(figsize=(6, 4))
        cats = ["spark", "spark_opt", "hadoop", "hadoop_opt"]
        y = [vals[c][d_idx] for c in cats]
        colors = ["tab:blue", "tab:blue", "tab:orange", "tab:orange"]
        alphas = [0.6, 0.95, 0.6, 0.95]
        bars = plt.bar(cats, y, color=colors)
        for b, a in zip(bars, alphas):
            b.set_alpha(a)
        plt.ylabel("Elapsed (s)")
        plt.title(f"Before vs Optimized — {d}")
        plt.tight_layout()
        out = PLOTS / f"optimizations_elapsed_{d}.png"
        plt.savefig(out)
        plt.close()

    # Combined view across datasets
    plt.figure(figsize=(8, 5))
    for sys, color, m in [("spark", "tab:blue", "o"), ("spark_opt", "tab:blue", "^"), ("hadoop", "tab:orange", "s"), ("hadoop_opt", "tab:orange", "D")]:
        y = vals[sys]
        x = list(range(len(DATASETS)))
        plt.plot(x, y, marker=m, color=color, label=sys, alpha=0.9 if sys.endswith("opt") else 0.7)
    plt.xticks(range(len(DATASETS)), DATASETS, rotation=15)
    plt.ylabel("Elapsed (s)")
    plt.title("Elapsed time before vs optimized")
    plt.legend()
    plt.tight_layout()
    plt.savefig(PLOTS / "optimizations_elapsed.png")
    plt.close()


def plot_disk_network():
    for key, label, fname in [
        ("disk_total_gb", "Total disk (GB)", "optimizations_disk.png"),
        ("network_total_mb", "Total network (MB)", "optimizations_network.png"),
    ]:
        vals = series_for(key)
        plt.figure(figsize=(8, 5))
        for sys, color, m in [("spark", "tab:blue", "o"), ("spark_opt", "tab:blue", "^"), ("hadoop", "tab:orange", "s"), ("hadoop_opt", "tab:orange", "D")]:
            y = vals[sys]
            x = list(range(len(DATASETS)))
            plt.plot(x, y, marker=m, color=color, label=sys, alpha=0.9 if sys.endswith("opt") else 0.7)
        plt.xticks(range(len(DATASETS)), DATASETS, rotation=15)
        plt.ylabel(label)
        plt.title(f"{label} before vs optimized")
        plt.legend()
        plt.tight_layout()
        plt.savefig(PLOTS / fname)
        plt.close()


def main() -> int:
    plot_elapsed()
    plot_disk_network()
    print(f"Wrote optimization plots to {PLOTS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
