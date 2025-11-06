#!/usr/bin/env python3
"""
Plot in-degree distributions (degree vs count) on linear and log-log scales.

Reads outputs from:
- /results/spark/<dataset>/distribution/part-*
- /results/hadoop/<dataset>/distribution/part-*

Writes PNG plots to /results/plots.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt


# Prefer container mount (/data/results); fall back to local ./results
RESULTS = Path("/data/results") if Path("/data/results").exists() else Path("results")
PLOTS = RESULTS / "plots"


def read_tsv_dir(dir_path: Path) -> List[Tuple[int, int]]:
    data: Dict[int, int] = {}
    if not dir_path.exists():
        return []
    for part in sorted(dir_path.glob("part-*")):
        with part.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                kv = s.split()
                if len(kv) < 2:
                    continue
                try:
                    k = int(kv[0])
                    v = int(kv[1])
                except ValueError:
                    continue
                data[k] = data.get(k, 0) + v
    items = sorted(data.items())
    return items


def _find_spark_dist_dir(dataset: str) -> Path:
    """Return the Spark distribution directory for a dataset.
    If exact match is missing, try a prefix match like soc-LiveJournal1_*.
    """
    exact = RESULTS / "spark" / dataset / "distribution"
    if exact.exists():
        return exact
    # Fallback: any directory starting with dataset
    spark_base = RESULTS / "spark"
    if spark_base.exists():
        for d in sorted(spark_base.iterdir()):
            if d.is_dir() and d.name.startswith(dataset):
                cand = d / "distribution"
                if cand.exists():
                    return cand
    return exact


def plot_dataset(name: str) -> None:
    spark_dir = _find_spark_dist_dir(name)
    # Hadoop distributions (standardized under /data/results/hadoop)
    hadoop_dir1 = RESULTS / "hadoop" / name / "distribution"
    # Legacy fallback when outputs were under /results/hadoop on host
    hadoop_dir2 = Path("/results/hadoop") / name / "distribution"
    spark_data = read_tsv_dir(spark_dir)
    hadoop_data = read_tsv_dir(hadoop_dir1) or read_tsv_dir(hadoop_dir2)
    if not spark_data and not hadoop_data:
        print(f"No data for {name}")
        return

    PLOTS.mkdir(parents=True, exist_ok=True)

    def _plot(loglog: bool, suffix: str):
        plt.figure(figsize=(7, 5))
        if spark_data:
            x, y = zip(*spark_data)
            plt.scatter(x, y, s=10, label="Spark", alpha=0.7)
        if hadoop_data:
            x, y = zip(*hadoop_data)
            plt.scatter(x, y, s=10, label="Hadoop", alpha=0.7)
        if loglog:
            plt.xscale("log")
            plt.yscale("log")
        plt.xlabel("In-degree")
        plt.ylabel("#Nodes")
        plt.title(f"In-degree distribution: {name}{' (log-log)' if loglog else ''}")
        plt.legend()
        plt.tight_layout()
        out = PLOTS / f"{name}{suffix}.png"
        plt.savefig(out)
        plt.close()
        print(f"Wrote {out}")

    _plot(False, "")
    _plot(True, "_loglog")

    # Also generate Spark-only and Hadoop-only plots for clarity
    def _plot_single(series: list[tuple[int, int]], label: str, color: str, loglog: bool, out_name: str):
        if not series:
            return
        plt.figure(figsize=(7, 5))
        x, y = zip(*series)
        plt.scatter(x, y, s=10, label=label, alpha=0.8, color=color)
        if loglog:
            plt.xscale("log")
            plt.yscale("log")
        plt.xlabel("In-degree")
        plt.ylabel("#Nodes")
        plt.title(f"{label} in-degree: {name}{' (log-log)' if loglog else ''}")
        plt.legend()
        plt.tight_layout()
        out = PLOTS / out_name
        plt.savefig(out)
        plt.close()
        print(f"Wrote {out}")

    _plot_single(spark_data, "Spark", "tab:blue", False, f"{name}_spark.png")
    _plot_single(spark_data, "Spark", "tab:blue", True, f"{name}_spark_loglog.png")
    _plot_single(hadoop_data, "Hadoop", "tab:orange", False, f"{name}_hadoop.png")
    _plot_single(hadoop_data, "Hadoop", "tab:orange", True, f"{name}_hadoop_loglog.png")


def main(argv: list[str]) -> int:
    datasets = [
        "soc-Pokec-relationships",
        "email-EuAll",
        "web-BerkStan",
        "soc-LiveJournal1",
    ]
    for d in datasets:
        plot_dataset(d)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
