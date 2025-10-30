#!/usr/bin/env python3
"""
Plot performance metrics comparison between Spark and Hadoop.

Reads /data/metrics/summary.csv produced by aggregate_metrics.py and creates
bar charts in /data/plots/metrics_*.png.
"""
from __future__ import annotations

import csv
from pathlib import Path
import os
from typing import Dict, List, cast

import matplotlib.pyplot as plt
import numpy as np

# Allow overriding the data root to support running outside containers.
DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/data"))
SUMMARY_DIR = DATA_ROOT / "metrics"
SUMMARY = SUMMARY_DIR / "summary.csv"
PLOTS = DATA_ROOT / "results" / "plots"


def pick_summary_file() -> Path:
    # Prefer main summary.csv, but if an alt exists and is newer, use it.
    candidates = list(SUMMARY_DIR.glob("summary*.csv"))
    if not candidates:
        return SUMMARY
    # Pick newest by modified time
    newest = max(candidates, key=lambda p: p.stat().st_mtime)
    return newest


def load_summary() -> List[Dict[str, str]]:
    summary_path = pick_summary_file()
    if not summary_path.exists():
        raise SystemExit(f"Missing summary CSV: {summary_path}. Run aggregate_metrics.py first.")
    with summary_path.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"Loaded {summary_path}")
    return rows


def to_float(s: str) -> float | None:
    try:
        return float(s)
    except Exception:
        return None


def group_by_dataset(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, Dict[str, float | None]]]:
    # structure: dataset -> framework_phase -> metric -> value
    out: Dict[str, Dict[str, Dict[str, float | None]]] = {}
    for r in rows:
        ds = r["dataset"]
        fw = r["framework"]
        ph = r["phase"]
        key = f"{fw}_{ph}"
        out.setdefault(ds, {})[key] = {
            "elapsed_seconds": to_float(r.get("elapsed_seconds", "")),
            "max_rss_kb": to_float(r.get("max_rss_kb", "")),
            "avg_cpu_util": to_float(r.get("avg_cpu_util", "")),
            "avg_dsk_read_kbps": to_float(r.get("avg_dsk_read_kbps", "")),
            "avg_dsk_writ_kbps": to_float(r.get("avg_dsk_writ_kbps", "")),
            "avg_net_recv_kbps": to_float(r.get("avg_net_recv_kbps", "")),
            "avg_net_send_kbps": to_float(r.get("avg_net_send_kbps", "")),
        }
    return out


def hadoop_total(ds_map: Dict[str, Dict[str, float | None]]) -> Dict[str, float | None]:
    # Sum indegree + distribution for time; take max for RSS; average CPU/util as mean of available
    phases = [v for k, v in ds_map.items() if k.startswith("hadoop_")]
    if not phases:
        return {}
    def safe_sum(key: str) -> float | None:
        vals = cast(List[float], [v.get(key) for v in phases if v.get(key) is not None])
        return sum(vals) if vals else None
    def safe_max(key: str) -> float | None:
        vals = cast(List[float], [v.get(key) for v in phases if v.get(key) is not None])
        return max(vals) if vals else None
    def safe_avg(key: str) -> float | None:
        vals = cast(List[float], [v.get(key) for v in phases if v.get(key) is not None])
        return (sum(vals) / len(vals)) if vals else None
    return {
        "elapsed_seconds": safe_sum("elapsed_seconds"),
        "max_rss_kb": safe_max("max_rss_kb"),
        "avg_cpu_util": safe_avg("avg_cpu_util"),
        "avg_dsk_read_kbps": safe_avg("avg_dsk_read_kbps"),
        "avg_dsk_writ_kbps": safe_avg("avg_dsk_writ_kbps"),
        "avg_net_recv_kbps": safe_avg("avg_net_recv_kbps"),
        "avg_net_send_kbps": safe_avg("avg_net_send_kbps"),
    }


def bar_plot(metric: str, title: str, ylabel: str, data: Dict[str, Dict[str, Dict[str, float | None]]]) -> None:
    PLOTS.mkdir(parents=True, exist_ok=True)
    datasets = sorted(data.keys())
    spark_vals: List[float] = []
    hadoop_vals: List[float] = []

    for ds in datasets:
        ds_map = data[ds]
        spark_v = (ds_map.get("spark_job", {}) or {}).get(metric)
        had_tot = hadoop_total(ds_map)
        hadoop_v = had_tot.get(metric) if had_tot else None
        spark_vals.append(spark_v if spark_v is not None else 0.0)
        hadoop_vals.append(hadoop_v if hadoop_v is not None else 0.0)

    x = np.arange(len(datasets))
    width = 0.35
    plt.figure(figsize=(max(7, len(datasets) * 1.5), 5))
    plt.bar(x - width/2, spark_vals, width, label="Spark")
    plt.bar(x + width/2, hadoop_vals, width, label="Hadoop (total)")
    plt.xticks(x, datasets, rotation=20)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.legend()
    plt.tight_layout()
    out = PLOTS / f"metrics_{metric}.png"
    plt.savefig(out)
    plt.close()
    print(f"Wrote {out}")


def main() -> int:
    rows = load_summary()
    grouped = group_by_dataset(rows)
    bar_plot("elapsed_seconds", "Execution time (lower is better)", "seconds", grouped)
    bar_plot("max_rss_kb", "Peak memory (approx, Max RSS)", "kB", grouped)
    bar_plot("avg_cpu_util", "Average CPU utilization (from dstat)", "%", grouped)
    bar_plot("avg_dsk_read_kbps", "Disk read throughput (avg)", "kB/s", grouped)
    bar_plot("avg_dsk_writ_kbps", "Disk write throughput (avg)", "kB/s", grouped)
    bar_plot("avg_net_recv_kbps", "Network receive (avg)", "kB/s", grouped)
    bar_plot("avg_net_send_kbps", "Network send (avg)", "kB/s", grouped)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
