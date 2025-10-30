import csv
from pathlib import Path


def load_summary(path: Path):
    with path.open() as f:
        return list(csv.DictReader(f))


def to_float(v: str) -> float:
    try:
        return float(v) if v else 0.0
    except ValueError:
        return 0.0


def main():
    rows = load_summary(Path("/data/metrics/summary.csv"))
    hadoop = {}
    spark = {}
    for r in rows:
        ds = r["dataset"]
        fw = r["framework"]
        metrics = {
            "elapsed_seconds": to_float(r["elapsed_seconds"]),
            "max_rss_kb": to_float(r["max_rss_kb"]),
            "cpu_user_s": to_float(r["cpu_user_s"]),
            "cpu_sys_s": to_float(r["cpu_sys_s"]),
            "avg_cpu_util": to_float(r["avg_cpu_util"]),
            "avg_mem_used_mb": to_float(r["avg_mem_used_mb"]),
            "avg_dsk_read_kbps": to_float(r["avg_dsk_read_kbps"]),
            "avg_dsk_writ_kbps": to_float(r["avg_dsk_writ_kbps"]),
            "avg_net_recv_kbps": to_float(r["avg_net_recv_kbps"]),
            "avg_net_send_kbps": to_float(r["avg_net_send_kbps"]),
        }
        if fw == "hadoop":
            agg = hadoop.setdefault(ds, {k: 0.0 for k in metrics})
            for k, v in metrics.items():
                if k.startswith("avg_"):
                    sumk = k + "_sum"
                    cntk = k + "_cnt"
                    agg[sumk] = agg.get(sumk, 0.0) + (v if v else 0.0)
                    agg[cntk] = agg.get(cntk, 0) + (1 if v else 0)
                else:
                    agg[k] += v
        elif fw == "spark":
            spark[ds] = metrics

    # finalize Hadoop averages
    for ds, agg in hadoop.items():
        for k in list(agg.keys()):
            if k.endswith("_sum"):
                base = k[:-4]
                cnt = agg.get(base + "_cnt", 0)
                agg[base] = (agg[k] / cnt) if cnt else 0.0
        for k in [x for x in list(agg.keys()) if x.endswith("_sum") or x.endswith("_cnt")]:
            agg.pop(k, None)

    print("Performance comparison (Spark vs Hadoop totals)\n")
    for ds in sorted(set(list(hadoop.keys()) + list(spark.keys()))):
        h = hadoop.get(ds)
        s = spark.get(ds)
        if not h or not s:
            continue
        print(f"- {ds}:")
        he = h["elapsed_seconds"]
        se = s["elapsed_seconds"]
        faster = "Spark faster" if se < he else ("Hadoop faster" if he < se else "Tie")
        diff = he - se
        print(f"    elapsed: Spark {se:.2f}s vs Hadoop {he:.2f}s -> {faster} (diff {diff:+.2f}s)")
        print(
            f"    mem (max RSS): Spark {s['max_rss_kb']:.0f} KB; Hadoop sum {h['max_rss_kb']:.0f} KB (note: use per-phase max in deeper analysis)"
        )
        print(
            f"    avg CPU util: Spark {s['avg_cpu_util']:.2f}%; Hadoop ~{h['avg_cpu_util']:.2f}% (phase-mean)"
        )
        print(
            f"    avg disk r/w (kB/s): Spark {s['avg_dsk_read_kbps']:.2f}/{s['avg_dsk_writ_kbps']:.2f}; Hadoop ~{h.get('avg_dsk_read_kbps',0.0):.2f}/{h.get('avg_dsk_writ_kbps',0.0):.2f}"
        )
        print(
            f"    avg net rx/tx (kB/s): Spark {s['avg_net_recv_kbps']:.2f}/{s['avg_net_send_kbps']:.2f}; Hadoop ~{h.get('avg_net_recv_kbps',0.0):.2f}/{h.get('avg_net_send_kbps',0.0):.2f}"
        )
        print()


if __name__ == "__main__":
    main()
