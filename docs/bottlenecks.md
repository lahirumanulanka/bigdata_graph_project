# Scaling Trends and Bottlenecks: Spark vs Hadoop

This report analyzes how performance changes as dataset size increases and highlights likely bottlenecks. It uses metrics collected in local Windows runs.

## Inputs and plots

- Dataset sizes (`results/metrics/dataset_sizes.json`):
  - email-EuAll ≈ 4.77 MB
  - web-BerkStan ≈ 105.1 MB
  - soc-LiveJournal1 ≈ 1.01 GB
- Scaling plots (`results/metrics/plots/`):
  - `scaling_elapsed.png` — elapsed time vs size (log x-axis)
  - `scaling_disk.png` — total disk I/O vs size
  - `scaling_network.png` — total network vs size
  - `scaling_memory.png` — max memory vs size

## Observations

1) Elapsed time vs size (scaling_elapsed.png)
- Hadoop shows faster wall time across all sizes in this environment.
- Spark has higher overhead for smaller datasets, and on Windows includes write-fallback overhead; the gap narrows for the largest dataset but remains.

2) Disk I/O vs size (scaling_disk.png)
- Disk I/O grows with data size for both systems.
- Hadoop shows higher total disk I/O at larger sizes, consistent with intermediate materialization between the two streaming jobs.
- Spark’s disk I/O remains lower, reflecting a single pipeline (plus write fallback), though startup/overhead costs impact elapsed time.

3) Network vs size (scaling_network.png)
- Totals remain very small because runs are local-mode; there is no distributed shuffle across hosts.
- Network overhead is not a bottleneck here.

4) Memory vs size (scaling_memory.png)
- Max memory sits around ~14–15 GB across datasets for both systems.
- Memory usage does not scale linearly with input size in these runs; it appears bounded by process/runtime behavior and the local environment.

## Likely bottlenecks (in this setup)

- Disk I/O: For larger inputs, disk becomes significant — especially for Hadoop due to stage boundaries. This aligns with the increase in total disk bytes for `soc-LiveJournal1`.
- Startup / orchestration overhead: Spark shows higher overhead at small-to-medium sizes (JVM startup, PySpark handshake, and on Windows, fallback path).
- CPU: Peaks below 90% and not consistently saturated; CPU is not the primary limiter.
- Network: Minimal in local runs; not a bottleneck.
- Memory: Max ~15 GB suggests headroom exists for these datasets; not the limiting factor here.

## Recommendations

Spark
- Ensure proper Hadoop binaries on Windows (set `HADOOP_HOME`) to avoid fallback and to reduce I/O quirks.
- Consider switching to DataFrames for potential planner optimizations.
- Increase parallelism (more partitions) for larger inputs; persist selectively to reduce repeated computation.

Hadoop Streaming
- Introduce combiners where appropriate (e.g., local aggregation of `(dst, 1)` before reducer) to reduce shuffle volume.
- Tune number of reducers; test a single reducer vs multiple for the histogram stage.
- Consider moving intermediate output to a faster disk or tempfs where feasible.

General
- Use SSD-backed storage; monitor per-disk metrics to confirm hotspots.
- On a real cluster, revisit network plots — distributed shuffle becomes significant, especially for large repartitions.
- Add average CPU from timeseries to the summary for a smoother utilization metric (currently only peak is summarized).

## Reproducing the plots

```powershell
python scripts/dataset_stats.py
python scripts/plot_scaling.py
```

Plots are written to `results/metrics/plots/`.
