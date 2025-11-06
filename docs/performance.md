# Execution Performance: Spark vs Hadoop

This document compares execution performance for Spark and Hadoop (or their local fallbacks when the runtime is unavailable) across three datasets. Metrics were collected by `scripts/metrics/runner.py` during `scripts/run_experiments.ps1` and plotted with `scripts/plot_metrics.py`.

Artifacts per dataset and system are at `results/metrics/<system>/<dataset>/`:
- `summary.json` (elapsed time, I/O totals, peak CPU, max memory)
- `timeseries.csv` (1 Hz samples of CPU %, memory MB, disk and network bytes)

Plots are under `results/metrics/plots/`:
- `<dataset>_cpu_mem.png` — CPU and Memory over time
- `<dataset>_io_net.png` — Disk and Network MB/s over time
- `<dataset>_summary.png` — Elapsed time, total disk (GB), total network (MB)

## Dataset: email-EuAll

- Spark:
  - Elapsed: 13.19 s
  - Disk total: 15,481,856 bytes ≈ 14.77 MB
  - Network total: 60,157 bytes ≈ 0.06 MB
  - Peak CPU: 63.9%
  - Max Memory: 14,854.41 MB
- Hadoop:
  - Elapsed: 2.04 s
  - Disk total: 9,979,904 bytes ≈ 9.52 MB
  - Network total: 242 bytes ≈ 0.00 MB
  - Peak CPU: 42.9%
  - Max Memory: 14,329.78 MB

See plots: `results/metrics/plots/email-EuAll_summary.png`

## Dataset: web-BerkStan

- Spark:
  - Elapsed: 19.30 s
  - Disk total: 21,743,616 bytes ≈ 20.74 MB
  - Network total: 320,952 bytes ≈ 0.31 MB
  - Peak CPU: 89.7%
  - Max Memory: 14,802.35 MB
- Hadoop:
  - Elapsed: 9.14 s
  - Disk total: 27,214,336 bytes ≈ 25.96 MB
  - Network total: 227,836 bytes ≈ 0.23 MB
  - Peak CPU: 75.0%
  - Max Memory: 14,391.70 MB

See plots: `results/metrics/plots/web-BerkStan_summary.png`

## Dataset: soc-LiveJournal1

- Spark:
  - Elapsed: 95.15 s
  - Disk total: 102,498,304 bytes ≈ 97.77 MB
  - Network total: 851,021 bytes ≈ 0.81 MB
  - Peak CPU: 70.2%
  - Max Memory: 14,999.77 MB
- Hadoop:
  - Elapsed: 87.98 s
  - Disk total: 124,263,424 bytes ≈ 118.56 MB
  - Network total: 572,411 bytes ≈ 0.55 MB
  - Peak CPU: 67.4%
  - Max Memory: 15,028.49 MB

See plots: `results/metrics/plots/soc-LiveJournal1_summary.png`

## Observations

- End-to-end time: Hadoop was faster on all three datasets in this Windows local setup. For small to medium inputs, Spark’s JVM startup + job setup overhead can dominate.
- Memory: Both approaches peaked at ~14–15 GB resident (Python + JVM). With larger inputs, Spark can trade memory vs spill depending on configuration.
- I/O: Hadoop shows higher total disk I/O on larger data due to the two-stage pipeline; Spark’s I/O depends on persistence and the number of shuffles.
- CPU profiles: Spark often spikes CPU during RDD transformations; Hadoop’s two jobs split CPU over two phases.

## Important context

- On this Windows host, PySpark could not save to the filesystem without `HADOOP_HOME`; the runner then executed a local Spark-equivalent fallback to write the outputs. Therefore the measured Spark times include Spark startup + error + fallback compute/write. On a properly configured Spark environment (or on Linux with Hadoop binaries), Spark write overhead would differ.
- All runs were single-machine, local-mode comparisons — not distributed cluster benchmarks. Cluster behavior (scheduler overhead, network shuffle, HDFS locality) will change the trade-offs.

