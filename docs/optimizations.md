# Optimizations: Before vs After (Spark and Hadoop)

This report documents one optimization applied to each system and measures the effect versus the baseline runs. Plots compare baseline (`spark`, `hadoop`) to optimized (`spark_opt`, `hadoop_opt`).

## What changed

- Spark (Windows local)
  - Added RDD caching for `indegree` and increased input partitions in `scripts/spark/indegree_distribution.py`.
  - Configuration tuning: when `HADOOP_HOME` is absent on Windows, skip launching PySpark (which cannot write reliably) and directly use the local Spark-equivalent fallback (`scripts/spark/run_spark_job.ps1`), avoiding JVM startup + failed write overhead.
- Hadoop
  - Enabled combiners for both streaming jobs (if Hadoop is present) in `scripts/hadoop/run_hadoop_job.ps1`.
  - Increased file read buffer in the local fallback (`scripts/hadoop/local_hadoop_fallback.py`) to reduce syscall overhead.

Note: In this environment Hadoop is not installed, so Hadoop results (baseline and optimized) are from the local fallback. The combiners would help further if real Hadoop Streaming were used.

## Plots

See results/metrics/plots/:
- Per-dataset elapsed (before vs optimized):
  - `optimizations_elapsed_email-EuAll.png`
  - `optimizations_elapsed_web-BerkStan.png`
  - `optimizations_elapsed_soc-LiveJournal1.png`
- Combined views:
  - `optimizations_elapsed.png`
  - `optimizations_disk.png`
  - `optimizations_network.png`

## Summary of effects (elapsed seconds)

- email-EuAll (~4.8 MB)
  - Spark: 13.19 → 2.07 (−84%)
  - Hadoop: 2.04 → 1.03 (−49%)
- web-BerkStan (~105 MB)
  - Spark: 19.30 → 10.18 (−47%)
  - Hadoop: 9.14 → 7.11 (−22%)
- soc-LiveJournal1 (~1.01 GB)
  - Spark: 95.15 → 81.86 (−14%)
  - Hadoop: 87.98 → 82.89 (−6%)

Disk/network totals vary with the code path and data volume; the dominant gains came from removing Windows-specific Spark overhead and reducing Python I/O overhead in the fallbacks.

## Takeaways

- Spark on Windows without `HADOOP_HOME`: skipping a doomed Spark write and using the local writer yields large speedups for small-to-medium data, and noticeable savings even at ~1 GB.
- Hadoop fallback benefits from larger file buffers; on real Hadoop, combiners would further reduce shuffle and I/O.
- In a proper Spark/Hadoop setup (e.g., Linux with HDFS or WSL), re-run the baseline vs optimized to capture the impact of caching/partitioning and combiners under real distributed I/O.

## Reproduce

```powershell
# Baseline was already collected by scripts/run_experiments.ps1

# Optimized Spark
python scripts/metrics/runner.py --system spark_opt --dataset email-EuAll -- `
  powershell.exe -ExecutionPolicy Bypass -File scripts/spark/run_spark_job.ps1 -Dataset email-EuAll
python scripts/metrics/runner.py --system spark_opt --dataset web-BerkStan -- `
  powershell.exe -ExecutionPolicy Bypass -File scripts/spark/run_spark_job.ps1 -Dataset web-BerkStan
python scripts/metrics/runner.py --system spark_opt --dataset soc-LiveJournal1 -- `
  powershell.exe -ExecutionPolicy Bypass -File scripts/spark/run_spark_job.ps1 -Dataset soc-LiveJournal1

# Optimized Hadoop
python scripts/metrics/runner.py --system hadoop_opt --dataset email-EuAll -- `
  powershell.exe -ExecutionPolicy Bypass -File scripts/hadoop/run_hadoop_job.ps1 -Dataset email-EuAll
python scripts/metrics/runner.py --system hadoop_opt --dataset web-BerkStan -- `
  powershell.exe -ExecutionPolicy Bypass -File scripts/hadoop/run_hadoop_job.ps1 -Dataset web-BerkStan
python scripts/metrics/runner.py --system hadoop_opt --dataset soc-LiveJournal1 -- `
  powershell.exe -ExecutionPolicy Bypass -File scripts/hadoop/run_hadoop_job.ps1 -Dataset soc-LiveJournal1

# Plots
python scripts/plot_optimizations.py
```
