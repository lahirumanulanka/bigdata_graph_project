# Scalability Check: soc-LiveJournal1

This note documents running the pipeline on the largest dataset provided in this repo: `soc-LiveJournal1`.

## Dataset size

Raw file size (bytes), computed by `scripts/dataset_stats.py` and stored in `results/metrics/dataset_sizes.json`:

- email-EuAll: 5,001,840 bytes (~4.77 MB)
- web-BerkStan: 110,135,305 bytes (~105.1 MB)
- soc-LiveJournal1: 1,080,598,042 bytes (~1.01 GB)

`soc-LiveJournal1` is ~10x bigger than `web-BerkStan` and ~216x bigger than `email-EuAll` in raw bytes.

## Run outputs and plots

After running `scripts/run_experiments.ps1`, outputs exist at:

- Spark: `results/spark/soc-LiveJournal1/{indegree,distribution}/...`
- Hadoop: `results/hadoop/soc-LiveJournal1/{indegree,distribution}/...`

Performance plots for the dataset are in `results/metrics/plots/`:

- `soc-LiveJournal1_cpu_mem.png`
- `soc-LiveJournal1_io_net.png`
- `soc-LiveJournal1_summary.png`

## Observed run metrics (from summary.json)

Spark (local with Windows fallback for writes):

- Elapsed: ~95.15 s
- Peak CPU: 70.2%
- Max Memory: ~15.0 GB
- Disk total: ~97.8 MB
- Network total: ~0.81 MB

Hadoop (local fallback implementation):

- Elapsed: ~87.98 s
- Peak CPU: 67.4%
- Max Memory: ~15.0 GB
- Disk total: ~118.6 MB
- Network total: ~0.55 MB

## Correctness

Validated with `scripts/validate_results.py`: Spark and Hadoop outputs match exactly for both indegree and distribution on this dataset.

## Notes

- On this Windows setup without `HADOOP_HOME`, PySpark starts and attempts to save, then the runner falls back to a local writer for outputs. The measured Spark time therefore includes startup and fallback overhead.
- Both implementations integrated cleanly at this 1+ GB input scale without code changes. Memory peaked around 15 GB.

> Next step (separate task): evaluate scaling across dataset sizes and identify bottlenecks (disk I/O, memory, network shuffle) with plots and a comparative analysis.
