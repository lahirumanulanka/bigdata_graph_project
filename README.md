# In-degree Distribution with Hadoop (MapReduce) and Spark

This project computes per-node in-degree and the in-degree distribution for large directed graphs using two separate implementations:

- Apache Spark (PySpark)
- Apache Hadoop (MapReduce via Hadoop Streaming)

It runs on Windows using local mode for Spark and Hadoop. If Hadoop is not installed, a local fallback produces the same outputs in the expected Hadoop folder structure.

## Data

Place SNAP-like edge list files under `data/raw/` (already provided in this repo):

- `data/raw/email-EuAll.txt`
- `data/raw/web-BerkStan.txt`
- `data/raw/soc-LiveJournal1.txt`

Each file is a whitespace-separated edge list with optional comment lines starting with `#`.

## Quick start (Windows PowerShell)

Prerequisites:

- Python 3.8+
- Java 8+ (required by PySpark)
- Optional: Hadoop installed (for true Hadoop Streaming runs); otherwise a local fallback will be used.

Steps:

1) Install Python dependencies

```
python -m pip install -r requirements.txt
```

2) Run experiments on three datasets (Spark + Hadoop)

```
pwsh scripts/run_experiments.ps1
```

This will write outputs to:

- Spark: `results/spark/<dataset>/{indegree,distribution}/part-*`
- Hadoop: `results/hadoop/<dataset>/{indegree,distribution}/part-r-00000`

If Hadoop is not present, the script will run a local fallback to generate the Hadoop-style outputs.

3) Validate and plot (optional)

```
python scripts/validate_results.py
python scripts/plot_distributions.py
```

Plots are written to `results/plots`.

## Performance metrics and plots

While running `scripts/run_experiments.ps1`, the pipeline measures process-level performance for each system (Spark and Hadoop or their local fallbacks):

- CPU usage (%), memory usage (MB) over time
- Disk I/O and network bytes over time (derived as rates)
- Summary totals and elapsed time

Artifacts are saved under `results/metrics/<system>/<dataset>/`:

- `timeseries.csv`: sampled metrics (1 Hz)
- `summary.json`: totals and command info

After a run, generate comparison plots with:

```
python scripts/plot_metrics.py
```

The following figures are written to `results/metrics/plots/` for each dataset:

- `<dataset>_cpu_mem.png`: CPU% and Memory over time
- `<dataset>_io_net.png`: Disk and Network MB/s over time
- `<dataset>_summary.png`: Elapsed time, total disk (GB), total network (MB)

## Running jobs manually

Spark (example for `email-EuAll`):

```
python scripts/spark/indegree_distribution.py --dataset email-EuAll
```

Hadoop Streaming (requires Hadoop and streaming jar):

```
# Job 1: indegree per node
hadoop jar %HADOOP_HOME%\share\hadoop\tools\lib\hadoop-streaming-*.jar ^
	-D mapreduce.job.reduces=1 ^
	-input data/raw/email-EuAll.txt ^
	-output results/hadoop/email-EuAll/indegree ^
	-mapper "python scripts/hadoop/mapper_in_degree.py" ^
	-reducer "python scripts/hadoop/reducer_in_degree.py"

# Job 2: distribution
hadoop jar %HADOOP_HOME%\share\hadoop\tools\lib\hadoop-streaming-*.jar ^
	-D mapreduce.job.reduces=1 ^
	-input results/hadoop/email-EuAll/indegree ^
	-output results/hadoop/email-EuAll/distribution ^
	-mapper "python scripts/hadoop/mapper_histogram.py" ^
	-reducer "python scripts/hadoop/reducer_histogram.py"
```

If Hadoop is missing, you can compute the same outputs locally:

```
python scripts/hadoop/local_hadoop_fallback.py --input data/raw/email-EuAll.txt --out results/hadoop/email-EuAll
```

## Notes

- The parsers ignore comment lines (`# ...`) and expect at least two whitespace-separated columns per edge: `<src> <dst>`.
- Spark runs in local mode by default via PySpark; ensure Java is installed and on PATH.
- Hadoop runs also work in local mode if you configure Hadoop accordingly; the provided PowerShell runner attempts to locate the Hadoop streaming JAR automatically.

## Documentation

See the `docs/` folder for comparative write-ups:

- `docs/correctness.md` — validation approach and results
- `docs/performance.md` — metrics, plots, and analysis
- `docs/system-design.md` — implementation details and trade-offs

## One-command runner

You can run the entire pipeline (deps → experiments → validation → plots → scaling → optimized runs) with one command:

```
python scripts/main.py
```

Options:

- Choose datasets:
	- `python scripts/main.py --datasets email-EuAll web-BerkStan`
- Skip optimized runs:
	- `python scripts/main.py --no-optimized`
- Run specific steps (in order):
	- `python scripts/main.py --steps deps experiments validate plots-distribution plots-metrics sizes plots-scaling`
- Dry run (print commands only):
	- `python scripts/main.py --dry-run`

