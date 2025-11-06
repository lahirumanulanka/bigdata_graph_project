# System Design and Data Processing Approach

This document contrasts the implementation approaches for Spark (PySpark) and Hadoop (Streaming with Python mappers/reducers), and captures practical considerations for this project on Windows.

## Spark implementation (PySpark)

- Entry point: `scripts/spark/indegree_distribution.py`
- Flow:
  1. Read `data/raw/<dataset>.txt` into an RDD (textFile)
  2. Parse lines, filter comments (`#`)
  3. Map to `(dst, 1)` and `reduceByKey` → per-node in-degree
  4. Map `(node, deg)` → `(deg, 1)` and `reduceByKey` → degree distribution
  5. Save as text under `results/spark/<dataset>/{indegree,distribution}/part-*`
- Characteristics:
  - Single job encapsulates both steps using RDD transformations; fewer external processes.
  - Fault tolerance via lineage; can persist or checkpoint if needed.
  - Developer ergonomics: compact, expressive Python + PySpark API.
  - Windows caveat: `saveAsTextFile` requires Hadoop binaries (`HADOOP_HOME`) to write reliably.
- Project adaptation:
  - `scripts/spark/local_spark_fallback.py` computes the same outputs in pure Python, used when PySpark cannot write (Windows without winutils).

## Hadoop implementation (Streaming)

- Components:
  - `scripts/hadoop/mapper_in_degree.py` → `(dst, 1)`
  - `scripts/hadoop/reducer_in_degree.py` → per-node in-degree
  - `scripts/hadoop/mapper_histogram.py` → `(deg, 1)`
  - `scripts/hadoop/reducer_histogram.py` → degree distribution
- Flow:
  1. Job 1 (indegree): map → reduce; output to `results/hadoop/<dataset>/indegree`
  2. Job 2 (distribution): read previous output; map → reduce; output to `results/hadoop/<dataset>/distribution`
- Characteristics:
  - Two separate jobs; explicit shuffle boundaries encourage simple, composable stages.
  - More I/O between stages due to materialized outputs.
  - Portable mapper/reducer scripts; easy to reason about key/value flow.
- Project adaptation:
  - `scripts/hadoop/local_hadoop_fallback.py` computes both stages locally when Hadoop/Streaming JAR is unavailable.

## Orchestration and tooling

- Orchestrator: `scripts/run_experiments.ps1` runs Spark then Hadoop for each dataset and wraps each run with a metrics recorder (`scripts/metrics/runner.py`).
- Metrics: CPU%, memory MB, disk/network bytes; saved to `results/metrics/<system>/<dataset>/` and plotted by `scripts/plot_metrics.py`.
- Validation: `scripts/validate_results.py` checks Spark vs Hadoop outputs for exact matches.

## Trade-offs and when to use which

- Spark
  - Pros: concise code, flexible pipelines (joins, multi-stage transformations), in-memory speed, rich ecosystem (DataFrames, SQL).
  - Cons: JVM startup/overhead, needs proper Hadoop/Spark setup for I/O, Windows friction without `HADOOP_HOME`.
- Hadoop Streaming
  - Pros: simple mental model, language-agnostic mappers/reducers, robust batch execution, minimal runtime dependencies.
  - Cons: more I/O between stages; less convenient for complex multi-stage pipelines compared to Spark.

## This project’s choices

- Both approaches are implemented and produce identical results.
- Fallbacks ensure reproducibility on Windows without Hadoop binaries while writing outputs in the expected folder structure.
- For larger-scale or production use, prefer:
  - Spark with proper cluster/HDFS setup for iterative analysis and complex pipelines; or
  - Hadoop when favoring stability of batch jobs with simple map/reduce stages and predictable I/O.

