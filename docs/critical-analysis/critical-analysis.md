# Hadoop vs Spark for Graph Data: A Critical Analysis

This analysis synthesizes our experimental results with system design principles to explain observed performance patterns, discuss which system fits large-scale graph processing better, and relate findings to theory.

## Why Hadoop and Spark show different performance patterns

1) Runtime model and materialization
- Hadoop (Streaming/MapReduce) materializes outputs between stages. Our pipeline used two jobs (indegree → histogram); the intermediate indegree is written to disk and read back. This increases total disk I/O (see `scaling_disk.png`) but keeps each stage simple and stateless.
- Spark keeps datasets (RDDs) in memory across transformations and only materializes at actions. For our job, `indegree` is consumed again to build the histogram, and caching avoids recomputation. In an ideal environment, this reduces I/O and favors Spark for multi-stage or iterative workflows.

2) Overheads and execution environment
- Spark incurs JVM startup, context initialization, and scheduler overhead. On this Windows host without `HADOOP_HOME`, Spark also fails to write and our runner falls back to a local writer; the baseline included that failed attempt, inflating Spark’s small-to-medium data times (e.g., `email-EuAll`, `web-BerkStan`). Once we optimized to skip the doomed write, Spark’s runtime dropped dramatically for those datasets.
- Hadoop Streaming (when available) launches external mapper/reducer processes and performs large sequential I/O. In our environment Hadoop wasn’t installed, so the local fallback dominated; its performance mostly reflects Python I/O and algorithmic work rather than distributed overheads.

3) Shuffle and network characteristics
- In local-mode, measured network usage is tiny (no real cluster shuffle). In a distributed cluster, Spark’s shuffle for wide transformations (reduceByKey, groupByKey) becomes a major factor and requires tuning (e.g., partition counts, serializer, memory fractions). Hadoop’s shuffle is baked into the map/reduce boundary and tends to be robust but I/O heavy.

4) Workload characteristics: one-pass vs iterative
- Our pipeline is essentially two one-pass aggregations. Hadoop’s model is well-suited to single-pass, embarrassingly parallel counting. Spark shows more advantages as the number of dependent stages increases, especially when data reuse or iteration is present.

## Which system is better for large-scale graph processing?

Short answer: it depends on the graph workload.

- Iterative graph algorithms (e.g., PageRank, connected components, BFS/SSSP, community detection, triangle counting):
  - Prefer Spark (or graph-specialized engines on Spark) because you can persist/cycle state in-memory across many supersteps/iterations. Spark’s GraphX/GraphFrames exploit RDD/DataFrame primitives for iterative graph analytics.
  - MapReduce can implement these via multiple jobs per iteration (or frameworks like Giraph), but repeated materialization causes heavy disk I/O and longer runtimes.

- Single-pass or few-pass computations (e.g., degree distributions, simple histograms, one-shot joins):
  - Hadoop MapReduce holds up well, especially on very large datasets with good HDFS throughput and when cluster memory is limited. The pipeline is simple, fault-tolerant, and predictable.
  - Spark is still effective, but the benefit over MapReduce is smaller unless there’s reuse across stages or subsequent interactive analysis.

- Operational considerations:
  - Spark shines for pipelines that benefit from caching, interactive exploration, SQL/DataFrames, and complex multi-stage DAGs.
  - Hadoop is a good fit for batch ETL and archival jobs with massive sequential I/O and simpler transformations.

## Alignment with theory and system design principles

- I/O vs memory trade-offs: Theoretical throughput for MapReduce emphasizes sequential disk I/O and streaming. Spark’s design optimizes for in-memory reuse and DAG scheduling. Our measurements reflect this: Hadoop exhibits higher total disk I/O; Spark’s disk totals are generally lower for comparable work, with elapsed time sensitive to overheads and configuration.

- Complexity of iterative computations: For T iterations over E edges, a MapReduce approach often needs O(T) jobs with repeated materialization, driving O(T·E) I/O. Spark can cache state to reduce I/O per iteration, replacing much of the cost with memory traffic and shuffles. This matches practice in large-scale graph analytics where Spark (or Pregel-like engines) outperforms pure MapReduce.

- Startup/constant factors: On modest data sizes, constant overheads dominate asymptotics. Our small/medium datasets showed Spark penalized by startup and Windows-specific write failures; once we removed that overhead, Spark improved markedly and narrowed the gap on the 1+ GB dataset.

- Fault tolerance and determinism: Both systems offer fault tolerance—Hadoop via materialized stages; Spark via lineage and (optionally) checkpoints. The theoretical guarantees align with the practical observation that both implementations produced identical results across datasets.

## Interpreting our experimental findings

- Baseline: Hadoop (local fallback) outperformed Spark on small/medium datasets due to Spark’s startup + failed-save overhead on Windows. On the largest dataset, the gap narrowed but remained.
- Optimized: Skipping the failed Spark write delivered large gains for small/medium data and notable gains for 1+ GB. Hadoop’s fallback saw smaller improvements from larger read buffers.
- Takeaway: Under a properly configured cluster (Linux, HDFS, HADOOP_HOME), we’d expect Spark to outperform MapReduce on multi-stage or iterative graph analytics, while MapReduce remains competitive for simple one-pass aggregations over extreme data volumes.

## Practical guidance

- Choose Spark when:
  - You have iterative algorithms or reuse intermediate results.
  - You need interactive analytics or SQL/DataFrame ecosystems.
  - You can provision enough memory and tune shuffle/partitions.

- Choose Hadoop MapReduce when:
  - The workload is simple, one-pass batch processing with extreme data sizes and limited memory per node.
  - Predictable, sequential I/O and straightforward operational model are priorities.

## References to artifacts

- Distribution plots: `results/plots/`
- Per-dataset metrics plots: `results/metrics/plots/*_summary.png`
- Scaling plots: `results/metrics/plots/scaling_*.png`
- Optimization plots: `results/metrics/plots/optimizations_*.png`
- Validation: `scripts/validate_results.py` (exact match across Hadoop and Spark for all datasets)
