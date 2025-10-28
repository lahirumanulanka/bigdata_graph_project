# Graph Degree

This project computes node in-degree on large graphs using both Hadoop MapReduce and Spark.

## Structure

- `data/`
  - `raw/`: raw SNAP datasets
  - `cleaned/`: cleaned edge lists ("src dst")
  - `sample/`: optional smaller samples
- `scripts/`: helper scripts (fetch, clean, run)
- `hadoop/`: MapReduce implementation (Maven)
- `spark/`: PySpark and optional Scala versions
- `results/`: outputs and plots
- `docker-compose.yml`: optional cluster orchestration
- `Dockerfile`: optional single image build

This repository contains placeholders only. Fill in scripts and code as needed.
