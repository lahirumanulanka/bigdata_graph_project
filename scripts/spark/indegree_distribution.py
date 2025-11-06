#!/usr/bin/env python3
"""
Compute in-degree per node and the in-degree distribution using PySpark.

Input format:
- SNAP-like edge list text files located at data/raw/<dataset>.txt
- Lines beginning with '#' are comments and are ignored
- Each non-comment line contains at least two integers or strings: <src> <dst>

Outputs:
- results/spark/<dataset>/indegree/part-*
  format: "<node>\t<in_degree>"
- results/spark/<dataset>/distribution/part-*
  format: "<in_degree>\t<count_of_nodes_with_that_in_degree>"

Run examples:
  python scripts/spark/indegree_distribution.py --dataset email-EuAll
  python scripts/spark/indegree_distribution.py --input data/raw/email-EuAll.txt --name email-EuAll
"""
from __future__ import annotations

import argparse
from pathlib import Path
from pyspark.storagelevel import StorageLevel
from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="In-degree and distribution with PySpark")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--dataset", help="Dataset name under data/raw without extension, e.g., email-EuAll")
    g.add_argument("--input", help="Explicit input file path")
    p.add_argument("--name", help="Output dataset name (defaults to dataset or stem of input)")
    p.add_argument(
        "--results-root",
        default="results/spark",
        help="Root directory for outputs (default: results/spark)",
    )
    return p.parse_args()


def is_valid_edge(line: str) -> bool:
    s = line.strip()
    if not s or s.startswith("#"):
        return False
    return True


def main() -> int:
    args = parse_args()

    if args.dataset:
        dataset = args.dataset
        input_path = Path("data") / "raw" / f"{dataset}.txt"
    else:
        input_path = Path(args.input)
        dataset = args.name or input_path.stem

    out_root = Path(args.results_root) / dataset
    out_indegree = out_root / "indegree"
    out_distribution = out_root / "distribution"

    # Ensure fresh outputs to avoid FileAlreadyExistsException
    for p in (out_indegree, out_distribution):
        if p.exists():
            # Best-effort cleanup; Spark will recreate
            import shutil
            shutil.rmtree(p, ignore_errors=True)

    spark = (
        SparkSession.builder.appName(f"indegree_distribution:{dataset}")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    sc = spark.sparkContext

    # Read as text and extract destination node (v) from edge u v
    # Increase partitions for large files to improve parallelism in local mode
    min_parts = max(2, sc.defaultParallelism * 2)
    lines = sc.textFile(str(input_path), minPartitions=min_parts)
    edges = (
        lines.filter(is_valid_edge)
        .map(lambda s: s.split())
        .filter(lambda toks: len(toks) >= 2)
        .map(lambda toks: (toks[1], 1))  # (dst, 1)
    )

    # In-degree per node
    indegree = edges.reduceByKey(lambda a, b: a + b)
    # Cache indegree since we use it for two actions (save + histogram)
    indegree.persist(StorageLevel.MEMORY_ONLY)

    # Save indegree as TSV
    indegree.map(lambda kv: f"{kv[0]}\t{kv[1]}").saveAsTextFile(str(out_indegree))

    # Distribution: (degree -> count of nodes)
    distribution = (
        indegree.map(lambda kv: (kv[1], 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey(ascending=True)
    )
    distribution.map(lambda kv: f"{kv[0]}\t{kv[1]}").saveAsTextFile(str(out_distribution))

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
