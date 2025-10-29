"""
PySpark job to compute indegree counts and degree distribution from edge list files.

Input format: space-separated "src dst" per line, comments with leading '#', ';', or '%'.

Usage:
  spark-submit indegree_spark.py <input_path> <output_dir>

Outputs:
  - <output_dir>/indegree/part-*: TSV of node\tindegree
  - <output_dir>/distribution/part-*: TSV of degree\tcount
"""
from __future__ import annotations

import sys
from pyspark.sql import SparkSession


def build_spark(app_name: str = "InDegreeSpark") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print("Usage: spark-submit indegree_spark.py <input_path> <output_dir>", file=sys.stderr)
        return 1
    input_path = argv[1]
    output_dir = argv[2].rstrip("/")

    spark = build_spark()
    sc = spark.sparkContext

    # Read as text, filter comments/empties
    rdd = sc.textFile(input_path)
    rdd = rdd.map(lambda s: s.strip()).filter(lambda s: s and not (s.startswith('#') or s.startswith('%') or s.startswith(';')))

    # Parse edges; keep first two tokens
    edges = rdd.map(lambda s: s.split()).filter(lambda parts: len(parts) >= 2).map(lambda parts: (parts[0], parts[1]))

    # In-degree: count by destination node
    indeg = edges.map(lambda e: (e[1], 1)).reduceByKey(lambda a, b: a + b)

    # Save indegree per node
    indegree_out = f"{output_dir}/indegree"
    indeg.map(lambda kv: f"{kv[0]}\t{kv[1]}").saveAsTextFile(indegree_out)

    # Degree distribution: (degree -> number of nodes)
    dist = indeg.map(lambda kv: (kv[1], 1)).reduceByKey(lambda a, b: a + b)
    dist_out = f"{output_dir}/distribution"
    dist.map(lambda kv: f"{kv[0]}\t{kv[1]}").saveAsTextFile(dist_out)

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
