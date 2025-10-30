#!/usr/bin/env bash
# Orchestrate Spark & Hadoop runs with performance metrics and plotting.
# Run from host (requires Docker Desktop).
# Usage: ./scripts/run_all.sh

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)

echo "[1/6] Ensure cluster is running"
docker compose -f "$ROOT_DIR/docker-compose.yml" up -d

echo "[2/6] Prepare spark-master (tools + datasets)"
# Install tools (root), convert CRLF, fetch & clean datasets
docker exec -u 0 spark-master bash -lc "apt-get update && apt-get install -y dstat python3-matplotlib || true"
docker exec spark-master bash -lc "sed -i 's/\r$//' /data/scripts/*.sh || true"
docker exec spark-master bash -lc "bash /data/scripts/fetch_snap.sh || true; python3 /data/scripts/clean_snap.py"

echo "[3/6] Run Spark experiments with metrics"
docker exec spark-master bash -lc "bash /data/scripts/run_spark_experiments.sh"

echo "[4/6] Prepare & run Hadoop experiments with metrics"
docker compose -f "$ROOT_DIR/docker-compose.yml" up -d resourcemanager nodemanager
docker exec -u 0 namenode bash -lc "apt-get update && apt-get install -y dstat || true"
docker exec namenode bash -lc "sed -i 's/\r$//' /data/scripts/*.sh || true"
docker exec namenode bash -lc "bash /data/scripts/build_hadoop.sh && bash /data/scripts/run_hadoop_experiments.sh"

echo "[5/6] Plot indegree distributions"
docker exec spark-master bash -lc "python3 /data/scripts/plot_distributions.py"

echo "[6/6] Aggregate and plot performance metrics"
docker exec spark-master bash -lc "python3 /data/scripts/aggregate_metrics.py && python3 /data/scripts/plot_metrics.py"

echo "[done] Results:"
echo " - Distributions: data/plots/*.png"
echo " - Metrics summary: data/metrics/summary.csv"
echo " - Metrics plots: data/plots/metrics_*.png"
