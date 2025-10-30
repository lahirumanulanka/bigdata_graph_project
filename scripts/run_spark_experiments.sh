#!/usr/bin/env bash
set -euo pipefail

# Run Spark indegree + distribution on selected datasets (local files).
# To run inside spark-master container.

export SPARK_HOME=${SPARK_HOME:-/opt/spark}
export PATH="$SPARK_HOME/bin:$PATH"

INPUT_DIR=/data/cleaned
OUT_BASE=/data/results/spark
METRICS_BASE=/data/metrics/spark
APP=/spark/indegree_spark.py

mkdir -p "$OUT_BASE" "$METRICS_BASE"

DATASETS=(
  email-EuAll
  web-BerkStan
  soc-LiveJournal1
)

WITH_METRICS=/data/scripts/with_metrics.sh

for base in "${DATASETS[@]}"; do
  input="$INPUT_DIR/${base}.edges"
  if [[ ! -f "$input" ]]; then
    echo "[warn] Missing $input, skipping" >&2
    continue
  fi
  out="$OUT_BASE/$base"
  rm -rf "$out"
  echo "[run] Spark: $base"
  mdir="$METRICS_BASE/$base"
  mkdir -p "$mdir"
  if [[ -x "$WITH_METRICS" ]]; then
    bash "$WITH_METRICS" "$mdir" job -- spark-submit "$APP" "$input" "$out"
  else
    # Fallback simple timing
    start=$(date +%s)
    spark-submit "$APP" "$input" "$out" | tee "/dev/null"
    end=$(date +%s)
    dur=$((end-start))
    echo "elapsed_seconds: $dur" >"$mdir/job.time"
    echo "0" >"$mdir/job.status"
  fi
done

echo "[done] Spark experiments completed"
