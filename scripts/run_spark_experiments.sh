#!/usr/bin/env bash
set -euo pipefail

# Run Spark indegree + distribution on selected datasets (local files).
# To run inside spark-master container.

export SPARK_HOME=${SPARK_HOME:-/opt/spark}
export PATH="$SPARK_HOME/bin:$PATH"

INPUT_DIR=/data/cleaned
OUT_BASE=/data/results/spark
METRICS=/data/metrics
APP=/spark/indegree_spark.py

mkdir -p "$OUT_BASE" "$METRICS"

DATASETS=(
  email-EuAll
  web-BerkStan
  soc-LiveJournal1
)

TIME_BIN=""
if command -v /usr/bin/time >/dev/null 2>&1; then TIME_BIN="/usr/bin/time -v"; fi

for base in "${DATASETS[@]}"; do
  input="$INPUT_DIR/${base}.edges"
  if [[ ! -f "$input" ]]; then
    echo "[warn] Missing $input, skipping" >&2
    continue
  fi
  out="$OUT_BASE/$base"
  rm -rf "$out"
  echo "[run] Spark: $base"
  if [[ -n "$TIME_BIN" ]]; then
    $TIME_BIN spark-submit "$APP" "$input" "$out" 2>"$METRICS/spark_${base}.time" | tee "/dev/null"
  else
    # Fallback simple timing
    start=$(date +%s)
    spark-submit "$APP" "$input" "$out" | tee "/dev/null"
    end=$(date +%s)
    dur=$((end-start))
    echo "elapsed_seconds: $dur" >"$METRICS/spark_${base}.time"
  fi
done

echo "[done] Spark experiments completed"
