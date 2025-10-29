#!/usr/bin/env bash
set -euo pipefail

# Ensure Hadoop CLI binaries are available
export HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop-3.2.1}
export PATH="$HADOOP_HOME/bin:$PATH"

# Run Hadoop MR indegree + degree distribution on selected datasets.
# To run inside namenode container.

JAR=/project/hadoop/target/graph-degree-hadoop.jar
INDEG_DRV=com.example.degree.InDegreeDriver
DIST_DRV=com.example.degree.DegreeDistDriver

INPUT_DIR=/data/cleaned
HDFS_BASE=/exp
OUT_BASE=$HDFS_BASE/out
RESULTS=/results/hadoop
METRICS=/results/metrics

mkdir -p "$RESULTS" "$METRICS"

# Choose at least three datasets (filenames without extension)
DATASETS=(
  email-EuAll
  web-BerkStan
  soc-LiveJournal1
)

time_cmd() {
  if command -v /usr/bin/time >/dev/null 2>&1; then echo "/usr/bin/time -v"; else echo "time"; fi
}

for base in "${DATASETS[@]}"; do
  local_in="$INPUT_DIR/${base}.edges"
  if [[ ! -f "$local_in" ]]; then
    echo "[warn] Missing $local_in, skipping" >&2
    continue
  fi

  echo "[prep] Uploading $local_in to HDFS"
  hdfs dfs -mkdir -p "$HDFS_BASE/in/$base"
  hdfs dfs -put -f "$local_in" "$HDFS_BASE/in/$base/"

  indeg_out="$OUT_BASE/indegree/$base"
  dist_out="$OUT_BASE/dist/$base"
  hdfs dfs -rm -r -f "$indeg_out" "$dist_out" >/dev/null 2>&1 || true

  echo "[run] Hadoop InDegree: $base"
  $(time_cmd) hadoop jar "$JAR" "$INDEG_DRV" "$HDFS_BASE/in/$base" "$indeg_out" \
    2>"$METRICS/hadoop_${base}_indegree.time" | tee "/dev/null"

  echo "[run] Hadoop DegreeDist: $base"
  $(time_cmd) hadoop jar "$JAR" "$DIST_DRV" "$indeg_out" "$dist_out" \
    2>"$METRICS/hadoop_${base}_dist.time" | tee "/dev/null"

  echo "[fetch] Copying outputs to host"
  mkdir -p "$RESULTS/$base"
  hdfs dfs -get -f "$indeg_out" "$RESULTS/$base/indegree" || true
  hdfs dfs -get -f "$dist_out" "$RESULTS/$base/distribution" || true
done

echo "[done] Hadoop experiments completed"
