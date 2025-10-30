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
# Write Hadoop results under the shared, writable mount
RESULTS=/data/results/hadoop
METRICS_BASE=/data/metrics/hadoop

mkdir -p "$RESULTS" "$METRICS_BASE"

# Choose at least three datasets (filenames without extension)
DATASETS=(
  email-EuAll
  web-BerkStan
  soc-LiveJournal1
)

WITH_METRICS=/data/scripts/with_metrics.sh

# Detect if HDFS is available; if not, fall back to LocalJobRunner over local FS
LOCAL_MODE=0
if ! hdfs dfs -ls / >/dev/null 2>&1; then
  echo "[info] HDFS not available; using local MapReduce mode (no HDFS)."
  LOCAL_MODE=1
fi

for base in "${DATASETS[@]}"; do
  local_in="$INPUT_DIR/${base}.edges"
  if [[ ! -f "$local_in" ]]; then
    echo "[warn] Missing $local_in, skipping" >&2
    continue
  fi

  dataset_local_mode=$LOCAL_MODE
  if [[ "$dataset_local_mode" -eq 0 ]]; then
    echo "[prep] Uploading $local_in to HDFS"
    set +e
    hdfs dfs -mkdir -p "$HDFS_BASE/in/$base"
    hdfs dfs -put -f "$local_in" "$HDFS_BASE/in/$base/"
    up_rc=$?
    set -e
    if [[ $up_rc -ne 0 ]]; then
      echo "[warn] HDFS upload failed for $base; switching to local MapReduce mode for this dataset."
      dataset_local_mode=1
    fi
  fi

  if [[ "$dataset_local_mode" -eq 0 ]]; then
    indeg_out="$OUT_BASE/indegree/$base"
    dist_out="$OUT_BASE/dist/$base"
    hdfs dfs -rm -r -f "$indeg_out" "$dist_out" >/dev/null 2>&1 || true
  else
    mkdir -p "$RESULTS/$base"
    indeg_out="$RESULTS/$base/indegree"
    dist_out="$RESULTS/$base/distribution"
    rm -rf "$indeg_out" "$dist_out"
  fi

  echo "[run] Hadoop InDegree: $base"
  mdir="$METRICS_BASE/$base"
  mkdir -p "$mdir"
  if [[ -x "$WITH_METRICS" ]]; then
    if [[ "$dataset_local_mode" -eq 0 ]]; then
      bash "$WITH_METRICS" "$mdir" indegree -- hadoop jar "$JAR" "$INDEG_DRV" "$HDFS_BASE/in/$base" "$indeg_out"
    else
      # Force local FS explicitly using file:// URIs so Hadoop doesn't try HDFS
      local_in_uri="file://$local_in"
      indeg_out_uri="file://$indeg_out"
      HADOOP_CLIENT_OPTS="-Dmapreduce.framework.name=local" \
        bash "$WITH_METRICS" "$mdir" indegree -- hadoop jar "$JAR" "$INDEG_DRV" "$local_in_uri" "$indeg_out_uri"
    fi
  else
    start=$(date +%s)
    if [[ "$dataset_local_mode" -eq 0 ]]; then
      hadoop jar "$JAR" "$INDEG_DRV" "$HDFS_BASE/in/$base" "$indeg_out" | tee "/dev/null"
    else
      local_in_uri="file://$local_in"
      indeg_out_uri="file://$indeg_out"
      HADOOP_CLIENT_OPTS="-Dmapreduce.framework.name=local" \
        hadoop jar "$JAR" "$INDEG_DRV" "$local_in_uri" "$indeg_out_uri" | tee "/dev/null"
    fi
    end=$(date +%s)
    dur=$((end-start))
    echo "elapsed_seconds: $dur" >"$mdir/indegree.time"
    echo "0" >"$mdir/indegree.status"
  fi

  echo "[run] Hadoop DegreeDist: $base"
  if [[ -x "$WITH_METRICS" ]]; then
    if [[ "$dataset_local_mode" -eq 0 ]]; then
      bash "$WITH_METRICS" "$mdir" distribution -- hadoop jar "$JAR" "$DIST_DRV" "$indeg_out" "$dist_out"
    else
      # Use file:// URIs for local filesystem paths
      indeg_out_uri="file://$indeg_out"
      dist_out_uri="file://$dist_out"
      HADOOP_CLIENT_OPTS="-Dmapreduce.framework.name=local" \
        bash "$WITH_METRICS" "$mdir" distribution -- hadoop jar "$JAR" "$DIST_DRV" "$indeg_out_uri" "$dist_out_uri"
    fi
  else
    start=$(date +%s)
    if [[ "$dataset_local_mode" -eq 0 ]]; then
      hadoop jar "$JAR" "$DIST_DRV" "$indeg_out" "$dist_out" | tee "/dev/null"
    else
      indeg_out_uri="file://$indeg_out"
      dist_out_uri="file://$dist_out"
      HADOOP_CLIENT_OPTS="-Dmapreduce.framework.name=local" \
        hadoop jar "$JAR" "$DIST_DRV" "$indeg_out_uri" "$dist_out_uri" | tee "/dev/null"
    fi
    end=$(date +%s)
    dur=$((end-start))
    echo "elapsed_seconds: $dur" >"$mdir/distribution.time"
    echo "0" >"$mdir/distribution.status"
  fi

  if [[ "$dataset_local_mode" -eq 0 ]]; then
    echo "[fetch] Copying outputs to host"
    mkdir -p "$RESULTS/$base"
    hdfs dfs -get -f "$indeg_out" "$RESULTS/$base/indegree" || true
    hdfs dfs -get -f "$dist_out" "$RESULTS/$base/distribution" || true
  else
    echo "[local] Outputs written under $RESULTS/$base/"
  fi
done

echo "[done] Hadoop experiments completed"
