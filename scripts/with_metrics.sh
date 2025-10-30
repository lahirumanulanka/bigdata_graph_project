#!/usr/bin/env bash
# Run a command while collecting performance metrics.
# - Uses /usr/bin/time -v (if available) to capture wall time and max RSS
# - Optionally runs dstat (if available) to log CPU/mem/disk/net at 1s interval
#
# Usage:
#   with_metrics.sh <metrics_dir> <tag> -- <command> [args...]
#
# Outputs in <metrics_dir> (created if missing):
#   <tag>.time         # GNU time -v output or fallback elapsed_seconds
#   <tag>.dstat.csv    # dstat CSV if dstat is available
#   <tag>.status       # exit code of the command

set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <metrics_dir> <tag> -- <command> [args...]" >&2
  exit 2
fi

METRICS_DIR=$1
TAG=$2
shift 2

if [[ "$1" != "--" ]]; then
  echo "Usage: $0 <metrics_dir> <tag> -- <command> [args...]" >&2
  exit 2
fi
shift

mkdir -p "$METRICS_DIR"
TIME_FILE="$METRICS_DIR/${TAG}.time"
DSTAT_FILE="$METRICS_DIR/${TAG}.dstat.csv"
STATUS_FILE="$METRICS_DIR/${TAG}.status"

# Start dstat if present
DSTAT_PID=""
SAR_CPU_PID=""
SAR_MEM_PID=""
SAR_DSK_PID=""
SAR_NET_PID=""
if command -v dstat >/dev/null 2>&1; then
  # dstat writes a CSV header once; sample every 1s
  # Note: suppress stdout/stderr to keep console clean
  # Use a conservative set of plugins to avoid permissions/compat issues
  dstat --time --cpu --mem --io --net --output "$DSTAT_FILE" 1 >/dev/null 2>&1 &
  DSTAT_PID=$!
  # Give dstat time to initialize and emit the first sample so that
  # very short jobs still get at least one data row after the headers.
  sleep 2
fi

# Start sar samplers if available (fallback to ensure samples)
if command -v sar >/dev/null 2>&1; then
  SAR_CPU_FILE="$METRICS_DIR/${TAG}.sar.cpu.txt"
  SAR_MEM_FILE="$METRICS_DIR/${TAG}.sar.mem.txt"
  SAR_DSK_FILE="$METRICS_DIR/${TAG}.sar.dsk.txt"
  SAR_NET_FILE="$METRICS_DIR/${TAG}.sar.net.txt"
  # Ensure C locale for predictable headers
  LC_ALL=C sar -u 1 >"$SAR_CPU_FILE" 2>/dev/null &
  SAR_CPU_PID=$!
  LC_ALL=C sar -r 1 >"$SAR_MEM_FILE" 2>/dev/null &
  SAR_MEM_PID=$!
  LC_ALL=C sar -b 1 >"$SAR_DSK_FILE" 2>/dev/null &
  SAR_DSK_PID=$!
  LC_ALL=C sar -n DEV 1 >"$SAR_NET_FILE" 2>/dev/null &
  SAR_NET_PID=$!
fi

# Prefer GNU time with verbose and output to file
EXIT_CODE=0
if command -v /usr/bin/time >/dev/null 2>&1; then
  # GNU time supports -o and -v together
  # shellcheck disable=SC2086
  /usr/bin/time -v -o "$TIME_FILE" "$@" || EXIT_CODE=$?
else
  # Fallback simple timing
  start=$(date +%s)
  # shellcheck disable=SC2086
  "$@" || EXIT_CODE=$?
  end=$(date +%s)
  dur=$((end-start))
  echo "elapsed_seconds: $dur" >"$TIME_FILE"
fi

# Stop dstat if running
if [[ -n "${DSTAT_PID}" ]]; then
  # Allow a brief grace period to capture a final sample
  sleep 1
  # Send SIGINT for graceful CSV finalization; fall back to TERM
  kill -INT "$DSTAT_PID" >/dev/null 2>&1 || kill "$DSTAT_PID" >/dev/null 2>&1 || true
  wait "$DSTAT_PID" 2>/dev/null || true
fi

# Stop sar samplers
for pid in "$SAR_CPU_PID" "$SAR_MEM_PID" "$SAR_DSK_PID" "$SAR_NET_PID"; do
  if [[ -n "$pid" ]]; then
    kill -INT "$pid" >/dev/null 2>&1 || kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" 2>/dev/null || true
  fi
done

echo "$EXIT_CODE" >"$STATUS_FILE"
exit "$EXIT_CODE"
