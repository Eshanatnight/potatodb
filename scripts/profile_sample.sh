#!/usr/bin/env bash
# Profile potatodb using macOS 'sample' while running a SQL workload.
#
# Usage:
#   ./scripts/profile_sample.sh [options] [sql_file]
#
# Options (environment variables):
#   SAMPLE_INTERVAL_MS  Sampling interval in milliseconds (default: 1)
#   SAMPLE_DURATION_SEC Duration to sample in seconds (default: 10)
#   SAMPLE_OUTPUT       Output file path (default: potatodb_sample_<timestamp>.txt)
#   DATA_DIR            PotatoDB data directory (default: ./potatodb_profile_data)
#
# Example:
#   SAMPLE_DURATION_SEC=5 ./scripts/profile_sample.sh sample_data.sql
#
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# Options
SAMPLE_INTERVAL_MS="${SAMPLE_INTERVAL_MS:-1}"
SAMPLE_DURATION_SEC="${SAMPLE_DURATION_SEC:-10}"
SAMPLE_OUTPUT="${SAMPLE_OUTPUT:-}"
SQL_FILE="${1:-$REPO_ROOT/sample_data.sql}"
DATA_DIR="${DATA_DIR:-./potatodb_profile_data}"
export POTATODB_BATCH_SIZE=32768
export POTATODB_PARQUET_WRITE_BATCH_SIZE=32768
export POTATODB_WRITE_BUFFER_MS=1000
export POTATODB_WRITE_BUFFER_BYTES=134217728
export POTATODB_TARGET_PARTITIONS=4
export POTATODB_ARROW_WAL_SYNC=every_ms
export POTATODB_ARROW_WAL_SYNC_MS=1000
export POTATODB_ARROW_WAL_SCRATCH_BYTES=8388608

if [[ ! -f "$SQL_FILE" ]]; then
    echo "Error: SQL file not found: $SQL_FILE" >&2
    exit 1
fi

# Build release binary if needed
if [[ ! -f "target/release/potatodb" ]]; then
    echo "Building potatodb (release)..."
    cargo build --profile profiling -p potatodb
fi

if [[ -z "$SAMPLE_OUTPUT" ]]; then
    SAMPLE_OUTPUT="potatodb_sample_$(date +%Y%m%d_%H%M%S).txt"
fi

echo "Profiling potatodb"
echo "  SQL file:      $SQL_FILE"
echo "  Data dir:      $DATA_DIR"
echo "  Sample:        every ${SAMPLE_INTERVAL_MS}ms for ${SAMPLE_DURATION_SEC}s"
echo "  Output:        $SAMPLE_OUTPUT"
echo ""

# Start potatodb in background
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"
"$REPO_ROOT/target/profiling/potatodb" --data-dir "$DATA_DIR" -f "$SQL_FILE" &
PID=$!

# Give it a moment to start
sleep 1

if ! kill -0 "$PID" 2>/dev/null; then
    echo "Error: potatodb exited before sampling could start" >&2
    wait "$PID" 2>/dev/null || true
    exit 1
fi

echo "Sampling PID $PID..."
# -mayDie: process may exit during sampling (e.g. -f runs once and exits)
sample "$PID" "$SAMPLE_DURATION_SEC" "$SAMPLE_INTERVAL_MS" -mayDie -file "$SAMPLE_OUTPUT" 2>/dev/null || true

# Wait for potatodb to finish
wait "$PID" 2>/dev/null || true

echo "Done. Report written to: $SAMPLE_OUTPUT"
