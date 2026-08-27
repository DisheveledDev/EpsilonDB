#!/bin/sh
# Starts a throwaway epsilond, runs the EQL HTTP integration tests, tears
# down. Mirrors tests/test_http_run.sh.
set -e
cd "$(dirname "$0")/.."

PORT=${EPSILON_TEST_EQL_PORT:-18992}
DATA=tests/data/eql_live

rm -rf "$DATA"
mkdir -p "$DATA"
bin/epsilond -p "$PORT" -d "$DATA" >"$DATA/server.log" 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null || true; wait $PID 2>/dev/null || true' EXIT

./tests/test_eql_api "$PORT"
