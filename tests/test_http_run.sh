#!/bin/sh
# Starts a throwaway epsilond, runs the HTTP integration tests, tears down.
set -e
cd "$(dirname "$0")/.."

PORT=${EPSILON_TEST_PORT:-18991}
DATA=tests/data/http_live

rm -rf "$DATA"
mkdir -p "$DATA"
bin/epsilond -p "$PORT" -d "$DATA" >"$DATA/server.log" 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null || true; wait $PID 2>/dev/null || true' EXIT

./tests/test_http "$PORT"
