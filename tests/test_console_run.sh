#!/bin/sh
# Starts a throwaway epsilond, runs the admin console integration tests, tears down.
set -e
cd "$(dirname "$0")/.."

PORT=${EPSILON_TEST_CONSOLE_PORT:-18992}
DATA=tests/data/console_live

rm -rf "$DATA"
mkdir -p "$DATA"
bin/epsilond -p "$PORT" -d "$DATA" >"$DATA/server.log" 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null || true; wait $PID 2>/dev/null || true' EXIT

./tests/test_console "$PORT"
