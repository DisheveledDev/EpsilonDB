#!/bin/sh
# Starts a throwaway epsilond, runs the Lua scripting HTTP integration
# tests, tears down. Mirrors tests/test_eql_apirun.sh.
set -e
cd "$(dirname "$0")/.."

PORT=${EPSILON_TEST_LUA_PORT:-18993}
DATA=tests/data/lua_live

rm -rf "$DATA"
mkdir -p "$DATA"
bin/epsilond -p "$PORT" -d "$DATA" -s "$DATA/epsilon-admin.sock" \
    >"$DATA/server.log" 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null || true; wait $PID 2>/dev/null || true' EXIT

./tests/test_lua_api "$PORT"

echo "lua api tests passed"
