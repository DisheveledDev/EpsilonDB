#!/bin/sh
# Stage 6e end-to-end: spawns a seed node and a joiner as real zestyd
# processes, then drives the join through the HTTP admin API.
set -e
cd "$(dirname "$0")/.."

SEED_HTTP=${1:-18993}
SEED_PEER=$((SEED_HTTP + 1000))
JOIN_HTTP=$((SEED_HTTP + 1))
JOIN_PEER=$((SEED_HTTP + 1001))

DATA=tests/data/join_live
rm -rf "$DATA"
mkdir -p "$DATA/a" "$DATA/b"

bin/zestyd -p "$SEED_HTTP" -n "$SEED_PEER" -d "$DATA/a" \
    >"$DATA/a/server.log" 2>&1 &
PID_A=$!
bin/zestyd -p "$JOIN_HTTP" -n "$JOIN_PEER" -d "$DATA/b" \
    >"$DATA/b/server.log" 2>&1 &
PID_B=$!
trap 'kill $PID_A $PID_B 2>/dev/null || true; wait $PID_A $PID_B 2>/dev/null || true' EXIT

./tests/test_join "$SEED_HTTP" "$SEED_PEER" "$JOIN_HTTP" "$JOIN_PEER"
