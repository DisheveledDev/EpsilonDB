#!/bin/sh
# Starts a throwaway epsilond, runs the EQL HTTP integration tests, tears
# down. Mirrors tests/test_http_run.sh.
set -e
cd "$(dirname "$0")/.."

PORT=${EPSILON_TEST_EQL_PORT:-18992}
DATA=tests/data/eql_live

rm -rf "$DATA"
mkdir -p "$DATA"
bin/epsilond -p "$PORT" -d "$DATA" -s "$DATA/epsilon-admin.sock" \n    >"$DATA/server.log" 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null || true; wait $PID 2>/dev/null || true' EXIT

./tests/test_eql_api "$PORT"

# ---- eql console (eql-e) ----
SOCK="$DATA/epsilon-admin.sock"
ls -la /tmp >/dev/null   # noop to keep set -e happy between blocks

out=$(bin/eql -s "$SOCK" 'SELECT COUNT(*) AS n FROM app.people.employees' | tail -1)
echo "$out" | grep -q "(1 row)" || { echo "eql select smoke failed: $out"; exit 1; }

out=$(bin/eql -r -s "$SOCK" 'SELECT id FROM app.people.employees')
echo "$out" | grep -q '"columns"' || { echo "eql raw failed"; exit 1; }

out=$(bin/eql -s "$SOCK" "INSERT INTO app.people.employees (id, name) VALUES ('zz1','Zed')")
echo "$out" | grep -q "row inserted" || { echo "eql insert failed: $out"; exit 1; }

out=$(bin/eql -s "$SOCK" 'SELECT name FROM app.people.employees WHERE id = '"'"'zz1'"'"'' | tail -2 | head -1)
echo "$out" | grep -q "Zed" || { echo "eql read-back failed: $out"; exit 1; }

echo "eql console tests passed"
