#!/bin/bash
# Filter-indexing investigation.
#
# EpsilonDB filters compile to  json_extract(value, ?)  with the JSON path
# BOUND AS A PARAMETER. SQLite expression indexes require the indexed
# expression to appear with literal arguments, so no index can ever be used
# by the current SQL. This measures the three realistic alternatives on a
# schema identical to src/engine/shard.c setup_schema().
set -u
cd /tmp || exit 1
ROWS=${ROWS:-200000}

python3 - "$ROWS" <<'PY'
import json, os, random, sqlite3, sys, time

rows = int(sys.argv[1])
random.seed(7)

SCHEMA = """
CREATE TABLE IF NOT EXISTS Data (
 id TEXT PRIMARY KEY, value BLOB, ttl INTEGER, timestamp INT, origin TEXT);
CREATE INDEX IF NOT EXISTS idx_ttl ON Data (ttl);
"""

def build(path):
    if os.path.exists(path): os.remove(path)
    db = sqlite3.connect(path)
    db.executescript(SCHEMA)
    now = int(time.time())
    batch = []
    for i in range(rows):
        doc = {"status": random.choice(["active","archived","pending","deleted"]),
               "score": random.randint(0, 10000),
               "owner": "user%d" % random.randint(1, 5000),
               "note": "x" * 40}
        batch.append((f"id-{i}", json.dumps(doc), None, now, None))
    db.executemany("INSERT INTO Data (id,value,ttl,timestamp,origin) VALUES (?,?,?,?,?)", batch)
    db.commit()
    return db

def size(path):
    return os.path.getsize(path)

def timed(db, sql, params, n=25):
    db.execute("PRAGMA cache_size=-2048")
    best = None
    for _ in range(n):
        t = time.perf_counter()
        db.execute(sql, params).fetchall()
        dt = time.perf_counter() - t
        best = dt if best is None else min(best, dt)
    return best * 1000.0

print(f"rows = {rows}\n")

# ---- A. baseline: exactly what shard.c emits today -------------------
p = "/tmp/a.sqlite"; db = build(p); base_size = size(p)
sql_bound = ("SELECT value FROM Data WHERE (ttl IS NULL OR ttl >= ?)"
             " AND json_type(value, ?) IS json_type(?, '$')"
             " AND json_extract(value, ?) IS json_extract(?, '$')")
args = (0, '$."status"', '"active"', '$."status"', '"active"')
t_base = timed(db, sql_bound, args)
plan = db.execute("EXPLAIN QUERY PLAN " + sql_bound, args).fetchall()
print(f"A. today (bound path, no index)   {t_base:8.2f} ms   file {base_size/1048576:6.2f} MB")
print(f"   plan: {plan[0][3]}")

# does an expression index even get considered with a bound path?
db.execute("CREATE INDEX idx_expr ON Data (json_extract(value, '$.\"status\"'))")
db.commit()
plan2 = db.execute("EXPLAIN QUERY PLAN " + sql_bound, args).fetchall()
print(f"   with an expression index present, bound-path plan: {plan2[0][3]}")
db.close()

# ---- B. inlined literal path + expression index ----------------------
p = "/tmp/b.sqlite"; db = build(p)
db.execute("CREATE INDEX idx_status ON Data (json_extract(value, '$.\"status\"'))")
db.commit()
b_size = size(p)
sql_inline = ("SELECT value FROM Data WHERE (ttl IS NULL OR ttl >= ?)"
              " AND json_extract(value, '$.\"status\"') = ?")
t_b = timed(db, sql_inline, (0, "active"))
plan = db.execute("EXPLAIN QUERY PLAN " + sql_inline, (0, "active")).fetchall()
print(f"\nB. inlined literal + expr index   {t_b:8.2f} ms   file {b_size/1048576:6.2f} MB"
      f"   (+{(b_size-base_size)/1048576:.2f} MB, +{100*(b_size-base_size)/base_size:.1f}%)")
print(f"   plan: {plan[0][3]}")
db.close()

# ---- C. generated column + index -------------------------------------
p = "/tmp/c.sqlite"; db = build(p)
db.execute("ALTER TABLE Data ADD COLUMN f_status TEXT "
           "GENERATED ALWAYS AS (json_extract(value, '$.\"status\"')) VIRTUAL")
db.execute("CREATE INDEX idx_gen ON Data (f_status)")
db.commit()
c_size = size(p)
sql_gen = "SELECT value FROM Data WHERE (ttl IS NULL OR ttl >= ?) AND f_status = ?"
t_c = timed(db, sql_gen, (0, "active"))
plan = db.execute("EXPLAIN QUERY PLAN " + sql_gen, (0, "active")).fetchall()
print(f"\nC. generated column + index       {t_c:8.2f} ms   file {c_size/1048576:6.2f} MB"
      f"   (+{(c_size-base_size)/1048576:.2f} MB, +{100*(c_size-base_size)/base_size:.1f}%)")
print(f"   plan: {plan[0][3]}")
db.close()

# ---- D. side table ---------------------------------------------------
p = "/tmp/d.sqlite"; db = build(p)
db.execute("CREATE TABLE DataFilter (id TEXT, key TEXT, val TEXT)")
db.execute("INSERT INTO DataFilter (id,key,val) "
           "SELECT id, 'status', json_extract(value,'$.\"status\"') FROM Data")
db.execute("CREATE INDEX idx_df ON DataFilter (key, val, id)")
db.commit()
d_size = size(p)
sql_side = ("SELECT d.value FROM Data d JOIN DataFilter f ON f.id = d.id"
            " WHERE (d.ttl IS NULL OR d.ttl >= ?) AND f.key='status' AND f.val = ?")
t_d = timed(db, sql_side, (0, "active"))
print(f"\nD. side table + index             {t_d:8.2f} ms   file {d_size/1048576:6.2f} MB"
      f"   (+{(d_size-base_size)/1048576:.2f} MB, +{100*(d_size-base_size)/base_size:.1f}%)")
db.close()

# ---- write cost ------------------------------------------------------
print("\nwrite cost (insert 20k rows):")
for label, extra in (("no index", None),
                     ("expr index", "CREATE INDEX ix ON Data (json_extract(value,'$.\"status\"'))"),
                     ("generated col + index",
                      "ALTER TABLE Data ADD COLUMN g TEXT GENERATED ALWAYS AS (json_extract(value,'$.\"status\"')) VIRTUAL;"
                      "CREATE INDEX ix2 ON Data (g)")):
    pp = "/tmp/w.sqlite"
    if os.path.exists(pp): os.remove(pp)
    db = sqlite3.connect(pp); db.executescript(SCHEMA)
    if extra: db.executescript(extra)
    now = int(time.time())
    batch = [(f"w-{i}", json.dumps({"status":"active","score":i}), None, now, None)
             for i in range(20000)]
    t = time.perf_counter()
    db.executemany("INSERT INTO Data (id,value,ttl,timestamp,origin) VALUES (?,?,?,?,?)", batch)
    db.commit()
    print(f"   {label:24} {1000*(time.perf_counter()-t):8.1f} ms")
    db.close()
PY
