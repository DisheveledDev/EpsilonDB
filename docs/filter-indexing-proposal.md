# Filter indexing: findings and proposal

Status: **proposal — no engine code changed on this branch.** Reproduce the
numbers with `docs/filter-index-study.sh` (needs Python 3 and SQLite; run it
in the same container the test suite uses).

Goal: make JSON filtering faster without materially growing shard files.

## 1. Why no index can help today

`prepare_live_query()` in `src/engine/shard.c` builds every filter as:

```sql
AND json_type(value, ?) IS json_type(?, '$')
AND json_extract(value, ?) IS json_extract(?, '$')
```

The **JSON path is a bound parameter**, not a literal. SQLite can only use
an expression index when the indexed expression appears in the query with
the *same literal arguments*, so a bound path can never match an index. This
is not a tuning problem; it is a property of the SQL we emit.

Measured (200,000 rows, `EXPLAIN QUERY PLAN`):

| | plan |
|---|---|
| today's SQL, no expression index | `MULTI-INDEX OR` (via `idx_ttl`) |
| today's SQL, **with** an expression index on `json_extract(value,'$."status"')` | `MULTI-INDEX OR` — index ignored |

So "just add an index" changes nothing. The SQL generator has to change
first.

## 2. History: this was tried and removed

`setup_schema()` still executes `DROP TABLE IF EXISTS DataFilter;`. A side
table named `DataFilter` existed in the first implementation and was removed
by commit `3eff0ad` ("updated how filters work to make them more flexible") —
the flexibility being exactly the bound-path form that blocks indexing.
Option D below is that design, re-measured, and it is the worst of the three
on both axes. Any new proposal has to beat it or explain why it differs.

## 3. Measurements

200,000 records, 4-key JSON documents, equality filter on `status`
(4 distinct values). Times are the best of 25 runs; file sizes are the whole
shard.

| option | query | shard file | delta | write (20k rows) |
|---|---|---|---|---|
| **A. today** (bound path) | 319.40 ms | 31.68 MB | — | 148.4 ms |
| **B. inlined literal + expression index** | 68.79 ms | 34.72 MB | +3.04 MB (+9.6%) | 220.8 ms |
| **C. generated column + index** | 68.90 ms | 34.71 MB | +3.04 MB (+9.6%) | 184.7 ms |
| **D. side table + index** | 101.57 ms | 43.92 MB | +12.25 MB (+38.7%) | not measured |

B and C both produce `SEARCH Data USING INDEX ... (=?)` and are ~4.6x faster
than today. D is slower *and* four times more expensive in storage, which is
consistent with it having been removed.

Vendored SQLite is 3.53.4, so both expression indexes (3.9+) and generated
columns (3.31+) are available.

## 4. Recommendation

**Option B — inline the JSON path as a literal and add an expression index
per configured key.** B and C are indistinguishable on read performance and
storage; B wins on operability:

- Adding or removing an indexed key is `CREATE INDEX` / `DROP INDEX`, with no
  `ALTER TABLE` on a live shard and nothing to migrate in existing files.
- C needs one generated column per indexed key, added by `ALTER TABLE`, and
  dropping them again is awkward.

C's lower write cost (184.7 ms vs 220.8 ms) is the one point in its favour;
if write amplification turns out to matter more than migration simplicity,
C is the fallback and needs no other change to this plan.

Neither should be applied to every key. Indexing is opt-in per
partition/keyspace, driven by the filter keys the analytics recorder already
tracks (`edb_analytics_record_query` stores filter-key names today), so the
set stays small and the +9.6% is paid only where it buys something.

## 5. Consequences that must be handled

1. **The path must be escaped, not interpolated.** Inlining a literal means
   building SQL from a user-supplied key. `json_path_for_key()` already
   escapes `"` and `\` for the JSON path; the SQL literal needs its own
   quoting (double any `'`) or the key becomes an injection vector. This is
   the single highest-risk part of the change.

2. **The statement cache needs attention, though not for the obvious
   reason.** `prepare_live_query()` does **not** use the cache at all — it
   calls `sqlite3_prepare_v2()` directly and finalises each time, so filter
   queries already pay a full prepare on every call. That is worth fixing
   alongside this work, because inlining literals is what finally makes
   caching filter statements worthwhile.

   When they are cached, two limits bite. `EDB_STMT_CACHE_SIZE` is 8, and
   `edb_cached_stmt.sql` is `char[192]` filled with
   `snprintf(slot->sql, sizeof(slot->sql), "%s", sql)` while lookup does
   `strcmp(sh->cache[i].sql, sql)` against the *untruncated* string. So any
   statement longer than 191 bytes can never match its own cache entry: it
   is re-prepared on every call and still evicts a live entry each time,
   making the cache worse than useless for long SQL. An inlined filter query
   is comfortably longer than 191 bytes. Both the slot size and the entry
   count must be raised (or the key changed to a hash) before caching
   filter statements — otherwise the cache silently degrades.

3. **Mixed queries.** A filter set combining an indexed and a non-indexed key
   should still inline only the indexed one and leave the rest bound.

4. **`ne` and range operators.** The measurement covers equality. `ne`
   deliberately uses `IS NOT`, which will not use an index; range operators
   (`gt`/`lt`) can, but only when the JSON value's type is consistent, which
   the current `json_type(...) IN ('integer','real')` guard already
   constrains.

5. **Index maintenance.** New indexes need creating on shard open for
   existing files, and the existing `reindex_seconds` maintenance already
   covers upkeep.

## 6. Suggested next step

Implement B behind a per-partition `indexed_keys` list, defaulting to empty
so nothing changes until a key is opted in, and re-run
`docs/filter-index-study.sh` against the real engine with `bin/epsilonbench`
to confirm the synthetic numbers hold.
