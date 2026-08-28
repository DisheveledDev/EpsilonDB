# EpsilonDB Lua Scripting Engine — Implementation Plan

## Overview

Embed a Lua scripting engine (already vendored under `vendor/lua/`, Lua 5.4)
into the EpsilonDB server so admins can attach code to database events. All
scripts live in the system database and replicate to every node automatically.
The web admin console gains a "Code" area with a **Create Function** button
that opens a modal for authoring functions and database actions.

### Execution model

Per event fire:

1. Open a fresh `lua_State` and load a sandboxed standard library.
2. Register the scripting standard library (C callbacks).
3. Concatenate every code record into one big string and load/run it, so
   every record's global function is available.
4. Build the entity: a table of the record's JSON fields plus the record
   id as a second argument.
5. Call the database-action function named
   `<database>_<partition>_<keyspace>_<event>` as `fn(entity, id)`.
6. For `beforeInsert`/`beforeUpdate`, a returned table becomes the document
   that gets written; every other return value (and returns from other
   events) is ignored.
7. Destroy the `lua_State`.

Everything is transient; nothing persists across events except the stored
code strings.

## Confirmed decisions

| Decision | Choice |
|---|---|
| Event points | `beforeInsert`, `afterInsert`, `beforeUpdate`, `afterUpdate`, `beforeDelete`, `afterDelete` |
| Dispatch | by naming convention: the engine invokes the function named `<db>_<partition>_<keyspace>_<event>` |
| Write-back | a beforeInsert/beforeUpdate handler returning a table replaces the written document |
| Veto mechanism | `rollback(reason)` rejects the originating write with 4xx |
| State | fresh `lua_State` per event, destroyed after |
| Records | one code record per function (named function or database action) |
| Stdlib | `get/query/count/put/remove/rollback/log/cluster` |

## Function model

Two kinds of entries, both stored as code records:

- **Named function**: `{"type":"function","name":..,"code":..}`. A plain
  library function, callable from any other function. The console's Create
  Function modal (kind "Named function") takes just a name and pre-fills the
  skeleton:

  ```lua
  function generateId ()

  end
  ```

- **Database action**: `{"type":"action","name":"<db>_<partition>_<keyspace>_
  <event>","database":..,"partition":..,"keyspace":..,"event":..,"code":..}`.
  The name encodes the event and scope, so the dispatcher needs no trigger
  registry: it looks the function up by the computed name. The console's
  Create Function modal (kind "Database action") offers dropdowns for
  database, partition, keyspace and activity. Choosing database `demo`,
  partition `people`, keyspace `staff` and activity `beforeDelete` creates
  the entry `demo_people_staff_beforeDelete` with the skeleton:

  ```lua
  function demo_people_staff_beforeDelete (entity, id)

  end
  ```

Events: `beforeInsert`, `afterInsert`, `beforeUpdate`, `afterUpdate`,
`beforeDelete`, `afterDelete`. A put of a new record fires the insert pair,
a put of an existing record the update pair. Handlers receive the record's
document as `entity` (a table of its JSON fields) and the record key as
`id`. If a `beforeInsert`/`beforeUpdate` function returns the entity table,
its values are what gets written into the database (mutating the parameter
without returning it has no effect; returning a non-table has no effect).
The return value of a `beforeDelete` handler has no effect on the delete,
and any value returned from an `after*` function is ignored.
`rollback(reason)` vetoes the originating write (before* only).
Database/partition/keyspace names used in actions must be valid Lua
identifiers so the generated function name is definable.

## Sections of work

### Section 1 — Lua runtime + standard library

`src/lua/epsilon_lua.{h,c}`:

- `edb_lua_run(...)` entry point implementing the execution model above.
- Sandboxed library set: `string`, `table`, `math`, `utf8`, `base` minus
  `dofile/loadfile/collectgarbage/print` (print redirects to `log`). No
  `io`, `os`, `debug`, `package`/`require`, `coroutine`.
- Stdlib catalog:
  - `get(partition, keyspace, id)` — read a record (respects read masks).
  - `query(partition, keyspace [, filters])` — collection read.
  - `count(partition, keyspace)` — record count.
  - `put(partition, keyspace, id, entity)` — write a document.
  - `remove(partition, keyspace, id)` — soft-delete.
  - `rollback(reason)` — abort event, reject originating write with 4xx;
    a runtime error if called outside an active event.
  - `log(...)` — write to the server log.
  - `cluster()` — `{node_id, leader, member_count, pending_changes}`.
  - `date` table — sandboxed os.date/os.time replacement:
    `date.now()` (epoch seconds), `date.time([{year,month,day,hour,min,
    sec,isdst}])`, `date.format(fmt [, t])` (strftime), `date.diff(t1,t2)`.
- Reads/writes route through `g_repl` when present, else the bare engine,
  mirroring `handle_data_put` and `epsilon_eql.c`.
- Re-entrancy depth guard: a `put()`/`remove()` inside a handler must not
  re-trigger handlers.
- `before*` handlers may return the entity (auto write-back) and/or call
  `rollback`; `after*` handlers are best-effort and never block the write.

### Section 2 — Config storage layer

`src/engine/epsilon_config.{h,c}` + `epsilon_config_internal.h`:

- `CFG_KEYSPACE_CODE = "config_code"` (new reserved `__system__` keyspace).
- New helpers `edb_code_save/delete/load` over the existing
  `store/fetch/remove_record` helpers.
- Named function records: `{"type":"function","code":..}`.
- Database action records: `{"type":"action","database":..,"partition":..,
  "keyspace":..,"event":..,"code":..}`.
- Replication is automatic via `cfg->replicate` → `api_replicate_config` →
  `edb_repl_write`; no extra plumbing.
- Add `config_code` to `edb_config_is_system_key` (GC protection) and
  `edb_config_system_keyspaces` (stage-6e join snapshot).

### Section 3 — Compile validation + dispatch core

- `edb_lua_validate(code, char **err)` — sandboxed `luaL_loadbuffer`
  (compile only, never execute).
- `edb_lua_fire(...)` — read all code records, run them as one chunk,
  compute the action name `<db>_<partition>_<keyspace>_<event>`, invoke the
  function as `fn(entity, id)`. When no record defines that function the
  event is a no-op (fast path without a Lua state).
- `edb_lua_action_name(...)` — builds the action function name.

### Section 4 — Engine integration

Attach dispatch to:

- `handle_data_put` → `beforeInsert`/`afterInsert` (new record) or
  `beforeUpdate`/`afterUpdate` (existing record)
- `handle_data_delete` → `beforeDelete` / `afterDelete`
- EQL write-back (`epsilon_eql.c`) → `afterInsert`/`afterUpdate`/
  `afterDelete`
- `api_apply_change_impl` (replication apply) → the after_* pair so
  replicas run scripts on replay (the pre-change record classifies
  inserts vs updates; deletes capture the deleted document).

### Section 5 — HTTP admin surface

`src/api/epsilon_api_lua.c` (handlers declared in `epsilon_api_internal.h`,
routes in `edb_api_register`):

| Route | Method | Purpose |
|---|---|---|
| `/admin/code` | GET | list functions (`?type=function`) / actions (`?type=action`) |
| `/admin/code` | POST | create/update a function or action; omitting `code` generates the skeleton |
| `/admin/code/validate` | POST | compile-check a snippet without saving |
| `/admin/code/<name>` | GET | fetch source |
| `/admin/code/<name>` | DELETE | remove function/action |

All gated by `require_admin_auth`. Function names and action scope
segments must be valid Lua identifiers (rejected otherwise so the
generated skeleton always compiles).

### Section 6 — Web admin "Code" area

`src/admin/admin_console.c`:

- New `['code','Code']` SECTIONS entry → `renderCode()`.
- A **Create Function** button opens a modal: kind (named function vs
  database action), for actions dropdowns of database, partition,
  keyspace (populated from the config registries, with a "(new…)"
  fallback for unwritten partitions/keyspaces) and activity, plus a live
  function-name preview. Create saves the entry and loads the generated
  skeleton into the editor.
- Two-pane layout: function list (name/kind/scope, edit/delete) and
  editor (validate/save). Saving an action re-derives the name from its
  stored scope, so the scope cannot drift.
- Reuse existing `api()` / `table()` / `mutate()` helpers.

### Section 7 — Tests

- `tests/test_lua.c` (unit, links Lua + ENGINE_LIB + REPL_SRC/CLUSTER_SRC):
  validate errors, stdlib ops, name-based dispatch, the insert/update
  split, return-based write-back (including nil/non-table returns),
  beforeDelete return-ignored, `rollback` rejection, sandbox denial
  (`os`/`io`/`loadfile`), transient state isolation.
- `tests/test_lua_api.c` + `tests/test_lua_apirun.sh` (real server, like
  `test_eql_apirun.sh`): skeleton generation for both kinds, REST +
  EQL write-back firing the right insert/update/delete events, vetoes,
  once-per-write afterInsert semantics, admin auth.

### Section 8 — Makefile, wiring, docs

- Add `LUA_SRC = src/lua/epsilon_lua.c`; link into `$(SERVER_BIN)` and
  `tests/test_lua`; register `test_lua_api` + runner; add entries to
  `TEST_SRC`/`TEST_BINS`/`test`.
- Add a Stage 9 section to `AGENTS.md` documenting the config keyspace,
  function naming, sandbox, and aggregate-execution model.

## Dependency graph

```
Section 1 (Lua runtime + stdlib)
        │
        ├──► Section 3 (validate + dispatch) ──► Section 4 (engine integration)
        │
Section 2 (config storage) ──► Section 3 ──► Section 4
                                     │
                                     └──────────────► Section 5 (HTTP) ──► Section 6 (web UI)
                                                                               │
Section 7 (tests)  depends on Sections 2, 3, 4 (unit) and 5 (API)               │
Section 8 (Makefile/docs) ── depends on 1, 2, 5, 7                              │
```

## Parallelization

The work splits into two largely independent tracks that can proceed
simultaneously (different authors, or one author interleaving):

### Track A — Engine (backend)

- **Section 1** (Lua runtime + stdlib) and **Section 2** (config storage) are
  independent of each other and can be built in parallel.
- **Section 3** (validate + dispatch) joins 1 and 2.
- **Section 4** (engine integration) follows directly from 3.

### Track B — Surfacing (frontend API + UI)

- **Section 5** (HTTP admin surface) independently defines its request/response
  contract. As long as that contract is agreed up front, it can be built in
  parallel with Sections 1–3.
- **Section 6** (web "Code" area) depends only on Section 5's API contract and
  can start as soon as the endpoints/shapes are frozen (or alongside 5).

### Recommended sequence

1. **Kick off in parallel:**
   - A1: Section 1 (Lua runtime + stdlib)
   - A2: Section 2 (config storage)
   - B1: Section 5 (HTTP surface) — freeze the JSON contract first so B2 can
     follow immediately
2. **Join:** Section 3 (dispatch) once A1 + A2 land.
3. **Then:**
   - Section 4 (engine integration) after Section 3.
   - Section 6 (web UI) after the Section 5 contract.
4. **Tests (Section 7)** can begin incrementally:
   - unit tests against Section 2 + 3 as they land,
   - integration tests after Section 4,
   - API tests after Section 5.
5. **Section 8** (Makefile + docs) is mostly a final pass but the Makefile
   wiring for `LUA_SRC` can be landed as soon as Section 1 exists.

### Quick summary of what can overlap

| Parallel pair | Rationale |
|---|---|
| 1 ⇄ 2 | runtime vs storage — no shared code |
| (1+2+3) ⇄ 5 | dispatch core vs HTTP contract — only a frozen API shape is needed |
| 5 ⇄ 6 | start UI as soon as endpoint shapes are agreed |
| 7 (unit) ⇄ 4 | validate/dispatch tests don't need engine integration |
| 8 (Makefile) ⇄ everything | the `LUA_SRC` wiring can land the moment Section 1 compiles |

The only hard sequential chains are: **2 → 3 → 4** (storage → dispatch →
engine), and **5 → 6** (API → UI). Everything else can be built concurrently.
