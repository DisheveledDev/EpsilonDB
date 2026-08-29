/* epsilon_lua.h - embedded Lua scripting engine.
 *
 * Administrators attach Lua functions to database events. Scripts live
 * in the system database (config_code keyspace) and replicate to every
 * node, so each node runs the same handlers for the same events.
 *
 * Every code record defines one global function:
 *   - a named function ("type":"function"): a plain library function,
 *     callable from any other function.
 *   - a database action ("type":"action"): a function named
 *     <database>_<partition>_<keyspace>_<event> (for example
 *     demo_people_staff_beforeDelete) that the engine invokes when the
 *     matching event fires for that scope.
 *
 * Handler signature: function name (entity, id). `entity` is the
 * record's document (a table of its JSON fields); `id` is the record
 * key. For beforeInsert/beforeUpdate, the value the function RETURNS is
 * the document that gets written (return a table to replace the stored
 * document; any other return value leaves the incoming document
 * untouched). Return values are ignored everywhere else: beforeDelete
 * cannot change the delete, and after* handlers are best-effort.
 *
 * Veto: rollback(reason) rejects the originating write (before* only;
 * after* handlers are best-effort and never block the write).
 *
 * Execution model (per event fire):
 *   1. Open a fresh lua_State and load a sandboxed standard library
 *      (string/table/math/utf8/base minus dofile/loadfile/
 *      collectgarbage; print redirects to log; no io/os/debug/package/
 *      coroutine, but a global `date` table covers time handling:
 *      date.now/time/format/diff).
 *   2. Register the scripting standard library (C callbacks).
 *   3. Concatenate every code record into one chunk, load and run it,
 *      so every record's global function is defined.
 *   4. Push the document as a table plus the id, and call the action
 *      function named <database>_<partition>_<keyspace>_<event>.
 *   5. For beforeInsert/beforeUpdate, a returned table becomes the
 *      document the caller writes.
 *   6. Destroy the lua_State. Nothing persists across events except the
 *      stored code strings.
 *
 * Re-entrancy: while a handler runs, any nested write (put/remove)
 * never re-triggers handlers, locally (depth guard) or on replicas (the
 * nested change carries "lua":true which the apply path skips).
 */

#ifndef EPSILON_LUA_H
#define EPSILON_LUA_H

#include <stdbool.h>
#include <stdint.h>

#include "../engine/epsilon_engine.h"
#include "../engine/epsilon_config.h"
#include "../socket/epsilon_repl.h"

/* Event points. A put of a new record fires the insert pair, a put of
 * an existing record the update pair; deletes fire the delete pair. */
typedef enum {
    EDB_LUA_BEFORE_INSERT = 0,
    EDB_LUA_AFTER_INSERT,
    EDB_LUA_BEFORE_UPDATE,
    EDB_LUA_AFTER_UPDATE,
    EDB_LUA_BEFORE_DELETE,
    EDB_LUA_AFTER_DELETE
} edb_lua_event;

/* Server context handed to the engine. repl/cluster may be NULL
 * (single-node). */
typedef struct {
    edb_engine *engine;   /* required */
    edb_config *config;   /* required: code store + partition masks */
    edb_repl *repl;       /* may be NULL */
    edb_cluster *cluster; /* may be NULL */
} edb_lua_ctx;

/* Event payload.
 *
 * `value` semantics per event:
 *   before_insert/before_update: the document about to be written
 *     (caller-owned; the engine REPLACES it with the returned entity
 *     when the handler returns a table, so the caller must serialize
 *     *value afterwards);
 *   after_insert/after_update: the document that was written (may be
 *     NULL: the engine fetches the current record itself);
 *   beforeDelete: the current document (may be NULL: the engine
 *     fetches the record itself);
 *   afterDelete: the document that was deleted (may be NULL: the
 *     handler receives a nil entity).
 *
 * user_groups/trusted control stdlib permission checks: trusted callers
 * (replication apply, admin flows) skip mask checks entirely; otherwise
 * the event's database/partition masks are checked against user_groups.
 */
typedef struct {
    const char *database;
    const char *partition;
    const char *keyspace;
    const char *id;
    cJSON **value;       /* pointer to caller-owned doc (may be replaced) */
    uint64_t user_groups;
    bool trusted;
} edb_lua_event_arg;

/* Result of firing an event. */
typedef enum {
    EDB_LUA_OK = 0,     /* completed; *value may hold a returned entity */
    EDB_LUA_ROLLBACK,   /* vetoed via rollback(reason) */
    EDB_LUA_ERROR       /* a script failed to compile or run */
} edb_lua_result;

/* Compile-only validation of a Lua snippet under a fresh sandboxed
 * state (never executed). Returns true when the snippet compiles; on
 * failure *err receives a malloc'd message (caller frees with free). */
bool edb_lua_validate(const char *code, char **err);

/* Fires one event: loads every code record, then invokes the global
 * function named <database>_<partition>_<keyspace>_<event> as
 * function(entity, id). When no record defines that function the event
 * is a no-op. On EDB_LUA_ROLLBACK, *reason receives a malloc'd rollback
 * reason. For before_insert/before_update the caller applies the
 * possibly replaced *arg->value and must reject the originating write
 * on ROLLBACK/ERROR; after_* failures are best-effort (EDB_LUA_OK). */
edb_lua_result edb_lua_fire(const edb_lua_ctx *ctx, edb_lua_event event,
                            edb_lua_event_arg *arg, char **reason);

/* Canonical name of an event ("beforeInsert", "afterInsert", ...). */
const char *edb_lua_event_name(edb_lua_event event);

/* Parses an event name into the enum. Returns false for unrecognized
 * names. */
bool edb_lua_event_parse(const char *name, edb_lua_event *out);

/* Builds the database-action function name for a scope and event
 * ("demo_people_staff_beforeDelete"). Returns false when the result
 * would not fit in `size`. */
bool edb_lua_action_name(const char *database, const char *partition,
                         const char *keyspace, edb_lua_event event,
                         char *out, size_t size);

#endif /* EPSILON_LUA_H */
