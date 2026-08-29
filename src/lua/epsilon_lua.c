/* epsilon_lua.c - embedded Lua scripting engine.
 *
 * See epsilon_lua.h for the execution model. This file implements the
 * sandboxed runtime, the scripting standard library, JSON <-> Lua
 * conversion (JSON null round-trips through a light-userdata sentinel
 * exposed to scripts as the global `null`), and the per-event dispatch
 * core: every code record is loaded as one chunk, then the function
 * named <database>_<partition>_<keyspace>_<event> is invoked as
 * function(entity, id).
 */

#include "epsilon_lua.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../epsilon_log.h"
#include "../../vendor/lua/lua.h"
#include "../../vendor/lua/lauxlib.h"
#include "../../vendor/lua/lualib.h"

/* Instruction budget per event. A count hook aborts runaway scripts;
 * ~200M VM instructions is a few seconds of CPU on modern hardware. */
#define EDB_LUA_MAX_INSTRUCTIONS 200000000LL
#define EDB_LUA_HOOK_STEP        100000

/* Max nesting depth while converting Lua tables <-> JSON (guards cyclic
 * tables, which JSON cannot represent). */
#define EDB_LUA_MAX_DEPTH 32

/* The JSON null sentinel: a light userdata whose address is unique. */
static char edb_lua_null_sentinel_storage;

/* Registry key holding the active session for the watchdog hook. */
static char edb_lua_session_key;

/* Re-entrancy depth: while an event handler is running, nested fires
 * return OK immediately so put()/remove() from inside a handler never
 * re-trigger handlers on this node. */
static _Thread_local int edb_lua_depth = 0;

typedef struct {
    const edb_lua_ctx *ctx;
    const edb_lua_event_arg *arg;
    edb_lua_event event;
    lua_State *L;
    bool in_event;              /* handler phase: rollback is live */
    bool rolled_back;
    char rollback_reason[512];
    long long instr_remaining;
    int convert_depth;
} edb_lua_session;

/* --- event names ------------------------------------------------------ */

const char *edb_lua_event_name(edb_lua_event event)
{
    switch (event) {
    case EDB_LUA_BEFORE_INSERT: return "beforeInsert";
    case EDB_LUA_AFTER_INSERT:  return "afterInsert";
    case EDB_LUA_BEFORE_UPDATE: return "beforeUpdate";
    case EDB_LUA_AFTER_UPDATE:  return "afterUpdate";
    case EDB_LUA_BEFORE_DELETE: return "beforeDelete";
    case EDB_LUA_AFTER_DELETE:  return "afterDelete";
    }
    return "unknown";
}

bool edb_lua_event_parse(const char *name, edb_lua_event *out)
{
    if (!name || !out) {
        return false;
    }
    if (strcmp(name, "beforeInsert") == 0) { *out = EDB_LUA_BEFORE_INSERT; return true; }
    if (strcmp(name, "afterInsert") == 0)  { *out = EDB_LUA_AFTER_INSERT; return true; }
    if (strcmp(name, "beforeUpdate") == 0) { *out = EDB_LUA_BEFORE_UPDATE; return true; }
    if (strcmp(name, "afterUpdate") == 0)  { *out = EDB_LUA_AFTER_UPDATE; return true; }
    if (strcmp(name, "beforeDelete") == 0) { *out = EDB_LUA_BEFORE_DELETE; return true; }
    if (strcmp(name, "afterDelete") == 0)  { *out = EDB_LUA_AFTER_DELETE; return true; }
    return false;
}

bool edb_lua_action_name(const char *database, const char *partition,
                         const char *keyspace, edb_lua_event event,
                         char *out, size_t size)
{
    if (!database || !partition || !keyspace || !out || !size) {
        return false;
    }
    int n = snprintf(out, size, "%s_%s_%s_%s", database, partition,
                     keyspace, edb_lua_event_name(event));
    return n >= 0 && (size_t)n < size;
}

/* --- JSON <-> Lua conversion ------------------------------------------ */

static void *null_sentinel(void)
{
    return &edb_lua_null_sentinel_storage;
}

/* Pushes a cJSON value as a Lua value. Objects become tables, arrays
 * become 1-based array tables, null becomes the sentinel (round-trips
 * back to JSON null), so unchanged entities serialize identically. */
static void push_json(lua_State *L, const cJSON *v, int depth)
{
    if (!v || depth > EDB_LUA_MAX_DEPTH) {
        lua_pushnil(L);
        return;
    }
    switch (v->type & 0xFF) {
    case cJSON_NULL:
        lua_pushlightuserdata(L, null_sentinel());
        break;
    case cJSON_False:
        lua_pushboolean(L, 0);
        break;
    case cJSON_True:
        lua_pushboolean(L, 1);
        break;
    case cJSON_Number:
        if (v->valuedouble >= -9007199254740992.0 &&
            v->valuedouble <= 9007199254740991.0 &&
            v->valuedouble == (double)(long long)v->valuedouble) {
            lua_pushinteger(L, (lua_Integer)v->valuedouble);
        } else {
            lua_pushnumber(L, v->valuedouble);
        }
        break;
    case cJSON_String:
        lua_pushstring(L, v->valuestring ? v->valuestring : "");
        break;
    case cJSON_Array: {
        lua_createtable(L, cJSON_GetArraySize(v), 0);
        int i = 1;
        const cJSON *item = NULL;
        cJSON_ArrayForEach(item, v) {
            push_json(L, item, depth + 1);
            lua_rawseti(L, -2, i++);
        }
        break;
    }
    case cJSON_Object: {
        lua_createtable(L, 0, cJSON_GetArraySize(v));
        const cJSON *item = NULL;
        cJSON_ArrayForEach(item, v) {
            if (item->string) {
                push_json(L, item, depth + 1);
                lua_setfield(L, -2, item->string);
            }
        }
        break;
    }
    default:
        lua_pushnil(L);
        break;
    }
}

/* Converts a Lua value at idx into a freshly built cJSON value. NULL on
 * conversion failure (with a message on the Lua stack via luaL_error). */
static cJSON *lua_to_json(edb_lua_session *s, lua_State *L, int idx)
{
    if (s->convert_depth > EDB_LUA_MAX_DEPTH) {
        luaL_error(L, "value nested too deeply");
        return NULL;
    }
    /* resolve to an absolute index: the conversion pushes onto the stack,
     * which would shift any relative index mid-iteration */
    idx = lua_absindex(L, idx);
    s->convert_depth++;
    cJSON *out = NULL;
    int t = lua_type(L, idx);
    switch (t) {
    case LUA_TNIL:
        out = cJSON_CreateNull();
        break;
    case LUA_TBOOLEAN:
        out = cJSON_CreateBool(lua_toboolean(L, idx) != 0);
        break;
    case LUA_TNUMBER:
        out = cJSON_CreateNumber(lua_tonumber(L, idx));
        break;
    case LUA_TSTRING: {
        const char *str = lua_tostring(L, idx);
        out = cJSON_CreateString(str ? str : "");
        break;
    }
    case LUA_TLIGHTUSERDATA:
        out = cJSON_CreateNull();
        break;
    case LUA_TTABLE: {
        /* Pass 1: decide array vs object. An array table has only
         * positive integer keys; object keys stringify. */
        bool as_array = true;
        lua_Integer max_key = 0;
        lua_pushnil(L);
        while (lua_next(L, idx) != 0) {
            if (as_array) {
                bool int_key = lua_isinteger(L, -2);
                if (int_key) {
                    lua_Integer k = lua_tointeger(L, -2);
                    int_key = k >= 1 && k <= (lua_Integer)(1 << 24);
                    if (int_key && k > max_key) {
                        max_key = k;
                    }
                }
                if (!int_key) {
                    as_array = false;
                }
            }
            lua_pop(L, 1);
        }
        if (as_array) {
            out = cJSON_CreateArray();
            for (lua_Integer i = 1; i <= max_key && out; i++) {
                lua_rawgeti(L, idx, i);
                cJSON *item = lua_to_json(s, L, -1);
                lua_pop(L, 1);
                if (!item) {
                    cJSON_Delete(out);
                    out = NULL;
                    break;
                }
                cJSON_AddItemToArray(out, item);
            }
        } else {
            out = cJSON_CreateObject();
            lua_pushnil(L);
            while (lua_next(L, idx) != 0 && out) {
                /* value at -1, key at -2 */
                if (!lua_isnil(L, -1)) {
                    size_t klen = 0;
                    const char *key = lua_tolstring(L, -2, &klen);
                    cJSON *item = lua_to_json(s, L, -1);
                    if (!key || !item) {
                        if (item) {
                            cJSON_Delete(item);
                        }
                        cJSON_Delete(out);
                        out = NULL;
                        lua_pop(L, 2);
                        break;
                    }
                    cJSON_AddItemToObject(out, key, item);
                }
                lua_pop(L, 1);
            }
        }
        break;
    }
    default:
        luaL_error(L, "unsupported value type '%s' in entity",
                   lua_typename(L, t));
        break;
    }
    s->convert_depth--;
    return out;
}

/* Converts the table at the top of the Lua stack into a cJSON document.
 * The "id" key is excluded (the record key travels in the write
 * envelope, never inside the document), as are nil values. NULL when
 * the top value is not a table or conversion fails. */
static cJSON *returned_entity_to_doc(edb_lua_session *s, lua_State *L)
{
    if (lua_type(L, -1) != LUA_TTABLE) {
        return NULL;
    }
    /* copy minus "id"/nil values into a scratch table so the generic
     * converter has nothing special to know about */
    int src = lua_gettop(L);
    lua_newtable(L);
    int dst = lua_gettop(L);
    lua_pushnil(L);
    while (lua_next(L, src) != 0) {
        /* key at -2, value at -1 */
        bool is_id = false;
        if (lua_type(L, -2) == LUA_TSTRING) {
            is_id = strcmp(lua_tostring(L, -2), "id") == 0;
        }
        if (!is_id && !lua_isnil(L, -1)) {
            lua_pushvalue(L, -2);   /* key */
            lua_pushvalue(L, -2);   /* value */
            lua_rawset(L, dst);
        }
        lua_pop(L, 1);              /* pop value, keep key */
    }
    /* the loop leaves [table, scratch]; convert the scratch table */
    lua_remove(L, src);
    cJSON *doc = lua_to_json(s, L, -1);
    lua_pop(L, 1);                  /* pop scratch table */
    return doc;
}

/* --- scripting standard library --------------------------------------- */

static edb_lua_session *session_of(lua_State *L)
{
    return (edb_lua_session *)lua_touserdata(L, lua_upvalueindex(1));
}

/* log(...) and the redirected print(): join args with tabs, one line. */
static int lua_log_impl(lua_State *L)
{
    int n = lua_gettop(L);
    size_t total = 1;
    for (int i = 1; i <= n; i++) {
        size_t len = 0;
        luaL_tolstring(L, i, &len);
        total += len + 1;
        lua_pop(L, 1);
    }
    char *buf = malloc(total + 1);
    if (!buf) {
        return 0;
    }
    size_t off = 0;
    for (int i = 1; i <= n; i++) {
        size_t len = 0;
        const char *str = luaL_tolstring(L, i, &len);
        if (i > 1 && off < total) {
            buf[off++] = '\t';
        }
        size_t copy = len;
        if (off + copy > total) {
            copy = total - off;
        }
        memcpy(buf + off, str, copy);
        off += copy;
        lua_pop(L, 1);
    }
    buf[off] = '\0';
    edb_log("INFO", "lua: %s", buf);
    free(buf);
    return 0;
}

/* Permission gate for stdlib reads/writes. Trusted callers (replication
 * apply, admin flows) skip checks; implicit partitions allow all. */
static bool lua_check_partition_perm(const edb_lua_session *s,
                                     const char *partition,
                                     edb_permission perm)
{
    if (s->arg->trusted) {
        return true;
    }
    edb_partition_info info;
    if (!edb_partition_get(s->ctx->config, s->arg->database, partition,
                           &info)) {
        return true;   /* implicit partition: allow-all masks */
    }
    uint64_t mask;
    switch (perm) {
    case EDB_PERM_CREATE: mask = info.create_mask; break;
    case EDB_PERM_UPDATE: mask = info.update_mask; break;
    case EDB_PERM_READ:   mask = info.read_mask; break;
    case EDB_PERM_DELETE: mask = info.delete_mask; break;
    default:              mask = ~0ULL; break;
    }
    return edb_check_perm(mask, s->arg->user_groups, perm);
}

/* Fetches one record's document (local view). */
static cJSON *lua_fetch_doc(const edb_lua_session *s, const char *partition,
                            const char *keyspace, const char *id)
{
    return edb_get(s->ctx->engine, partition, keyspace, id);
}

/* Writes (or deletes) a record through the engine write paths, mirroring
 * the REST handlers and EQL write-back. Script-generated changes carry
 * "lua":true so replication apply never re-triggers handlers. */
static bool lua_write_change(const edb_lua_session *s, const char *partition,
                             const char *keyspace, const char *id,
                             const cJSON *value, bool *quorum_lost)
{
    *quorum_lost = false;
    bool ok = false;
    if (s->ctx->repl) {
        long long ts = (long long)time(NULL);
        cJSON *change = cJSON_CreateObject();
        if (!change) {
            return false;
        }
        if (value) {
            cJSON_AddStringToObject(change, "op", "put");
            cJSON_AddItemToObject(change, "value",
                                  cJSON_Duplicate(value, 1));
            cJSON_AddNumberToObject(change, "ttl_abs", -1);
        } else {
            cJSON_AddStringToObject(change, "op", "delete");
        }
        cJSON_AddStringToObject(change, "db", s->arg->database);
        cJSON_AddStringToObject(change, "partition", partition);
        cJSON_AddStringToObject(change, "keyspace", keyspace);
        cJSON_AddStringToObject(change, "id", id);
        cJSON_AddNumberToObject(change, "ts", (double)ts);
        cJSON_AddBoolToObject(change, "lua", 1);
        char *text = cJSON_PrintUnformatted(change);
        cJSON_Delete(change);
        if (!text) {
            return false;
        }
        edb_repl_status st = edb_repl_write(s->ctx->repl, s->arg->database,
                                            text);
        free(text);
        ok = st == EDB_REPL_OK;
        *quorum_lost = st == EDB_REPL_QUORUM_LOST;
    } else if (value) {
        char *json = cJSON_PrintUnformatted(value);
        ok = json &&
             edb_put(s->ctx->engine, partition, keyspace, id, json, -1);
        free(json);
    } else {
        ok = edb_delete(s->ctx->engine, partition, keyspace, id);
    }
    if (ok) {
        edb_partition_ensure(s->ctx->config, s->arg->database, partition,
                             keyspace, NULL);
    }
    return ok;
}

/* Pushes a document (plus injected "id") as a Lua table. */
static void push_doc_with_id(lua_State *L, const char *id, const cJSON *doc)
{
    if (!doc) {
        lua_pushnil(L);
        return;
    }
    push_json(L, doc, 0);
    if (lua_istable(L, -1) && id) {
        lua_pushstring(L, id);
        lua_setfield(L, -2, "id");
    }
}

static int lua_std_get(lua_State *L)
{
    edb_lua_session *s = session_of(L);
    const char *partition = luaL_checkstring(L, 1);
    const char *keyspace = luaL_checkstring(L, 2);
    const char *id = luaL_checkstring(L, 3);
    if (!lua_check_partition_perm(s, partition, EDB_PERM_READ)) {
        return luaL_error(L, "permission denied reading '%s'", partition);
    }
    cJSON *doc = NULL;
    if (s->ctx->repl) {
        cJSON *row = edb_repl_read_get(s->ctx->repl, s->arg->database,
                                       partition, keyspace, id);
        const cJSON *value = row
            ? cJSON_GetObjectItemCaseSensitive(row, "value")
            : NULL;
        doc = value ? cJSON_Duplicate(value, 1) : NULL;
        cJSON_Delete(row);
    } else {
        doc = lua_fetch_doc(s, partition, keyspace, id);
    }
    push_doc_with_id(L, id, doc);
    cJSON_Delete(doc);
    return 1;
}

/* query(partition, keyspace [, filters]) -> array of docs (id injected) */
static int lua_std_query(lua_State *L)
{
    edb_lua_session *s = session_of(L);
    const char *partition = luaL_checkstring(L, 1);
    const char *keyspace = luaL_checkstring(L, 2);
    if (!lua_check_partition_perm(s, partition, EDB_PERM_READ)) {
        return luaL_error(L, "permission denied reading '%s'", partition);
    }
    cJSON *filters = NULL;
    if (lua_gettop(L) >= 3 && !lua_isnil(L, 3)) {
        filters = lua_to_json(s, L, 3);
        if (!filters) {
            return 0;   /* luaL_error already raised */
        }
        if (!edb_filters_valid(filters)) {
            cJSON_Delete(filters);
            return luaL_error(L, "invalid filters");
        }
    }
    cJSON *rows = NULL;
    if (s->ctx->repl) {
        rows = edb_repl_read_query_meta(s->ctx->repl, s->arg->database,
                                        partition, keyspace, filters);
    } else {
        rows = edb_query_ts(s->ctx->engine, partition, keyspace, filters);
    }
    cJSON_Delete(filters);
    lua_newtable(L);
    if (rows) {
        const cJSON *row = NULL;
        int i = 1;
        cJSON_ArrayForEach(row, rows) {
            const cJSON *id = cJSON_GetObjectItemCaseSensitive(row, "id");
            const cJSON *value =
                cJSON_GetObjectItemCaseSensitive(row, "value");
            const char *idstr = cJSON_IsString(id) && id->valuestring
                                    ? id->valuestring
                                    : NULL;
            push_doc_with_id(L, idstr, value);
            lua_rawseti(L, -2, i++);
        }
        cJSON_Delete(rows);
    }
    return 1;
}

/* count(partition, keyspace) -> integer */
static int lua_std_count(lua_State *L)
{
    edb_lua_session *s = session_of(L);
    const char *partition = luaL_checkstring(L, 1);
    const char *keyspace = luaL_checkstring(L, 2);
    if (!lua_check_partition_perm(s, partition, EDB_PERM_READ)) {
        return luaL_error(L, "permission denied reading '%s'", partition);
    }
    size_t n = 0;
    if (s->ctx->repl) {
        char **ids = edb_repl_read_ids(s->ctx->repl, s->arg->database,
                                       partition, keyspace, NULL, &n);
        edb_free_strings(ids);
    } else {
        char **ids = edb_ids(s->ctx->engine, partition, keyspace, NULL, &n);
        edb_free_strings(ids);
    }
    lua_pushinteger(L, (lua_Integer)n);
    return 1;
}

/* put(partition, keyspace, id, entity) -> boolean.
 * The entity table's "id" key (injected by get/query) is excluded from
 * the stored document, mirroring EQL write-back where the record key
 * travels in the envelope. */
static int lua_std_put(lua_State *L)
{
    edb_lua_session *s = session_of(L);
    const char *partition = luaL_checkstring(L, 1);
    const char *keyspace = luaL_checkstring(L, 2);
    const char *id = luaL_checkstring(L, 3);
    luaL_checktype(L, 4, LUA_TTABLE);
    cJSON *existing = lua_fetch_doc(s, partition, keyspace, id);
    edb_permission perm = existing ? EDB_PERM_UPDATE : EDB_PERM_CREATE;
    cJSON_Delete(existing);
    if (!lua_check_partition_perm(s, partition, perm)) {
        return luaL_error(L, "permission denied writing '%s'", partition);
    }
    /* Serialize the entity without its "id" key. */
    cJSON *doc = cJSON_CreateObject();
    lua_pushnil(L);
    while (lua_next(L, 4) != 0) {
        bool is_id = false;
        if (lua_type(L, -2) == LUA_TSTRING) {
            is_id = strcmp(lua_tostring(L, -2), "id") == 0;
        }
        if (!is_id && !lua_isnil(L, -1)) {
            size_t klen = 0;
            const char *key = lua_tolstring(L, -2, &klen);
            cJSON *item = lua_to_json(s, L, -1);
            if (!key || !item) {
                cJSON_Delete(doc);
                return 0;   /* luaL_error already raised */
            }
            cJSON_AddItemToObject(doc, key, item);
        }
        lua_pop(L, 1);
    }
    bool quorum_lost = false;
    bool ok = lua_write_change(s, partition, keyspace, id, doc,
                               &quorum_lost);
    cJSON_Delete(doc);
    if (quorum_lost) {
        return luaL_error(L, "quorum unavailable: write rejected");
    }
    lua_pushboolean(L, ok ? 1 : 0);
    return 1;
}

/* remove(partition, keyspace, id) -> boolean */
static int lua_std_remove(lua_State *L)
{
    edb_lua_session *s = session_of(L);
    const char *partition = luaL_checkstring(L, 1);
    const char *keyspace = luaL_checkstring(L, 2);
    const char *id = luaL_checkstring(L, 3);
    if (!lua_check_partition_perm(s, partition, EDB_PERM_DELETE)) {
        return luaL_error(L, "permission denied deleting '%s'", partition);
    }
    bool quorum_lost = false;
    bool ok = lua_write_change(s, partition, keyspace, id, NULL,
                               &quorum_lost);
    if (quorum_lost) {
        return luaL_error(L, "quorum unavailable: delete rejected");
    }
    lua_pushboolean(L, ok ? 1 : 0);
    return 1;
}

/* rollback([reason]) - veto the originating write. Only valid while a
 * handler runs; before* events honor it, after* ignore it. */
static int lua_std_rollback(lua_State *L)
{
    edb_lua_session *s = session_of(L);
    if (!s->in_event) {
        return luaL_error(L, "rollback() called outside an active event");
    }
    s->rolled_back = true;
    const char *reason = luaL_optstring(L, 1, "rejected by script");
    snprintf(s->rollback_reason, sizeof(s->rollback_reason), "%s", reason);
    return 0;
}

/* cluster() -> {node_id, leader, member_count, pending_changes} */
static int lua_std_cluster(lua_State *L)
{
    edb_lua_session *s = session_of(L);
    const char *node_id = "";
    const char *leader = "";
    lua_Integer members = 1;
    lua_Integer pending = 0;
    if (s->ctx->cluster) {
        node_id = edb_cluster_self_id(s->ctx->cluster);
        const char *lead = edb_cluster_leader(s->ctx->cluster);
        leader = lead ? lead : "";
        edb_peer_info peers[256];
        members = (lua_Integer)edb_cluster_peers(s->ctx->cluster, peers,
                                                 sizeof(peers) /
                                                     sizeof(peers[0]));
        if (members < 1) {
            members = 1;
        }
    }
    if (s->ctx->repl) {
        pending = (lua_Integer)edb_repl_pending_total(s->ctx->repl);
    }
    lua_newtable(L);
    lua_pushstring(L, node_id ? node_id : "");
    lua_setfield(L, -2, "node_id");
    lua_pushstring(L, leader);
    lua_setfield(L, -2, "leader");
    lua_pushinteger(L, members);
    lua_setfield(L, -2, "member_count");
    lua_pushinteger(L, pending);
    lua_setfield(L, -2, "pending_changes");
    return 1;
}

/* --- date/time library ------------------------------------------------ */
/* Sandboxed replacement for os.date/os.time: a global `date` table with
 * epoch-second helpers. `os` stays blocked, so scripts keep time
 * handling without any of os' other escapes. */

/* date.now() -> current epoch seconds. */
static int lua_date_now(lua_State *L)
{
    lua_pushinteger(L, (lua_Integer)time(NULL));
    return 1;
}

/* date.time([table]) -> epoch seconds. A table may carry year, month,
 * day, hour, min, sec and isdst fields (mktime-normalized, like
 * os.time); without arguments returns the current time. */
static int lua_date_time(lua_State *L)
{
    time_t t = time(NULL);
    if (lua_gettop(L) >= 1 && !lua_isnil(L, 1)) {
        luaL_checktype(L, 1, LUA_TTABLE);
        struct tm tmv;
        memset(&tmv, 0, sizeof(tmv));
        lua_getfield(L, 1, "year");
        if (lua_isnumber(L, -1)) {
            tmv.tm_year = (int)lua_tointeger(L, -1) - 1900;
        }
        lua_pop(L, 1);
        lua_getfield(L, 1, "month");
        if (lua_isnumber(L, -1)) {
            tmv.tm_mon = (int)lua_tointeger(L, -1) - 1;
        }
        lua_pop(L, 1);
        lua_getfield(L, 1, "day");
        if (lua_isnumber(L, -1)) {
            tmv.tm_mday = (int)lua_tointeger(L, -1);
        }
        lua_pop(L, 1);
        lua_getfield(L, 1, "hour");
        if (lua_isnumber(L, -1)) {
            tmv.tm_hour = (int)lua_tointeger(L, -1);
        }
        lua_pop(L, 1);
        lua_getfield(L, 1, "min");
        if (lua_isnumber(L, -1)) {
            tmv.tm_min = (int)lua_tointeger(L, -1);
        }
        lua_pop(L, 1);
        lua_getfield(L, 1, "sec");
        if (lua_isnumber(L, -1)) {
            tmv.tm_sec = (int)lua_tointeger(L, -1);
        }
        lua_pop(L, 1);
        lua_getfield(L, 1, "isdst");
        if (lua_isboolean(L, -1)) {
            tmv.tm_isdst = lua_toboolean(L, -1) ? 1 : 0;
        } else {
            tmv.tm_isdst = -1;
        }
        lua_pop(L, 1);
        t = mktime(&tmv);
        if (t == (time_t)-1) {
            return luaL_error(L, "date.time: invalid date table");
        }
    }
    lua_pushinteger(L, (lua_Integer)t);
    return 1;
}

/* date.format(fmt [, t]) -> string. strftime-style formatting (%Y %m %d
 * %H %M %S ...) of the timestamp `t` (default: now), local time. */
static int lua_date_format(lua_State *L)
{
    const char *fmt = luaL_checkstring(L, 1);
    time_t t = time(NULL);
    if (lua_gettop(L) >= 2 && !lua_isnil(L, 2)) {
        t = (time_t)luaL_checkinteger(L, 2);
    }
    struct tm tmv;
    if (!localtime_r(&t, &tmv)) {
        return luaL_error(L, "date.format: cannot convert timestamp");
    }
    char buf[256];
    if (strftime(buf, sizeof(buf), fmt, &tmv) == 0) {
        buf[0] = '\0';   /* empty result or truncated format */
    }
    lua_pushstring(L, buf);
    return 1;
}

/* date.diff(t1, t2) -> t2 - t1 in seconds. */
static int lua_date_diff(lua_State *L)
{
    lua_Integer t1 = luaL_checkinteger(L, 1);
    lua_Integer t2 = luaL_checkinteger(L, 2);
    lua_pushinteger(L, t2 - t1);
    return 1;
}

/* Registers one C function into the table at the top of the stack. */
static void set_tabfn(lua_State *L, const char *name, lua_CFunction fn)
{
    lua_pushcfunction(L, fn);
    lua_setfield(L, -2, name);
}

/* --- sandbox ---------------------------------------------------------- */

/* Aborts a script that exceeds its instruction budget. */
static void lua_watchdog(lua_State *L, lua_Debug *ar)
{
    (void)ar;
    lua_pushlightuserdata(L, &edb_lua_session_key);
    lua_rawget(L, LUA_REGISTRYINDEX);
    edb_lua_session *s = (edb_lua_session *)lua_touserdata(L, -1);
    lua_pop(L, 1);
    if (s) {
        s->instr_remaining -= EDB_LUA_HOOK_STEP;
        if (s->instr_remaining <= 0) {
            luaL_error(L, "script exceeded the instruction limit");
        }
    }
}

/* Registers one stdlib function with the session as its upvalue. */
static void register_fn(lua_State *L, edb_lua_session *s, const char *name,
                        lua_CFunction fn)
{
    lua_pushlightuserdata(L, s);
    lua_pushcclosure(L, fn, 1);
    lua_setglobal(L, name);
}

/* Opens the sandboxed standard library: string/table/math/utf8/base
 * minus dofile/loadfile/collectgarbage; print redirects to log. No io,
 * os, debug, package/require, coroutine. */
static void open_sandbox(lua_State *L, edb_lua_session *s)
{
    luaL_requiref(L, LUA_GNAME, luaopen_base, 1);
    lua_pop(L, 1);
    luaL_requiref(L, LUA_TABLIBNAME, luaopen_table, 1);
    lua_pop(L, 1);
    luaL_requiref(L, LUA_STRLIBNAME, luaopen_string, 1);
    lua_pop(L, 1);
    luaL_requiref(L, LUA_MATHLIBNAME, luaopen_math, 1);
    lua_pop(L, 1);
    luaL_requiref(L, LUA_UTF8LIBNAME, luaopen_utf8, 1);
    lua_pop(L, 1);

    /* strip the base library's file/GC escape hatches */
    lua_pushnil(L);
    lua_setglobal(L, "dofile");
    lua_pushnil(L);
    lua_setglobal(L, "loadfile");
    lua_pushnil(L);
    lua_setglobal(L, "collectgarbage");
    lua_pushnil(L);
    lua_setglobal(L, "warn");

    /* print redirects to the server log */
    register_fn(L, s, "print", lua_log_impl);

    /* the JSON null sentinel, exposed to scripts */
    lua_pushlightuserdata(L, null_sentinel());
    lua_setglobal(L, "null");

    /* scripting stdlib */
    register_fn(L, s, "get", lua_std_get);
    register_fn(L, s, "query", lua_std_query);
    register_fn(L, s, "count", lua_std_count);
    register_fn(L, s, "put", lua_std_put);
    register_fn(L, s, "remove", lua_std_remove);
    register_fn(L, s, "log", lua_log_impl);
    register_fn(L, s, "cluster", lua_std_cluster);

    /* date/time library (sandboxed os.date/os.time replacement) */
    lua_newtable(L);
    set_tabfn(L, "now", lua_date_now);
    set_tabfn(L, "time", lua_date_time);
    set_tabfn(L, "format", lua_date_format);
    set_tabfn(L, "diff", lua_date_diff);
    lua_setglobal(L, "date");

    /* rollback is a stub until the handler phase starts */
    register_fn(L, s, "rollback", lua_std_rollback);
}

/* Installs the instruction watchdog. */
static void install_watchdog(lua_State *L, edb_lua_session *s)
{
    s->instr_remaining = EDB_LUA_MAX_INSTRUCTIONS;
    lua_pushlightuserdata(L, &edb_lua_session_key);
    lua_pushlightuserdata(L, s);
    lua_rawset(L, LUA_REGISTRYINDEX);
    lua_sethook(L, lua_watchdog, LUA_MASKCOUNT, EDB_LUA_HOOK_STEP);
}

/* --- dispatch core ---------------------------------------------------- */

/* True when any record carries the given name (records are keyed by
 * name, so at most one matches). */
static bool has_record_named(const cJSON *records, const char *name)
{
    const cJSON *rec = NULL;
    cJSON_ArrayForEach(rec, records) {
        const cJSON *nm = cJSON_GetObjectItemCaseSensitive(rec, "name");
        if (cJSON_IsString(nm) && nm->valuestring &&
            strcmp(nm->valuestring, name) == 0) {
            return true;
        }
    }
    return false;
}

/* Concatenates every code record's source, sorted by name (the config
 * layer returns records ordered by name), into one chunk buffer. */
static char *concat_code(const cJSON *records)
{
    size_t cap = 4096;
    size_t len = 0;
    char *buf = malloc(cap);
    if (!buf) {
        return NULL;
    }
    buf[0] = '\0';
    const cJSON *rec = NULL;
    cJSON_ArrayForEach(rec, records) {
        const cJSON *code = cJSON_GetObjectItemCaseSensitive(rec, "code");
        if (!cJSON_IsString(code) || !code->valuestring) {
            continue;
        }
        size_t clen = strlen(code->valuestring);
        while (len + clen + 2 > cap) {
            cap *= 2;
            char *grown = realloc(buf, cap);
            if (!grown) {
                free(buf);
                return NULL;
            }
            buf = grown;
        }
        memcpy(buf + len, code->valuestring, clen);
        len += clen;
        buf[len++] = '\n';
        buf[len] = '\0';
    }
    return buf;
}

/* Logs a Lua error message (strip trailing newline, include prefix). */
static void log_lua_error(const char *what, const char *message)
{
    char cleaned[512];
    snprintf(cleaned, sizeof(cleaned), "%s", message ? message : "?");
    size_t n = strlen(cleaned);
    while (n > 0 && (cleaned[n - 1] == '\n' || cleaned[n - 1] == '\r')) {
        cleaned[--n] = '\0';
    }
    edb_log("ERROR", "lua %s: %s", what, cleaned);
}

edb_lua_result edb_lua_fire(const edb_lua_ctx *ctx, edb_lua_event event,
                            edb_lua_event_arg *arg, char **reason)
{
    if (reason) {
        *reason = NULL;
    }
    if (!ctx || !ctx->engine || !ctx->config || !arg ||
        !arg->database || !arg->partition || !arg->keyspace || !arg->id) {
        return EDB_LUA_ERROR;
    }
    /* re-entrancy: a nested fire (write from inside a handler) never
     * re-triggers handlers on this node */
    if (edb_lua_depth > 0) {
        return EDB_LUA_OK;
    }

    /* resolve the action before paying for a Lua state */
    char action_name[512];
    cJSON *records = edb_code_list(ctx->config);
    bool has_action =
        records &&
        edb_lua_action_name(arg->database, arg->partition, arg->keyspace,
                            event, action_name, sizeof(action_name)) &&
        has_record_named(records, action_name);
    if (!has_action) {
        cJSON_Delete(records);
        return EDB_LUA_OK;
    }

    edb_lua_depth++;

    edb_lua_session session;
    memset(&session, 0, sizeof(session));
    session.ctx = ctx;
    session.arg = arg;
    session.event = event;

    bool before_write = event == EDB_LUA_BEFORE_INSERT ||
                        event == EDB_LUA_BEFORE_UPDATE ||
                        event == EDB_LUA_BEFORE_DELETE;
    bool return_matters = event == EDB_LUA_BEFORE_INSERT ||
                          event == EDB_LUA_BEFORE_UPDATE;

    /* original document (borrowed unless owned) */
    cJSON *original = NULL;
    bool original_owned = false;
    if (arg->value && *arg->value) {
        original = *arg->value;
    } else if (event == EDB_LUA_AFTER_INSERT ||
               event == EDB_LUA_AFTER_UPDATE ||
               event == EDB_LUA_BEFORE_DELETE) {
        original = lua_fetch_doc(&session, arg->partition, arg->keyspace,
                                 arg->id);
        original_owned = original != NULL;
    }
    /* after_delete never fetches: the record is already gone and a
     * caller-provided doc (captured pre-delete) is the only source */

    edb_lua_result result = EDB_LUA_OK;
    lua_State *L = luaL_newstate();
    if (!L) {
        edb_log("ERROR", "lua: failed to create state");
        result = before_write ? EDB_LUA_ERROR : EDB_LUA_OK;
        goto done;
    }
    session.L = L;
    open_sandbox(L, &session);
    install_watchdog(L, &session);

    /* 1. load and run the concatenated code records */
    char *code = concat_code(records);
    if (code && code[0] != '\0') {
        if (luaL_loadbuffer(L, code, strlen(code), "=code") != LUA_OK) {
            log_lua_error("code compile", lua_tostring(L, -1));
            result = before_write ? EDB_LUA_ERROR : EDB_LUA_OK;
            free(code);
            goto close;
        }
        if (lua_pcall(L, 0, 0, 0) != LUA_OK) {
            log_lua_error("code run", lua_tostring(L, -1));
            result = before_write ? EDB_LUA_ERROR : EDB_LUA_OK;
            free(code);
            goto close;
        }
    }
    free(code);

    /* 2. handler phase: rollback becomes live */
    session.in_event = true;

    /* 3. run the action function: fn(entity, id) */
    lua_getglobal(L, action_name);
    if (!lua_isfunction(L, -1)) {
        lua_pop(L, 1);
        char msg[512];
        snprintf(msg, sizeof(msg),
                 "action function '%s' is not defined by its code record",
                 action_name);
        log_lua_error("dispatch", msg);
        result = before_write ? EDB_LUA_ERROR : EDB_LUA_OK;
        goto close;
    }
    if (original && (original->type & 0xFF) == cJSON_Object) {
        lua_newtable(L);
        const cJSON *item = NULL;
        cJSON_ArrayForEach(item, original) {
            if (item->string) {
                push_json(L, item, 0);
                lua_setfield(L, -2, item->string);
            }
        }
    } else {
        /* no document: nil entity for after_delete, else empty table */
        if (event == EDB_LUA_AFTER_DELETE) {
            lua_pushnil(L);
        } else {
            lua_newtable(L);
        }
    }
    lua_pushstring(L, arg->id ? arg->id : "");
    if (lua_pcall(L, 2, 1, 0) != LUA_OK) {
        log_lua_error("handler", lua_tostring(L, -1));
        lua_pop(L, 1);
        if (before_write) {
            result = EDB_LUA_ERROR;   /* fail closed on before_* */
        } else {
            result = EDB_LUA_OK;      /* after_* are best-effort */
        }
    }

    /* 4. veto handling */
    if (session.rolled_back && result == EDB_LUA_OK) {
        if (before_write) {
            if (reason) {
                *reason = strdup(session.rollback_reason);
            }
            result = EDB_LUA_ROLLBACK;
            goto close;
        }
        edb_log("WARN", "lua: rollback ignored in %s handler (%s)",
                edb_lua_event_name(event), session.rollback_reason);
    }

    /* 5. write-back: for before_insert/before_update a returned table
     * becomes the document the caller writes; every other return value
     * (and every return from other events) is ignored */
    if (result == EDB_LUA_OK && return_matters && lua_gettop(L) >= 1) {
        if (lua_type(L, -1) == LUA_TTABLE) {
            cJSON *new_doc = returned_entity_to_doc(&session, L);
            if (!new_doc) {
                log_lua_error("entity conversion", lua_tostring(L, -1));
                result = EDB_LUA_ERROR;
                goto close;
            }
            /* hand the returned document to the caller's write */
            if (arg->value) {
                cJSON_Delete(*arg->value);
                *arg->value = new_doc;
            } else {
                cJSON_Delete(new_doc);
            }
        } else if (!lua_isnil(L, -1)) {
            /* nil means "no replacement": the common no-return case */
            edb_log("WARN",
                    "lua: %s handler returned a %s; only an entity "
                    "table is written back",
                    edb_lua_event_name(event),
                    lua_typename(L, lua_type(L, -1)));
        }
    }

close:
    session.in_event = false;
    lua_close(L);
done:
    if (original_owned) {
        cJSON_Delete(original);
    }
    cJSON_Delete(records);
    edb_lua_depth--;
    return result;
}

/* --- compile-only validation ------------------------------------------ */

bool edb_lua_validate(const char *code, char **err)
{
    if (err) {
        *err = NULL;
    }
    if (!code) {
        if (err) {
            *err = strdup("no code supplied");
        }
        return false;
    }
    /* an empty chunk is valid Lua but useless as a code record */
    bool blank = true;
    for (const char *p = code; *p; p++) {
        if (*p != ' ' && *p != '\t' && *p != '\n' && *p != '\r') {
            blank = false;
            break;
        }
    }
    if (blank) {
        if (err) {
            *err = strdup("code is empty");
        }
        return false;
    }
    lua_State *L = luaL_newstate();
    if (!L) {
        if (err) {
            *err = strdup("failed to create Lua state");
        }
        return false;
    }
    /* compile only: no libraries, the sandbox never runs the chunk */
    bool ok = luaL_loadbuffer(L, code, strlen(code), "=code") == LUA_OK;
    if (!ok && err) {
        const char *msg = lua_tostring(L, -1);
        if (msg) {
            size_t n = strlen(msg);
            while (n > 0 &&
                   (msg[n - 1] == '\n' || msg[n - 1] == '\r')) {
                n--;
            }
            *err = strndup(msg, n);
        } else {
            *err = strdup("compile failed");
        }
    }
    lua_close(L);
    return ok;
}
