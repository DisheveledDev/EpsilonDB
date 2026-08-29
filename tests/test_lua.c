/* Tests for the Lua scripting engine: sandbox validation,
 * config code storage, action dispatch by naming convention, the
 * insert/update split, return-based write-back, rollback veto, stdlib
 * data ops, and transient state isolation. */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "../src/engine/epsilon_config.h"
#include "../src/lua/epsilon_lua.h"

static int tests_run = 0;
static int tests_failed = 0;

#define CHECK(cond)                                                       \
    do {                                                                  \
        tests_run++;                                                      \
        if (!(cond)) {                                                    \
            tests_failed++;                                               \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__,       \
                    #cond);                                               \
        }                                                                 \
    } while (0)

static void rm_rf(const char *path)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", path);
    if (system(cmd) != 0) {
        fprintf(stderr, "warning: rm -rf %s failed\n", path);
    }
}

static edb_engine *eng;
static edb_config *cfg;

static void save_fn(const char *name, const char *code)
{
    cJSON *rec = cJSON_CreateObject();
    cJSON_AddStringToObject(rec, "type", "function");
    cJSON_AddStringToObject(rec, "code", code);
    CHECK(edb_code_save(cfg, name, rec));
    cJSON_Delete(rec);
}

static void save_action(const char *event, const char *db, const char *part,
                        const char *ks, const char *code)
{
    edb_lua_event ev;
    CHECK(edb_lua_event_parse(event, &ev));
    char name[512];
    CHECK(edb_lua_action_name(db, part, ks, ev, name, sizeof(name)));
    cJSON *rec = cJSON_CreateObject();
    cJSON_AddStringToObject(rec, "type", "action");
    cJSON_AddStringToObject(rec, "database", db);
    cJSON_AddStringToObject(rec, "partition", part);
    cJSON_AddStringToObject(rec, "keyspace", ks);
    cJSON_AddStringToObject(rec, "event", event);
    cJSON_AddStringToObject(rec, "code", code);
    CHECK(edb_code_save(cfg, name, rec));
    cJSON_Delete(rec);
}

static edb_lua_result fire(edb_lua_event event, const char *db,
                           const char *part, const char *ks, const char *id,
                           cJSON **value, char **reason)
{
    edb_lua_ctx ctx = { eng, cfg, NULL, NULL };
    edb_lua_event_arg arg = { db, part, ks, id, value, 0, true };
    return edb_lua_fire(&ctx, event, &arg, reason);
}

static void test_validate(void)
{
    char *err = NULL;
    CHECK(edb_lua_validate("function f(entity, id) return entity end", &err));
    CHECK(err == NULL);

    CHECK(!edb_lua_validate("function f( end", &err));
    CHECK(err != NULL);
    free(err);
    err = NULL;

    CHECK(!edb_lua_validate("", &err));
    CHECK(err != NULL);
    free(err);
}

static void test_event_parse(void)
{
    edb_lua_event out;
    CHECK(edb_lua_event_parse("beforeInsert", &out) &&
          out == EDB_LUA_BEFORE_INSERT);
    CHECK(edb_lua_event_parse("afterInsert", &out) &&
          out == EDB_LUA_AFTER_INSERT);
    CHECK(edb_lua_event_parse("beforeUpdate", &out) &&
          out == EDB_LUA_BEFORE_UPDATE);
    CHECK(edb_lua_event_parse("afterUpdate", &out) &&
          out == EDB_LUA_AFTER_UPDATE);
    CHECK(edb_lua_event_parse("beforeDelete", &out) &&
          out == EDB_LUA_BEFORE_DELETE);
    CHECK(edb_lua_event_parse("afterDelete", &out) &&
          out == EDB_LUA_AFTER_DELETE);
    CHECK(!edb_lua_event_parse("before_put", &out));
    CHECK(!edb_lua_event_parse("onUpdate", &out));
    CHECK(!edb_lua_event_parse("bogus", &out));
    CHECK(strcmp(edb_lua_event_name(EDB_LUA_BEFORE_INSERT),
                 "beforeInsert") == 0);
    CHECK(strcmp(edb_lua_event_name(EDB_LUA_AFTER_DELETE),
                 "afterDelete") == 0);

    /* action naming convention */
    char name[512];
    CHECK(edb_lua_action_name("demo", "people", "staff",
                              EDB_LUA_BEFORE_DELETE, name, sizeof(name)));
    CHECK(strcmp(name, "demo_people_staff_beforeDelete") == 0);
    CHECK(edb_lua_action_name("demo", "people", "staff",
                              EDB_LUA_AFTER_INSERT, name, sizeof(name)));
    CHECK(strcmp(name, "demo_people_staff_afterInsert") == 0);
    CHECK(!edb_lua_action_name("demo", "people", "staff",
                               EDB_LUA_AFTER_INSERT, name, 8));
}

static void test_code_store(void)
{
    cJSON *fn = cJSON_CreateObject();
    cJSON_AddStringToObject(fn, "type", "function");
    cJSON_AddStringToObject(fn, "code", "function a() end");
    CHECK(edb_code_save(cfg, "fn-a", fn));
    cJSON_Delete(fn);

    cJSON *rec = edb_code_load(cfg, "fn-a");
    CHECK(rec != NULL);
    if (rec) {
        const cJSON *code = cJSON_GetObjectItemCaseSensitive(rec, "code");
        CHECK(cJSON_IsString(code) &&
              strcmp(code->valuestring, "function a() end") == 0);
        cJSON_Delete(rec);
    }

    /* upsert keeps the name field fresh */
    cJSON *fn2 = cJSON_CreateObject();
    cJSON_AddStringToObject(fn2, "type", "function");
    cJSON_AddStringToObject(fn2, "name", "stale");
    cJSON_AddStringToObject(fn2, "code", "function b() end");
    CHECK(edb_code_save(cfg, "fn-a", fn2));
    cJSON_Delete(fn2);
    rec = edb_code_load(cfg, "fn-a");
    CHECK(rec != NULL);
    if (rec) {
        const cJSON *name = cJSON_GetObjectItemCaseSensitive(rec, "name");
        const cJSON *code = cJSON_GetObjectItemCaseSensitive(rec, "code");
        CHECK(cJSON_IsString(name) && strcmp(name->valuestring, "fn-a") == 0);
        CHECK(cJSON_IsString(code) &&
              strcmp(code->valuestring, "function b() end") == 0);
        cJSON_Delete(rec);
    }

    /* list is ordered by name */
    save_fn("zzz", "function z() end");
    cJSON *all = edb_code_list(cfg);
    CHECK(all != NULL && cJSON_GetArraySize(all) == 2);
    if (all && cJSON_GetArraySize(all) == 2) {
        const cJSON *n0 = cJSON_GetObjectItemCaseSensitive(
            cJSON_GetArrayItem(all, 0), "name");
        const cJSON *n1 = cJSON_GetObjectItemCaseSensitive(
            cJSON_GetArrayItem(all, 1), "name");
        CHECK(cJSON_IsString(n0) && strcmp(n0->valuestring, "fn-a") == 0);
        CHECK(cJSON_IsString(n1) && strcmp(n1->valuestring, "zzz") == 0);
    }
    cJSON_Delete(all);

    CHECK(edb_code_delete(cfg, "fn-a"));
    CHECK(edb_code_load(cfg, "fn-a") == NULL);
    CHECK(!edb_code_delete(cfg, "missing"));
}

static void test_no_action_fast_path(void)
{
    char *reason = NULL;
    cJSON *doc = cJSON_Parse("{\"x\":1}");
    cJSON *docp = doc;
    CHECK(fire(EDB_LUA_BEFORE_INSERT, "demo", "p", "k", "id1", &docp,
               &reason) == EDB_LUA_OK);
    CHECK(docp == doc);   /* untouched */
    cJSON_Delete(doc);
}

static void test_action_dispatch(void)
{
    save_action("beforeUpdate", "demo", "people", "staff",
                "function demo_people_staff_beforeUpdate(entity, id)\n"
                "  entity.level = 'update'\n"
                "  return entity\n"
                "end\n");

    /* the exact scope fires */
    cJSON *doc = cJSON_Parse("{\"name\":\"n\"}");
    cJSON *docp = doc;
    char *reason = NULL;
    CHECK(fire(EDB_LUA_BEFORE_UPDATE, "demo", "people", "staff", "id1",
               &docp, &reason) == EDB_LUA_OK);
    const cJSON *lvl = cJSON_GetObjectItemCaseSensitive(docp, "level");
    CHECK(cJSON_IsString(lvl) && strcmp(lvl->valuestring, "update") == 0);
    cJSON_Delete(docp);

    /* another keyspace of the same partition does not fire */
    doc = cJSON_Parse("{\"name\":\"n\"}");
    docp = doc;
    CHECK(fire(EDB_LUA_BEFORE_UPDATE, "demo", "people", "customers", "id2",
               &docp, &reason) == EDB_LUA_OK);
    CHECK(docp == doc);
    cJSON_Delete(docp);

    /* another database does not fire */
    doc = cJSON_Parse("{\"name\":\"n\"}");
    docp = doc;
    CHECK(fire(EDB_LUA_BEFORE_UPDATE, "other", "people", "staff", "id3",
               &docp, &reason) == EDB_LUA_OK);
    CHECK(docp == doc);
    cJSON_Delete(docp);

    /* another event does not fire (the action name encodes the event) */
    doc = cJSON_Parse("{\"name\":\"n\"}");
    docp = doc;
    CHECK(fire(EDB_LUA_BEFORE_INSERT, "demo", "people", "staff", "id4",
               &docp, &reason) == EDB_LUA_OK);
    CHECK(docp == doc);
    cJSON_Delete(docp);

    /* a named function whose name matches the action convention fires
     * too: dispatch is purely name-based */
    save_fn("demo_people_staff_beforeInsert",
            "function demo_people_staff_beforeInsert(entity, id) "
            "entity.via_fn = true return entity end\n");
    doc = cJSON_Parse("{\"name\":\"n\"}");
    docp = doc;
    CHECK(fire(EDB_LUA_BEFORE_INSERT, "demo", "people", "staff", "id5",
               &docp, &reason) == EDB_LUA_OK);
    CHECK(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(docp, "via_fn")));
    cJSON_Delete(docp);

    edb_code_delete(cfg, "demo_people_staff_beforeInsert");
    edb_code_delete(cfg, "demo_people_staff_beforeUpdate");
}

static void test_insert_update_split(void)
{
    save_action("beforeInsert", "demo", "p", "k",
                "function demo_p_k_beforeInsert(entity, id)\n"
                "  entity.op = 'insert'\n"
                "  return entity\n"
                "end\n");
    save_action("beforeUpdate", "demo", "p", "k",
                "function demo_p_k_beforeUpdate(entity, id)\n"
                "  entity.op = 'update'\n"
                "  return entity\n"
                "end\n");

    cJSON *doc = cJSON_Parse("{\"x\":1}");
    cJSON *docp = doc;
    char *reason = NULL;
    CHECK(fire(EDB_LUA_BEFORE_INSERT, "demo", "p", "k", "id1", &docp,
               &reason) == EDB_LUA_OK);
    const cJSON *op = cJSON_GetObjectItemCaseSensitive(docp, "op");
    CHECK(cJSON_IsString(op) && strcmp(op->valuestring, "insert") == 0);
    cJSON_Delete(docp);

    doc = cJSON_Parse("{\"x\":1}");
    docp = doc;
    CHECK(fire(EDB_LUA_BEFORE_UPDATE, "demo", "p", "k", "id1", &docp,
               &reason) == EDB_LUA_OK);
    op = cJSON_GetObjectItemCaseSensitive(docp, "op");
    CHECK(cJSON_IsString(op) && strcmp(op->valuestring, "update") == 0);
    cJSON_Delete(docp);

    edb_code_delete(cfg, "demo_p_k_beforeInsert");
    edb_code_delete(cfg, "demo_p_k_beforeUpdate");
}

static void test_before_return_replaces_doc(void)
{
    save_action("beforeInsert", "demo", "p", "k",
                "function demo_p_k_beforeInsert(entity, id)\n"
                "  entity.name = 'built'\n"
                "  entity.id = 'dropped'\n"
                "  return entity\n"
                "end\n");

    /* a returned table replaces the document (the "id" key is dropped,
     * JSON null fields survive round-trips) */
    cJSON *doc = cJSON_Parse("{\"stamp\":\"x\",\"z\":null}");
    cJSON *docp = doc;
    char *reason = NULL;
    CHECK(fire(EDB_LUA_BEFORE_INSERT, "demo", "p", "k", "id1", &docp,
               &reason) == EDB_LUA_OK);
    const cJSON *name = cJSON_GetObjectItemCaseSensitive(docp, "name");
    const cJSON *stamp = cJSON_GetObjectItemCaseSensitive(docp, "stamp");
    const cJSON *z = cJSON_GetObjectItemCaseSensitive(docp, "z");
    CHECK(cJSON_IsString(name) && strcmp(name->valuestring, "built") == 0);
    CHECK(cJSON_IsString(stamp) && strcmp(stamp->valuestring, "x") == 0);
    CHECK(cJSON_IsNull(z));
    CHECK(cJSON_GetObjectItemCaseSensitive(docp, "id") == NULL);
    cJSON_Delete(docp);

    /* returning nil leaves the incoming document untouched */
    edb_code_delete(cfg, "demo_p_k_beforeInsert");
    save_action("beforeInsert", "demo", "p", "k",
                "function demo_p_k_beforeInsert(entity, id)\n"
                "  entity.touched = true\n"   /* mutation without return */
                "end\n");
    doc = cJSON_Parse("{\"stamp\":\"x\"}");
    docp = doc;
    CHECK(fire(EDB_LUA_BEFORE_INSERT, "demo", "p", "k", "id2", &docp,
               &reason) == EDB_LUA_OK);
    CHECK(docp == doc);   /* same pointer: nothing was written back */
    CHECK(cJSON_GetObjectItemCaseSensitive(docp, "touched") == NULL);
    cJSON_Delete(docp);

    /* a non-table return is ignored too */
    edb_code_delete(cfg, "demo_p_k_beforeInsert");
    save_action("beforeInsert", "demo", "p", "k",
                "function demo_p_k_beforeInsert(entity, id)\n"
                "  return 42\n"
                "end\n");
    doc = cJSON_Parse("{\"stamp\":\"x\"}");
    docp = doc;
    CHECK(fire(EDB_LUA_BEFORE_INSERT, "demo", "p", "k", "id3", &docp,
               &reason) == EDB_LUA_OK);
    CHECK(docp == doc);
    cJSON_Delete(docp);

    edb_code_delete(cfg, "demo_p_k_beforeInsert");
}

static void test_before_delete_return_ignored(void)
{
    edb_put(eng, "p", "k", "id1", "{\"name\":\"n\"}", -1);
    save_action("beforeDelete", "demo", "p", "k",
                "function demo_p_k_beforeDelete(entity, id)\n"
                "  return { resurrected = true }\n"
                "end\n");

    /* the returned table has no effect: the caller's doc is untouched
     * and the delete proceeds */
    cJSON *doc = cJSON_Parse("{\"name\":\"n\"}");
    cJSON *docp = doc;
    char *reason = NULL;
    CHECK(fire(EDB_LUA_BEFORE_DELETE, "demo", "p", "k", "id1", &docp,
               &reason) == EDB_LUA_OK);
    CHECK(docp == doc);
    cJSON_Delete(docp);
    CHECK(edb_delete(eng, "p", "k", "id1"));
    CHECK(edb_get(eng, "p", "k", "id1") == NULL);

    edb_code_delete(cfg, "demo_p_k_beforeDelete");
}

static void test_rollback_veto(void)
{
    save_action("beforeInsert", "demo", "p", "k",
                "function demo_p_k_beforeInsert(entity, id)\n"
                "  if not entity.name then rollback('name is required') end\n"
                "end\n");

    cJSON *doc = cJSON_Parse("{\"vendor\":\"acme\"}");
    cJSON *docp = doc;
    char *reason = NULL;
    CHECK(fire(EDB_LUA_BEFORE_INSERT, "demo", "p", "k", "id1", &docp,
               &reason) == EDB_LUA_ROLLBACK);
    CHECK(reason != NULL && strcmp(reason, "name is required") == 0);
    free(reason);
    CHECK(docp == doc);   /* untouched on veto */
    cJSON_Delete(docp);

    /* the handler passes the id param through */
    doc = cJSON_Parse("{\"name\":\"n\"}");
    docp = doc;
    CHECK(fire(EDB_LUA_BEFORE_INSERT, "demo", "p", "k", "id1", &docp,
               &reason) == EDB_LUA_OK);
    cJSON_Delete(docp);
    edb_code_delete(cfg, "demo_p_k_beforeInsert");

    /* rollback is ignored for after_* events (best-effort) */
    edb_put(eng, "p", "k", "id2", "{\"name\":\"n\"}", -1);
    save_action("afterInsert", "demo", "p", "k",
                "function demo_p_k_afterInsert(entity, id) "
                "rollback('late') end\n");
    CHECK(fire(EDB_LUA_AFTER_INSERT, "demo", "p", "k", "id2", NULL,
               &reason) == EDB_LUA_OK);
    cJSON *got = edb_get(eng, "p", "k", "id2");
    CHECK(got != NULL);
    cJSON_Delete(got);
    edb_code_delete(cfg, "demo_p_k_afterInsert");
}

static void test_before_delete_veto(void)
{
    edb_put(eng, "p", "k", "id3", "{\"name\":\"n\"}", -1);
    save_action("beforeDelete", "demo", "p", "k",
                "function demo_p_k_beforeDelete(entity, id)\n"
                "  if entity.name == 'n' then rollback('still needed') end\n"
                "end\n");
    char *reason = NULL;
    CHECK(fire(EDB_LUA_BEFORE_DELETE, "demo", "p", "k", "id3", NULL,
               &reason) == EDB_LUA_ROLLBACK);
    CHECK(reason != NULL && strcmp(reason, "still needed") == 0);
    free(reason);
    /* record survives the veto */
    cJSON *got = edb_get(eng, "p", "k", "id3");
    CHECK(got != NULL);
    cJSON_Delete(got);
    edb_code_delete(cfg, "demo_p_k_beforeDelete");

    /* without a veto the delete proceeds */
    CHECK(fire(EDB_LUA_BEFORE_DELETE, "demo", "p", "k", "id3", NULL,
               &reason) == EDB_LUA_OK);
    CHECK(edb_delete(eng, "p", "k", "id3"));
    CHECK(edb_get(eng, "p", "k", "id3") == NULL);
}

static void test_after_handlers(void)
{
    /* after_* receives the written entity and may write other records;
     * its return value is ignored */
    save_action("afterInsert", "demo", "p", "k",
                "function demo_p_k_afterInsert(entity, id)\n"
                "  put('people', 'audit', 'a1', "
                "{ note = 'inserted', who = entity.name })\n"
                "  return { ignored = true }\n"
                "end\n");
    edb_put(eng, "p", "k", "id4", "{\"name\":\"n\"}", -1);
    char *reason = NULL;
    CHECK(fire(EDB_LUA_AFTER_INSERT, "demo", "p", "k", "id4", NULL,
               &reason) == EDB_LUA_OK);
    cJSON *audit = edb_get(eng, "people", "audit", "a1");
    CHECK(audit != NULL);
    if (audit) {
        const cJSON *note =
            cJSON_GetObjectItemCaseSensitive(audit, "note");
        CHECK(cJSON_IsString(note) &&
              strcmp(note->valuestring, "inserted") == 0);
        cJSON_Delete(audit);
    }
    /* the returned table did not overwrite the record */
    cJSON *got = edb_get(eng, "p", "k", "id4");
    CHECK(got != NULL);
    if (got) {
        CHECK(cJSON_GetObjectItemCaseSensitive(got, "ignored") == NULL);
        cJSON_Delete(got);
    }

    /* afterDelete sees the caller-captured document */
    edb_put(eng, "p", "k", "id5", "{\"name\":\"bye\"}", -1);
    cJSON *deleted = cJSON_Parse("{\"name\":\"bye\"}");
    save_action("afterDelete", "demo", "p", "k",
                "function demo_p_k_afterDelete(entity, id)\n"
                "  put('people', 'audit', 'a2', { who = entity.name, key = id })\n"
                "end\n");
    CHECK(fire(EDB_LUA_AFTER_DELETE, "demo", "p", "k", "id5", &deleted,
               &reason) == EDB_LUA_OK);
    cJSON_Delete(deleted);
    audit = edb_get(eng, "people", "audit", "a2");
    CHECK(audit != NULL);
    if (audit) {
        const cJSON *who = cJSON_GetObjectItemCaseSensitive(audit, "who");
        const cJSON *key = cJSON_GetObjectItemCaseSensitive(audit, "key");
        CHECK(cJSON_IsString(who) && strcmp(who->valuestring, "bye") == 0);
        CHECK(cJSON_IsString(key) && strcmp(key->valuestring, "id5") == 0);
        cJSON_Delete(audit);
    }
    edb_code_delete(cfg, "demo_p_k_afterDelete");
    edb_code_delete(cfg, "demo_p_k_afterInsert");
}

static void test_stdlib_data_ops(void)
{
    save_action("beforeInsert", "demo", "p", "k",
                "function demo_p_k_beforeInsert(entity, id)\n"
                "  put('people', 'auditops', 'a1', { note = 'script', owner = entity.name })\n"
                "  local got = get('people', 'auditops', 'a1')\n"
                "  entity.got_note = got.note\n"
                "  entity.got_id = got.id\n"
                "  entity.seen = count('people', 'auditops')\n"
                "  local rows = query('people', 'auditops', { key = 'note', operator = 'eq', value = 'script' })\n"
                "  entity.qcount = #rows\n"
                "  entity.qnote = rows[1].note\n"
                "  remove('people', 'auditops', 'a1')\n"
                "  entity.left = count('people', 'auditops')\n"
                "  entity.missing = get('people', 'auditops', 'a1') == nil\n"
                "  local cl = cluster()\n"
                "  entity.members = cl.member_count\n"
                "  entity.has_log = (log ~= nil)\n"
                "  log('probe fired')\n"
                "  return entity\n"
                "end\n");

    cJSON *doc = cJSON_Parse("{\"name\":\"n\"}");
    cJSON *docp = doc;
    char *reason = NULL;
    CHECK(fire(EDB_LUA_BEFORE_INSERT, "demo", "p", "k", "id6", &docp,
               &reason) == EDB_LUA_OK);
    const cJSON *got_note = cJSON_GetObjectItemCaseSensitive(docp, "got_note");
    const cJSON *got_id = cJSON_GetObjectItemCaseSensitive(docp, "got_id");
    const cJSON *seen = cJSON_GetObjectItemCaseSensitive(docp, "seen");
    const cJSON *qcount = cJSON_GetObjectItemCaseSensitive(docp, "qcount");
    const cJSON *qnote = cJSON_GetObjectItemCaseSensitive(docp, "qnote");
    const cJSON *left = cJSON_GetObjectItemCaseSensitive(docp, "left");
    const cJSON *missing = cJSON_GetObjectItemCaseSensitive(docp, "missing");
    const cJSON *members = cJSON_GetObjectItemCaseSensitive(docp, "members");
    const cJSON *has_log = cJSON_GetObjectItemCaseSensitive(docp, "has_log");
    CHECK(cJSON_IsString(got_note) &&
          strcmp(got_note->valuestring, "script") == 0);
    CHECK(cJSON_IsString(got_id) && strcmp(got_id->valuestring, "a1") == 0);
    CHECK(cJSON_IsNumber(seen) && seen->valuedouble == 1);
    CHECK(cJSON_IsNumber(qcount) && qcount->valuedouble == 1);
    CHECK(cJSON_IsString(qnote) && strcmp(qnote->valuestring, "script") == 0);
    CHECK(cJSON_IsNumber(left) && left->valuedouble == 0);
    CHECK(cJSON_IsTrue(missing));
    CHECK(cJSON_IsNumber(members) && members->valuedouble == 1);
    CHECK(cJSON_IsTrue(has_log));
    cJSON_Delete(docp);
    edb_code_delete(cfg, "demo_p_k_beforeInsert");
}

static void test_date_library(void)
{
    save_action("beforeInsert", "demo", "p", "k",
                "function demo_p_k_beforeInsert(entity, id)\n"
                "  entity.year = date.format('%Y')\n"
                "  local t = date.time({ year = 2001, month = 2, day = 3, hour = 4, min = 5, sec = 6 })\n"
                "  entity.d1 = date.format('%Y-%m-%d', t)\n"
                "  entity.two = date.format('%H:%M', t)\n"
                "  entity.diff = date.diff(100, 250)\n"
                "  entity.now_ok = (date.now() > 1000000000)\n"
                "  entity.dt_num = date.time(nil)\n"
                "  return entity\n"
                "end\n");
    cJSON *doc = cJSON_Parse("{}");
    cJSON *docp = doc;
    char *reason = NULL;
    CHECK(fire(EDB_LUA_BEFORE_INSERT, "demo", "p", "k", "id-date", &docp,
               &reason) == EDB_LUA_OK);

    /* the current local year, formatted by the library */
    time_t now = time(NULL);
    struct tm tmv;
    localtime_r(&now, &tmv);
    char expect_year[8];
    snprintf(expect_year, sizeof(expect_year), "%d", tmv.tm_year + 1900);
    const cJSON *year = cJSON_GetObjectItemCaseSensitive(docp, "year");
    CHECK(cJSON_IsString(year) &&
          strcmp(year->valuestring, expect_year) == 0);

    /* a fixed date table round-trips through time()+format() */
    const cJSON *d1 = cJSON_GetObjectItemCaseSensitive(docp, "d1");
    CHECK(cJSON_IsString(d1) && strcmp(d1->valuestring, "2001-02-03") == 0);
    const cJSON *two = cJSON_GetObjectItemCaseSensitive(docp, "two");
    CHECK(cJSON_IsString(two) && strcmp(two->valuestring, "04:05") == 0);
    const cJSON *diff = cJSON_GetObjectItemCaseSensitive(docp, "diff");
    CHECK(cJSON_IsNumber(diff) && diff->valuedouble == 150);
    CHECK(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(docp, "now_ok")));
    CHECK(cJSON_IsNumber(cJSON_GetObjectItemCaseSensitive(docp, "dt_num")));

    cJSON_Delete(docp);
    edb_code_delete(cfg, "demo_p_k_beforeInsert");
}

static void test_sandbox_denial(void)
{
    save_action("beforeInsert", "demo", "p", "k",
                "function demo_p_k_beforeInsert(entity, id)\n"
                "  entity.io_present = (io ~= nil)\n"
                "  entity.os_present = (os ~= nil)\n"
                "  entity.debug_present = (debug ~= nil)\n"
                "  entity.package_present = (package ~= nil)\n"
                "  entity.require_present = (require ~= nil)\n"
                "  entity.coroutine_present = (coroutine ~= nil)\n"
                "  entity.loadfile_present = (loadfile ~= nil)\n"
                "  entity.dofile_present = (dofile ~= nil)\n"
                "  entity.collectgarbage_present = (collectgarbage ~= nil)\n"
                "  entity.print_present = (print ~= nil)\n"
                "  entity.string_present = (string ~= nil)\n"
                "  entity.table_present = (table ~= nil)\n"
                "  entity.math_present = (math ~= nil)\n"
                "  entity.utf8_present = (utf8 ~= nil)\n"
                "  return entity\n"
                "end\n");
    cJSON *doc = cJSON_Parse("{}");
    cJSON *docp = doc;
    char *reason = NULL;
    CHECK(fire(EDB_LUA_BEFORE_INSERT, "demo", "p", "k", "id7", &docp,
               &reason) == EDB_LUA_OK);
    CHECK(!cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(docp, "io_present")));
    CHECK(!cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(docp, "os_present")));
    CHECK(!cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(docp, "debug_present")));
    CHECK(!cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(docp, "package_present")));
    CHECK(!cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(docp, "require_present")));
    CHECK(!cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(docp, "coroutine_present")));
    CHECK(!cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(docp, "loadfile_present")));
    CHECK(!cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(docp, "dofile_present")));
    CHECK(!cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(docp, "collectgarbage_present")));
    CHECK(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(docp, "print_present")));
    CHECK(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(docp, "string_present")));
    CHECK(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(docp, "table_present")));
    CHECK(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(docp, "math_present")));
    CHECK(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(docp, "utf8_present")));
    cJSON_Delete(docp);
    edb_code_delete(cfg, "demo_p_k_beforeInsert");
}

static void test_transient_state(void)
{
    save_fn("counter_lib", "COUNTER = (COUNTER or 0) + 1\n");
    save_action("beforeInsert", "demo", "p", "k",
                "function demo_p_k_beforeInsert(entity, id)\n"
                "  entity.counter = COUNTER\n"
                "  return entity\n"
                "end\n");
    for (int i = 0; i < 2; i++) {
        cJSON *doc = cJSON_Parse("{}");
        cJSON *docp = doc;
        char *reason = NULL;
        CHECK(fire(EDB_LUA_BEFORE_INSERT, "demo", "p", "k", "id8", &docp,
                   &reason) == EDB_LUA_OK);
        const cJSON *counter =
            cJSON_GetObjectItemCaseSensitive(docp, "counter");
        /* each event runs in a fresh state: the record global resets */
        CHECK(cJSON_IsNumber(counter) && counter->valuedouble == 1);
        cJSON_Delete(docp);
    }
    edb_code_delete(cfg, "counter_lib");
    edb_code_delete(cfg, "demo_p_k_beforeInsert");
}

static void test_handler_errors(void)
{
    /* a before_* script error fails closed */
    save_action("beforeInsert", "demo", "p", "k",
                "function demo_p_k_beforeInsert(entity, id) "
                "error('boom') end\n");
    cJSON *doc = cJSON_Parse("{\"x\":1}");
    cJSON *docp = doc;
    char *reason = NULL;
    CHECK(fire(EDB_LUA_BEFORE_INSERT, "demo", "p", "k", "id9", &docp,
               &reason) == EDB_LUA_ERROR);
    cJSON_Delete(docp);
    edb_code_delete(cfg, "demo_p_k_beforeInsert");

    /* an after_* script error is best-effort and does not block */
    save_action("afterInsert", "demo", "p", "k",
                "function demo_p_k_afterInsert(entity, id) "
                "error('boom') end\n");
    CHECK(fire(EDB_LUA_AFTER_INSERT, "demo", "p", "k", "id9", NULL,
               &reason) == EDB_LUA_OK);
    edb_code_delete(cfg, "demo_p_k_afterInsert");

    /* a record whose code no longer defines its function fails closed
     * for before_* */
    save_action("beforeInsert", "demo", "p", "k",
                "function renamed(entity, id) end\n");
    doc = cJSON_Parse("{\"x\":1}");
    docp = doc;
    CHECK(fire(EDB_LUA_BEFORE_INSERT, "demo", "p", "k", "id9", &docp,
               &reason) == EDB_LUA_ERROR);
    cJSON_Delete(docp);
    edb_code_delete(cfg, "demo_p_k_beforeInsert");
}

static void test_stdlib_permission_denial(void)
{
    /* a restrictive partition read mask blocks untrusted script reads */
    save_action("beforeInsert", "demo", "secure", "vault",
                "function demo_secure_vault_beforeInsert(entity, id)\n"
                "  local x = get('secure', 'vault', 'k1')\n"
                "  entity.ok = true\n"
                "  return entity\n"
                "end\n");
    CHECK(edb_partition_create(cfg, "demo", "secure",
                               0, 0, 1ULL /* read: only group 1 */, 0));
    CHECK(edb_put(eng, "secure", "vault", "k1", "{\"secret\":1}", -1));

    edb_lua_ctx ctx = { eng, cfg, NULL, NULL };
    cJSON *doc = cJSON_Parse("{}");
    cJSON *docp = doc;
    char *reason = NULL;
    edb_lua_event_arg arg = { "demo", "secure", "vault", "k1",
                              &docp, 0, false };   /* untrusted, no groups */
    CHECK(edb_lua_fire(&ctx, EDB_LUA_BEFORE_INSERT, &arg, &reason) ==
          EDB_LUA_ERROR);
    cJSON_Delete(docp);

    /* trusted callers bypass the mask */
    doc = cJSON_Parse("{}");
    docp = doc;
    edb_lua_event_arg arg2 = { "demo", "secure", "vault", "k1",
                               &docp, 0, true };
    CHECK(edb_lua_fire(&ctx, EDB_LUA_BEFORE_INSERT, &arg2, &reason) ==
          EDB_LUA_OK);
    CHECK(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(docp, "ok")));
    cJSON_Delete(docp);
    edb_code_delete(cfg, "demo_secure_vault_beforeInsert");
}

static void test_rollback_outside_event(void)
{
    /* rollback at code load time is outside an active event */
    save_fn("toprollback", "rollback('premature')\n");
    save_action("beforeInsert", "demo", "p", "k",
                "function demo_p_k_beforeInsert(entity, id) end\n");
    cJSON *doc = cJSON_Parse("{}");
    cJSON *docp = doc;
    char *reason = NULL;
    CHECK(fire(EDB_LUA_BEFORE_INSERT, "demo", "p", "k", "id10", &docp,
               &reason) == EDB_LUA_ERROR);
    cJSON_Delete(docp);
    edb_code_delete(cfg, "toprollback");
    edb_code_delete(cfg, "demo_p_k_beforeInsert");
}

static void test_watchdog(void)
{
    save_action("beforeInsert", "demo", "p", "k",
                "function demo_p_k_beforeInsert(entity, id) "
                "while true do end end\n");
    cJSON *doc = cJSON_Parse("{}");
    cJSON *docp = doc;
    char *reason = NULL;
    CHECK(fire(EDB_LUA_BEFORE_INSERT, "demo", "p", "k", "id11", &docp,
               &reason) == EDB_LUA_ERROR);
    cJSON_Delete(docp);
    edb_code_delete(cfg, "demo_p_k_beforeInsert");
}

static void test_id_param(void)
{
    /* the id param matches the record key and entity has no injected
     * metadata keys */
    save_action("beforeInsert", "demo", "p", "k",
                "function demo_p_k_beforeInsert(entity, id)\n"
                "  entity.key = id\n"
                "  entity.has_db = entity.database ~= nil\n"
                "  entity.has_id = entity.id ~= nil\n"
                "  return entity\n"
                "end\n");
    cJSON *doc = cJSON_Parse("{}");
    cJSON *docp = doc;
    char *reason = NULL;
    CHECK(fire(EDB_LUA_BEFORE_INSERT, "demo", "p", "k", "rec-1", &docp,
               &reason) == EDB_LUA_OK);
    const cJSON *key = cJSON_GetObjectItemCaseSensitive(docp, "key");
    CHECK(cJSON_IsString(key) && strcmp(key->valuestring, "rec-1") == 0);
    CHECK(!cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(docp, "has_db")));
    CHECK(!cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(docp, "has_id")));
    cJSON_Delete(docp);
    edb_code_delete(cfg, "demo_p_k_beforeInsert");
}

int main(void)
{
    rm_rf("tests/data/luatest");
    eng = edb_engine_open("tests/data/luatest");
    CHECK(eng != NULL);
    cfg = edb_config_open(eng);
    CHECK(cfg != NULL);

    test_validate();
    test_event_parse();
    test_code_store();
    test_no_action_fast_path();
    test_action_dispatch();
    test_insert_update_split();
    test_before_return_replaces_doc();
    test_before_delete_return_ignored();
    test_rollback_veto();
    test_before_delete_veto();
    test_after_handlers();
    test_stdlib_data_ops();
    test_date_library();
    test_sandbox_denial();
    test_transient_state();
    test_handler_errors();
    test_stdlib_permission_denial();
    test_rollback_outside_event();
    test_watchdog();
    test_id_param();

    edb_config_close(cfg);
    edb_engine_close(eng);
    printf("%d checks, %d failures\n", tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
