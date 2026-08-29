/* epsilon_api_data.c - data
 * Part of the split epsilon_api module; see epsilon_api_internal.h.
 */

#include "epsilon_api_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>


bool handle_data_put(const edb_http_request *req,
                            edb_http_response *res)
{
    char db[128], part[256], ks[128], id[512];
    if (!split_data_path(req->path, db, part, ks, id)) {
        respond_error(res, 400,
                      "expected /data/{db}/{partition}/{keyspace}/{id}");
        return true;
    }
    if (strstr(req->query, "filter=")) {
        respond_error(res, 400, "filters apply to reads, not stored records");
        return true;
    }

    if (restore_blocked(res)) {
        return true;
    }

    bool auth_ok = false;
    uint64_t groups = authenticate(req, req, &auth_ok);
    if (!auth_ok) {
        respond_error(res, 401, "unauthorized");
        return true;
    }

    cJSON *doc = NULL;
    if (!body_json(req, &doc) || !cJSON_IsObject(doc)) {
        respond_error(res, 400, "body must be a JSON object");
        cJSON_Delete(doc);
        return true;
    }

    long long ttl = -1;
    char ttlbuf[32];
    if (query_param(req, "ttl", ttlbuf, sizeof(ttlbuf))) {
        ttl = strtoll(ttlbuf, NULL, 10);
    }
    cJSON *existing = edb_get(g_ctx.engine, part, ks, id);
    bool is_update = existing != NULL;
    edb_permission permission = existing ? EDB_PERM_UPDATE : EDB_PERM_CREATE;
    cJSON_Delete(existing);
    if (!authorize_partition(g_ctx.config, db, part, groups, permission, res)) {
        cJSON_Delete(doc);
        return true;
    }

    /* beforeInsert/beforeUpdate scripts may replace the
     * document (returned entity) or veto the write (scripts never run
     * for __system__ config writes) */
    {
        char *veto = NULL;
        edb_lua_result lr = api_fire_lua(
            is_update ? EDB_LUA_BEFORE_UPDATE : EDB_LUA_BEFORE_INSERT,
            db, part, ks, id, &doc, groups, false, &veto);
        if (lr == EDB_LUA_ROLLBACK) {
            char msg[640];
            snprintf(msg, sizeof(msg), "rejected: %s",
                     veto ? veto : "script rollback");
            free(veto);
            cJSON_Delete(doc);
            respond_error(res, 403, msg);
            return true;
        }
        free(veto);
        if (lr == EDB_LUA_ERROR) {
            cJSON_Delete(doc);
            respond_error(res, 500, "script error");
            return true;
        }
    }

    char *json = cJSON_PrintUnformatted(doc);
    cJSON_Delete(doc);
    if (!json) {
        respond_error(res, 500, "encode failed");
        return true;
    }

    long long started = api_now_us();
    bool ok = false;
    if (g_repl) {
        long long ts = (long long)time(NULL);
        cJSON *change = cJSON_CreateObject();
        if (change) {
            cJSON_AddStringToObject(change, "op", "put");
            cJSON_AddStringToObject(change, "db", db);
            cJSON_AddStringToObject(change, "partition", part);
            cJSON_AddStringToObject(change, "keyspace", ks);
            cJSON_AddStringToObject(change, "id", id);
            cJSON *value = cJSON_Parse(json);
            if (!value) {
                cJSON_Delete(change);
                free(json);
                respond_error(res, 500, "encode failed");
                return true;
            }
            cJSON_AddItemToObject(change, "value", value);
            cJSON_AddNumberToObject(change, "ttl_abs",
                                    ttl >= 0 ? (double)(ts + ttl) : -1);
            cJSON_AddNumberToObject(change, "ts", (double)ts);
            char *change_json = cJSON_PrintUnformatted(change);
            cJSON_Delete(change);
            if (change_json) {
                edb_repl_status status = edb_repl_write(g_repl, db,
                                                        change_json);
                free(change_json);
                if (status == EDB_REPL_QUORUM_LOST) {
                    free(json);
                    respond_error(res, 503,
                                  "quorum unavailable: write rejected");
                    return true;
                }
                ok = status == EDB_REPL_OK;
            }
        }
    } else {
        ok = edb_put(g_ctx.engine, part, ks, id, json, ttl);
    }
    if (ok) {
        edb_partition_ensure(g_ctx.config, db, part, ks, NULL);
    }
    free(json);

    if (g_analytics && ok) {
        edb_analytics_record_write(g_analytics, part, ks, is_update,
                                   api_now_us() - started);
    }

    if (!ok) {
        respond_error(res, 500, "storage failed");
        return true;
    }
    /* afterInsert/afterUpdate scripts are best-effort and never
     * block. With a repl service attached the local apply already fired
     * the event (once per node, trusted), so only the single-node path
     * fires here. */
    if (!g_repl) {
        char *veto = NULL;
        (void)api_fire_lua(
            is_update ? EDB_LUA_AFTER_UPDATE : EDB_LUA_AFTER_INSERT,
            db, part, ks, id, NULL, groups, false, &veto);
        free(veto);
    }
    respond_json(res, 200, NULL);
    res->body = edb_http_body_printf(&res->body_len,
                                     "{\"status\":\"ok\",\"id\":\"%s\"}",
                                     id);
    return true;
}

bool handle_data_get(const edb_http_request *req,
                            edb_http_response *res)
{
    char db[128], part[256], ks[128], id[512];
    if (!split_data_path(req->path, db, part, ks, id)) {
        respond_error(res, 400, "expected /data/{db}/{partition}/{keyspace}/{id}");
        return true;
    }

    if (restore_blocked(res)) {
        return true;
    }

    bool auth_ok = false;
    uint64_t groups = authenticate(req, req, &auth_ok);
    if (!auth_ok) {
        respond_error(res, 401, "unauthorized");
        return true;
    }
    if (!authorize_partition(g_ctx.config, db, part, groups, EDB_PERM_READ,
                             res)) {
        return true;
    }

    long long started = api_now_us();
    cJSON *doc = g_repl
                     ? edb_repl_read_get(g_repl, db, part, ks, id)
                     : edb_get(g_ctx.engine, part, ks, id);
    long long elapsed = api_now_us() - started;
    if (g_analytics) {
        edb_analytics_record_read(g_analytics, part, ks, elapsed);
    }
    if (!doc) {
        respond_error(res, 404, "not found");
        return true;
    }
    respond_json(res, 200, doc);
    return true;
}

bool handle_data_delete(const edb_http_request *req,
                               edb_http_response *res)
{
    char db[128], part[256], ks[128], id[512];
    if (!split_data_path(req->path, db, part, ks, id)) {
        respond_error(res, 400, "expected /data/{db}/{partition}/{keyspace}/{id}");
        return true;
    }

    if (restore_blocked(res)) {
        return true;
    }

    bool auth_ok = false;
    uint64_t groups = authenticate(req, req, &auth_ok);
    if (!auth_ok) {
        respond_error(res, 401, "unauthorized");
        return true;
    }
    if (!authorize_partition(g_ctx.config, db, part, groups, EDB_PERM_DELETE,
                             res)) {
        return true;
    }

    /* before_delete scripts may veto the delete. The current
     * document is captured once and handed to both handlers (the
     * before_* handler receives the entity, the after_* handler sees
     * what was deleted). */
    cJSON *deleted = edb_get(g_ctx.engine, part, ks, id);
    {
        char *veto = NULL;
        edb_lua_result lr = api_fire_lua(EDB_LUA_BEFORE_DELETE, db, part,
                                         ks, id, &deleted, groups, false,
                                         &veto);
        if (lr == EDB_LUA_ROLLBACK) {
            char msg[640];
            snprintf(msg, sizeof(msg), "rejected: %s",
                     veto ? veto : "script rollback");
            free(veto);
            cJSON_Delete(deleted);
            respond_error(res, 403, msg);
            return true;
        }
        free(veto);
        if (lr == EDB_LUA_ERROR) {
            cJSON_Delete(deleted);
            respond_error(res, 500, "script error");
            return true;
        }
    }

    bool ok;
    long long started = api_now_us();
    if (g_repl) {
        long long ts = (long long)time(NULL);
        cJSON *change = cJSON_CreateObject();
        if (!change) {
            respond_error(res, 500, "encode failed");
            return true;
        }
        cJSON_AddStringToObject(change, "op", "delete");
        cJSON_AddStringToObject(change, "db", db);
        cJSON_AddStringToObject(change, "partition", part);
        cJSON_AddStringToObject(change, "keyspace", ks);
        cJSON_AddStringToObject(change, "id", id);
        char tsbuf[32];
        snprintf(tsbuf, sizeof(tsbuf), "%lld", ts);
        cJSON_AddRawToObject(change, "ts", tsbuf);
        char *change_json = cJSON_PrintUnformatted(change);
        cJSON_Delete(change);
        if (!change_json) {
            respond_error(res, 500, "encode failed");
            return true;
        }
        edb_repl_status st =
            edb_repl_write(g_repl, db, change_json);
        free(change_json);
        if (st == EDB_REPL_LOCAL_FAIL) {
            respond_error(res, 500, "delete failed");
            return true;
        }
        if (st == EDB_REPL_QUORUM_LOST) {
            respond_error(res, 503,
                          "quorum unavailable: delete rejected");
            return true;
        }
        ok = true;
    } else {
        ok = edb_delete(g_ctx.engine, part, ks, id);
    }
    if (!ok) {
        respond_error(res, 500, "delete failed");
        return true;
    }
    /* after_delete scripts are best-effort and never block; the
     * local apply already fired the event when replication is attached */
    if (!g_repl) {
        char *veto = NULL;
        (void)api_fire_lua(EDB_LUA_AFTER_DELETE, db, part, ks, id,
                           &deleted, groups, false, &veto);
        free(veto);
    }
    cJSON_Delete(deleted);
    if (g_analytics) {
        edb_analytics_record_delete(g_analytics, part, ks,
                                    api_now_us() - started);
    }
    res->status = 200;
    res->content_type = "application/json";
    res->body = edb_http_body_printf(&res->body_len,
                                     "{\"status\":\"deleted\",\"id\":\"%s\"}",
                                     id);
    return true;
}

/* Lists the keyspaces that have been written under a database/partition. */
static size_t partition_keyspaces(const char *db, const char *partition,
                                  char out[][128], size_t cap)
{
    size_t total = 0;
    edb_keyspace_info *list = edb_keyspace_list(g_ctx.config, &total);
    size_t found = 0;
    for (size_t i = 0; list && i < total && found < cap; i++) {
        if (strcmp(list[i].database, db) == 0 &&
            strcmp(list[i].partition, partition) == 0) {
            snprintf(out[found], 128, "%s", list[i].name);
            found++;
        }
    }
    free(list);
    return found;
}

typedef struct {
    long long ts;
    cJSON *value;   /* borrowed */
    const char *id; /* borrowed record key */
} ts_value;

static int cmp_ts_desc(const void *a, const void *b)
{
    const ts_value *x = a;
    const ts_value *y = b;
    if (x->ts < y->ts) {
        return 1;
    }
    if (x->ts > y->ts) {
        return -1;
    }
    return 0;
}

/* Returns a new object with "id" as the first field followed by every
 * field of `value` (except any existing "id", which the record key
 * supersedes). Caller frees. */
static cJSON *doc_with_id(const cJSON *value, const char *id)
{
    cJSON *doc = cJSON_CreateObject();
    if (!doc) {
        return NULL;
    }
    cJSON_AddStringToObject(doc, "id", id ? id : "");
    if (cJSON_IsObject(value)) {
        cJSON *field = NULL;
        cJSON_ArrayForEach(field, value) {
            if (strcmp(field->string, "id") == 0) {
                continue;   /* record key takes the "id" column */
            }
            cJSON_AddItemToObject(doc, field->string,
                                  cJSON_Duplicate(field, 1));
        }
    }
    return doc;
}

/* Converts timestamp-tagged meta rows [{"id","timestamp","value"}, ...]
 * into plain documents, each with the record key injected as a leading
 * "id" field. Non-object values are dropped (they cannot carry a key). */
static cJSON *flatten_meta_rows(cJSON *meta)
{
    cJSON *out = cJSON_CreateArray();
    if (!out) {
        return NULL;
    }
    cJSON *row = NULL;
    cJSON_ArrayForEach(row, meta) {
        const cJSON *jid = cJSON_GetObjectItemCaseSensitive(row, "id");
        cJSON *jval = cJSON_GetObjectItemCaseSensitive(row, "value");
        if (!cJSON_IsObject(jval)) {
            continue;
        }
        cJSON *doc = doc_with_id(
            jval, cJSON_IsString(jid) && jid->valuestring
                      ? jid->valuestring : "");
        if (!doc) {
            continue;
        }
        cJSON_AddItemToArray(out, doc);
    }
    return out;
}

/* Runs a query over every keyspace in a partition, merges the
 * timestamp-tagged rows and returns them newest-first with the record key
 * injected as a leading "id" field. Handles both the replicated and
 * single-node paths. */
static cJSON *partition_wide_query(const char *db, const char *part,
                                   const cJSON *filters)
{
    char keyspaces[64][128];
    size_t nks = partition_keyspaces(db, part, keyspaces, 64);

    cJSON *rows = cJSON_CreateArray();
    if (!rows) {
        return NULL;
    }
    for (size_t i = 0; i < nks; i++) {
        cJSON *meta = g_repl
                          ? edb_repl_read_query_meta(g_repl, db, part,
                                                     keyspaces[i], filters)
                          : edb_query_ts(g_ctx.engine, part, keyspaces[i],
                                         filters);
        if (!meta) {
            continue;
        }
        cJSON *row = NULL;
        cJSON_ArrayForEach(row, meta) {
            cJSON_AddItemToArray(rows, cJSON_Duplicate(row, 1));
        }
        cJSON_Delete(meta);
    }

    size_t count = (size_t)cJSON_GetArraySize(rows);
    ts_value *values = calloc(count ? count : 1, sizeof(*values));
    if (!values) {
        cJSON_Delete(rows);
        return NULL;
    }
    size_t n = 0;
    cJSON *row = NULL;
    cJSON_ArrayForEach(row, rows) {
        const cJSON *jts = cJSON_GetObjectItemCaseSensitive(row, "timestamp");
        cJSON *jval = cJSON_GetObjectItemCaseSensitive(row, "value");
        const cJSON *jid = cJSON_GetObjectItemCaseSensitive(row, "id");
        if (!cJSON_IsObject(jval)) {
            continue;
        }
        values[n].ts = cJSON_IsNumber(jts) ? (long long)jts->valuedouble : 0;
        values[n].value = jval;
        values[n].id = cJSON_IsString(jid) && jid->valuestring
                           ? jid->valuestring : NULL;
        n++;
    }
    qsort(values, n, sizeof(*values), cmp_ts_desc);

    cJSON *out = cJSON_CreateArray();
    if (!out) {
        free(values);
        cJSON_Delete(rows);
        return NULL;
    }
    for (size_t i = 0; i < n; i++) {
        cJSON *doc = doc_with_id(values[i].value, values[i].id);
        if (doc) {
            cJSON_AddItemToArray(out, doc);
        }
    }
    free(values);
    cJSON_Delete(rows);
    return out;
}

/* Collection reads accept structured JSON filters in POST bodies and
 * unfiltered GET requests for convenience. */
static size_t collect_filter_keys(const cJSON *filters, const char **keys,
                                  size_t cap)
{
    size_t n = 0;
    if (cJSON_IsArray(filters)) {
        const cJSON *f = NULL;
        cJSON_ArrayForEach(f, filters) {
            if (n >= cap) {
                break;
            }
            if (!cJSON_IsObject(f)) {
                continue;
            }
            const cJSON *k = cJSON_GetObjectItemCaseSensitive(f, "key");
            if (cJSON_IsString(k) && k->valuestring) {
                keys[n++] = k->valuestring;
            }
        }
    } else if (cJSON_IsObject(filters)) {
        const cJSON *k = cJSON_GetObjectItemCaseSensitive(filters, "key");
        if (n < cap && cJSON_IsString(k) && k->valuestring) {
            keys[n++] = k->valuestring;
        }
    }
    return n;
}

bool handle_data_collect(const edb_http_request *req,
                                edb_http_response *res)
{
    const char *path = req->path;
    if (strncmp(path, "/data/", 6) != 0) {
        respond_error(res, 400, "expected /data/...");
        return true;
    }

    if (restore_blocked(res)) {
        return true;
    }

    char work[1024];
    snprintf(work, sizeof(work), "%s", path + 6);

    char *tokens[8];
    int ntokens = 0;
    char *save = NULL;
    char *tok = strtok_r(work, "/", &save);
    while (tok && ntokens < 8) {
        tokens[ntokens++] = tok;
        tok = strtok_r(NULL, "/", &save);
    }

    const char *operation = ntokens > 0 ? tokens[ntokens - 1] : "";
    bool collection = strcmp(operation, "ids") == 0 ||
                      strcmp(operation, "all") == 0 ||
                      strcmp(operation, "query") == 0;
    if (!collection) {
        return strcmp(req->method, "GET") == 0
                   ? handle_data_get(req, res)
                   : (respond_error(res, 404, "unknown operation"), true);
    }

    char db[128], part[256], ks[128];
    bool keyspace_omitted = false;
    if (ntokens == 4) {
        snprintf(db, sizeof(db), "%s", tokens[0]);
        snprintf(part, sizeof(part), "%s", tokens[1]);
        snprintf(ks, sizeof(ks), "%s", tokens[2]);
    } else if (ntokens == 3 && strcmp(operation, "query") == 0) {
        /* /data/{db}/{partition}/query: keyspace omitted => whole partition */
        snprintf(db, sizeof(db), "%s", tokens[0]);
        snprintf(part, sizeof(part), "%s", tokens[1]);
        ks[0] = '\0';
        keyspace_omitted = true;
    } else {
        respond_error(res, 400,
                      "expected /data/{db}/{partition}/{keyspace}/<op>");
        return true;
    }

    bool auth_ok = false;
    uint64_t groups = authenticate(req, req, &auth_ok);
    if (!auth_ok) {
        respond_error(res, 401, "unauthorized");
        return true;
    }
    if (!authorize_partition(g_ctx.config, db, part, groups, EDB_PERM_READ,
                             res)) {
        return true;
    }

    cJSON *body = NULL;
    const cJSON *filters = NULL;
    if (req->body_len > 0) {
        if (!body_json(req, &body)) {
            respond_error(res, 400, "body must be valid JSON");
            return true;
        }
        const cJSON *wrapped = cJSON_IsObject(body)
                                   ? cJSON_GetObjectItemCaseSensitive(body,
                                                                      "filters")
                                   : NULL;
        filters = wrapped ? wrapped : body;
        if (!edb_filters_valid(filters)) {
            cJSON_Delete(body);
            respond_error(res, 400, "invalid JSON filter");
            return true;
        }
    }
    if (strstr(req->query, "filter=") || strstr(req->query, "field=")) {
        cJSON_Delete(body);
        respond_error(res, 400, "use structured JSON filters in the body");
        return true;
    }

    const char *filter_keys[64];
    size_t nkeys = collect_filter_keys(filters, filter_keys, 64);
    long long started = api_now_us();
    if (strcmp(operation, "ids") == 0) {
        size_t count = 0;
        char **ids = g_repl
                         ? edb_repl_read_ids(g_repl, db, part, ks, filters,
                                             &count)
                         : edb_ids(g_ctx.engine, part, ks, filters, &count);
        cJSON *array = cJSON_CreateArray();
        for (size_t i = 0; array && ids && i < count; i++) {
            cJSON_AddItemToArray(array, cJSON_CreateString(ids[i]));
        }
        edb_free_strings(ids);
        respond_json(res, 200, array);
    } else if (keyspace_omitted) {
        cJSON *documents = partition_wide_query(db, part, filters);
        respond_json(res, 200,
                     documents ? documents : cJSON_CreateArray());
    } else {
        /* all and query share identical engine semantics (a filter may be
         * empty); both return the record key as a leading "id" field. */
        cJSON *meta = g_repl
                          ? edb_repl_read_query_meta(g_repl, db, part, ks,
                                                     filters)
                          : edb_query_ts(g_ctx.engine, part, ks, filters);
        cJSON *documents = meta ? flatten_meta_rows(meta) : NULL;
        if (meta) {
            cJSON_Delete(meta);
        }
        respond_json(res, 200,
                     documents ? documents : cJSON_CreateArray());
    }
    if (g_analytics) {
        edb_analytics_record_query(g_analytics, part, ks, filter_keys, nkeys,
                                   api_now_us() - started);
    }
    cJSON_Delete(body);
    return true;
}

/* ------------------------------------------------------------------ */
/* admin routes                                                        */
