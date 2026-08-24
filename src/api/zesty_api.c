#include "zesty_api.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "../../vendor/cjson/cJSON.h"

/* ------------------------------------------------------------------ */
/* helpers                                                             */

typedef struct {
    zdb_engine *engine;
    zdb_config *config;
} api_ctx;

static api_ctx g_ctx;   /* handlers receive no user pointer; single server */
static zdb_cluster *g_cluster;   /* may be NULL: clustering disabled */
static zdb_repl *g_repl;         /* may be NULL: replication disabled */
static pthread_mutex_t g_data_put_lock = PTHREAD_MUTEX_INITIALIZER;

void zdb_api_set_cluster(zdb_cluster *cluster)
{
    g_cluster = cluster;
}

static void free_collected(char **arr, size_t count);
static char **strings_from_json(const cJSON *arr, size_t *count_out);
static bool api_apply_change_impl(void *ud, const cJSON *change);
static cJSON *api_read_request(void *ud, const cJSON *request);
static bool api_replicate_config(void *ctx, const char *keyspace,
                                 const char *id, const char *json,
                                 const char *type_filter);

void zdb_api_set_repl(zdb_repl *repl)
{
    g_repl = repl;
    if (repl) {
        zdb_repl_set_handlers(repl, api_apply_change_impl,
                              api_read_request, NULL);
    }
    if (g_ctx.config) {
        zdb_config_set_replicator(g_ctx.config,
                                  repl ? api_replicate_config : NULL, repl);
    }
}

static bool api_replicate_config(void *ctx, const char *keyspace,
                                 const char *id, const char *json,
                                 const char *type_filter)
{
    zdb_repl *repl = ctx;
    cJSON *change = cJSON_CreateObject();
    if (!repl || !change) {
        cJSON_Delete(change);
        return false;
    }
    cJSON_AddStringToObject(change, "op", json ? "put" : "delete");
    cJSON_AddStringToObject(change, "db", ZDB_SYSTEM_DB);
    cJSON_AddStringToObject(change, "partition", ZDB_SYSTEM_DB);
    cJSON_AddStringToObject(change, "keyspace", keyspace);
    cJSON_AddStringToObject(change, "id", id);
    cJSON_AddNumberToObject(change, "ts", (double)time(NULL));
    if (json) {
        cJSON *value = cJSON_Parse(json);
        if (!value) {
            cJSON_Delete(change);
            return false;
        }
        cJSON_AddItemToObject(change, "value", value);
        cJSON_AddNumberToObject(change, "ttl_abs", -1);
        cJSON *filters = cJSON_AddArrayToObject(change, "filters");
        if (filters && type_filter) {
            cJSON_AddItemToArray(filters, cJSON_CreateString(type_filter));
        }
    }
    char *encoded = cJSON_PrintUnformatted(change);
    cJSON_Delete(change);
    if (!encoded) {
        return false;
    }
    zdb_repl_status status = zdb_repl_write(repl, ZDB_SYSTEM_DB, encoded);
    free(encoded);
    return status == ZDB_REPL_OK;
}


/* --- stage 5 replication handlers ----------------------------------- */

/* Applies a replicated change document to the local engine. Used both
 * for peer REPL frames (dispatcher) and for the local leg of fan-out. */
static bool api_apply_change_impl(void *ud, const cJSON *change)
{
    (void)ud;
    const cJSON *jop = cJSON_GetObjectItemCaseSensitive(change, "op");
    const cJSON *jdb = cJSON_GetObjectItemCaseSensitive(change, "db");
    const cJSON *jpart =
        cJSON_GetObjectItemCaseSensitive(change, "partition");
    const cJSON *jks =
        cJSON_GetObjectItemCaseSensitive(change, "keyspace");
    const cJSON *jid = cJSON_GetObjectItemCaseSensitive(change, "id");
    const cJSON *jts = cJSON_GetObjectItemCaseSensitive(change, "ts");
    const cJSON *jorigin =
        cJSON_GetObjectItemCaseSensitive(change, "origin");
    const char *origin = cJSON_IsString(jorigin) && jorigin->valuestring
                             ? jorigin->valuestring
                             : "";

    if (!cJSON_IsString(jop) || !cJSON_IsString(jdb) ||
        !cJSON_IsString(jpart) || !cJSON_IsString(jks) ||
        !cJSON_IsString(jid) || !cJSON_IsNumber(jts)) {
        return false;
    }
    long long ts = (long long)jts->valuedouble;

    const char **filters = NULL;
    size_t nfilters = 0;
    const cJSON *jfilters =
        cJSON_GetObjectItemCaseSensitive(change, "filters");
    if (cJSON_IsArray(jfilters)) {
        int n = cJSON_GetArraySize(jfilters);
        filters = malloc((size_t)n * sizeof(char *));
        if (filters) {
            const cJSON *f = NULL;
            cJSON_ArrayForEach(f, jfilters) {
                if (cJSON_IsString(f) && f->valuestring) {
                    filters[nfilters++] = f->valuestring;
                }
            }
        }
    }

    bool ok;
    if (strcmp(jop->valuestring, "put") == 0) {
        const cJSON *jval =
            cJSON_GetObjectItemCaseSensitive(change, "value");
        const cJSON *jttl =
            cJSON_GetObjectItemCaseSensitive(change, "ttl_abs");
        if (!cJSON_IsObject(jval)) {
            free(filters);
            return false;
        }
        char *value_json = cJSON_PrintUnformatted(jval);
        if (!value_json) {
            free(filters);
            return false;
        }
        long long ttl_abs = cJSON_IsNumber(jttl)
                                ? (long long)jttl->valuedouble
                                : -1;
        ok = zdb_replica_put_origin(g_ctx.engine, jpart->valuestring,
                                    jks->valuestring, jid->valuestring,
                                    value_json, ttl_abs, ts, origin, filters,
                                    nfilters);
        free(value_json);

        /* transparent partition registration on replicas too */
        if (ok && strcmp(jdb->valuestring, ZDB_SYSTEM_DB) != 0) {
            zdb_partition_ensure(g_ctx.config, jdb->valuestring,
                                 jpart->valuestring, jks->valuestring,
                                 NULL);
        }
    } else if (strcmp(jop->valuestring, "delete") == 0) {
        ok = zdb_replica_delete_origin(g_ctx.engine, jpart->valuestring,
                                       jks->valuestring, jid->valuestring, ts,
                                       origin);
    } else {
        ok = false;
    }
    free(filters);
    return ok;
}

/* Answers a quorum read request from a peer against the local engine. */
static cJSON *api_read_request(void *ud, const cJSON *request)
{
    (void)ud;
    const cJSON *jq = cJSON_GetObjectItemCaseSensitive(request, "q");
    const cJSON *jpart =
        cJSON_GetObjectItemCaseSensitive(request, "partition");
    const cJSON *jks =
        cJSON_GetObjectItemCaseSensitive(request, "keyspace");
    if (!cJSON_IsString(jq) || !cJSON_IsString(jpart) ||
        !cJSON_IsString(jks)) {
        return NULL;
    }
    const char *part = jpart->valuestring;
    const char *ks = jks->valuestring;

    size_t nfilters = 0;
    size_t nfields = 0;
    char **filters = strings_from_json(
        cJSON_GetObjectItemCaseSensitive(request, "filters"), &nfilters);
    char **fields = strings_from_json(
        cJSON_GetObjectItemCaseSensitive(request, "fields"), &nfields);

    cJSON *out = cJSON_CreateObject();
    if (!out) {
        free_collected(filters, nfilters);
        free_collected(fields, nfields);
        return NULL;
    }

    if (strcmp(jq->valuestring, "get") == 0) {
        const cJSON *jid =
            cJSON_GetObjectItemCaseSensitive(request, "id");
        long long ts = 0;
        cJSON *doc = cJSON_IsString(jid) && jid->valuestring
                         ? zdb_get_ts(g_ctx.engine, part, ks,
                                      jid->valuestring, &ts)
                         : NULL;
        if (doc) {
            cJSON *row = cJSON_CreateObject();
            const cJSON *idstr =
                cJSON_GetObjectItemCaseSensitive(request, "id");
            cJSON_AddStringToObject(row, "id", idstr->valuestring);
            cJSON_AddNumberToObject(row, "timestamp", (double)ts);
            cJSON_AddItemToObject(row, "value", doc);
            cJSON_AddItemToObject(out, "row", row);
        } else {
            cJSON_AddNullToObject(out, "row");
        }
    } else if (strcmp(jq->valuestring, "all_ts") == 0 ||
               strcmp(jq->valuestring, "query_ts") == 0) {
        cJSON *rows =
            jq->valuestring[0] == 'a'
                ? zdb_all_ts(g_ctx.engine, part, ks,
                             (const char **)filters, nfilters)
                : zdb_query_ts(g_ctx.engine, part, ks,
                               (const char **)filters, nfilters,
                               (const char **)fields, nfields);
        if (!rows) {
            rows = cJSON_CreateArray();
        }
        cJSON_AddItemToObject(out, "rows", rows);
    } else if (strcmp(jq->valuestring, "ids") == 0) {
        size_t n = 0;
        char **ids = zdb_ids(g_ctx.engine, part, ks,
                             (const char **)filters, nfilters, &n);
        cJSON *arr = cJSON_AddArrayToObject(out, "ids");
        for (size_t i = 0; arr && ids && i < n; i++) {
            cJSON_AddItemToArray(arr, cJSON_CreateString(ids[i]));
        }
        zdb_free_strings(ids);
    } else {
        cJSON_Delete(out);
        out = NULL;
    }

    free_collected(filters, nfilters);
    free_collected(fields, nfields);
    return out;
}

static void respond_json(zdb_http_response *res, int status, cJSON *obj)
{
    res->status = status;
    res->content_type = "application/json";
    if (obj) {
        res->body = cJSON_PrintUnformatted(obj);
        cJSON_Delete(obj);
        res->body_len = res->body ? strlen(res->body) : 0;
    } else {
        res->body = NULL;
        res->body_len = 0;
    }
}

static void respond_error(zdb_http_response *res, int status,
                          const char *message)
{
    cJSON *obj = cJSON_CreateObject();
    if (obj) {
        cJSON_AddStringToObject(obj, "error", message);
    }
    respond_json(res, status, obj);
}

static bool body_json(const zdb_http_request *req, cJSON **out)
{
    if (!req->body || req->body_len == 0) {
        return false;
    }
    cJSON *parsed = cJSON_ParseWithLength(req->body, req->body_len);
    if (!parsed) {
        return false;
    }
    *out = parsed;
    return true;
}

static bool json_u64_value(const cJSON *item, uint64_t *out)
{
    if (cJSON_IsString(item) && item->valuestring) {
        errno = 0;
        char *end = NULL;
        unsigned long long value = strtoull(item->valuestring, &end, 10);
        if (errno == 0 && end && *end == '\0') {
            *out = (uint64_t)value;
            return true;
        }
        return false;
    }
    if (cJSON_IsNumber(item) && item->valuedouble >= 0 &&
        item->valuedouble <= 9007199254740991.0) {
        uint64_t value = (uint64_t)item->valuedouble;
        if ((double)value == item->valuedouble) {
            *out = value;
            return true;
        }
    }
    return false;
}

static bool query_param(const zdb_http_request *req, const char *name,
                        char *out, size_t cap)
{
    size_t name_len = strlen(name);
    const char *q = req->query;
    while (*q) {
        const char *amp = strchr(q, '&');
        size_t seg_end = amp ? (size_t)(amp - q) : strlen(q);
        if (seg_end > name_len + 1 && strncmp(q, name, name_len) == 0 &&
            q[name_len] == '=') {
            size_t vlen = seg_end - name_len - 1;
            snprintf(out, cap, "%.*s", (int)vlen, q + name_len + 1);
            return true;
        }
        if (!amp) {
            break;
        }
        q = amp + 1;
    }
    out[0] = '\0';
    return false;
}

/* Collects repeated ?filter=key=value params into a malloc'd array. */
static char **collect_params(const zdb_http_request *req, const char *name,
                             size_t *count_out)
{
    *count_out = 0;
    size_t cap = 8;
    char **arr = malloc(cap * sizeof(char *));
    if (!arr) {
        return NULL;
    }
    size_t name_len = strlen(name);
    const char *q = req->query;
    while (q && *q) {
        const char *amp = strchr(q, '&');
        size_t seg_len = amp ? (size_t)(amp - q) : strlen(q);
        if (seg_len > name_len + 1 && strncmp(q, name, name_len) == 0 &&
            q[name_len] == '=') {
            if (*count_out == cap) {
                cap *= 2;
                char **grown = realloc(arr, cap * sizeof(char *));
                if (!grown) {
                    goto fail;
                }
                arr = grown;
            }
            char *val = malloc(seg_len - name_len);   /* name + '=' removed */
            if (!val) {
                goto fail;
            }
            memcpy(val, q + name_len + 1, seg_len - name_len - 1);
            val[seg_len - name_len - 1] = '\0';
            arr[(*count_out)++] = val;
        }
        q = amp ? amp + 1 : NULL;
    }
    if (*count_out == 0) {
        free(arr);
        return NULL;
    }
    return arr;
fail:
    for (size_t i = 0; i < *count_out; i++) {
        free(arr[i]);
    }
    free(arr);
    *count_out = 0;
    return NULL;
}

static void free_collected(char **arr, size_t count)
{
    if (!arr) {
        return;
    }
    for (size_t i = 0; i < count; i++) {
        free(arr[i]);
    }
    free(arr);
}

/* Copies a JSON array of strings into a malloc'd char* array. */
static char **strings_from_json(const cJSON *arr, size_t *count_out)
{
    *count_out = 0;
    if (!cJSON_IsArray(arr)) {
        return NULL;
    }
    int n = cJSON_GetArraySize(arr);
    if (n <= 0) {
        return NULL;
    }
    char **out = calloc((size_t)n, sizeof(char *));
    if (!out) {
        return NULL;
    }
    const cJSON *i = NULL;
    cJSON_ArrayForEach(i, arr) {
        if (cJSON_IsString(i) && i->valuestring) {
            out[(*count_out)++] = strdup(i->valuestring);
        }
    }
    if (*count_out == 0) {
        free(out);
        return NULL;
    }
    return out;
}

/* --- stage 5 replication handlers ----------------------------------- */

/* ------------------------------------------------------------------ */
/* authentication                                                      */

/* Resolves the caller's group bitmask from the request. Accepts either an
 * "Authorization: Bearer <user>" header or an "authorization" JSON key in
 * the body / an "authorization" query param carrying the user name.
 * Returns the group mask; sets *ok. Unknown/absent users yield mask with
 * ok=false unless the special user "anonymous" exists. */
static uint64_t authenticate(const zdb_http_request *req,
                             const zdb_http_request *unused, bool *ok)
{
    (void)unused;
    *ok = true;

    if (req->trusted) {
        return ~0ULL;
    }

    uint64_t groups = 0;
    char token_buf[256] = "";
    const char *token = NULL;
    const char *hdr = zdb_http_header(req, "Authorization");
    if (hdr) {
        const char *value = strncmp(hdr, "Bearer ", 7) == 0 ? hdr + 7 : hdr;
        if (snprintf(token_buf, sizeof(token_buf), "%s", value) <
            (int)sizeof(token_buf)) {
            token = token_buf;
        }
    }
    if (!token) {
        hdr = zdb_http_header(req, "authorization");
        if (hdr && snprintf(token_buf, sizeof(token_buf), "%s", hdr) <
                       (int)sizeof(token_buf)) {
            token = token_buf;
        }
    }
    if (!token && req->query[0] &&
        query_param(req, "authorization", token_buf, sizeof(token_buf))) {
        token = token_buf;
    }
    if (!token && req->body) {
        cJSON *body = NULL;
        if (body_json(req, &body)) {
            const cJSON *authorization =
                cJSON_GetObjectItemCaseSensitive(body, "authorization");
            if (cJSON_IsString(authorization) && authorization->valuestring &&
                snprintf(token_buf, sizeof(token_buf), "%s",
                         authorization->valuestring) < (int)sizeof(token_buf)) {
                token = token_buf;
            }
            cJSON_Delete(body);
        }
    }

    if (g_ctx.config && token && *token) {
        zdb_user_info user;
        if (zdb_user_get(g_ctx.config, token, &user)) {
            groups = user.groups;
        } else {
            *ok = false;
        }
    } else {
        size_t nusers = 0;
        zdb_user_info *users = zdb_user_list(g_ctx.config, &nusers);
        bool bootstrapped = users != NULL && nusers > 0;
        free(users);
        groups = bootstrapped ? 0ULL : ~0ULL;
    }
    return groups;
}

/* Looks up partition masks and checks one permission. Unknown partitions
 * are treated as open (allow-all masks) because partitions materialise
 * transparently on first write; explicit records exist to restrict them. */
static bool authorize_partition(zdb_config *cfg, const char *database,
                                const char *partition, uint64_t user_groups,
                                zdb_permission perm, zdb_http_response *res)
{
    zdb_partition_info part;
    if (!zdb_partition_get(cfg, database, partition, &part)) {
        return true;   /* implicit partition: allow-all masks */
    }
    uint64_t mask;
    switch (perm) {
    case ZDB_PERM_CREATE: mask = part.create_mask; break;
    case ZDB_PERM_UPDATE: mask = part.update_mask; break;
    case ZDB_PERM_READ:   mask = part.read_mask; break;
    case ZDB_PERM_DELETE: mask = part.delete_mask; break;
    default:              mask = ~0ULL; break;
    }
    if (!zdb_check_perm(mask, user_groups, perm)) {
        respond_error(res, 403, "permission denied");
        return false;
    }
    return true;
}

/* ------------------------------------------------------------------ */
/* data routes                                                         */

static bool split_data_path(const char *path, char db[128], char part[256],
                            char ks[128], char id[512])
{
    /* expected: /data/{db}/{partition}/{keyspace}/{id} */
    if (strncmp(path, "/data/", 6) != 0) {
        return false;
    }
    char work[1024];
    snprintf(work, sizeof(work), "%s", path + 6);

    /* strip trailing slash */
    size_t len = strlen(work);
    if (len && work[len - 1] == '/') {
        work[len - 1] = '\0';
    }

    char *save = NULL;
    char *tok_db = strtok_r(work, "/", &save);
    char *tok_part = tok_db ? strtok_r(NULL, "/", &save) : NULL;
    char *tok_ks = tok_part ? strtok_r(NULL, "/", &save) : NULL;
    char *rest = tok_ks ? strtok_r(NULL, "", &save) : NULL;

    if (!tok_db || !tok_part || !tok_ks || !rest || !*rest ||
        strchr(rest, '/')) {
        return false;
    }
    snprintf(db, 128, "%s", tok_db);
    snprintf(part, 256, "%s", tok_part);
    snprintf(ks, 128, "%s", tok_ks);
    snprintf(id, 512, "%s", rest);
    return true;
}

static bool handle_data_put(const zdb_http_request *req,
                            zdb_http_response *res)
{
    char db[128], part[256], ks[128], id[512];
    if (!split_data_path(req->path, db, part, ks, id)) {
        respond_error(res, 400,
                      "expected /data/{db}/{partition}/{keyspace}/{id}");
        return true;
    }

    bool auth_ok = false;
    uint64_t groups = authenticate(req, req, &auth_ok);
    if (!auth_ok) {
        respond_error(res, 401, "unknown user");
        return true;
    }

    cJSON *doc = NULL;
    if (!body_json(req, &doc) || !cJSON_IsObject(doc)) {
        respond_error(res, 400, "body must be a JSON object");
        cJSON_Delete(doc);
        return true;
    }
    char *json = cJSON_PrintUnformatted(doc);
    cJSON_Delete(doc);
    if (!json) {
        respond_error(res, 500, "encode failed");
        return true;
    }

    long long ttl = -1;
    char ttlbuf[32];
    if (query_param(req, "ttl", ttlbuf, sizeof(ttlbuf))) {
        ttl = strtoll(ttlbuf, NULL, 10);
    }
    size_t nfilters = 0;
    char **filters = collect_params(req, "filter", &nfilters);

    pthread_mutex_lock(&g_data_put_lock);
    cJSON *existing = zdb_get(g_ctx.engine, part, ks, id);
    zdb_permission permission = existing ? ZDB_PERM_UPDATE : ZDB_PERM_CREATE;
    cJSON_Delete(existing);
    if (!authorize_partition(g_ctx.config, db, part, groups, permission, res)) {
        pthread_mutex_unlock(&g_data_put_lock);
        free(json);
        free_collected(filters, nfilters);
        return true;
    }

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
            cJSON_AddItemToObject(change, "value", cJSON_Parse(json));
            cJSON_AddNumberToObject(change, "ttl_abs",
                                    ttl >= 0 ? (double)(ts + ttl) : -1);
            cJSON_AddNumberToObject(change, "ts", (double)ts);
            cJSON *filter_array = cJSON_AddArrayToObject(change, "filters");
            for (size_t i = 0; filter_array && i < nfilters; i++) {
                cJSON_AddItemToArray(filter_array,
                                     cJSON_CreateString(filters[i]));
            }
            char *change_json = cJSON_PrintUnformatted(change);
            cJSON_Delete(change);
            if (change_json) {
                zdb_repl_status status = zdb_repl_write(g_repl, db,
                                                        change_json);
                free(change_json);
                if (status == ZDB_REPL_QUORUM_LOST) {
                    pthread_mutex_unlock(&g_data_put_lock);
                    free(json);
                    free_collected(filters, nfilters);
                    respond_error(res, 503,
                                  "quorum unavailable: write rejected");
                    return true;
                }
                ok = status == ZDB_REPL_OK;
            }
        }
    } else {
        ok = zdb_put(g_ctx.engine, part, ks, id, json, ttl,
                     (const char **)filters, nfilters);
    }
    if (ok) {
        zdb_partition_ensure(g_ctx.config, db, part, ks, NULL);
    }
    pthread_mutex_unlock(&g_data_put_lock);
    free(json);
    free_collected(filters, nfilters);

    if (!ok) {
        respond_error(res, 500, "storage failed");
        return true;
    }
    respond_json(res, 200, NULL);
    res->body = zdb_http_body_printf(&res->body_len,
                                     "{\"status\":\"ok\",\"id\":\"%s\"}",
                                     id);
    return true;
}

static bool handle_data_get(const zdb_http_request *req,
                            zdb_http_response *res)
{
    char db[128], part[256], ks[128], id[512];
    if (!split_data_path(req->path, db, part, ks, id)) {
        respond_error(res, 400, "expected /data/{db}/{partition}/{keyspace}/{id}");
        return true;
    }

    bool auth_ok = false;
    uint64_t groups = authenticate(req, req, &auth_ok);
    if (!auth_ok) {
        respond_error(res, 401, "unknown user");
        return true;
    }
    if (!authorize_partition(g_ctx.config, db, part, groups, ZDB_PERM_READ,
                             res)) {
        return true;
    }

    cJSON *doc = g_repl
                     ? zdb_repl_read_get(g_repl, db, part, ks, id)
                     : zdb_get(g_ctx.engine, part, ks, id);
    if (!doc) {
        respond_error(res, 404, "not found");
        return true;
    }
    respond_json(res, 200, doc);
    return true;
}

static bool handle_data_delete(const zdb_http_request *req,
                               zdb_http_response *res)
{
    char db[128], part[256], ks[128], id[512];
    if (!split_data_path(req->path, db, part, ks, id)) {
        respond_error(res, 400, "expected /data/{db}/{partition}/{keyspace}/{id}");
        return true;
    }

    bool auth_ok = false;
    uint64_t groups = authenticate(req, req, &auth_ok);
    if (!auth_ok) {
        respond_error(res, 401, "unknown user");
        return true;
    }
    if (!authorize_partition(g_ctx.config, db, part, groups, ZDB_PERM_DELETE,
                             res)) {
        return true;
    }

    bool ok;
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
        zdb_repl_status st =
            zdb_repl_write(g_repl, db, change_json);
        free(change_json);
        if (st == ZDB_REPL_LOCAL_FAIL) {
            respond_error(res, 500, "delete failed");
            return true;
        }
        if (st == ZDB_REPL_QUORUM_LOST) {
            respond_error(res, 503,
                          "quorum unavailable: delete rejected");
            return true;
        }
        ok = true;
    } else {
        ok = zdb_delete(g_ctx.engine, part, ks, id);
    }
    if (!ok) {
        respond_error(res, 500, "delete failed");
        return true;
    }
    res->status = 200;
    res->content_type = "application/json";
    res->body = zdb_http_body_printf(&res->body_len,
                                     "{\"status\":\"deleted\",\"id\":\"%s\"}",
                                     id);
    return true;
}

/* POST body may carry {"fields":{"k":"v",...}} narrowing the result set
 * beyond any ?filter= params (which use the indexed filter path).
 * Also handles GET .../all|ids (no body) for convenience. */
static bool handle_data_collect(const zdb_http_request *req,
                                zdb_http_response *res)
{
    char db[128], part[256], ks[128];
    const char *p = req->path;
    if (sscanf(p, "/data/%127[^/]/%255[^/]/%127[^/]", db, part, ks) != 3) {
        respond_error(res, 400,
                      "expected /data/{db}/{partition}/{keyspace}/<op>");
        return true;
    }
    const char *op = strrchr(p, '/');
    op = op ? op + 1 : "";

    /* id lookup (GET .../{keyspace}/{id}, 4 segments) falls through to
     * handle_data_get; collection ops are .../keyspace/all|ids|query */
    bool is_op = strcmp(op, "ids") == 0 || strcmp(op, "all") == 0 ||
                 strcmp(op, "query") == 0;
    if (!is_op) {
        if (strcmp(req->method, "GET") != 0) {
            respond_error(res, 404, "unknown operation");
            return true;
        }
        /* count segments: /data + db/partition/keyspace[/id] */
        int slashes = 0;
        for (const char *c = p; *c; c++) {
            if (*c == '/') {
                slashes++;
            }
        }
        if (slashes < 4) {
            /* /data/db/partition/keyspace has exactly 4 slashes; anything
             * less is malformed */
            respond_error(res, 400,
                          "expected id: /data/{db}/{partition}/{keyspace}/{id}");
            return true;
        }
        return handle_data_get(req, res);
    }

    bool auth_ok = false;
    uint64_t groups = authenticate(req, req, &auth_ok);
    if (!auth_ok) {
        respond_error(res, 401, "unknown user");
        return true;
    }
    if (!authorize_partition(g_ctx.config, db, part, groups, ZDB_PERM_READ,
                             res)) {
        return true;
    }

    size_t nfilters = 0;
    char **filters = collect_params(req, "filter", &nfilters);

    size_t nfields = 0;
    char **fields = NULL;
    cJSON *body = NULL;
    if (body_json(req, &body)) {
        const cJSON *f = cJSON_GetObjectItemCaseSensitive(body, "fields");
        if (cJSON_IsObject(f)) {
            int count = cJSON_GetArraySize(f);
            fields = malloc((size_t)count * sizeof(char *));
            if (fields) {
                const cJSON *item = NULL;
                cJSON_ArrayForEach(item, f) {
                    if (!cJSON_IsString(item)) {
                        continue;
                    }
                    size_t need = strlen(item->string) +
                                  strlen(item->valuestring) + 2;
                    fields[nfields] = malloc(need);
                    if (!fields[nfields]) {
                        continue;
                    }
                    snprintf(fields[nfields], need, "%s=%s", item->string,
                             item->valuestring);
                    nfields++;
                }
            }
        }
        cJSON_Delete(body);
    }

    if (strcmp(op, "ids") == 0) {
        size_t n = 0;
        char **ids =
            g_repl
                ? zdb_repl_read_ids(g_repl, db, part, ks,
                                    (const char **)filters, nfilters, &n)
                : zdb_ids(g_ctx.engine, part, ks,
                          (const char **)filters, nfilters, &n);
        cJSON *arr = cJSON_CreateArray();
        for (size_t i = 0; ids && i < n; i++) {
            cJSON_AddItemToArray(arr, cJSON_CreateString(ids[i]));
        }
        zdb_free_strings(ids);
        respond_json(res, 200, arr);
    } else if (strcmp(op, "all") == 0) {
        cJSON *docs =
            g_repl
                ? zdb_repl_read_all(g_repl, db, part, ks,
                                    (const char **)filters, nfilters)
                : zdb_all(g_ctx.engine, part, ks,
                          (const char **)filters, nfilters);
        respond_json(res, 200, docs ? docs : cJSON_CreateArray());
    } else if (strcmp(op, "query") == 0) {
        cJSON *docs =
            g_repl
                ? zdb_repl_read_query(g_repl, db, part, ks,
                                      (const char **)filters, nfilters,
                                      (const char **)fields, nfields)
                : zdb_query(g_ctx.engine, part, ks,
                            (const char **)filters, nfilters,
                            (const char **)fields, nfields);
        respond_json(res, 200, docs ? docs : cJSON_CreateArray());
    } else {
        respond_error(res, 404, "unknown operation");
    }

    free_collected(filters, nfilters);
    if (fields) {
        for (size_t i = 0; i < nfields; i++) {
            free(fields[i]);
        }
        free(fields);
    }
    return true;
}

/* ------------------------------------------------------------------ */
/* admin routes                                                        */

static bool require_admin_auth(const zdb_http_request *req,
                               zdb_http_response *res)
{
    /* admin operations are only permitted to authenticated users that hold
     * bit 1 (the first security group). */
    bool auth_ok = false;
    uint64_t groups = authenticate(req, req, &auth_ok);
    if (!auth_ok) {
        respond_error(res, 401, "unknown user");
        return false;
    }
    if (!(groups & 1ULL) && groups != ~0ULL) {
        respond_error(res, 403, "admin access required");
        return false;
    }
    return true;
}

static bool handle_admin_databases(const zdb_http_request *req,
                                   zdb_http_response *res)
{
    if (!require_admin_auth(req, res)) {
        return true;
    }
    if (strcmp(req->method, "GET") == 0) {
        size_t n = 0;
        zdb_database_info *list = zdb_database_list(g_ctx.config, &n);
        cJSON *arr = cJSON_CreateArray();
        for (size_t i = 0; list && i < n; i++) {
            cJSON *o = cJSON_CreateObject();
            cJSON_AddStringToObject(o, "name", list[i].name);
            cJSON_AddNumberToObject(o, "replication_factor",
                                    list[i].replication_factor);
            cJSON_AddItemToArray(arr, o);
        }
        free(list);
        respond_json(res, 200, arr);
        return true;
    }
    if (strcmp(req->method, "POST") == 0) {
        cJSON *body = NULL;
        if (!body_json(req, &body) || !cJSON_IsObject(body)) {
            respond_error(res, 400, "JSON object required");
            cJSON_Delete(body);
            return true;
        }
        const cJSON *name = cJSON_GetObjectItemCaseSensitive(body, "name");
        const cJSON *rf = cJSON_GetObjectItemCaseSensitive(
            body, "replication_factor");
        if (!cJSON_IsString(name) || !cJSON_IsNumber(rf)) {
            respond_error(res, 400,
                          "name and replication_factor required");
            cJSON_Delete(body);
            return true;
        }
        bool ok = zdb_database_create(g_ctx.config, name->valuestring,
                                      rf->valueint);
        cJSON_Delete(body);
        if (!ok) {
            respond_error(res, 409, "create failed (duplicate?)");
            return true;
        }
        respond_json(res, 201, NULL);
        res->body = zdb_http_body_printf(&res->body_len, "{\"created\":true}");
        return true;
    }
    respond_error(res, 405, "method not allowed");
    return true;
}

static bool handle_admin_groups(const zdb_http_request *req,
                                zdb_http_response *res)
{
    if (!require_admin_auth(req, res)) {
        return true;
    }
    if (strcmp(req->method, "GET") == 0) {
        size_t n = 0;
        zdb_group_info *list = zdb_group_list(g_ctx.config, &n);
        cJSON *arr = cJSON_CreateArray();
        for (size_t i = 0; list && i < n; i++) {
            cJSON *o = cJSON_CreateObject();
            cJSON_AddStringToObject(o, "name", list[i].name);
            char bits[32];
            snprintf(bits, sizeof(bits), "%llu",
                     (unsigned long long)list[i].bit_position);
            cJSON_AddRawToObject(o, "bit_position", bits);
            cJSON_AddItemToArray(arr, o);
        }
        free(list);
        respond_json(res, 200, arr);
        return true;
    }
    if (strcmp(req->method, "POST") == 0) {
        cJSON *body = NULL;
        if (!body_json(req, &body) || !cJSON_IsObject(body)) {
            respond_error(res, 400, "JSON object required");
            cJSON_Delete(body);
            return true;
        }
        const cJSON *name = cJSON_GetObjectItemCaseSensitive(body, "name");
        if (!cJSON_IsString(name)) {
            respond_error(res, 400, "name required");
            cJSON_Delete(body);
            return true;
        }
        bool ok = zdb_group_create(g_ctx.config, name->valuestring);
        cJSON_Delete(body);
        if (!ok) {
            respond_error(res, 409, "create failed (duplicate/full)");
            return true;
        }
        respond_json(res, 201, NULL);
        res->body = zdb_http_body_printf(&res->body_len, "{\"created\":true}");
        return true;
    }
    respond_error(res, 405, "method not allowed");
    return true;
}

static bool handle_admin_users(const zdb_http_request *req,
                               zdb_http_response *res)
{
    if (!require_admin_auth(req, res)) {
        return true;
    }
    if (strcmp(req->method, "GET") == 0) {
        size_t n = 0;
        zdb_user_info *list = zdb_user_list(g_ctx.config, &n);
        cJSON *arr = cJSON_CreateArray();
        for (size_t i = 0; list && i < n; i++) {
            cJSON *o = cJSON_CreateObject();
            cJSON_AddStringToObject(o, "name", list[i].name);
            char bits[32];
            snprintf(bits, sizeof(bits), "%llu",
                     (unsigned long long)list[i].groups);
            cJSON_AddRawToObject(o, "groups", bits);
            cJSON_AddItemToArray(arr, o);
        }
        free(list);
        respond_json(res, 200, arr);
        return true;
    }
    if (strcmp(req->method, "POST") == 0) {
        cJSON *body = NULL;
        if (!body_json(req, &body) || !cJSON_IsObject(body)) {
            respond_error(res, 400, "JSON object required");
            cJSON_Delete(body);
            return true;
        }
        const cJSON *name = cJSON_GetObjectItemCaseSensitive(body, "name");
        const cJSON *groups = cJSON_GetObjectItemCaseSensitive(body,
                                                               "groups");
        uint64_t group_mask = 0;
        if (!cJSON_IsString(name) || !json_u64_value(groups, &group_mask)) {
            respond_error(res, 400, "name and groups required");
            cJSON_Delete(body);
            return true;
        }
        bool ok = zdb_user_create(g_ctx.config, name->valuestring,
                                  group_mask);
        cJSON_Delete(body);
        if (!ok) {
            respond_error(res, 409, "create failed (duplicate?)");
            return true;
        }
        respond_json(res, 201, NULL);
        res->body = zdb_http_body_printf(&res->body_len, "{\"created\":true}");
        return true;
    }
    respond_error(res, 405, "method not allowed");
    return true;
}

static bool handle_admin_partitions(const zdb_http_request *req,
                                    zdb_http_response *res)
{
    if (!require_admin_auth(req, res)) {
        return true;
    }
    if (strcmp(req->method, "GET") == 0) {
        char dbbuf[128];
        bool have_db = query_param(req, "database", dbbuf, sizeof(dbbuf));
        size_t n = 0;
        zdb_partition_info *list =
            zdb_partition_list(g_ctx.config, have_db ? dbbuf : "", &n);
        if (!have_db) {
            /* list across all databases */
            free(list);
            size_t ndb = 0;
            zdb_database_info *dbs = zdb_database_list(g_ctx.config, &ndb);
            cJSON *arr2 = cJSON_CreateArray();
            for (size_t d = 0; dbs && d < ndb; d++) {
                size_t cnt = 0;
                zdb_partition_info *parts = zdb_partition_list(
                    g_ctx.config, dbs[d].name, &cnt);
                for (size_t i = 0; parts && i < cnt; i++) {
                    cJSON *o = cJSON_CreateObject();
                    cJSON_AddStringToObject(o, "database", parts[i].database);
                    cJSON_AddStringToObject(o, "name", parts[i].name);
                    char b[32];
                    snprintf(b, sizeof(b), "%llu",
                             (unsigned long long)parts[i].read_mask);
                    cJSON_AddRawToObject(o, "read_mask", b);
                    snprintf(b, sizeof(b), "%llu",
                             (unsigned long long)parts[i].update_mask);
                    cJSON_AddRawToObject(o, "update_mask", b);
                    snprintf(b, sizeof(b), "%llu",
                             (unsigned long long)parts[i].create_mask);
                    cJSON_AddRawToObject(o, "create_mask", b);
                    snprintf(b, sizeof(b), "%llu",
                             (unsigned long long)parts[i].delete_mask);
                    cJSON_AddRawToObject(o, "delete_mask", b);
                    cJSON_AddItemToArray(arr2, o);
                }
                free(parts);
            }
            free(dbs);
            respond_json(res, 200, arr2);
            return true;
        }
        cJSON *arr = cJSON_CreateArray();
        for (size_t i = 0; list && i < n; i++) {
            cJSON *o = cJSON_CreateObject();
            cJSON_AddStringToObject(o, "database", list[i].database);
            cJSON_AddStringToObject(o, "name", list[i].name);
            char b[32];
            snprintf(b, sizeof(b), "%llu",
                     (unsigned long long)list[i].read_mask);
            cJSON_AddRawToObject(o, "read_mask", b);
            snprintf(b, sizeof(b), "%llu",
                     (unsigned long long)list[i].update_mask);
            cJSON_AddRawToObject(o, "update_mask", b);
            snprintf(b, sizeof(b), "%llu",
                     (unsigned long long)list[i].create_mask);
            cJSON_AddRawToObject(o, "create_mask", b);
            snprintf(b, sizeof(b), "%llu",
                     (unsigned long long)list[i].delete_mask);
            cJSON_AddRawToObject(o, "delete_mask", b);
            cJSON_AddItemToArray(arr, o);
        }
        free(list);
        respond_json(res, 200, arr);
        return true;
    }
    if (strcmp(req->method, "POST") == 0) {
        cJSON *body = NULL;
        if (!body_json(req, &body) || !cJSON_IsObject(body)) {
            respond_error(res, 400, "JSON object required");
            cJSON_Delete(body);
            return true;
        }
        const cJSON *database = cJSON_GetObjectItemCaseSensitive(body,
                                                             "database");
        const cJSON *name = cJSON_GetObjectItemCaseSensitive(body, "name");
        const cJSON *cm = cJSON_GetObjectItemCaseSensitive(body,
                                                           "create_mask");
        const cJSON *um = cJSON_GetObjectItemCaseSensitive(body,
                                                           "update_mask");
        const cJSON *rm = cJSON_GetObjectItemCaseSensitive(body,
                                                           "read_mask");
        const cJSON *dm = cJSON_GetObjectItemCaseSensitive(body,
                                                           "delete_mask");
        uint64_t create_mask = 0;
        uint64_t update_mask = 0;
        uint64_t read_mask = 0;
        uint64_t delete_mask = 0;
        if (!cJSON_IsString(database) || !cJSON_IsString(name) ||
            !json_u64_value(cm, &create_mask) ||
            !json_u64_value(um, &update_mask) ||
            !json_u64_value(rm, &read_mask) ||
            !json_u64_value(dm, &delete_mask)) {
            respond_error(res, 400,
                          "database, name and all four masks required");
            cJSON_Delete(body);
            return true;
        }
        bool ok = zdb_partition_create(g_ctx.config, database->valuestring,
                                       name->valuestring, create_mask,
                                       update_mask, read_mask, delete_mask);
        cJSON_Delete(body);
        if (!ok) {
            respond_error(res, 409, "create failed (duplicate?)");
            return true;
        }
        respond_json(res, 201, NULL);
        res->body = zdb_http_body_printf(&res->body_len, "{\"created\":true}");
        return true;
    }
    respond_error(res, 405, "method not allowed");
    return true;
}

/* Generic DELETE handler for single-entity admin resources:
 * /admin/{databases|groups|users}/<name> and
 * /admin/partitions/<database>/<name> */
static bool handle_admin_delete(const zdb_http_request *req,
                                zdb_http_response *res)
{
    if (!require_admin_auth(req, res)) {
        return true;
    }
    const char *p = req->path;
    bool ok = false;

    if (strncmp(p, "/admin/databases/", 17) == 0) {
        ok = zdb_database_delete(g_ctx.config, p + 17);
    } else if (strncmp(p, "/admin/groups/", 14) == 0) {
        ok = zdb_group_delete(g_ctx.config, p + 14);
    } else if (strncmp(p, "/admin/users/", 13) == 0) {
        ok = zdb_user_delete(g_ctx.config, p + 13);
    } else if (strncmp(p, "/admin/partitions/", 18) == 0) {
        char database[128];
        char name[256];
        if (sscanf(p + 18, "%127[^/]/%255s", database, name) != 2) {
            respond_error(res, 400,
                          "expected /admin/partitions/<database>/<name>");
            return true;
        }
        ok = zdb_partition_delete(g_ctx.config, database, name);
    } else {
        respond_error(res, 404, "unknown resource");
        return true;
    }

    if (!ok) {
        respond_error(res, 404, "delete failed (not found?)");
        return true;
    }
    res->status = 200;
    res->content_type = "application/json";
    res->body = zdb_http_body_printf(&res->body_len, "{\"deleted\":true}");
    return true;
}

/* GET /admin/keyspaces: the registry of used
 * database/partition/keyspace triples (populated transparently by
 * writes). Optional ?database=<name> narrows the listing. */
static bool handle_admin_keyspaces(const zdb_http_request *req,
                                   zdb_http_response *res)
{
    if (!require_admin_auth(req, res)) {
        return true;
    }
    char dbfilter[128] = "";
    bool have_db = query_param(req, "database", dbfilter, sizeof(dbfilter));

    size_t n = 0;
    zdb_keyspace_info *list = zdb_keyspace_list(g_ctx.config, &n);
    cJSON *arr = cJSON_CreateArray();
    for (size_t i = 0; list && i < n; i++) {
        if (have_db && strcmp(list[i].database, dbfilter) != 0) {
            continue;
        }
        cJSON *o = cJSON_CreateObject();
        cJSON_AddStringToObject(o, "database", list[i].database);
        cJSON_AddStringToObject(o, "partition", list[i].partition);
        cJSON_AddStringToObject(o, "name", list[i].name);
        cJSON_AddItemToArray(arr, o);
    }
    free(list);
    respond_json(res, 200, arr);
    return true;
}

/* GET /admin/cluster: membership, leader, generation and range table.
 * POST /admin/join {addr, port}: dial the seed peer and merge meshes. */
static bool handle_admin_cluster(const zdb_http_request *req,
                                 zdb_http_response *res)
{
    if (strcmp(req->method, "GET") == 0) {
        if (!require_admin_auth(req, res)) {
            return true;
        }
        if (!g_cluster) {
            respond_error(res, 400, "clustering disabled"
                                    " (start with -n <peer_port>)");
            return true;
        }
        cJSON *o = cJSON_CreateObject();
        cJSON_AddStringToObject(o, "node_id", zdb_cluster_self_id(g_cluster));
        const char *leader = zdb_cluster_leader(g_cluster);
        cJSON_AddStringToObject(o, "leader", leader ? leader : "none");
        cJSON_AddBoolToObject(o, "is_leader",
                              zdb_cluster_is_leader(g_cluster));
        /* stage 6e: live/target structure versions, rebalance lock state
         * and per-node compliance for observability */
        long long tgen = zdb_cluster_target_generation(g_cluster);
        cJSON_AddNumberToObject(o, "generation",
                                (double)zdb_cluster_generation(g_cluster));
        cJSON_AddNumberToObject(o, "live_version",
                                (double)zdb_cluster_generation(g_cluster));
        cJSON_AddNumberToObject(o, "target_version", (double)tgen);
        cJSON_AddBoolToObject(o, "rebalance_in_progress", tgen > 0);
        char *lockjson =
            zdb_setting_get(g_ctx.config, "cluster.rebalance_lock");
        if (lockjson) {
            cJSON *jl = cJSON_Parse(lockjson);
            free(lockjson);
            const cJSON *jln = jl ? cJSON_GetObjectItemCaseSensitive(
                                        jl, "node")
                                  : NULL;
            cJSON_AddStringToObject(o, "rebalance_lock",
                                    cJSON_IsString(jln) && jln->valuestring
                                        ? jln->valuestring
                                        : "held");
            cJSON_Delete(jl);
        } else {
            cJSON_AddNullToObject(o, "rebalance_lock");
        }

        zdb_peer_info peers[64];
        size_t n = zdb_cluster_peers(g_cluster, peers, 64);
        cJSON *arr = cJSON_AddArrayToObject(o, "nodes");
        for (size_t i = 0; arr && i < n; i++) {
            cJSON *p = cJSON_CreateObject();
            cJSON_AddStringToObject(p, "id", peers[i].id);
            cJSON_AddStringToObject(p, "addr", peers[i].addr);
            cJSON_AddNumberToObject(p, "port", peers[i].port);
            cJSON_AddBoolToObject(p, "online", peers[i].online);
            if (peers[i].compliant_gen > 0) {
                cJSON_AddNumberToObject(p, "compliant",
                                        (double)peers[i].compliant_gen);
            }
            cJSON_AddItemToArray(arr, p);
        }

        zdb_range_info tranges[64];
        size_t nt =
            zdb_cluster_target_ranges(g_cluster, tranges, 64);
        cJSON *tarr = cJSON_AddArrayToObject(o, "target_ranges");
        for (size_t i = 0; tarr && i < nt; i++) {
            cJSON *r = cJSON_CreateObject();
            cJSON_AddStringToObject(r, "owner", tranges[i].node_id);
            char span[80];
            snprintf(span, sizeof(span), "%.8s..%.8s", tranges[i].start,
                     tranges[i].end);
            cJSON_AddStringToObject(r, "hash_span", span);
            cJSON_AddItemToArray(tarr, r);
        }

        zdb_range_info ranges[64];
        size_t nr = zdb_cluster_ranges(g_cluster, ranges, 64);
        cJSON *rarr = cJSON_AddArrayToObject(o, "ranges");
        for (size_t i = 0; rarr && i < nr; i++) {
            cJSON *r = cJSON_CreateObject();
            cJSON_AddStringToObject(r, "owner", ranges[i].node_id);
            char span[80];
            snprintf(span, sizeof(span), "%.8s..%.8s", ranges[i].start,
                     ranges[i].end);
            cJSON_AddStringToObject(r, "hash_span", span);
            cJSON_AddItemToArray(rarr, r);
        }

        respond_json(res, 200, o);
        return true;
    }
    respond_error(res, 405, "method not allowed");
    return true;
}


static bool catchup_from_holder(const char key[33], const char *partition,
                                const char *keyspace)
{
    char holders[64][ZDB_NODE_ID_MAX];
    size_t count = zdb_cluster_holders(g_cluster, key, holders, 64);
    zdb_peer_info peers[64];
    size_t npeers = zdb_cluster_peers(g_cluster, peers, 64);
    for (size_t i = 0; i < count; i++) {
        if (strcmp(holders[i], zdb_cluster_self_id(g_cluster)) == 0) {
            continue;
        }
        for (size_t p = 0; p < npeers; p++) {
            if (strcmp(peers[p].id, holders[i]) == 0 && peers[p].addr[0] &&
                peers[p].port > 0 &&
                zdb_repl_catchup_required(g_repl, peers[p].addr,
                                           peers[p].port, partition,
                                           keyspace)) {
                return true;
            }
        }
    }
    return false;
}



static bool handle_admin_join(const zdb_http_request *req,
                              zdb_http_response *res)
{
    if (strcmp(req->method, "POST") != 0) {
        respond_error(res, 405, "method not allowed");
        return true;
    }
    if (!require_admin_auth(req, res)) {
        return true;
    }
    if (!g_cluster) {
        respond_error(res, 400, "clustering disabled"
                                " (start with -n <peer_port>)");
        return true;
    }
    cJSON *body = NULL;
    if (!body_json(req, &body) || !cJSON_IsObject(body)) {
        respond_error(res, 400, "JSON body required: {addr, port}");
        cJSON_Delete(body);
        return true;
    }
    const cJSON *addr = cJSON_GetObjectItemCaseSensitive(body, "addr");
    const cJSON *port = cJSON_GetObjectItemCaseSensitive(body, "port");
    if (!cJSON_IsString(addr) || !addr->valuestring ||
        !cJSON_IsNumber(port) || port->valueint <= 0 ||
        port->valueint > 65535) {
        respond_error(res, 400, "addr (string) and port (1-65535)"
                                " required");
        cJSON_Delete(body);
        return true;
    }
    int rc = zdb_cluster_join(g_cluster, addr->valuestring,
                              port->valueint);
    char seed_addr[ZDB_ADDR_MAX];
    int seed_port = port->valueint;
    snprintf(seed_addr, sizeof(seed_addr), "%s", addr->valuestring);
    cJSON_Delete(body);
    if (rc == -2) {
        respond_error(res, 409, "rebalance in progress: one node may"
                                " join at a time; retry later");
        return true;
    }
    if (rc != 0) {
        respond_error(res, 502, "cannot reach seed peer");
        return true;
    }
    respond_json(res, 200, NULL);

    /* --- stage 6e: run the full rebalance flow for this node -------- */
    /* Snapshot the reserved config shards first so lists/auth/settings
     * work here, then wait for the leader to publish the target and
     * snapshot every data shard the wave assigns to us. While that runs,
     * maintainer auto-compliance is disabled so we cannot report
     * compliant before our data has landed; once synced we mark
     * ourselves compliant and the leader promotes automatically. */
    bool synced = true;
    char fail_detail[128] = "";
    zdb_cluster_set_auto_compliant(g_cluster, false);

    const char *sys_ks[8];
    size_t nsys = zdb_config_system_keyspaces(sys_ks, 8);
    for (size_t i = 0; i < nsys && synced; i++) {
        if (!zdb_repl_catchup(g_repl, seed_addr, seed_port,
                              ZDB_SYSTEM_DB, sys_ks[i])) {
            synced = false;
            snprintf(fail_detail, sizeof(fail_detail),
                     "config sync failed for %s", sys_ks[i]);
        }
    }

    bool pending = false;
    for (int i = 0; i < 100 && !pending; i++) {
        pending = zdb_cluster_target_generation(g_cluster) > 0;
        if (!pending) {
            usleep(100 * 1000);
        }
    }
    if (synced && pending && zdb_cluster_needs_sync(g_cluster)) {
        size_t nks = 0;
        zdb_keyspace_info *kss =
            zdb_keyspace_list(g_ctx.config, &nks);
        for (size_t i = 0; kss && i < nks && synced; i++) {
            char key[33];
            char path[1024];
            if (!zdb_shard_path(g_ctx.engine, kss[i].partition,
                                kss[i].name, path, sizeof(path), key) ||
                zdb_config_is_system_key(g_ctx.config, key)) {
                continue;
            }
            const char *towner =
                zdb_cluster_target_owner(g_cluster, key);
            if (!towner ||
                strcmp(towner, zdb_cluster_self_id(g_cluster)) != 0) {
                continue;
            }
            if (!catchup_from_holder(key, kss[i].partition, kss[i].name) ||
                !zdb_shard_validate(g_ctx.engine, kss[i].partition,
                                    kss[i].name)) {
                synced = false;
                snprintf(fail_detail, sizeof(fail_detail),
                         "sync failed for %s/%s", kss[i].partition,
                         kss[i].name);
            }
        }
        free(kss);
    }
    if (synced) {
        zdb_cluster_mark_compliant(g_cluster);
    } else {
        /* roll the cluster back to the live structure */
        zdb_cluster_request_void(g_cluster);
    }
    zdb_cluster_set_auto_compliant(g_cluster, true);

    if (!synced) {
        respond_error(res, 500, fail_detail[0] ? fail_detail
                                               : "join sync failed;"
                                                 " rolled back");
        return true;
    }
    free(res->body);
    res->body = zdb_http_body_printf(&res->body_len,
                                     "{\"joined\":true,\"synced\":true}");
    return true;
}

static bool handle_settings(const zdb_http_request *req,
                            zdb_http_response *res)
{
    if (!require_admin_auth(req, res)) {
        return true;
    }
    const char *prefix = "/admin/settings";
    size_t plen = strlen(prefix);
    const char *rest = req->path + plen;   /* "" or "/<name>" */

    if (strcmp(req->method, "GET") == 0) {
        if (!*rest) {
            /* list all settings */
            size_t n = 0;
            char **names = zdb_setting_list(g_ctx.config, &n);
            cJSON *arr = cJSON_CreateArray();
            for (size_t i = 0; names && i < n; i++) {
                cJSON *o = cJSON_CreateObject();
                cJSON_AddStringToObject(o, "name", names[i]);
                char *value = zdb_setting_get(g_ctx.config, names[i]);
                cJSON *parsed = value ? cJSON_Parse(value) : NULL;
                if (parsed) {
                    cJSON_AddItemToObject(o, "value", parsed);
                } else {
                    cJSON_AddStringToObject(o, "value",
                                            value ? value : "null");
                }
                free(value);
                cJSON_AddItemToArray(arr, o);
            }
            zdb_free_strings(names);
            respond_json(res, 200, arr);
            return true;
        }
        rest++;   /* skip '/' */
        char *value = zdb_setting_get(g_ctx.config, rest);
        if (!value) {
            respond_error(res, 404, "unknown setting");
            return true;
        }
        res->status = 200;
        res->content_type = "application/json";
        res->body = value;
        res->body_len = strlen(value);
        return true;
    }

    if (strcmp(req->method, "POST") == 0 || strcmp(req->method, "PUT") == 0) {
        if (!*rest) {
            respond_error(res, 400,
                          "expected /admin/settings/<name> with JSON body");
            return true;
        }
        rest++;
        /* body must be valid JSON (any type) */
        cJSON *check = NULL;
        if (!body_json(req, &check)) {
            respond_error(res, 400, "body must be valid JSON");
            return true;
        }
        char *printed = cJSON_PrintUnformatted(check);
        cJSON_Delete(check);
        if (!zdb_setting_set(g_ctx.config, rest, printed ? printed : "null")) {
            free(printed);
            respond_error(res, 500, "setting store failed");
            return true;
        }
        free(printed);
        res->status = 200;
        res->content_type = "application/json";
        res->body = zdb_http_body_printf(&res->body_len,
                                         "{\"set\":\"%s\"}", rest);
        return true;
    }

    if (strcmp(req->method, "DELETE") == 0) {
        if (!*rest) {
            respond_error(res, 400, "expected /admin/settings/<name>");
            return true;
        }
        if (!zdb_setting_delete(g_ctx.config, rest + 1)) {
            respond_error(res, 404, "unknown setting");
            return true;
        }
        res->status = 200;
        res->content_type = "application/json";
        res->body = zdb_http_body_printf(&res->body_len, "{\"deleted\":true}");
        return true;
    }

    respond_error(res, 405, "method not allowed");
    return true;
}

static bool handle_status(const zdb_http_request *req,
                          zdb_http_response *res)
{
    (void)req;
    cJSON *o = cJSON_CreateObject();
    cJSON_AddStringToObject(o, "service", "zestydb");
    cJSON_AddStringToObject(o, "version", "0.1.0");
    cJSON_AddBoolToObject(o, "clustered", false);
    respond_json(res, 200, o);
    return true;
}

/* ------------------------------------------------------------------ */

bool zdb_api_register(zdb_http_server *srv, zdb_engine *engine,
                      zdb_config *config)
{
    if (!srv || !engine || !config) {
        return false;
    }
    g_ctx.engine = engine;
    g_ctx.config = config;
    zdb_config_set_replicator(config, g_repl ? api_replicate_config : NULL,
                              g_repl);

    bool ok = true;
    ok &= zdb_http_add_handler(srv, "PUT", "/data/", handle_data_put);
    ok &= zdb_http_add_handler(srv, "DELETE", "/data/", handle_data_delete);
    /* GET: id lookups and .../all|ids (collect checks the last segment) */
    ok &= zdb_http_add_handler(srv, "GET", "/data/", handle_data_collect);
    ok &= zdb_http_add_handler(srv, "POST", "/data/", handle_data_collect);
    ok &= zdb_http_add_handler(srv, "GET", "/admin/databases",
                               handle_admin_databases);
    ok &= zdb_http_add_handler(srv, "POST", "/admin/databases",
                               handle_admin_databases);
    ok &= zdb_http_add_handler(srv, "GET", "/admin/groups",
                               handle_admin_groups);
    ok &= zdb_http_add_handler(srv, "POST", "/admin/groups",
                               handle_admin_groups);
    ok &= zdb_http_add_handler(srv, "GET", "/admin/users",
                               handle_admin_users);
    ok &= zdb_http_add_handler(srv, "POST", "/admin/users",
                               handle_admin_users);
    ok &= zdb_http_add_handler(srv, "GET", "/admin/partitions",
                               handle_admin_partitions);
    ok &= zdb_http_add_handler(srv, "POST", "/admin/partitions",
                               handle_admin_partitions);
    ok &= zdb_http_add_handler(srv, "GET", "/admin/keyspaces",
                               handle_admin_keyspaces);
    ok &= zdb_http_add_handler(srv, "GET", "/admin/cluster",
                               handle_admin_cluster);
    ok &= zdb_http_add_handler(srv, "POST", "/admin/join",
                               handle_admin_join);
    /* deletes use the trailing-slash form so list routes stay intact;
     * longest-prefix matching sends /admin/users/<name> here */
    ok &= zdb_http_add_handler(srv, "DELETE", "/admin/",
                               handle_admin_delete);
    ok &= zdb_http_add_handler(srv, "GET", "/admin/settings",
                               handle_settings);
    ok &= zdb_http_add_handler(srv, "POST", "/admin/settings",
                               handle_settings);
    ok &= zdb_http_add_handler(srv, "PUT", "/admin/settings",
                               handle_settings);
    ok &= zdb_http_add_handler(srv, "DELETE", "/admin/settings",
                               handle_settings);
    ok &= zdb_http_add_handler(srv, "GET", "/status", handle_status);
    return ok;
}
