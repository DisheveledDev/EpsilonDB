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
#include "../engine/random.h"
#include "../engine/zesty_analytics.h"
#include "../engine/zesty_benchmark.h"

/* ------------------------------------------------------------------ */
/* helpers                                                             */

typedef struct {
    zdb_engine *engine;
    zdb_config *config;
} api_ctx;

static api_ctx g_ctx;   /* handlers receive no user pointer; single server */
static zdb_cluster *g_cluster;   /* may be NULL: clustering disabled */
static zdb_repl *g_repl;         /* may be NULL: replication disabled */
static zdb_analytics *g_analytics; /* may be NULL: analytics disabled */
static pthread_mutex_t g_data_put_lock = PTHREAD_MUTEX_INITIALIZER;

/* Monotonic microseconds for latency measurement. */
static long long api_now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000LL + ts.tv_nsec / 1000;
}

/* ------------------------------------------------------------------ */
/* admin console sessions                                              */

#define ZDB_SESSION_CAPACITY 256
#define ZDB_SESSION_TTL      43200   /* seconds: 12 hours */

typedef struct {
    char token[65];
    char username[128];
    uint64_t groups;
    long long expires;
    bool used;
} zdb_session;

static zdb_session g_sessions[ZDB_SESSION_CAPACITY];
static pthread_mutex_t g_session_lock = PTHREAD_MUTEX_INITIALIZER;

static long long session_now(void)
{
    return (long long)time(NULL);
}

static bool session_lookup(const char *token, char username[128],
                           uint64_t *groups)
{
    if (!token) {
        return false;
    }
    long long now = session_now();
    pthread_mutex_lock(&g_session_lock);
    for (int i = 0; i < ZDB_SESSION_CAPACITY; i++) {
        zdb_session *s = &g_sessions[i];
        if (s->used && strcmp(s->token, token) == 0) {
            if (s->expires <= now) {
                s->used = false;
                pthread_mutex_unlock(&g_session_lock);
                return false;
            }
            snprintf(username, 128, "%s", s->username);
            *groups = s->groups;
            pthread_mutex_unlock(&g_session_lock);
            return true;
        }
    }
    pthread_mutex_unlock(&g_session_lock);
    return false;
}

static void session_create(const char *username, uint64_t groups,
                           char token_out[65])
{
    char token[65];
    zdb_random_hex(token, 64);

    pthread_mutex_lock(&g_session_lock);
    zdb_session *slot = NULL;
    for (int i = 0; i < ZDB_SESSION_CAPACITY; i++) {
        if (!g_sessions[i].used || g_sessions[i].expires <= session_now()) {
            slot = &g_sessions[i];
            break;
        }
    }
    if (!slot) {
        slot = &g_sessions[0];
    }
    memset(slot, 0, sizeof(*slot));
    snprintf(slot->token, sizeof(slot->token), "%s", token);
    snprintf(slot->username, sizeof(slot->username), "%s", username);
    slot->groups = groups;
    slot->expires = session_now() + ZDB_SESSION_TTL;
    slot->used = true;
    pthread_mutex_unlock(&g_session_lock);

    snprintf(token_out, 65, "%s", token);
}

static void session_destroy(const char *token)
{
    if (!token) {
        return;
    }
    pthread_mutex_lock(&g_session_lock);
    for (int i = 0; i < ZDB_SESSION_CAPACITY; i++) {
        if (g_sessions[i].used && strcmp(g_sessions[i].token, token) == 0) {
            g_sessions[i].used = false;
            break;
        }
    }
    pthread_mutex_unlock(&g_session_lock);
}

void zdb_api_set_cluster(zdb_cluster *cluster)
{
    g_cluster = cluster;
}

static bool api_apply_change_impl(void *ud, const cJSON *change)
{
    (void)ud;
    const cJSON *operation = cJSON_GetObjectItemCaseSensitive(change, "op");
    const cJSON *database = cJSON_GetObjectItemCaseSensitive(change, "db");
    const cJSON *partition =
        cJSON_GetObjectItemCaseSensitive(change, "partition");
    const cJSON *keyspace =
        cJSON_GetObjectItemCaseSensitive(change, "keyspace");
    const cJSON *id = cJSON_GetObjectItemCaseSensitive(change, "id");
    const cJSON *timestamp = cJSON_GetObjectItemCaseSensitive(change, "ts");
    const cJSON *origin_item =
        cJSON_GetObjectItemCaseSensitive(change, "origin");
    const char *origin = cJSON_IsString(origin_item)
                             ? origin_item->valuestring
                             : "";
    if (!cJSON_IsString(operation) || !cJSON_IsString(database) ||
        !cJSON_IsString(partition) || !cJSON_IsString(keyspace) ||
        !cJSON_IsString(id) || !cJSON_IsNumber(timestamp)) {
        return false;
    }
    long long modified = (long long)timestamp->valuedouble;
    bool ok = false;
    if (strcmp(operation->valuestring, "put") == 0) {
        const cJSON *value = cJSON_GetObjectItemCaseSensitive(change, "value");
        const cJSON *ttl = cJSON_GetObjectItemCaseSensitive(change, "ttl_abs");
        char *encoded = cJSON_IsObject(value)
                            ? cJSON_PrintUnformatted(value)
                            : NULL;
        if (!encoded) {
            return false;
        }
        long long absolute_ttl = cJSON_IsNumber(ttl)
                                     ? (long long)ttl->valuedouble
                                     : -1;
        ok = zdb_replica_put_origin(
            g_ctx.engine, partition->valuestring, keyspace->valuestring,
            id->valuestring, encoded, absolute_ttl, modified, origin);
        free(encoded);
        if (ok && strcmp(database->valuestring, ZDB_SYSTEM_DB) != 0) {
            zdb_partition_ensure(g_ctx.config, database->valuestring,
                                 partition->valuestring,
                                 keyspace->valuestring, NULL);
        }
    } else if (strcmp(operation->valuestring, "delete") == 0) {
        ok = zdb_replica_delete_origin(
            g_ctx.engine, partition->valuestring, keyspace->valuestring,
            id->valuestring, modified, origin);
    }
    return ok;
}
static cJSON *api_read_request(void *ud, const cJSON *request)
{
    (void)ud;
    const cJSON *query = cJSON_GetObjectItemCaseSensitive(request, "q");
    const cJSON *partition =
        cJSON_GetObjectItemCaseSensitive(request, "partition");
    const cJSON *keyspace =
        cJSON_GetObjectItemCaseSensitive(request, "keyspace");
    const cJSON *filters =
        cJSON_GetObjectItemCaseSensitive(request, "filters");
    if (!cJSON_IsString(query) || !cJSON_IsString(partition) ||
        !cJSON_IsString(keyspace) || !zdb_filters_valid(filters)) {
        return NULL;
    }
    cJSON *out = cJSON_CreateObject();
    if (!out) {
        return NULL;
    }
    if (strcmp(query->valuestring, "get") == 0) {
        const cJSON *id = cJSON_GetObjectItemCaseSensitive(request, "id");
        long long timestamp = 0;
        cJSON *document = cJSON_IsString(id)
                              ? zdb_get_ts(g_ctx.engine,
                                           partition->valuestring,
                                           keyspace->valuestring,
                                           id->valuestring, &timestamp)
                              : NULL;
        if (document) {
            cJSON *row = cJSON_CreateObject();
            cJSON_AddStringToObject(row, "id", id->valuestring);
            cJSON_AddNumberToObject(row, "timestamp", (double)timestamp);
            cJSON_AddItemToObject(row, "value", document);
            cJSON_AddItemToObject(out, "row", row);
        } else {
            cJSON_AddNullToObject(out, "row");
        }
    } else if (strcmp(query->valuestring, "all_ts") == 0 ||
               strcmp(query->valuestring, "query_ts") == 0) {
        cJSON *rows = strcmp(query->valuestring, "all_ts") == 0
                          ? zdb_all_ts(g_ctx.engine,
                                       partition->valuestring,
                                       keyspace->valuestring, filters)
                          : zdb_query_ts(g_ctx.engine,
                                         partition->valuestring,
                                         keyspace->valuestring, filters);
        cJSON_AddItemToObject(out, "rows", rows ? rows : cJSON_CreateArray());
    } else if (strcmp(query->valuestring, "ids") == 0) {
        size_t count = 0;
        char **ids = zdb_ids(g_ctx.engine, partition->valuestring,
                             keyspace->valuestring, filters, &count);
        cJSON *array = cJSON_AddArrayToObject(out, "ids");
        for (size_t i = 0; array && ids && i < count; i++) {
            cJSON_AddItemToArray(array, cJSON_CreateString(ids[i]));
        }
        zdb_free_strings(ids);
    } else {
        cJSON_Delete(out);
        return NULL;
    }
    return out;
}
static bool api_replicate_config(void *ctx, const char *keyspace,
                                 const char *id, const char *json);

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

/* Writes one node's analytics snapshot into the __system__ store (and out
 * to the cluster through the replication service when clustering is on). */
static bool api_analytics_flush(void *ctx, const char *node_id,
                                const char *json, long long ttl_abs)
{
    (void)ctx;
    if (g_repl) {
        cJSON *change = cJSON_CreateObject();
        if (!change) {
            return false;
        }
        cJSON_AddStringToObject(change, "op", "put");
        cJSON_AddStringToObject(change, "db", ZDB_SYSTEM_DB);
        cJSON_AddStringToObject(change, "partition", ZDB_SYSTEM_DB);
        cJSON_AddStringToObject(change, "keyspace", ZDB_ANALYTICS_KEYSPACE);
        cJSON_AddStringToObject(change, "id", node_id);
        cJSON_AddNumberToObject(change, "ts", (double)time(NULL));
        cJSON_AddNumberToObject(change, "ttl_abs", (double)ttl_abs);
        cJSON *value = cJSON_Parse(json);
        if (!value) {
            cJSON_Delete(change);
            return false;
        }
        cJSON_AddItemToObject(change, "value", value);
        char *encoded = cJSON_PrintUnformatted(change);
        cJSON_Delete(change);
        if (!encoded) {
            return false;
        }
        zdb_repl_status status = zdb_repl_write(g_repl, ZDB_SYSTEM_DB,
                                                encoded);
        free(encoded);
        return status == ZDB_REPL_OK;
    }
    long long now = (long long)time(NULL);
    long long ttl_rel = ttl_abs > now ? ttl_abs - now : 0;
    return zdb_put(g_ctx.engine, ZDB_SYSTEM_DB, ZDB_ANALYTICS_KEYSPACE,
                   node_id, json, ttl_rel);
}

/* Extra cluster metrics merged into each node's analytics snapshot. */
static cJSON *api_analytics_cluster_metrics(void *ctx)
{
    (void)ctx;
    cJSON *o = cJSON_CreateObject();
    if (!o) {
        return NULL;
    }
    cJSON_AddNumberToObject(o, "pending_changes",
                            (double)(g_repl ? zdb_repl_pending_total(g_repl)
                                            : 0));
    return o;
}

void zdb_api_analytics_start(zdb_config *cfg, const char *node_id)
{
    if (!cfg || g_analytics) {
        return;
    }
    g_analytics = zdb_analytics_start(cfg, node_id, api_analytics_flush, NULL);
    zdb_analytics_set_cluster_metrics(g_analytics,
                                      api_analytics_cluster_metrics, NULL);
}

void zdb_api_analytics_stop(void)
{
    zdb_analytics_stop(g_analytics);
    g_analytics = NULL;
}


static bool api_replicate_config(void *ctx, const char *keyspace,
                                 const char *id, const char *json)
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
        char session_username[128];
        if (session_lookup(token, session_username, &groups)) {
            /* authenticated via an admin console session token */
        } else {
            zdb_user_info user;
            if (zdb_user_get(g_ctx.config, token, &user)) {
                groups = user.groups;
            } else {
                *ok = false;
            }
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
    if (strstr(req->query, "filter=")) {
        respond_error(res, 400, "filters apply to reads, not stored records");
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
    pthread_mutex_lock(&g_data_put_lock);
    cJSON *existing = zdb_get(g_ctx.engine, part, ks, id);
    bool is_update = existing != NULL;
    zdb_permission permission = existing ? ZDB_PERM_UPDATE : ZDB_PERM_CREATE;
    cJSON_Delete(existing);
    if (!authorize_partition(g_ctx.config, db, part, groups, permission, res)) {
        pthread_mutex_unlock(&g_data_put_lock);
        free(json);
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
            cJSON_AddItemToObject(change, "value", cJSON_Parse(json));
            cJSON_AddNumberToObject(change, "ttl_abs",
                                    ttl >= 0 ? (double)(ts + ttl) : -1);
            cJSON_AddNumberToObject(change, "ts", (double)ts);
            char *change_json = cJSON_PrintUnformatted(change);
            cJSON_Delete(change);
            if (change_json) {
                zdb_repl_status status = zdb_repl_write(g_repl, db,
                                                        change_json);
                free(change_json);
                if (status == ZDB_REPL_QUORUM_LOST) {
                    pthread_mutex_unlock(&g_data_put_lock);
                    free(json);
                    respond_error(res, 503,
                                  "quorum unavailable: write rejected");
                    return true;
                }
                ok = status == ZDB_REPL_OK;
            }
        }
    } else {
        ok = zdb_put(g_ctx.engine, part, ks, id, json, ttl);
    }
    if (ok) {
        zdb_partition_ensure(g_ctx.config, db, part, ks, NULL);
    }
    pthread_mutex_unlock(&g_data_put_lock);
    free(json);

    if (g_analytics && ok) {
        zdb_analytics_record_write(g_analytics, part, ks, is_update,
                                   api_now_us() - started);
    }

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

    long long started = api_now_us();
    cJSON *doc = g_repl
                     ? zdb_repl_read_get(g_repl, db, part, ks, id)
                     : zdb_get(g_ctx.engine, part, ks, id);
    long long elapsed = api_now_us() - started;
    if (g_analytics) {
        zdb_analytics_record_read(g_analytics, part, ks, elapsed);
    }
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
    if (g_analytics) {
        zdb_analytics_record_delete(g_analytics, part, ks,
                                    api_now_us() - started);
    }
    res->status = 200;
    res->content_type = "application/json";
    res->body = zdb_http_body_printf(&res->body_len,
                                     "{\"status\":\"deleted\",\"id\":\"%s\"}",
                                     id);
    return true;
}

/* Lists the keyspaces that have been written under a database/partition. */
static size_t partition_keyspaces(const char *db, const char *partition,
                                  char out[][128], size_t cap)
{
    size_t total = 0;
    zdb_keyspace_info *list = zdb_keyspace_list(g_ctx.config, &total);
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
                          ? zdb_repl_read_query_meta(g_repl, db, part,
                                                     keyspaces[i], filters)
                          : zdb_query_ts(g_ctx.engine, part, keyspaces[i],
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

static bool handle_data_collect(const zdb_http_request *req,
                                zdb_http_response *res)
{
    const char *path = req->path;
    if (strncmp(path, "/data/", 6) != 0) {
        respond_error(res, 400, "expected /data/...");
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
        respond_error(res, 401, "unknown user");
        return true;
    }
    if (!authorize_partition(g_ctx.config, db, part, groups, ZDB_PERM_READ,
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
        if (!zdb_filters_valid(filters)) {
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
                         ? zdb_repl_read_ids(g_repl, db, part, ks, filters,
                                             &count)
                         : zdb_ids(g_ctx.engine, part, ks, filters, &count);
        cJSON *array = cJSON_CreateArray();
        for (size_t i = 0; array && ids && i < count; i++) {
            cJSON_AddItemToArray(array, cJSON_CreateString(ids[i]));
        }
        zdb_free_strings(ids);
        respond_json(res, 200, array);
    } else if (keyspace_omitted) {
        cJSON *documents = partition_wide_query(db, part, filters);
        respond_json(res, 200,
                     documents ? documents : cJSON_CreateArray());
    } else {
        /* all and query share identical engine semantics (a filter may be
         * empty); both return the record key as a leading "id" field. */
        cJSON *meta = g_repl
                          ? zdb_repl_read_query_meta(g_repl, db, part, ks,
                                                     filters)
                          : zdb_query_ts(g_ctx.engine, part, ks, filters);
        cJSON *documents = meta ? flatten_meta_rows(meta) : NULL;
        if (meta) {
            cJSON_Delete(meta);
        }
        respond_json(res, 200,
                     documents ? documents : cJSON_CreateArray());
    }
    if (g_analytics) {
        zdb_analytics_record_query(g_analytics, part, ks, filter_keys, nkeys,
                                   api_now_us() - started);
    }
    cJSON_Delete(body);
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
            cJSON_AddStringToObject(o, "groups", bits);
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
        const cJSON *password = cJSON_GetObjectItemCaseSensitive(body,
                                                                 "password");
        uint64_t group_mask = 0;
        if (!cJSON_IsString(name) || !json_u64_value(groups, &group_mask)) {
            respond_error(res, 400, "name and groups required");
            cJSON_Delete(body);
            return true;
        }
        bool ok = zdb_user_create(g_ctx.config, name->valuestring,
                                  group_mask);
        if (ok && cJSON_IsString(password) && password->valuestring &&
            *password->valuestring) {
            ok = zdb_user_set_password(g_ctx.config, name->valuestring,
                                       password->valuestring);
        }
        cJSON_Delete(body);
        if (!ok) {
            respond_error(res, 409, "create failed (duplicate?)");
            return true;
        }
        respond_json(res, 201, NULL);
        res->body = zdb_http_body_printf(&res->body_len, "{\"created\":true}");
        return true;
    }
    if (strcmp(req->method, "PUT") == 0) {
        cJSON *body = NULL;
        if (!body_json(req, &body) || !cJSON_IsObject(body)) {
            respond_error(res, 400, "JSON object required");
            cJSON_Delete(body);
            return true;
        }
        const cJSON *name = cJSON_GetObjectItemCaseSensitive(body, "name");
        const cJSON *groups = cJSON_GetObjectItemCaseSensitive(body,
                                                               "groups");
        const cJSON *password = cJSON_GetObjectItemCaseSensitive(body,
                                                                 "password");
        uint64_t group_mask = 0;
        if (!cJSON_IsString(name) || !json_u64_value(groups, &group_mask)) {
            respond_error(res, 400, "name and groups required");
            cJSON_Delete(body);
            return true;
        }
        bool ok = zdb_user_set_groups(g_ctx.config, name->valuestring,
                                      group_mask);
        if (ok && cJSON_IsString(password) && password->valuestring &&
            *password->valuestring) {
            ok = zdb_user_set_password(g_ctx.config, name->valuestring,
                                       password->valuestring);
        }
        cJSON_Delete(body);
        if (!ok) {
            respond_error(res, 404, "user not found");
            return true;
        }
        respond_json(res, 200, NULL);
        res->body = zdb_http_body_printf(&res->body_len, "{\"updated\":true}");
        return true;
    }
    respond_error(res, 405, "method not allowed");
    return true;
}

/* Parses optional shard-tuning fields from a partition request body. Returns
 * true when at least one tuning field was present. */
static bool partition_settings_from_json(const cJSON *body,
                                        zdb_shard_settings *out)
{
    zdb_shard_settings_default(out);
    bool present = false;
    const cJSON *cs = cJSON_GetObjectItemCaseSensitive(body, "cache_size");
    if (cJSON_IsNumber(cs)) {
        out->cache_size = (long long)cs->valuedouble;
        present = true;
    }
    const cJSON *jm = cJSON_GetObjectItemCaseSensitive(body, "journal_mode");
    if (cJSON_IsString(jm) && jm->valuestring) {
        snprintf(out->journal_mode, sizeof(out->journal_mode), "%s",
                 jm->valuestring);
        present = true;
    }
    const cJSON *vs = cJSON_GetObjectItemCaseSensitive(body, "vacuum_seconds");
    if (cJSON_IsNumber(vs)) {
        out->vacuum_seconds = (long long)vs->valuedouble;
        present = true;
    }
    const cJSON *rs = cJSON_GetObjectItemCaseSensitive(body, "reindex_seconds");
    if (cJSON_IsNumber(rs)) {
        out->reindex_seconds = (long long)rs->valuedouble;
        present = true;
    }
    return present;
}

static void partition_add_settings_json(cJSON *o,
                                        const zdb_partition_info *p)
{
    cJSON_AddNumberToObject(o, "cache_size", (double)p->cache_size);
    cJSON_AddStringToObject(o, "journal_mode", p->journal_mode);
    cJSON_AddNumberToObject(o, "vacuum_seconds", (double)p->vacuum_seconds);
    cJSON_AddNumberToObject(o, "reindex_seconds", (double)p->reindex_seconds);
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
                    cJSON_AddStringToObject(o, "read_mask", b);
                    snprintf(b, sizeof(b), "%llu",
                             (unsigned long long)parts[i].update_mask);
                    cJSON_AddStringToObject(o, "update_mask", b);
                    snprintf(b, sizeof(b), "%llu",
                             (unsigned long long)parts[i].create_mask);
                    cJSON_AddStringToObject(o, "create_mask", b);
                    snprintf(b, sizeof(b), "%llu",
                             (unsigned long long)parts[i].delete_mask);
                    cJSON_AddStringToObject(o, "delete_mask", b);
                    partition_add_settings_json(o, &parts[i]);
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
            cJSON_AddStringToObject(o, "read_mask", b);
            snprintf(b, sizeof(b), "%llu",
                     (unsigned long long)list[i].update_mask);
            cJSON_AddStringToObject(o, "update_mask", b);
            snprintf(b, sizeof(b), "%llu",
                     (unsigned long long)list[i].create_mask);
            cJSON_AddStringToObject(o, "create_mask", b);
            snprintf(b, sizeof(b), "%llu",
                     (unsigned long long)list[i].delete_mask);
            cJSON_AddStringToObject(o, "delete_mask", b);
            partition_add_settings_json(o, &list[i]);
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
        if (ok) {
            zdb_shard_settings settings;
            if (partition_settings_from_json(body, &settings)) {
                zdb_partition_set_settings(g_ctx.config,
                                           database->valuestring,
                                           name->valuestring, &settings);
            }
        }
        cJSON_Delete(body);
        if (!ok) {
            respond_error(res, 409, "create failed (duplicate?)");
            return true;
        }
        respond_json(res, 201, NULL);
        res->body = zdb_http_body_printf(&res->body_len, "{\"created\":true}");
        return true;
    }
    if (strcmp(req->method, "PUT") == 0) {
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
        bool ok = zdb_partition_set_masks(g_ctx.config, database->valuestring,
                                          name->valuestring, create_mask,
                                          update_mask, read_mask,
                                          delete_mask);
        if (ok) {
            zdb_shard_settings settings;
            if (partition_settings_from_json(body, &settings)) {
                zdb_partition_set_settings(g_ctx.config,
                                           database->valuestring,
                                           name->valuestring, &settings);
            }
        }
        cJSON_Delete(body);
        if (!ok) {
            respond_error(res, 404, "partition not found");
            return true;
        }
        respond_json(res, 200, NULL);
        res->body = zdb_http_body_printf(&res->body_len, "{\"updated\":true}");
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

/* GET /admin/analytics: aggregated cluster-wide workload/performance. */
static bool handle_admin_analytics(const zdb_http_request *req,
                                   zdb_http_response *res)
{
    if (strcmp(req->method, "GET") != 0) {
        respond_error(res, 405, "method not allowed");
        return true;
    }
    if (!require_admin_auth(req, res)) {
        return true;
    }
    cJSON *report = zdb_analytics_report(g_analytics);
    respond_json(res, 200, report ? report : cJSON_CreateObject());
    return true;
}

/* POST /admin/benchmark: run a throwaway workload benchmark. */
static bool handle_admin_benchmark(const zdb_http_request *req,
                                   zdb_http_response *res)
{
    if (strcmp(req->method, "POST") != 0) {
        respond_error(res, 405, "method not allowed");
        return true;
    }
    if (!require_admin_auth(req, res)) {
        return true;
    }
    cJSON *body = NULL;
    if (req->body_len > 0 && !body_json(req, &body)) {
        respond_error(res, 400, "body must be valid JSON");
        return true;
    }
    int replication_factor = 1;
    long long cache_size = 0;
    const char *journal_mode = "TRUNCATE";
    int partitions = 10;
    int records = 100000;
    if (body && cJSON_IsObject(body)) {
        const cJSON *j;
        j = cJSON_GetObjectItemCaseSensitive(body, "replication_factor");
        if (cJSON_IsNumber(j)) replication_factor = (int)j->valuedouble;
        j = cJSON_GetObjectItemCaseSensitive(body, "cache_size");
        if (cJSON_IsNumber(j)) cache_size = (long long)j->valuedouble;
        j = cJSON_GetObjectItemCaseSensitive(body, "journal_mode");
        if (cJSON_IsString(j) && j->valuestring) journal_mode = j->valuestring;
        j = cJSON_GetObjectItemCaseSensitive(body, "partitions");
        if (cJSON_IsNumber(j)) partitions = (int)j->valuedouble;
        j = cJSON_GetObjectItemCaseSensitive(body, "records");
        if (cJSON_IsNumber(j)) records = (int)j->valuedouble;
    }
    cJSON_Delete(body);
    cJSON *report = zdb_benchmark_run(g_ctx.config, replication_factor,
                                      cache_size, journal_mode, partitions,
                                      records);
    respond_json(res, 200, report ? report : cJSON_CreateObject());
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


/* Removes every non-system shard file so a joining node starts fresh
 * (its local demo/scratch data is discarded in favour of the shared
 * cluster data). Config shards are left in place: the seed sync below
 * snapshots them over the local copies. */
static void wipe_local_shards(void)
{
    char keys[512][33];
    size_t n = zdb_engine_shard_keys(g_ctx.engine, keys, 512);
    for (size_t i = 0; i < n; i++) {
        if (zdb_config_is_system_key(g_ctx.config, keys[i])) {
            continue;
        }
        zdb_shard_gc(g_ctx.engine, keys[i]);
    }
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
    const cJSON *secret = cJSON_GetObjectItemCaseSensitive(body, "secret");
    if (!cJSON_IsString(addr) || !addr->valuestring ||
        !cJSON_IsNumber(port) || port->valueint <= 0 ||
        port->valueint > 65535) {
        respond_error(res, 400, "addr (string) and port (1-65535)"
                                " required");
        cJSON_Delete(body);
        return true;
    }
    /* Optional cluster secret: derive and enable mesh encryption before
     * dialling so the HELLO is authenticated. A wrong (or missing)
     * secret fails the handshake and the join is refused. */
    const char *secret_str =
        cJSON_IsString(secret) && secret->valuestring && *secret->valuestring
            ? secret->valuestring
            : NULL;
    uint8_t enc_key[32], mac_key[32];
    bool key_set = false;
    if (secret_str) {
        if (zdb_cluster_derive_keys(secret_str, enc_key, mac_key) != 0) {
            cJSON_Delete(body);
            respond_error(res, 400, "invalid cluster secret");
            return true;
        }
        zstp_set_mesh_key(enc_key, mac_key);
        key_set = true;
    }
    int rc = zdb_cluster_join(g_cluster, addr->valuestring,
                              port->valueint);
    char seed_addr[ZDB_ADDR_MAX];
    int seed_port = port->valueint;
    snprintf(seed_addr, sizeof(seed_addr), "%s", addr->valuestring);
    cJSON_Delete(body);
    if (rc == -2) {
        if (key_set) {
            zstp_set_mesh_key(NULL, NULL);
        }
        respond_error(res, 409, "rebalance in progress: one node may"
                                " join at a time; retry later");
        return true;
    }
    if (rc != 0) {
        if (key_set) {
            zstp_set_mesh_key(NULL, NULL);
        }
        respond_error(res, 502, "cannot reach seed peer (wrong secret?)");
        return true;
    }
    if (key_set) {
        zdb_cluster_persist_keys(zdb_engine_path(g_ctx.engine), enc_key,
                                 mac_key);
    }
    wipe_local_shards();
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
/* admin console authentication                                        */

static const char *bearer_token(const zdb_http_request *req, char out[256])
{
    const char *hdr = zdb_http_header(req, "Authorization");
    if (!hdr) {
        hdr = zdb_http_header(req, "authorization");
    }
    if (hdr) {
        const char *value = strncmp(hdr, "Bearer ", 7) == 0 ? hdr + 7 : hdr;
        if (snprintf(out, 256, "%s", value) < 256) {
            return out;
        }
    }
    return NULL;
}

static void respond_groups_json(zdb_http_response *res, int status,
                                const char *token, const char *username,
                                uint64_t groups)
{
    char group_bits[32];
    snprintf(group_bits, sizeof(group_bits), "%llu",
             (unsigned long long)groups);
    cJSON *o = cJSON_CreateObject();
    if (token) {
        cJSON_AddStringToObject(o, "token", token);
    }
    cJSON_AddStringToObject(o, "username", username);
    cJSON_AddRawToObject(o, "groups", group_bits);
    respond_json(res, status, o);
}

static bool handle_console_state(const zdb_http_request *req,
                                 zdb_http_response *res)
{
    bool setup_required = g_ctx.config && !zdb_admin_exists(g_ctx.config);
    bool authenticated = false;
    char username[128] = "";
    uint64_t groups = 0;
    char token[256];
    const char *presented = bearer_token(req, token);
    if (presented) {
        authenticated = session_lookup(presented, username, &groups);
    }
    size_t peers = 0;
    if (g_cluster) {
        zdb_peer_info info[64];
        peers = zdb_cluster_peers(g_cluster, info, 64);
    }
    cJSON *o = cJSON_CreateObject();
    cJSON_AddBoolToObject(o, "setup_required", setup_required);
    cJSON_AddBoolToObject(o, "authenticated", authenticated);
    cJSON_AddStringToObject(o, "username", authenticated ? username : "");
    cJSON_AddBoolToObject(o, "clustered", g_cluster != NULL);
    cJSON_AddNumberToObject(o, "peers", (double)peers);
    respond_json(res, 200, o);
    return true;
}

static bool handle_admin_login(const zdb_http_request *req,
                               zdb_http_response *res)
{
    if (strcmp(req->method, "POST") != 0) {
        respond_error(res, 405, "method not allowed");
        return true;
    }
    cJSON *body = NULL;
    if (!body_json(req, &body) || !cJSON_IsObject(body)) {
        respond_error(res, 400, "JSON object required");
        cJSON_Delete(body);
        return true;
    }
    const cJSON *username = cJSON_GetObjectItemCaseSensitive(body, "username");
    const cJSON *password = cJSON_GetObjectItemCaseSensitive(body, "password");
    if (!cJSON_IsString(username) || !cJSON_IsString(password) ||
        !zdb_user_verify_password(g_ctx.config, username->valuestring,
                                  password->valuestring)) {
        cJSON_Delete(body);
        respond_error(res, 401, "invalid credentials");
        return true;
    }
    zdb_user_info user;
    if (!zdb_user_get(g_ctx.config, username->valuestring, &user)) {
        cJSON_Delete(body);
        respond_error(res, 401, "invalid credentials");
        return true;
    }
    char token[65];
    session_create(user.name, user.groups, token);
    cJSON_Delete(body);
    respond_groups_json(res, 200, token, user.name, user.groups);
    return true;
}

/* ------------------------------------------------------------------ */
/* first-run demo data                                                 */

typedef struct {
    const char *partition;
    const char *keyspace;
    const char *id;
    const char *json;
} demo_record;

static const demo_record DEMO_RECORDS[] = {
    { "People", "employees", "e1001",
      "{\"name\":\"Alice Johnson\",\"title\":\"Software Engineer\","
      "\"department\":\"Engineering\",\"email\":\"alice@acme.example\","
      "\"salary\":95000,\"manager\":\"e1004\",\"active\":true}" },
    { "People", "employees", "e1002",
      "{\"name\":\"Bob Smith\",\"title\":\"Product Manager\","
      "\"department\":\"Product\",\"email\":\"bob@acme.example\","
      "\"salary\":88000,\"manager\":\"e1004\",\"active\":true}" },
    { "People", "employees", "e1003",
      "{\"name\":\"Carol Williams\",\"title\":\"Data Analyst\","
      "\"department\":\"Data\",\"email\":\"carol@acme.example\","
      "\"salary\":72000,\"manager\":\"e1005\",\"active\":true}" },
    { "People", "employees", "e1004",
      "{\"name\":\"Dave Brown\",\"title\":\"Engineering Director\","
      "\"department\":\"Engineering\",\"email\":\"dave@acme.example\","
      "\"salary\":140000,\"manager\":null,\"active\":true}" },
    { "People", "employees", "e1005",
      "{\"name\":\"Eve Davis\",\"title\":\"Head of Data\","
      "\"department\":\"Data\",\"email\":\"eve@acme.example\","
      "\"salary\":130000,\"manager\":null,\"active\":true}" },
    { "Departments", "depts", "eng",
      "{\"name\":\"Engineering\",\"head\":\"Dave Brown\","
      "\"budget\":2500000,\"headcount\":42}" },
    { "Departments", "depts", "prod",
      "{\"name\":\"Product\",\"head\":\"Bob Smith\","
      "\"budget\":900000,\"headcount\":12}" },
    { "Departments", "depts", "data",
      "{\"name\":\"Data\",\"head\":\"Eve Davis\","
      "\"budget\":1200000,\"headcount\":18}" },
    { "Departments", "depts", "sales",
      "{\"name\":\"Sales\",\"head\":\"Frank Lee\","
      "\"budget\":1500000,\"headcount\":30}" },
    { "Departments", "depts", "hr",
      "{\"name\":\"Human Resources\",\"head\":\"Grace Kim\","
      "\"budget\":400000,\"headcount\":6}" },
    { "Projects", "projects", "p100",
      "{\"name\":\"Website Redesign\",\"owner\":\"Product\","
      "\"status\":\"active\",\"budget\":180000,\"progress\":0.65}" },
    { "Projects", "projects", "p200",
      "{\"name\":\"Mobile App\",\"owner\":\"Engineering\","
      "\"status\":\"active\",\"budget\":320000,\"progress\":0.4}" },
    { "Projects", "projects", "p300",
      "{\"name\":\"Data Warehouse\",\"owner\":\"Data\","
      "\"status\":\"planned\",\"budget\":150000,\"progress\":0.0}" },
    { "Locations", "offices", "l1",
      "{\"city\":\"London\",\"country\":\"United Kingdom\","
      "\"address\":\"1 Acme Way\",\"headcount\":58,\"hq\":true}" },
    { "Locations", "offices", "l2",
      "{\"city\":\"New York\",\"country\":\"United States\","
      "\"address\":\"500 Park Ave\",\"headcount\":34,\"hq\":false}" },
    { "Locations", "offices", "l3",
      "{\"city\":\"Berlin\",\"country\":\"Germany\","
      "\"address\":\"Mitte 12\",\"headcount\":16,\"hq\":false}" },
};

/* Seeds a local example company database so a fresh node has something
 * to explore before it joins a cluster. Data is written straight to the
 * engine (not replicated) so it stays purely local; the join flow wipes
 * it when the node adopts the shared cluster data. */
static void seed_demo_data(void)
{
    zdb_database_create(g_ctx.config, "demo", 1);
    size_t n = sizeof(DEMO_RECORDS) / sizeof(DEMO_RECORDS[0]);
    for (size_t i = 0; i < n; i++) {
        const demo_record *r = &DEMO_RECORDS[i];
        zdb_put(g_ctx.engine, r->partition, r->keyspace, r->id, r->json, -1);
        zdb_partition_ensure(g_ctx.config, "demo", r->partition,
                             r->keyspace, NULL);
    }
}

static bool handle_admin_setup(const zdb_http_request *req,
                               zdb_http_response *res)
{
    if (strcmp(req->method, "POST") != 0) {
        respond_error(res, 405, "method not allowed");
        return true;
    }
    if (g_ctx.config && zdb_admin_exists(g_ctx.config)) {
        respond_error(res, 409, "admin already configured");
        return true;
    }
    cJSON *body = NULL;
    if (!body_json(req, &body) || !cJSON_IsObject(body)) {
        respond_error(res, 400, "JSON object required");
        cJSON_Delete(body);
        return true;
    }
    const cJSON *username = cJSON_GetObjectItemCaseSensitive(body, "username");
    const cJSON *password = cJSON_GetObjectItemCaseSensitive(body, "password");
    const cJSON *secret = cJSON_GetObjectItemCaseSensitive(body, "secret");
    const char *name = cJSON_IsString(username) && username->valuestring &&
                               *username->valuestring
                           ? username->valuestring
                           : "admin";
    if (!cJSON_IsString(password) || !password->valuestring ||
        strlen(password->valuestring) < 4 ||
        strlen(password->valuestring) > 256) {
        cJSON_Delete(body);
        respond_error(res, 400, "password must be 4-256 characters");
        return true;
    }
    /* Optional cluster secret: derive mesh keys, persist them and enable
     * frame encryption. Nodes must present the same secret to join. */
    const char *secret_str =
        cJSON_IsString(secret) && secret->valuestring &&
                *secret->valuestring
            ? secret->valuestring
            : NULL;
    uint8_t enc_key[32], mac_key[32];
    if (secret_str) {
        if (zdb_cluster_derive_keys(secret_str, enc_key, mac_key) != 0) {
            cJSON_Delete(body);
            respond_error(res, 400, "invalid cluster secret");
            return true;
        }
        zdb_cluster_persist_keys(zdb_engine_path(g_ctx.engine), enc_key,
                                 mac_key);
        zstp_set_mesh_key(enc_key, mac_key);
    }
    /* First run: create the default SysAdmins group (bit 1) and add the
     * admin user to it, then set the admin password. */
    zdb_group_create(g_ctx.config, "SysAdmins");
    zdb_group_info sysadmins;
    uint64_t admin_groups = 1ULL;
    if (zdb_group_get(g_ctx.config, "SysAdmins", &sysadmins)) {
        admin_groups = 1ULL << (sysadmins.bit_position - 1);
    }
    if (!zdb_user_create(g_ctx.config, name, admin_groups)) {
        cJSON_Delete(body);
        respond_error(res, 409, "could not create admin user");
        return true;
    }
    if (!zdb_user_set_password(g_ctx.config, name, password->valuestring)) {
        cJSON_Delete(body);
        respond_error(res, 500, "could not store password");
        return true;
    }
    seed_demo_data();
    char token[65];
    session_create(name, admin_groups, token);
    cJSON_Delete(body);
    respond_groups_json(res, 200, token, name, admin_groups);
    return true;
}

static bool handle_admin_logout(const zdb_http_request *req,
                                zdb_http_response *res)
{
    char token[256];
    const char *presented = bearer_token(req, token);
    if (presented) {
        session_destroy(presented);
    }
    respond_json(res, 200, NULL);
    res->body = zdb_http_body_printf(&res->body_len, "{\"ok\":true}");
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
    zdb_config_register_settings(config);

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
    ok &= zdb_http_add_handler(srv, "PUT", "/admin/users",
                               handle_admin_users);
    ok &= zdb_http_add_handler(srv, "GET", "/admin/partitions",
                               handle_admin_partitions);
    ok &= zdb_http_add_handler(srv, "POST", "/admin/partitions",
                               handle_admin_partitions);
    ok &= zdb_http_add_handler(srv, "PUT", "/admin/partitions",
                               handle_admin_partitions);
    ok &= zdb_http_add_handler(srv, "GET", "/admin/keyspaces",
                               handle_admin_keyspaces);
    ok &= zdb_http_add_handler(srv, "GET", "/admin/analytics",
                               handle_admin_analytics);
    ok &= zdb_http_add_handler(srv, "POST", "/admin/benchmark",
                               handle_admin_benchmark);
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
    ok &= zdb_http_add_handler(srv, "GET", "/admin/console/state",
                               handle_console_state);
    ok &= zdb_http_add_handler(srv, "POST", "/admin/login",
                               handle_admin_login);
    ok &= zdb_http_add_handler(srv, "POST", "/admin/setup",
                               handle_admin_setup);
    ok &= zdb_http_add_handler(srv, "POST", "/admin/logout",
                               handle_admin_logout);
    return ok;
}
