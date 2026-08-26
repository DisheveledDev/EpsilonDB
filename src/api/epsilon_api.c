#include "epsilon_api_internal.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "../../vendor/cjson/cJSON.h"
#include "../engine/random.h"
#include "../engine/epsilon_analytics.h"
#include "../engine/epsilon_benchmark.h"

/* ------------------------------------------------------------------ */
/* helpers                                                             */

api_ctx g_ctx;   /* handlers receive no user pointer; single server */
edb_cluster *g_cluster;   /* may be NULL: clustering disabled */
edb_repl *g_repl;         /* may be NULL: replication disabled */
edb_analytics *g_analytics; /* may be NULL: analytics disabled */

/* Restore lock: set by POST /admin/restore/lock (per node, in memory and
 * persisted as the server.restore_lock setting for observability). While
 * set, every data read/write endpoint answers 503 so a restore can wipe
 * and re-place shard files without clients seeing partial state. */
volatile bool g_restore_locked = false;

void respond_error(edb_http_response *res, int status,
                          const char *message);

bool restore_blocked(edb_http_response *res)
{
    if (g_restore_locked) {
        respond_error(res, 503, "restore in progress");
        return true;
    }
    return false;
}

void restore_set_locked(bool locked)
{
    g_restore_locked = locked;
    if (g_ctx.config) {
        edb_setting_set(g_ctx.config, "server.restore_lock",
                        locked ? "true" : "false");
    }
}

/* Monotonic microseconds for latency measurement. */
long long api_now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000LL + ts.tv_nsec / 1000;
}

/* ------------------------------------------------------------------ */
/* admin console sessions                                              */

void edb_api_set_cluster(edb_cluster *cluster)
{
    g_cluster = cluster;
}

static bool api_apply_change_impl(void *ud, const cJSON *change)
{
    (void)ud;
    /* restore lock: refuse to apply replicated changes so nothing touches
     * the shard files while they are being wiped and replaced; writers
     * keep the changes cached and replay them after the unlock */
    if (g_restore_locked) {
        return false;
    }
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
        ok = edb_replica_put_origin(
            g_ctx.engine, partition->valuestring, keyspace->valuestring,
            id->valuestring, encoded, absolute_ttl, modified, origin);
        free(encoded);
        if (ok && strcmp(database->valuestring, EDB_SYSTEM_DB) != 0) {
            edb_partition_ensure(g_ctx.config, database->valuestring,
                                 partition->valuestring,
                                 keyspace->valuestring, NULL);
        }
    } else if (strcmp(operation->valuestring, "delete") == 0) {
        ok = edb_replica_delete_origin(
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
        !cJSON_IsString(keyspace) || !edb_filters_valid(filters)) {
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
                              ? edb_get_ts(g_ctx.engine,
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
                          ? edb_all_ts(g_ctx.engine,
                                       partition->valuestring,
                                       keyspace->valuestring, filters)
                          : edb_query_ts(g_ctx.engine,
                                         partition->valuestring,
                                         keyspace->valuestring, filters);
        cJSON_AddItemToObject(out, "rows", rows ? rows : cJSON_CreateArray());
    } else if (strcmp(query->valuestring, "ids") == 0) {
        size_t count = 0;
        char **ids = edb_ids(g_ctx.engine, partition->valuestring,
                             keyspace->valuestring, filters, &count);
        cJSON *array = cJSON_AddArrayToObject(out, "ids");
        for (size_t i = 0; array && ids && i < count; i++) {
            cJSON_AddItemToArray(array, cJSON_CreateString(ids[i]));
        }
        edb_free_strings(ids);
    } else {
        cJSON_Delete(out);
        return NULL;
    }
    return out;
}
static bool api_replicate_config(void *ctx, const char *keyspace,
                                 const char *id, const char *json);

void edb_api_set_repl(edb_repl *repl)
{
    g_repl = repl;
    if (repl) {
        edb_repl_set_handlers(repl, api_apply_change_impl,
                              api_read_request, NULL);
    }
    if (g_ctx.config) {
        edb_config_set_replicator(g_ctx.config,
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
        cJSON_AddStringToObject(change, "db", EDB_SYSTEM_DB);
        cJSON_AddStringToObject(change, "partition", EDB_SYSTEM_DB);
        cJSON_AddStringToObject(change, "keyspace", EDB_ANALYTICS_KEYSPACE);
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
        edb_repl_status status = edb_repl_write(g_repl, EDB_SYSTEM_DB,
                                                encoded);
        free(encoded);
        return status == EDB_REPL_OK;
    }
    long long now = (long long)time(NULL);
    long long ttl_rel = ttl_abs > now ? ttl_abs - now : 0;
    return edb_put(g_ctx.engine, EDB_SYSTEM_DB, EDB_ANALYTICS_KEYSPACE,
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
                            (double)(g_repl ? edb_repl_pending_total(g_repl)
                                            : 0));
    return o;
}

void edb_api_analytics_start(edb_config *cfg, const char *node_id)
{
    if (!cfg || g_analytics) {
        return;
    }
    g_analytics = edb_analytics_start(cfg, node_id, api_analytics_flush, NULL);
    edb_analytics_set_cluster_metrics(g_analytics,
                                      api_analytics_cluster_metrics, NULL);
}

void edb_api_analytics_stop(void)
{
    edb_analytics_stop(g_analytics);
    g_analytics = NULL;
}


static bool api_replicate_config(void *ctx, const char *keyspace,
                                 const char *id, const char *json)
{
    edb_repl *repl = ctx;
    cJSON *change = cJSON_CreateObject();
    if (!repl || !change) {
        cJSON_Delete(change);
        return false;
    }
    cJSON_AddStringToObject(change, "op", json ? "put" : "delete");
    cJSON_AddStringToObject(change, "db", EDB_SYSTEM_DB);
    cJSON_AddStringToObject(change, "partition", EDB_SYSTEM_DB);
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
    edb_repl_status status = edb_repl_write(repl, EDB_SYSTEM_DB, encoded);
    free(encoded);
    return status == EDB_REPL_OK;
}


/* --- stage 5 replication handlers ----------------------------------- */

void respond_json(edb_http_response *res, int status, cJSON *obj)
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

void respond_error(edb_http_response *res, int status,
                          const char *message)
{
    cJSON *obj = cJSON_CreateObject();
    if (obj) {
        cJSON_AddStringToObject(obj, "error", message);
    }
    respond_json(res, status, obj);
}

bool body_json(const edb_http_request *req, cJSON **out)
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

bool json_u64_value(const cJSON *item, uint64_t *out)
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

bool query_param(const edb_http_request *req, const char *name,
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
uint64_t authenticate(const edb_http_request *req,
                             const edb_http_request *unused, bool *ok)
{
    (void)unused;
    *ok = true;

    if (req->trusted) {
        return ~0ULL;
    }

    uint64_t groups = 0;
    char token_buf[256] = "";
    const char *token = NULL;
    const char *hdr = edb_http_header(req, "Authorization");
    if (hdr) {
        const char *value = strncmp(hdr, "Bearer ", 7) == 0 ? hdr + 7 : hdr;
        if (snprintf(token_buf, sizeof(token_buf), "%s", value) <
            (int)sizeof(token_buf)) {
            token = token_buf;
        }
    }
    if (!token) {
        hdr = edb_http_header(req, "authorization");
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
            edb_user_info user;
            if (edb_user_get(g_ctx.config, token, &user)) {
                groups = user.groups;
            } else {
                *ok = false;
            }
        }
    } else {
        size_t nusers = 0;
        edb_user_info *users = edb_user_list(g_ctx.config, &nusers);
        bool bootstrapped = users != NULL && nusers > 0;
        free(users);
        groups = bootstrapped ? 0ULL : ~0ULL;
    }
    return groups;
}

/* Looks up partition masks and checks one permission. Unknown partitions
 * are treated as open (allow-all masks) because partitions materialise
 * transparently on first write; explicit records exist to restrict them. */
bool authorize_partition(edb_config *cfg, const char *database,
                                const char *partition, uint64_t user_groups,
                                edb_permission perm, edb_http_response *res)
{
    edb_partition_info part;
    if (!edb_partition_get(cfg, database, partition, &part)) {
        return true;   /* implicit partition: allow-all masks */
    }
    uint64_t mask;
    switch (perm) {
    case EDB_PERM_CREATE: mask = part.create_mask; break;
    case EDB_PERM_UPDATE: mask = part.update_mask; break;
    case EDB_PERM_READ:   mask = part.read_mask; break;
    case EDB_PERM_DELETE: mask = part.delete_mask; break;
    default:              mask = ~0ULL; break;
    }
    if (!edb_check_perm(mask, user_groups, perm)) {
        respond_error(res, 403, "permission denied");
        return false;
    }
    return true;
}

/* ------------------------------------------------------------------ */
/* data routes                                                         */

bool split_data_path(const char *path, char db[128], char part[256],
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

bool edb_api_register(edb_http_server *srv, edb_engine *engine,
                      edb_config *config)
{
    if (!srv || !engine || !config) {
        return false;
    }
    g_ctx.engine = engine;
    g_ctx.config = config;
    edb_config_set_replicator(config, g_repl ? api_replicate_config : NULL,
                              g_repl);
    edb_config_register_settings(config);

    bool ok = true;
    ok &= edb_http_add_handler(srv, "PUT", "/data/", handle_data_put);
    ok &= edb_http_add_handler(srv, "DELETE", "/data/", handle_data_delete);
    /* GET: id lookups and .../all|ids (collect checks the last segment) */
    ok &= edb_http_add_handler(srv, "GET", "/data/", handle_data_collect);
    ok &= edb_http_add_handler(srv, "POST", "/data/", handle_data_collect);
    ok &= edb_http_add_handler(srv, "GET", "/admin/databases",
                               handle_admin_databases);
    ok &= edb_http_add_handler(srv, "POST", "/admin/databases",
                               handle_admin_databases);
    ok &= edb_http_add_handler(srv, "GET", "/admin/groups",
                               handle_admin_groups);
    ok &= edb_http_add_handler(srv, "POST", "/admin/groups",
                               handle_admin_groups);
    ok &= edb_http_add_handler(srv, "GET", "/admin/users",
                               handle_admin_users);
    ok &= edb_http_add_handler(srv, "POST", "/admin/users",
                               handle_admin_users);
    ok &= edb_http_add_handler(srv, "PUT", "/admin/users",
                               handle_admin_users);
    ok &= edb_http_add_handler(srv, "GET", "/admin/partitions",
                               handle_admin_partitions);
    ok &= edb_http_add_handler(srv, "POST", "/admin/partitions",
                               handle_admin_partitions);
    ok &= edb_http_add_handler(srv, "PUT", "/admin/partitions",
                               handle_admin_partitions);
    ok &= edb_http_add_handler(srv, "GET", "/admin/keyspaces",
                               handle_admin_keyspaces);
    ok &= edb_http_add_handler(srv, "GET", "/admin/analytics",
                               handle_admin_analytics);
    ok &= edb_http_add_handler(srv, "POST", "/admin/benchmark",
                               handle_admin_benchmark);
    ok &= edb_http_add_handler(srv, "GET", "/admin/cluster",
                               handle_admin_cluster);
    ok &= edb_http_add_handler(srv, "POST", "/admin/join",
                               handle_admin_join);
    ok &= edb_http_add_handler(srv, "POST", "/admin/remove-node",
                               handle_admin_remove_node);
    /* backup / restore */
    ok &= edb_http_add_handler(srv, "GET", "/admin/backup/manifest",
                               handle_backup_manifest);
    ok &= edb_http_add_handler(srv, "GET", "/admin/backup/",
                               handle_backup_shard_download);
    ok &= edb_http_add_handler(srv, "POST", "/admin/restore/lock",
                               handle_restore_lock);
    ok &= edb_http_add_handler(srv, "POST", "/admin/restore/unlock",
                               handle_restore_unlock);
    ok &= edb_http_add_handler(srv, "POST", "/admin/restore/wipe",
                               handle_restore_wipe);
    ok &= edb_http_add_handler(srv, "PUT", "/admin/restore/",
                               handle_restore_shard_upload);
    /* deletes use the trailing-slash form so list routes stay intact;
     * longest-prefix matching sends /admin/users/<name> here */
    ok &= edb_http_add_handler(srv, "DELETE", "/admin/",
                               handle_admin_delete);
    ok &= edb_http_add_handler(srv, "GET", "/admin/settings",
                               handle_settings);
    ok &= edb_http_add_handler(srv, "POST", "/admin/settings",
                               handle_settings);
    ok &= edb_http_add_handler(srv, "PUT", "/admin/settings",
                               handle_settings);
    ok &= edb_http_add_handler(srv, "DELETE", "/admin/settings",
                               handle_settings);
    ok &= edb_http_add_handler(srv, "GET", "/status", handle_status);
    ok &= edb_http_add_handler(srv, "GET", "/admin/console/state",
                               handle_console_state);
    ok &= edb_http_add_handler(srv, "POST", "/admin/login",
                               handle_admin_login);
    ok &= edb_http_add_handler(srv, "POST", "/admin/setup",
                               handle_admin_setup);
    ok &= edb_http_add_handler(srv, "POST", "/admin/logout",
                               handle_admin_logout);
    return ok;
}