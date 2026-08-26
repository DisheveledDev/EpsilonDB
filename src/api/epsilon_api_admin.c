/* epsilon_api_admin.c - admin
 * Part of the split epsilon_api module; see epsilon_api_internal.h.
 */

#include "epsilon_api_internal.h"
#include "../engine/epsilon_benchmark.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>


bool require_admin_auth(const edb_http_request *req,
                               edb_http_response *res)
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

bool handle_admin_databases(const edb_http_request *req,
                                   edb_http_response *res)
{
    if (!require_admin_auth(req, res)) {
        return true;
    }
    if (strcmp(req->method, "GET") == 0) {
        size_t n = 0;
        edb_database_info *list = edb_database_list(g_ctx.config, &n);
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
        bool ok = edb_database_create(g_ctx.config, name->valuestring,
                                      rf->valueint);
        cJSON_Delete(body);
        if (!ok) {
            respond_error(res, 409, "create failed (duplicate?)");
            return true;
        }
        respond_json(res, 201, NULL);
        res->body = edb_http_body_printf(&res->body_len, "{\"created\":true}");
        return true;
    }
    respond_error(res, 405, "method not allowed");
    return true;
}

bool handle_admin_groups(const edb_http_request *req,
                                edb_http_response *res)
{
    if (!require_admin_auth(req, res)) {
        return true;
    }
    if (strcmp(req->method, "GET") == 0) {
        size_t n = 0;
        edb_group_info *list = edb_group_list(g_ctx.config, &n);
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
        bool ok = edb_group_create(g_ctx.config, name->valuestring);
        cJSON_Delete(body);
        if (!ok) {
            respond_error(res, 409, "create failed (duplicate/full)");
            return true;
        }
        respond_json(res, 201, NULL);
        res->body = edb_http_body_printf(&res->body_len, "{\"created\":true}");
        return true;
    }
    respond_error(res, 405, "method not allowed");
    return true;
}

bool handle_admin_users(const edb_http_request *req,
                               edb_http_response *res)
{
    if (!require_admin_auth(req, res)) {
        return true;
    }
    if (strcmp(req->method, "GET") == 0) {
        size_t n = 0;
        edb_user_info *list = edb_user_list(g_ctx.config, &n);
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
        bool ok = edb_user_create(g_ctx.config, name->valuestring,
                                  group_mask);
        if (ok && cJSON_IsString(password) && password->valuestring &&
            *password->valuestring) {
            ok = edb_user_set_password(g_ctx.config, name->valuestring,
                                       password->valuestring);
        }
        cJSON_Delete(body);
        if (!ok) {
            respond_error(res, 409, "create failed (duplicate?)");
            return true;
        }
        respond_json(res, 201, NULL);
        res->body = edb_http_body_printf(&res->body_len, "{\"created\":true}");
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
        bool ok = edb_user_set_groups(g_ctx.config, name->valuestring,
                                      group_mask);
        if (ok && cJSON_IsString(password) && password->valuestring &&
            *password->valuestring) {
            ok = edb_user_set_password(g_ctx.config, name->valuestring,
                                       password->valuestring);
        }
        cJSON_Delete(body);
        if (!ok) {
            respond_error(res, 404, "user not found");
            return true;
        }
        respond_json(res, 200, NULL);
        res->body = edb_http_body_printf(&res->body_len, "{\"updated\":true}");
        return true;
    }
    respond_error(res, 405, "method not allowed");
    return true;
}

/* Parses optional shard-tuning fields from a partition request body. Returns
 * true when at least one tuning field was present. */
static bool partition_settings_from_json(const cJSON *body,
                                        edb_shard_settings *out)
{
    edb_shard_settings_default(out);
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
                                        const edb_partition_info *p)
{
    cJSON_AddNumberToObject(o, "cache_size", (double)p->cache_size);
    cJSON_AddStringToObject(o, "journal_mode", p->journal_mode);
    cJSON_AddNumberToObject(o, "vacuum_seconds", (double)p->vacuum_seconds);
    cJSON_AddNumberToObject(o, "reindex_seconds", (double)p->reindex_seconds);
}

bool handle_admin_partitions(const edb_http_request *req,
                                    edb_http_response *res)
{
    if (!require_admin_auth(req, res)) {
        return true;
    }
    if (strcmp(req->method, "GET") == 0) {
        char dbbuf[128];
        bool have_db = query_param(req, "database", dbbuf, sizeof(dbbuf));
        size_t n = 0;
        edb_partition_info *list =
            edb_partition_list(g_ctx.config, have_db ? dbbuf : "", &n);
        if (!have_db) {
            /* list across all databases */
            free(list);
            size_t ndb = 0;
            edb_database_info *dbs = edb_database_list(g_ctx.config, &ndb);
            cJSON *arr2 = cJSON_CreateArray();
            for (size_t d = 0; dbs && d < ndb; d++) {
                size_t cnt = 0;
                edb_partition_info *parts = edb_partition_list(
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
        bool ok = edb_partition_create(g_ctx.config, database->valuestring,
                                       name->valuestring, create_mask,
                                       update_mask, read_mask, delete_mask);
        if (ok) {
            edb_shard_settings settings;
            if (partition_settings_from_json(body, &settings)) {
                edb_partition_set_settings(g_ctx.config,
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
        res->body = edb_http_body_printf(&res->body_len, "{\"created\":true}");
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
        bool ok = edb_partition_set_masks(g_ctx.config, database->valuestring,
                                          name->valuestring, create_mask,
                                          update_mask, read_mask,
                                          delete_mask);
        if (ok) {
            edb_shard_settings settings;
            if (partition_settings_from_json(body, &settings)) {
                edb_partition_set_settings(g_ctx.config,
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
        res->body = edb_http_body_printf(&res->body_len, "{\"updated\":true}");
        return true;
    }
    respond_error(res, 405, "method not allowed");
    return true;
}

/* Generic DELETE handler for single-entity admin resources:
 * /admin/{databases|groups|users}/<name> and
 * /admin/partitions/<database>/<name> */
bool handle_admin_delete(const edb_http_request *req,
                                edb_http_response *res)
{
    if (!require_admin_auth(req, res)) {
        return true;
    }
    const char *p = req->path;
    bool ok = false;

    if (strncmp(p, "/admin/databases/", 17) == 0) {
        ok = edb_database_delete(g_ctx.config, p + 17);
    } else if (strncmp(p, "/admin/groups/", 14) == 0) {
        ok = edb_group_delete(g_ctx.config, p + 14);
    } else if (strncmp(p, "/admin/users/", 13) == 0) {
        ok = edb_user_delete(g_ctx.config, p + 13);
    } else if (strncmp(p, "/admin/partitions/", 18) == 0) {
        char database[128];
        char name[256];
        if (sscanf(p + 18, "%127[^/]/%255s", database, name) != 2) {
            respond_error(res, 400,
                          "expected /admin/partitions/<database>/<name>");
            return true;
        }
        ok = edb_partition_delete(g_ctx.config, database, name);
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
    res->body = edb_http_body_printf(&res->body_len, "{\"deleted\":true}");
    return true;
}

/* GET /admin/keyspaces: the registry of used
 * database/partition/keyspace triples (populated transparently by
 * writes). Optional ?database=<name> narrows the listing. */
bool handle_admin_keyspaces(const edb_http_request *req,
                                   edb_http_response *res)
{
    if (!require_admin_auth(req, res)) {
        return true;
    }
    char dbfilter[128] = "";
    bool have_db = query_param(req, "database", dbfilter, sizeof(dbfilter));

    size_t n = 0;
    edb_keyspace_info *list = edb_keyspace_list(g_ctx.config, &n);
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
bool handle_admin_analytics(const edb_http_request *req,
                                   edb_http_response *res)
{
    if (strcmp(req->method, "GET") != 0) {
        respond_error(res, 405, "method not allowed");
        return true;
    }
    if (!require_admin_auth(req, res)) {
        return true;
    }
    cJSON *report = edb_analytics_report(g_analytics);
    respond_json(res, 200, report ? report : cJSON_CreateObject());
    return true;
}

/* POST /admin/benchmark: run a throwaway workload benchmark. */
bool handle_admin_benchmark(const edb_http_request *req,
                                   edb_http_response *res)
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
    /* copy out of the body: the body is freed below and the benchmark
     * report keeps the journal-mode string */
    char journal_buf[16];
    const char *journal_mode = "TRUNCATE";
    int partitions = 10;
    int records = 100000;
    int threads = 0;
    if (body && cJSON_IsObject(body)) {
        const cJSON *j;
        j = cJSON_GetObjectItemCaseSensitive(body, "replication_factor");
        if (cJSON_IsNumber(j)) replication_factor = (int)j->valuedouble;
        j = cJSON_GetObjectItemCaseSensitive(body, "cache_size");
        if (cJSON_IsNumber(j)) cache_size = (long long)j->valuedouble;
        j = cJSON_GetObjectItemCaseSensitive(body, "journal_mode");
        if (cJSON_IsString(j) && j->valuestring && *j->valuestring) {
            snprintf(journal_buf, sizeof(journal_buf), "%s",
                     j->valuestring);
            journal_mode = journal_buf;
        }
        j = cJSON_GetObjectItemCaseSensitive(body, "partitions");
        if (cJSON_IsNumber(j)) partitions = (int)j->valuedouble;
        j = cJSON_GetObjectItemCaseSensitive(body, "records");
        if (cJSON_IsNumber(j)) records = (int)j->valuedouble;
        j = cJSON_GetObjectItemCaseSensitive(body, "threads");
        if (cJSON_IsNumber(j)) threads = (int)j->valuedouble;
    }
    cJSON_Delete(body);
    cJSON *report = edb_benchmark_run(g_ctx.config, replication_factor,
                                      cache_size, journal_mode, partitions,
                                      records, threads);
    respond_json(res, 200, report ? report : cJSON_CreateObject());
    return true;
}

/* GET /admin/cluster: membership, leader, generation and range table.
 * POST /admin/join {addr, port}: dial the seed peer and merge meshes. */
