/* epsilon_api_cluster.c - cluster
 * Part of the split epsilon_api module; see epsilon_api_internal.h.
 */

#include "epsilon_api_internal.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>


bool handle_admin_cluster(const edb_http_request *req,
                                 edb_http_response *res)
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
        cJSON_AddStringToObject(o, "node_id", edb_cluster_self_id(g_cluster));
        const char *leader = edb_cluster_leader(g_cluster);
        cJSON_AddStringToObject(o, "leader", leader ? leader : "none");
        cJSON_AddBoolToObject(o, "is_leader",
                              edb_cluster_is_leader(g_cluster));
        /* stage 6e: live/target structure versions, rebalance lock state
         * and per-node compliance for observability */
        long long tgen = edb_cluster_target_generation(g_cluster);
        cJSON_AddNumberToObject(o, "generation",
                                (double)edb_cluster_generation(g_cluster));
        cJSON_AddNumberToObject(o, "live_version",
                                (double)edb_cluster_generation(g_cluster));
        cJSON_AddNumberToObject(o, "target_version", (double)tgen);
        cJSON_AddBoolToObject(o, "rebalance_in_progress", tgen > 0);
        char *lockjson =
            edb_setting_get(g_ctx.config, "cluster.rebalance_lock");
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

        edb_peer_info peers[64];
        size_t n = edb_cluster_peers(g_cluster, peers, 64);
        cJSON *arr = cJSON_AddArrayToObject(o, "nodes");
        for (size_t i = 0; arr && i < n; i++) {
            cJSON *p = cJSON_CreateObject();
            cJSON_AddStringToObject(p, "id", peers[i].id);
            cJSON_AddStringToObject(p, "addr", peers[i].addr);
            cJSON_AddNumberToObject(p, "port", peers[i].port);
            if (peers[i].http_port > 0) {
                cJSON_AddNumberToObject(p, "http_port",
                                        peers[i].http_port);
            }
            cJSON_AddBoolToObject(p, "online", peers[i].online);
            cJSON_AddBoolToObject(p, "removed", peers[i].removed);
            cJSON_AddNumberToObject(p, "last_seen",
                                    (double)peers[i].last_seen);
            if (peers[i].compliant_gen > 0) {
                cJSON_AddNumberToObject(p, "compliant",
                                        (double)peers[i].compliant_gen);
            }
            cJSON_AddItemToArray(arr, p);
        }

        edb_range_info tranges[64];
        size_t nt =
            edb_cluster_target_ranges(g_cluster, tranges, 64);
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

        edb_range_info ranges[64];
        size_t nr = edb_cluster_ranges(g_cluster, ranges, 64);
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


/* ------------------------------------------------------------------ */
/* backup / restore                                                    */
/*                                                                     */
/* Backup (read-only, safe to run live):                               */
/*   GET /admin/backup/manifest        JSON list of local shard files  */
/*   GET /admin/backup/shard/<key>     consistent sqlite copy (bytes)  */
/* Restore (cluster-wide, gated by the restore lock):                  */
/*   POST /admin/restore/lock          quiesce data ops on this node   */
/*   POST /admin/restore/wipe          delete non-system shard files   */
/*   PUT /admin/restore/shard/<key>    chunked upload (?offset&final)  */
/*   POST /admin/restore/unlock        re-enable data ops              */

static bool valid_shard_key(const char *key)
{
    if (!key || strlen(key) != 32) {
        return false;
    }
    for (size_t i = 0; i < 32; i++) {
        if (!((key[i] >= '0' && key[i] <= '9') ||
              (key[i] >= 'a' && key[i] <= 'f'))) {
            return false;
        }
    }
    return true;
}

/* Collects the local shard keys, growing the buffer until the directory
 * scan fits. Returns a malloc'd array of 33-byte keys and the count
 * (caller frees). */
static char (*local_shard_keys(size_t *count_out))[33]
{
    *count_out = 0;
    size_t cap = 256;
    for (;;) {
        char(*keys)[33] = malloc(cap * sizeof(*keys));
        if (!keys) {
            return NULL;
        }
        size_t n = edb_engine_shard_keys(g_ctx.engine, keys, cap);
        if (n < cap) {
            *count_out = n;
            return keys;
        }
        free(keys);
        cap *= 2;
    }
}

bool handle_backup_manifest(const edb_http_request *req,
                                   edb_http_response *res)
{
    if (strcmp(req->method, "GET") != 0) {
        respond_error(res, 405, "method not allowed");
        return true;
    }
    if (!require_admin_auth(req, res)) {
        return true;
    }
    size_t n = 0;
    char(*keys)[33] = local_shard_keys(&n);
    cJSON *o = cJSON_CreateObject();
    cJSON_AddStringToObject(o, "node_id",
                            g_cluster ? edb_cluster_self_id(g_cluster)
                                      : "single");
    cJSON *arr = cJSON_AddArrayToObject(o, "shards");
    for (size_t i = 0; arr && keys && i < n; i++) {
        char path[1100];
        snprintf(path, sizeof(path), "%s/%s.sqlite",
                 edb_engine_path(g_ctx.engine), keys[i]);
        struct stat st;
        cJSON *s = cJSON_CreateObject();
        cJSON_AddStringToObject(s, "key", keys[i]);
        cJSON_AddBoolToObject(s, "system",
                              edb_config_is_system_key(g_ctx.config,
                                                       keys[i]));
        if (stat(path, &st) == 0) {
            cJSON_AddNumberToObject(s, "size", (double)st.st_size);
        }
        cJSON_AddItemToArray(arr, s);
    }
    free(keys);
    respond_json(res, 200, o);
    return true;
}

/* Streams a transactionally consistent copy of one local shard using the
 * sqlite3_backup_* online backup API (safe while writes continue). */
bool handle_backup_shard_download(const edb_http_request *req,
                                         edb_http_response *res)
{
    if (strcmp(req->method, "GET") != 0) {
        respond_error(res, 405, "method not allowed");
        return true;
    }
    if (!require_admin_auth(req, res)) {
        return true;
    }
    const char *prefix = "/admin/backup/shard/";
    const char *key = req->path + strlen(prefix);
    if (!valid_shard_key(key)) {
        respond_error(res, 400, "invalid shard key");
        return true;
    }
    const char *dir = edb_engine_path(g_ctx.engine);
    if (!dir) {
        respond_error(res, 500, "no data directory");
        return true;
    }
    char src[1100], tmp[1100];
    snprintf(src, sizeof(src), "%s/%s.sqlite", dir, key);
    snprintf(tmp, sizeof(tmp), "%s/.%s.bkup.XXXXXX", dir, key);
    int tmp_fd = mkstemp(tmp);
    if (tmp_fd < 0) {
        respond_error(res, 500, "cannot create snapshot file");
        return true;
    }
    close(tmp_fd);

    /* consistent copy via the online backup API */
    sqlite3 *sdb = NULL;
    sqlite3 *tdb = NULL;
    sqlite3_backup *bak = NULL;
    int rc = sqlite3_open_v2(src, &sdb,
                             SQLITE_OPEN_READONLY | SQLITE_OPEN_FULLMUTEX,
                             NULL);
    if (rc == SQLITE_OK) {
        sqlite3_open_v2(tmp, &tdb, SQLITE_OPEN_READWRITE, NULL);
    }
    if (sdb && tdb) {
        bak = sqlite3_backup_init(tdb, "main", sdb, "main");
    }
    if (bak) {
        int src_rc;
        do {
            src_rc = sqlite3_backup_step(bak, -1);
        } while (src_rc == SQLITE_OK || src_rc == SQLITE_BUSY ||
                 src_rc == SQLITE_LOCKED);
        sqlite3_backup_finish(bak);
    }
    if (sdb) {
        sqlite3_close(sdb);
    }
    if (tdb) {
        sqlite3_exec(tdb, "COMMIT;", NULL, NULL, NULL);
        sqlite3_close(tdb);
    }

    /* read the snapshot into the response body */
    FILE *fp = fopen(tmp, "rb");
    if (!fp) {
        unlink(tmp);
        respond_error(res, 500, "snapshot read failed");
        return true;
    }
    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    if (size < 0) {
        fclose(fp);
        unlink(tmp);
        respond_error(res, 500, "snapshot read failed");
        return true;
    }
    char *body = malloc((size_t)size + 1);
    if (!body) {
        fclose(fp);
        unlink(tmp);
        respond_error(res, 500, "out of memory");
        return true;
    }
    if (size > 0 && fread(body, 1, (size_t)size, fp) != (size_t)size) {
        fclose(fp);
        free(body);
        unlink(tmp);
        respond_error(res, 500, "snapshot read failed");
        return true;
    }
    fclose(fp);
    unlink(tmp);
    body[size] = '\0';
    res->status = 200;
    res->content_type = "application/octet-stream";
    res->body = body;
    res->body_len = (size_t)size;
    return true;
}

bool handle_restore_lock(const edb_http_request *req,
                                edb_http_response *res)
{
    if (strcmp(req->method, "POST") != 0) {
        respond_error(res, 405, "method not allowed");
        return true;
    }
    if (!require_admin_auth(req, res)) {
        return true;
    }
    restore_set_locked(true);
    respond_json(res, 200, cJSON_CreateString("locked"));
    return true;
}

bool handle_restore_unlock(const edb_http_request *req,
                                  edb_http_response *res)
{
    if (strcmp(req->method, "POST") != 0) {
        respond_error(res, 405, "method not allowed");
        return true;
    }
    if (!require_admin_auth(req, res)) {
        return true;
    }
    restore_set_locked(false);
    respond_json(res, 200, cJSON_CreateString("unlocked"));
    return true;
}

/* Deletes every non-system shard file. The config shards (auth, ranges,
 * settings, keyspace registry) are kept so the cluster identity survives
 * and the restored data is reachable through the existing topology. */
bool handle_restore_wipe(const edb_http_request *req,
                                edb_http_response *res)
{
    if (strcmp(req->method, "POST") != 0) {
        respond_error(res, 405, "method not allowed");
        return true;
    }
    if (!require_admin_auth(req, res)) {
        return true;
    }
    if (!g_restore_locked) {
        respond_error(res, 409, "restore lock not held");
        return true;
    }
    size_t n = 0;
    char(*keys)[33] = local_shard_keys(&n);
    size_t wiped = 0;
    for (size_t i = 0; keys && i < n; i++) {
        if (edb_config_is_system_key(g_ctx.config, keys[i])) {
            continue;
        }
        if (edb_shard_gc(g_ctx.engine, keys[i])) {
            wiped++;
        }
    }
    free(keys);
    cJSON *o = cJSON_CreateObject();
    cJSON_AddNumberToObject(o, "wiped", (double)wiped);
    respond_json(res, 200, o);
    return true;
}

/* Chunked shard upload: the client PUTs the file in <= 8 MB pieces with
 * ?offset=N (0 = start/truncate) and ?final=1 on the last piece, which
 * runs an integrity check and atomically renames the file into place. */
bool handle_restore_shard_upload(const edb_http_request *req,
                                        edb_http_response *res)
{
    if (strcmp(req->method, "PUT") != 0) {
        respond_error(res, 405, "method not allowed");
        return true;
    }
    if (!require_admin_auth(req, res)) {
        return true;
    }
    if (!g_restore_locked) {
        respond_error(res, 409, "restore lock not held");
        return true;
    }
    const char *prefix = "/admin/restore/shard/";
    const char *key = req->path + strlen(prefix);
    if (!valid_shard_key(key)) {
        respond_error(res, 400, "invalid shard key");
        return true;
    }
    if (edb_config_is_system_key(g_ctx.config, key)) {
        respond_error(res, 400, "system shards cannot be restored");
        return true;
    }
    const char *dir = edb_engine_path(g_ctx.engine);
    if (!dir) {
        respond_error(res, 500, "no data directory");
        return true;
    }
    char qoffset[32] = "", qfinal[8] = "";
    query_param(req, "offset", qoffset, sizeof(qoffset));
    query_param(req, "final", qfinal, sizeof(qfinal));
    long long offset = qoffset[0] ? strtoll(qoffset, NULL, 10) : 0;
    bool final = strcmp(qfinal, "1") == 0;

    char tmp[1100];
    snprintf(tmp, sizeof(tmp), "%s/%s.sqlite.rtmp", dir, key);
    int flags = O_WRONLY | O_CREAT;
    if (offset == 0) {
        flags |= O_TRUNC;
    }
    int fd = open(tmp, flags, 0644);
    if (fd < 0) {
        respond_error(res, 500, "cannot open restore file");
        return true;
    }
    ssize_t written = pwrite(fd, req->body, req->body_len, (off_t)offset);
    if (written != (ssize_t)req->body_len) {
        close(fd);
        respond_error(res, 500, "write failed");
        return true;
    }
    off_t size = lseek(fd, 0, SEEK_END);
    close(fd);

    if (!final) {
        cJSON *o = cJSON_CreateObject();
        cJSON_AddNumberToObject(o, "offset", (double)size);
        respond_json(res, 200, o);
        return true;
    }

    /* final chunk: verify the database before renaming it into place */
    sqlite3 *db = NULL;
    bool ok = false;
    if (sqlite3_open_v2(tmp, &db, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
        sqlite3_stmt *stmt = NULL;
        if (sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &stmt,
                               NULL) == SQLITE_OK && stmt) {
            ok = sqlite3_step(stmt) == SQLITE_ROW;
            const char *result =
                ok ? (const char *)sqlite3_column_text(stmt, 0) : NULL;
            ok = result && strcmp(result, "ok") == 0;
            sqlite3_finalize(stmt);
        }
        sqlite3_close(db);
    }
    if (!ok) {
        unlink(tmp);
        respond_error(res, 400, "uploaded shard failed integrity check");
        return true;
    }
    char final_path[1100];
    snprintf(final_path, sizeof(final_path), "%s/%s.sqlite", dir, key);
    if (rename(tmp, final_path) != 0) {
        unlink(tmp);
        respond_error(res, 500, "cannot place shard file");
        return true;
    }
    char side[1150];
    snprintf(side, sizeof(side), "%s-wal", final_path);
    unlink(side);
    snprintf(side, sizeof(side), "%s-shm", final_path);
    unlink(side);
    snprintf(side, sizeof(side), "%s-journal", final_path);
    unlink(side);
    cJSON *o = cJSON_CreateObject();
    cJSON_AddBoolToObject(o, "restored", true);
    respond_json(res, 200, o);
    return true;
}


/* Removes every non-system shard file so a joining node starts fresh
 * (its local demo/scratch data is discarded in favour of the shared
 * cluster data). Config shards are left in place: the seed sync below
 * snapshots them over the local copies. */

static bool catchup_from_holder(const char key[33], const char *partition,
                                const char *keyspace)
{
    char holders[64][EDB_NODE_ID_MAX];
    size_t count = edb_cluster_holders(g_cluster, key, holders, 64);
    edb_peer_info peers[64];
    size_t npeers = edb_cluster_peers(g_cluster, peers, 64);
    for (size_t i = 0; i < count; i++) {
        if (strcmp(holders[i], edb_cluster_self_id(g_cluster)) == 0) {
            continue;
        }
        for (size_t p = 0; p < npeers; p++) {
            if (strcmp(peers[p].id, holders[i]) == 0 && peers[p].addr[0] &&
                peers[p].port > 0 &&
                edb_repl_catchup_required(g_repl, peers[p].addr,
                                           peers[p].port, partition,
                                           keyspace)) {
                return true;
            }
        }
    }
    return false;
}



bool handle_admin_join(const edb_http_request *req,
                              edb_http_response *res)
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
        if (edb_cluster_derive_keys(secret_str, enc_key, mac_key) != 0) {
            cJSON_Delete(body);
            respond_error(res, 400, "invalid cluster secret");
            return true;
        }
        estp_set_mesh_key(enc_key, mac_key);
        key_set = true;
    }
    int rc = edb_cluster_join(g_cluster, addr->valuestring,
                              port->valueint);
    char seed_addr[EDB_ADDR_MAX];
    int seed_port = port->valueint;
    snprintf(seed_addr, sizeof(seed_addr), "%s", addr->valuestring);
    cJSON_Delete(body);
    if (rc == -2) {
        if (key_set) {
            estp_set_mesh_key(NULL, NULL);
        }
        respond_error(res, 409, "rebalance in progress: one node may"
                                " join at a time; retry later");
        return true;
    }
    if (rc == -3) {
        if (key_set) {
            estp_set_mesh_key(NULL, NULL);
        }
        respond_error(res, 410, "this node has been removed from the"
                                " cluster; re-join to be re-admitted");
        return true;
    }
    if (rc != 0) {
        if (key_set) {
            estp_set_mesh_key(NULL, NULL);
        }
        respond_error(res, 502, "cannot reach seed peer (wrong secret?)");
        return true;
    }
    if (key_set) {
        edb_cluster_persist_keys(edb_engine_path(g_ctx.engine), enc_key,
                                 mac_key);
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
    edb_cluster_set_auto_compliant(g_cluster, false);

    const char *sys_ks[8];
    size_t nsys = edb_config_system_keyspaces(sys_ks, 8);
    for (size_t i = 0; i < nsys && synced; i++) {
        if (!edb_repl_catchup(g_repl, seed_addr, seed_port,
                              EDB_SYSTEM_DB, sys_ks[i])) {
            synced = false;
            snprintf(fail_detail, sizeof(fail_detail),
                     "config sync failed for %s", sys_ks[i]);
        }
    }

    bool pending = false;
    for (int i = 0; i < 100 && !pending; i++) {
        pending = edb_cluster_target_generation(g_cluster) > 0;
        if (!pending) {
            struct timespec delay = { .tv_sec = 0, .tv_nsec = 100 * 1000000L };
            nanosleep(&delay, NULL);
        }
    }
    if (synced && pending && edb_cluster_needs_sync(g_cluster)) {
        size_t nks = 0;
        edb_keyspace_info *kss =
            edb_keyspace_list(g_ctx.config, &nks);
        for (size_t i = 0; kss && i < nks && synced; i++) {
            char key[33];
            char path[1024];
            if (!edb_shard_path(g_ctx.engine, kss[i].partition,
                                kss[i].name, path, sizeof(path), key) ||
                edb_config_is_system_key(g_ctx.config, key)) {
                continue;
            }
            const char *towner =
                edb_cluster_target_owner(g_cluster, key);
            if (!towner ||
                strcmp(towner, edb_cluster_self_id(g_cluster)) != 0) {
                continue;
            }
            if (!catchup_from_holder(key, kss[i].partition, kss[i].name) ||
                !edb_shard_validate(g_ctx.engine, kss[i].partition,
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
        edb_cluster_mark_compliant(g_cluster);
    } else {
        /* roll the cluster back to the live structure */
        edb_cluster_request_void(g_cluster);
    }
    edb_cluster_set_auto_compliant(g_cluster, true);

    if (!synced) {
        respond_error(res, 500, fail_detail[0] ? fail_detail
                                               : "join sync failed;"
                                                 " rolled back");
        return true;
    }
    free(res->body);
    res->body = edb_http_body_printf(&res->body_len,
                                     "{\"joined\":true,\"synced\":true}");
    return true;
}

/* POST /admin/remove-node {node_id}: tombstone a node and re-shard. */
bool handle_admin_remove_node(const edb_http_request *req,
                                     edb_http_response *res)
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
        respond_error(res, 400, "JSON body required: {node_id}");
        cJSON_Delete(body);
        return true;
    }
    const cJSON *jid = cJSON_GetObjectItemCaseSensitive(body, "node_id");
    if (!cJSON_IsString(jid) || !jid->valuestring || !*jid->valuestring) {
        respond_error(res, 400, "node_id (string) required");
        cJSON_Delete(body);
        return true;
    }
    if (!edb_cluster_remove_node(g_cluster, jid->valuestring)) {
        cJSON_Delete(body);
        respond_error(res, 404, "node not found (or cannot remove self)");
        return true;
    }
    cJSON_Delete(body);
    res->status = 200;
    res->content_type = "application/json";
    res->body = edb_http_body_printf(&res->body_len, "{\"removed\":true}");
    return true;
}
