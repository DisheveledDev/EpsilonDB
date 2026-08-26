/* epsilon_repl.c - stage 5 replication implementation. See epsilon_repl.h.
 *
 * Transport: every fan-out message uses its own short-lived peer
 * connection (dial, HELLO, payload frame, reply frame, close). This
 * keeps the data plane off the mesh connection objects, whose lifecycle
 * is owned by the stage 4 reader threads. Inbound REPL/QUERY frames on
 * mesh connections are answered through the dispatcher hook installed
 * by edb_repl_start, so both transports work.
 *
 * Change cache: a dedicated sqlite database "changes.sqlite" in the
 * data directory records every change that could not be acknowledged by
 * its target peer. A maintenance thread watches peer online/offline
 * transitions and drains each peer's queue in order when it returns;
 * queues also drain at startup for peers already online (crash
 * recovery). Acknowledged changes are deleted; LWW on both sides makes
 * replays idempotent.
 */

#include "epsilon_repl_internal.h"

#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "../../vendor/cjson/cJSON.h"
#include "../engine/md5.h"
#include "../engine/epsilon_engine.h"
#include "../../vendor/sqlite/sqlite3.h"
#include "../epsilon_log.h"
#include "estp_wire.h"
#include "epsilon_snap.h"

#define MAX_PEERS_SNAPSHOT 64

/* ------------------------------------------------------------------ */
/* small helpers                                                       */

long long repl_epoch_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (long long)ts.tv_sec;
}

long long repl_mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

void repl_sleep_ms(int ms)
{
    struct timespec ts = { .tv_sec = ms / 1000,
                           .tv_nsec = (long)(ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

char *json_print(const cJSON *doc)
{
    return doc ? cJSON_PrintUnformatted(doc) : NULL;
}

/* Bounded socket waits so a hung peer cannot stall the caller forever */
void repl_set_socket_timeouts(int fd, int ms)
{
    struct timeval tv = { .tv_sec = ms / 1000,
                          .tv_usec = (ms % 1000) * 1000 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

/* One request/reply exchange on a private connection. Returns the
 * reply type and malloc'd payload, or -1/NULL on any failure. */
int rpc_once(const char *addr, int port, estp_type send_type,
                    const char *payload, estp_type want_type,
                    char **reply_out)
{
    *reply_out = NULL;
    int fd = estp_dial(addr, port);
    if (fd < 0) {
        return -1;
    }
    repl_set_socket_timeouts(fd, REPL_CONNECT_TIMEOUT_MS);

    int rc = -1;
    cJSON *hello = cJSON_CreateObject();
    if (!hello) {
        goto done;
    }
    /* identity is not needed by the receiver for one-shot frames, but
     * the mesh requires HELLO as the first frame on any connection */
    cJSON_AddStringToObject(hello, "node_id", "ephemeral");
    char *hello_str = json_print(hello);
    cJSON_Delete(hello);
    if (!hello_str ||
        estp_send_frame(fd, ESTP_HELLO, hello_str, NULL) != 0) {
        free(hello_str);
        goto done;
    }
    free(hello_str);

    if (estp_send_frame(fd, send_type, payload, NULL) != 0) {
        goto done;
    }

    /* absorb their HELLO, then wait for the actual reply */
    long long deadline = repl_mono_ms() + REPL_FANOUT_DEADLINE_MS;
    for (;;) {
        long long left = deadline - repl_mono_ms();
        if (left <= 0) {
            goto done;
        }
        char *in = NULL;
        int t = estp_recv_frame(fd, &in);
        if (t < 0) {
            free(in);
            goto done;
        }
        if (t == (int)want_type) {
            *reply_out = in;
            rc = t;
            break;
        }
        free(in);   /* their HELLO or anything unexpected */
    }

done:
    close(fd);
    return rc;
}

/* ------------------------------------------------------------------ */
/* ------------------------------------------------------------------ */
/* dispatcher: answer inbound REPL / QUERY / FLUSH frames              */

/* True when an ACK payload means "delivered" (ok present and true, or
 * the field is absent for older peers that never sent it). */
bool ack_ok(const char *reply)
{
    if (!reply) {
        return false;
    }
    cJSON *doc = cJSON_Parse(reply);
    if (!doc) {
        return false;
    }
    const cJSON *jok = cJSON_GetObjectItemCaseSensitive(doc, "ok");
    bool ok = jok ? cJSON_IsTrue(jok) : true;
    cJSON_Delete(doc);
    return ok;
}

static bool repl_dispatch(void *ctx, int msg_type, const char *payload,
                          int *reply_type, char **reply_json)
{
    edb_repl *rp = ctx;
    *reply_json = NULL;
    cJSON *req = cJSON_Parse(payload);
    if (!req) {
        return false;
    }

    if (msg_type == ESTP_REPL && rp && rp->apply) {
        /* stage 6c: while this node is syncing shard snapshots, refuse
         * incoming writes so the sender caches them for later replay */
        pthread_mutex_lock(&rp->sync_lock);
        bool syncing = rp->syncing;
        pthread_mutex_unlock(&rp->sync_lock);

        bool ok = false;
        if (!syncing) {
            ok = rp->apply(rp->ud, req);
        }
        cJSON_Delete(req);
        cJSON *ack = cJSON_CreateObject();
        if (!ack) {
            return false;
        }
        cJSON_AddBoolToObject(ack, "ok", ok);
        *reply_type = ESTP_ACK;
        *reply_json = json_print(ack);
        cJSON_Delete(ack);
        return *reply_json != NULL;
    }

    if (msg_type == ESTP_FLUSH && rp) {
        /* stage 6c: a peer asks us to flush our cached changes for it.
         * Drain synchronously and report how many remain. */
        const cJSON *jt =
            cJSON_GetObjectItemCaseSensitive(req, "target");
        char target[EDB_NODE_ID_MAX] = "";
        if (cJSON_IsString(jt) && jt->valuestring) {
            snprintf(target, sizeof(target), "%.63s", jt->valuestring);
        }
        cJSON_Delete(req);

        size_t remaining = 0;
        if (target[0]) {
            remaining = edb_repl_drain_peer(rp, target);
        }
        cJSON *ack = cJSON_CreateObject();
        if (!ack) {
            return false;
        }
        cJSON_AddBoolToObject(ack, "ok", remaining == 0);
        cJSON_AddNumberToObject(ack, "pending", (double)remaining);
        *reply_type = ESTP_ACK;
        *reply_json = json_print(ack);
        cJSON_Delete(ack);
        return *reply_json != NULL;
    }

    if (msg_type == ESTP_QUERY && rp && rp->read) {
        cJSON *res = rp->read(rp->ud, req);
        cJSON_Delete(req);
        if (!res) {
            res = cJSON_CreateObject();
            if (res) {
                cJSON_AddNullToObject(res, "row");
            }
        }
        *reply_type = ESTP_RESULT;
        *reply_json = json_print(res);
        cJSON_Delete(res);
        return *reply_json != NULL;
    }

    cJSON_Delete(req);
    return false;
}

/* ------------------------------------------------------------------ */
/* write path                                                          */

/* Extracts db/partition/keyspace/id plus put-specific fields from a
 * change document and applies it to the local engine. */
bool apply_change_local(edb_repl *rp, const cJSON *change)
{
    const cJSON *operation = cJSON_GetObjectItemCaseSensitive(change, "op");
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
    if (!cJSON_IsString(operation) || !cJSON_IsString(partition) ||
        !cJSON_IsString(keyspace) || !cJSON_IsString(id) ||
        !cJSON_IsNumber(timestamp)) {
        return false;
    }
    long long modified = (long long)timestamp->valuedouble;
    if (strcmp(operation->valuestring, "put") == 0) {
        const cJSON *value = cJSON_GetObjectItemCaseSensitive(change, "value");
        const cJSON *ttl = cJSON_GetObjectItemCaseSensitive(change, "ttl_abs");
        char *encoded = cJSON_IsObject(value) ? json_print(value) : NULL;
        if (!encoded) {
            return false;
        }
        long long absolute_ttl = cJSON_IsNumber(ttl)
                                     ? (long long)ttl->valuedouble
                                     : -1;
        bool ok = edb_replica_put_origin(
            rp->cfg_engine, partition->valuestring, keyspace->valuestring,
            id->valuestring, encoded, absolute_ttl, modified, origin);
        free(encoded);
        return ok;
    }
    return strcmp(operation->valuestring, "delete") == 0 &&
           edb_replica_delete_origin(
               rp->cfg_engine, partition->valuestring, keyspace->valuestring,
               id->valuestring, modified, origin);
}

int replication_factor(edb_repl *rp, const char *db)
{
    edb_database_info info;
    if (rp && rp->cfg && edb_database_get(rp->cfg, db, &info) &&
        info.replication_factor > 0) {
        return info.replication_factor;
    }
    return 1;
}

size_t holder_ids(edb_repl *rp, const char *partition,
                         const char *keyspace, int rf,
                         char holders[MAX_PEERS_SNAPSHOT][EDB_NODE_ID_MAX])
{
    char path[1024];
    char key[33];
    if (!edb_shard_path(rp->cfg_engine, partition, keyspace, path,
                        sizeof(path), key)) {
        return 0;
    }
    char candidates[MAX_PEERS_SNAPSHOT][EDB_NODE_ID_MAX];
    size_t count = edb_cluster_holders(rp->cluster, key, candidates,
                                       MAX_PEERS_SNAPSHOT);
    edb_peer_info peers[MAX_PEERS_SNAPSHOT];
    size_t npeers =
        edb_cluster_peers(rp->cluster, peers, MAX_PEERS_SNAPSHOT);
    size_t used = 0;
    for (int online_pass = 1; online_pass >= 0 && used < (size_t)rf;
         online_pass--) {
        for (size_t i = 0; i < count && used < (size_t)rf; i++) {
            bool online = strcmp(candidates[i], rp->self_id) == 0;
            for (size_t p = 0; p < npeers && !online; p++) {
                if (strcmp(peers[p].id, candidates[i]) == 0) {
                    online = peers[p].online;
                }
            }
            if (online != (online_pass != 0)) {
                continue;
            }
            snprintf(holders[used], EDB_NODE_ID_MAX, "%s", candidates[i]);
            used++;
        }
    }
    return used;
}

bool id_in_holders(const char *id,
                          char holders[MAX_PEERS_SNAPSHOT][EDB_NODE_ID_MAX],
                          size_t count)
{
    for (size_t i = 0; i < count; i++) {
        if (strcmp(id, holders[i]) == 0) {
            return true;
        }
    }
    return false;
}


edb_repl_status edb_repl_write(edb_repl *rp, const char *db,
                               const char *change_json)
{
    if (!rp || !db || !change_json) {
        return EDB_REPL_LOCAL_FAIL;
    }
    cJSON *change = cJSON_Parse(change_json);
    if (!change) {
        return EDB_REPL_LOCAL_FAIL;
    }
    const cJSON *partition =
        cJSON_GetObjectItemCaseSensitive(change, "partition");
    const cJSON *keyspace =
        cJSON_GetObjectItemCaseSensitive(change, "keyspace");
    if (!cJSON_IsString(partition) || !partition->valuestring ||
        !cJSON_IsString(keyspace) || !keyspace->valuestring) {
        cJSON_Delete(change);
        return EDB_REPL_LOCAL_FAIL;
    }

    pthread_mutex_lock(&rp->replay_lock);
    long long seq = ++rp->change_seq;
    pthread_mutex_unlock(&rp->replay_lock);
    char cid[96];
    snprintf(cid, sizeof(cid), "%s-%lld-%lld", rp->self_id, repl_epoch_now(),
             seq);
    cJSON_AddStringToObject(change, "cid", cid);
    if (!cJSON_GetObjectItemCaseSensitive(change, "origin")) {
        cJSON_AddStringToObject(change, "origin", rp->self_id);
    }
    char *payload = json_print(change);
    if (!payload) {
        cJSON_Delete(change);
        return EDB_REPL_LOCAL_FAIL;
    }

    int rf = replication_factor(rp, db);
    char holders[MAX_PEERS_SNAPSHOT][EDB_NODE_ID_MAX];
    size_t nholders = holder_ids(rp, partition->valuestring,
                                 keyspace->valuestring, rf, holders);
    if (nholders == 0) {
        snprintf(holders[0], EDB_NODE_ID_MAX, "%s", rp->self_id);
        nholders = 1;
    }
    int required = (int)nholders / 2 + 1;
    bool self_holder = id_in_holders(rp->self_id, holders, nholders);

    edb_peer_info peers[MAX_PEERS_SNAPSHOT];
    size_t npeers =
        edb_cluster_peers(rp->cluster, peers, MAX_PEERS_SNAPSHOT);
    bool delivered[MAX_PEERS_SNAPSHOT] = {0};
    int acknowledgements = self_holder ? 1 : 0;

    for (size_t i = 0; i < npeers; i++) {
        if (strcmp(peers[i].id, rp->self_id) == 0) {
            delivered[i] = true;
            continue;
        }
        if (!peers[i].online || !peers[i].addr[0] || peers[i].port <= 0) {
            continue;
        }
        char *reply = NULL;
        int type = rpc_once(peers[i].addr, peers[i].port, ESTP_REPL, payload,
                            ESTP_ACK, &reply);
        if (type == ESTP_ACK && ack_ok(reply)) {
            delivered[i] = true;
            if (id_in_holders(peers[i].id, holders, nholders)) {
                acknowledgements++;
            }
        }
        free(reply);
    }

    if (acknowledgements < required) {
        edb_log("WARN",
                "quorum lost writing '%s': %d/%d holders reached",
                db, acknowledgements, required);
        for (size_t i = 0; i < npeers; i++) {
            if (!delivered[i] && strcmp(peers[i].id, rp->self_id) != 0 &&
                peers[i].id[0]) {
                char *copy = strdup(payload);
                if (copy) {
                    cache_append(&rp->cache, peers[i].id, cid, copy);
                }
            }
        }
        free(payload);
        cJSON_Delete(change);
        return EDB_REPL_QUORUM_LOST;
    }

    if (!apply_change_local(rp, change)) {
        for (size_t i = 0; i < npeers; i++) {
            if (!delivered[i] && strcmp(peers[i].id, rp->self_id) != 0 &&
                peers[i].id[0]) {
                char *copy = strdup(payload);
                if (copy) {
                    cache_append(&rp->cache, peers[i].id, cid, copy);
                }
            }
        }
        free(payload);
        cJSON_Delete(change);
        return EDB_REPL_LOCAL_FAIL;
    }
    cJSON_Delete(change);

    for (size_t i = 0; i < npeers; i++) {
        if (!delivered[i] && strcmp(peers[i].id, rp->self_id) != 0 &&
            peers[i].id[0]) {
            char *copy = strdup(payload);
            if (copy) {
                cache_append(&rp->cache, peers[i].id, cid, copy);
            }
        }
    }
    free(payload);
    return EDB_REPL_OK;
}

/* ------------------------------------------------------------------ */
/* replay                                                              */

/* Sends one cached change right now. Returns true when acknowledged. */
static bool deliver_cached(edb_repl *rp, const edb_peer_info *peer,
                           const char *cid, const char *payload)
{
    char *reply = NULL;
    int t = rpc_once(peer->addr, peer->port, ESTP_REPL, payload, ESTP_ACK,
                     &reply);
    bool ok = (t == ESTP_ACK) && ack_ok(reply);
    free(reply);
    if (ok) {
        cache_remove(&rp->cache, peer->id, cid);
        return true;
    }
    return false;
}


/* Resolves a node id to its current dialable peer info. Returns false
 * when unknown or not dialable. */
bool find_peer(edb_repl *rp, const char *node_id, edb_peer_info *out)
{
    edb_peer_info peers[MAX_PEERS_SNAPSHOT];
    size_t n = edb_cluster_peers(rp->cluster, peers, MAX_PEERS_SNAPSHOT);
    for (size_t i = 0; i < n; i++) {
        if (strcmp(peers[i].id, node_id) == 0 &&
            peers[i].addr[0] != '\0' && peers[i].port > 0) {
            *out = peers[i];
            return true;
        }
    }
    return false;
}

size_t edb_repl_pending_for(edb_repl *rp, const char *node_id)
{
    if (!rp || !node_id) {
        return 0;
    }
    /* cache_load gives us at most 256; count directly for accuracy */
    if (!rp->cache.db) {
        return 0;
    }
    pthread_mutex_lock(&rp->cache.lock);
    sqlite3_stmt *stmt = NULL;
    size_t count = 0;
    if (sqlite3_prepare_v2(rp->cache.db,
                           "SELECT COUNT(*) FROM PendingChanges"
                           " WHERE target = ?",
                           -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, node_id, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            count = (size_t)sqlite3_column_int64(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }
    pthread_mutex_unlock(&rp->cache.lock);
    return count;
}

size_t edb_repl_pending_total(edb_repl *rp)
{
    if (!rp || !rp->cache.db) {
        return 0;
    }
    pthread_mutex_lock(&rp->cache.lock);
    sqlite3_stmt *stmt = NULL;
    size_t count = 0;
    if (sqlite3_prepare_v2(rp->cache.db,
                           "SELECT COUNT(*) FROM PendingChanges",
                           -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            count = (size_t)sqlite3_column_int64(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }
    pthread_mutex_unlock(&rp->cache.lock);
    return count;
}

size_t edb_repl_drain_peer(edb_repl *rp, const char *node_id)
{
    if (!rp || !node_id) {
        return 0;
    }
    /* single-flight: the maintenance thread may already be draining this
     * peer; if so, report what is left rather than racing it */
    pthread_mutex_lock(&rp->replay_lock);
    if (rp->replaying[0] != '\0' &&
        strcmp(rp->replaying, node_id) == 0) {
        pthread_mutex_unlock(&rp->replay_lock);
        return edb_repl_pending_for(rp, node_id);
    }
    snprintf(rp->replaying, sizeof(rp->replaying), "%s", node_id);
    pthread_mutex_unlock(&rp->replay_lock);

    edb_peer_info peer;
    if (!find_peer(rp, node_id, &peer)) {
        pthread_mutex_lock(&rp->replay_lock);
        rp->replaying[0] = '\0';
        pthread_mutex_unlock(&rp->replay_lock);
        return edb_repl_pending_for(rp, node_id);
    }

    /* drain in batches until the queue is empty or delivery stalls */
    for (int round = 0; round < 64; round++) {
        size_t count = 0;
        pending_row *rows =
            cache_load(&rp->cache, node_id, 256, &count);
        if (!rows || count == 0) {
            free_rows(rows, 0);
            break;
        }
        bool progress = false;
        for (size_t i = 0; i < count; i++) {
            if (deliver_cached(rp, &peer, rows[i].cid,
                               rows[i].payload)) {
                progress = true;
            } else {
                break;   /* stall: retry on a later flush/tick */
            }
        }
        free_rows(rows, count);
        if (!progress) {
            break;
        }
    }

    pthread_mutex_lock(&rp->replay_lock);
    rp->replaying[0] = '\0';
    pthread_mutex_unlock(&rp->replay_lock);
    return edb_repl_pending_for(rp, node_id);
}

/* Drains the queue for one peer. Single-flight per peer via
 * rp->replaying. Runs on the maintenance thread only. */
static void replay_for_peer(edb_repl *rp, const edb_peer_info *peer)
{
    pthread_mutex_lock(&rp->replay_lock);
    if (strcmp(rp->replaying, peer->id) == 0) {
        pthread_mutex_unlock(&rp->replay_lock);
        return;
    }
    snprintf(rp->replaying, sizeof(rp->replaying), "%.63s", peer->id);
    pthread_mutex_unlock(&rp->replay_lock);

    size_t count = 0;
    pending_row *rows = cache_load(&rp->cache, peer->id, 256, &count);
    size_t delivered = 0;
    for (size_t i = 0; i < count; i++) {
        if (peer->addr[0] == '\0' || peer->port <= 0) {
            break;
        }
        if (deliver_cached(rp, peer, rows[i].cid, rows[i].payload)) {
            delivered++;
        } else {
            break;   /* stop at first failure: try again next tick */
        }
    }
    free_rows(rows, count);
    if (count && delivered == count) {
        edb_log("INFO", "replayed %zu cached changes to %s", delivered,
                peer->id);
    }

    pthread_mutex_lock(&rp->replay_lock);
    rp->replaying[0] = '\0';
    pthread_mutex_unlock(&rp->replay_lock);
}

static void *repl_maint_main(void *arg)
{
    edb_repl *rp = arg;
    for (;;) {
        pthread_mutex_lock(&rp->sync_lock);
        bool running = rp->running;
        pthread_mutex_unlock(&rp->sync_lock);
        if (!running) {
            break;
        }
        repl_sleep_ms(REPL_TICK_MS);
        edb_peer_info peers[MAX_PEERS_SNAPSHOT];
        size_t count = edb_cluster_peers(rp->cluster, peers,
                                         MAX_PEERS_SNAPSHOT);
        for (size_t i = 0; i < count; i++) {
            if (peers[i].online && strcmp(peers[i].id, rp->self_id) != 0) {
                replay_for_peer(rp, &peers[i]);
            }
        }
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/* quorum reads                                                        */

/* How many responding replicas must agree. Responding-based so losing
 * followers degrades availability instead of failing reads. */

/* Sends a QUERY to all other online peers; returns how many replied and
 * fills replies[] with parsed result documents (caller frees). */
void edb_repl_set_syncing(edb_repl *rp, bool syncing)
{
    if (!rp) {
        return;
    }
    pthread_mutex_lock(&rp->sync_lock);
    rp->syncing = syncing;
    pthread_mutex_unlock(&rp->sync_lock);
}

/* Sends a FLUSH request to every online, dialable peer asking it to
 * drain its cached changes for us; loops until all report an empty
 * queue or the deadline passes. Returns true when every peer reported
 * pending == 0. */
bool edb_repl_flush(edb_repl *rp)
{
    if (!rp) {
        return false;
    }
    long long deadline = repl_mono_ms() + 20000;
    for (;;) {
        edb_peer_info peers[MAX_PEERS_SNAPSHOT];
        size_t count = edb_cluster_peers(rp->cluster, peers,
                                         MAX_PEERS_SNAPSHOT);
        bool all_empty = true;
        for (size_t i = 0; i < count; i++) {
            if (strcmp(peers[i].id, rp->self_id) == 0 || !peers[i].online) {
                continue;
            }
            if (!peers[i].addr[0] || peers[i].port <= 0) {
                all_empty = false;
                continue;
            }
            char request[96];
            snprintf(request, sizeof(request), "{\"target\":\"%.63s\"}",
                     rp->self_id);
            char *reply = NULL;
            int type = rpc_once(peers[i].addr, peers[i].port, ESTP_FLUSH,
                                request, ESTP_ACK, &reply);
            bool valid_empty = false;
            if (type == ESTP_ACK && reply) {
                cJSON *doc = cJSON_Parse(reply);
                const cJSON *ok = doc
                                      ? cJSON_GetObjectItemCaseSensitive(doc,
                                                                         "ok")
                                      : NULL;
                const cJSON *pending =
                    doc ? cJSON_GetObjectItemCaseSensitive(doc, "pending")
                        : NULL;
                valid_empty = cJSON_IsTrue(ok) && cJSON_IsNumber(pending) &&
                              pending->valuedouble == 0;
                cJSON_Delete(doc);
            }
            free(reply);
            if (!valid_empty) {
                all_empty = false;
            }
        }
        if (all_empty) {
            return true;
        }
        if (repl_mono_ms() >= deadline) {
            return false;
        }
        repl_sleep_ms(100);
    }
}

static bool repl_catchup(edb_repl *rp, const char *owner_addr,
                         int owner_port, const char *partition,
                         const char *keyspace, bool source_required)
{
    if (!rp || !owner_addr || !partition || !keyspace) {
        return false;
    }

    edb_repl_set_syncing(rp, true);
    char key[33];
    char path[1024];
    if (!edb_shard_path(rp->cfg_engine, partition, keyspace, path,
                        sizeof(path), key)) {
        edb_repl_set_syncing(rp, false);
        return false;
    }
    int snapshot_rc = source_required
                          ? edb_snap_fetch_required(owner_addr, owner_port, key,
                                                    rp->data_dir)
                          : edb_snap_fetch(owner_addr, owner_port, key,
                                           rp->data_dir);
    if (snapshot_rc != 0 ||
        !edb_shard_invalidate(rp->cfg_engine, partition, keyspace)) {
        edb_repl_set_syncing(rp, false);
        return false;
    }

    edb_repl_set_syncing(rp, false);
    return edb_repl_flush(rp);
}

bool edb_repl_catchup(edb_repl *rp, const char *owner_addr, int owner_port,
                      const char *partition, const char *keyspace)
{
    return repl_catchup(rp, owner_addr, owner_port, partition, keyspace,
                        false);
}

bool edb_repl_catchup_required(edb_repl *rp, const char *owner_addr,
                               int owner_port, const char *partition,
                               const char *keyspace)
{
    return repl_catchup(rp, owner_addr, owner_port, partition, keyspace,
                        true);
}

/* ------------------------------------------------------------------ */
/* lifecycle                                                           */

void edb_repl_set_handlers(edb_repl *rp, edb_repl_apply_fn apply,
                           edb_repl_read_fn read, void *ud)
{
    if (!rp) {
        return;
    }
    rp->apply = apply;
    rp->read = read;
    rp->ud = ud;
}

edb_repl *edb_repl_start(edb_cluster *cluster, edb_config *cfg,
                         const char *data_dir)
{
    if (!cluster || !cfg || !data_dir) {
        return NULL;
    }
    edb_repl *rp = calloc(1, sizeof(*rp));
    if (!rp) {
        return NULL;
    }
    rp->cluster = cluster;
    rp->cfg = cfg;
    rp->cfg_engine = edb_config_engine(cfg);
    snprintf(rp->self_id, sizeof(rp->self_id), "%s",
             edb_cluster_self_id(cluster));
    snprintf(rp->data_dir, sizeof(rp->data_dir), "%s", data_dir);
    pthread_mutex_init(&rp->sync_lock, NULL);
    pthread_mutex_init(&rp->replay_lock, NULL);
    rp->syncing = false;

    if (!cache_open(&rp->cache, data_dir)) {
        edb_log("WARN", "change cache unavailable; writes will not"
                        " be cached for offline nodes");
    }

    /* install the per-cluster dispatcher before the maintenance thread
     * starts servicing frames */
    edb_cluster_set_dispatcher(cluster, repl_dispatch, rp);

    /* set running before the thread starts: the loop checks it first
     * thing and would exit immediately on a lost race otherwise */
    rp->running = true;
    if (pthread_create(&rp->maint_thread, NULL, repl_maint_main, rp) != 0) {
        rp->running = false;
        edb_cluster_set_dispatcher(cluster, NULL, NULL);
        cache_close(&rp->cache);
        pthread_mutex_destroy(&rp->sync_lock);
        pthread_mutex_destroy(&rp->replay_lock);
        free(rp);
        return NULL;
    }

    return rp;
}

void edb_repl_stop(edb_repl *rp)
{
    if (!rp) {
        return;
    }
    pthread_mutex_lock(&rp->sync_lock);
    rp->running = false;
    pthread_mutex_unlock(&rp->sync_lock);
    pthread_join(rp->maint_thread, NULL);
    edb_cluster_set_dispatcher(rp->cluster, NULL, NULL);
    cache_close(&rp->cache);
    pthread_mutex_destroy(&rp->sync_lock);
    pthread_mutex_destroy(&rp->replay_lock);
    free(rp);
}