/* zesty_repl.c - stage 5 replication implementation. See zesty_repl.h.
 *
 * Transport: every fan-out message uses its own short-lived peer
 * connection (dial, HELLO, payload frame, reply frame, close). This
 * keeps the data plane off the mesh connection objects, whose lifecycle
 * is owned by the stage 4 reader threads. Inbound REPL/QUERY frames on
 * mesh connections are answered through the dispatcher hook installed
 * by zdb_repl_start, so both transports work.
 *
 * Change cache: a dedicated sqlite database "changes.sqlite" in the
 * data directory records every change that could not be acknowledged by
 * its target peer. A maintenance thread watches peer online/offline
 * transitions and drains each peer's queue in order when it returns;
 * queues also drain at startup for peers already online (crash
 * recovery). Acknowledged changes are deleted; LWW on both sides makes
 * replays idempotent.
 */

#include "zesty_repl.h"

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
#include "../engine/zesty_engine.h"
#include "../sqlite/sqlite3.h"
#include "zstp_wire.h"
#include "zesty_snap.h"

#define MAX_PEERS_SNAPSHOT 64
#define REPL_CONNECT_TIMEOUT_MS 1500
#define REPL_FANOUT_DEADLINE_MS 2000
#define REPL_TICK_MS            500

/* ------------------------------------------------------------------ */
/* small helpers                                                       */

static long long epoch_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (long long)ts.tv_sec;
}

static long long mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void sleep_ms(int ms)
{
    struct timespec ts = { .tv_sec = ms / 1000,
                           .tv_nsec = (long)(ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

static char *json_print(const cJSON *doc)
{
    return doc ? cJSON_PrintUnformatted(doc) : NULL;
}

/* Bounded socket waits so a hung peer cannot stall the caller forever */
static void set_socket_timeouts(int fd, int ms)
{
    struct timeval tv = { .tv_sec = ms / 1000,
                          .tv_usec = (ms % 1000) * 1000 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

/* One request/reply exchange on a private connection. Returns the
 * reply type and malloc'd payload, or -1/NULL on any failure. */
static int rpc_once(const char *addr, int port, zstp_type send_type,
                    const char *payload, zstp_type want_type,
                    char **reply_out)
{
    *reply_out = NULL;
    int fd = zstp_dial(addr, port);
    if (fd < 0) {
        return -1;
    }
    set_socket_timeouts(fd, REPL_CONNECT_TIMEOUT_MS);

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
        zstp_send_frame(fd, ZSTP_HELLO, hello_str, NULL) != 0) {
        free(hello_str);
        goto done;
    }
    free(hello_str);

    if (zstp_send_frame(fd, send_type, payload, NULL) != 0) {
        goto done;
    }

    /* absorb their HELLO, then wait for the actual reply */
    long long deadline = mono_ms() + REPL_FANOUT_DEADLINE_MS;
    for (;;) {
        long long left = deadline - mono_ms();
        if (left <= 0) {
            goto done;
        }
        char *in = NULL;
        int t = zstp_recv_frame(fd, &in);
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
/* change cache (persisted sqlite log of unacknowledged changes)       */

typedef struct {
    sqlite3 *db;
    pthread_mutex_t lock;
} change_cache;

static bool cache_open(change_cache *cc, const char *data_dir)
{
    size_t len = strlen(data_dir) + sizeof("/changes.sqlite");
    char *path = malloc(len);
    if (!path) {
        return false;
    }
    snprintf(path, len, "%s/changes.sqlite", data_dir);
    int rc = sqlite3_open_v2(path, &cc->db,
                             SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
                                 SQLITE_OPEN_FULLMUTEX,
                             NULL);
    free(path);
    if (rc != SQLITE_OK) {
        if (cc->db) {
            sqlite3_close(cc->db);
            cc->db = NULL;
        }
        return false;
    }
    sqlite3_busy_timeout(cc->db, 5000);
    char *err = NULL;
    if (sqlite3_exec(cc->db,
                     "PRAGMA journal_mode=DELETE;"
                     "PRAGMA synchronous=FULL;"
                     "CREATE TABLE IF NOT EXISTS PendingChanges ("
                     " seq INTEGER PRIMARY KEY AUTOINCREMENT,"
                     " target TEXT NOT NULL,"
                     " cid TEXT NOT NULL UNIQUE,"
                     " payload TEXT NOT NULL,"
                     " created INT NOT NULL"
                     ");",
                     NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "zdb: change cache init failed: %s\n",
                err ? err : "?");
        sqlite3_free(err);
        sqlite3_close(cc->db);
        cc->db = NULL;
        return false;
    }
    pthread_mutex_init(&cc->lock, NULL);
    return true;
}

static void cache_close(change_cache *cc)
{
    if (!cc->db) {
        return;
    }
    pthread_mutex_lock(&cc->lock);
    sqlite3_close(cc->db);
    cc->db = NULL;
    pthread_mutex_unlock(&cc->lock);
    pthread_mutex_destroy(&cc->lock);
}

/* Takes ownership of payload. */
static void cache_append(change_cache *cc, const char *target,
                         const char *cid, const char *payload)
{
    if (!cc->db) {
        free((void *)payload);
        return;
    }
    pthread_mutex_lock(&cc->lock);
    sqlite3_stmt *stmt = NULL;
    bool ok =
        sqlite3_prepare_v2(cc->db,
                           "INSERT OR REPLACE INTO PendingChanges"
                           " (target, cid, payload, created)"
                           " VALUES (?, ?, ?, ?)",
                           -1, &stmt, NULL) == SQLITE_OK;
    if (ok) {
        sqlite3_bind_text(stmt, 1, target, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, cid, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 3, payload, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt, 4, epoch_now());
        if (sqlite3_step(stmt) != SQLITE_DONE) {
            fprintf(stderr, "zdb: change cache insert failed: %s\n",
                    sqlite3_errmsg(cc->db));
        }
    }
    if (stmt) {
        sqlite3_finalize(stmt);
    }
    pthread_mutex_unlock(&cc->lock);
    free((void *)payload);
}

static void cache_remove(change_cache *cc, const char *target,
                         const char *cid)
{
    if (!cc->db) {
        return;
    }
    pthread_mutex_lock(&cc->lock);
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(cc->db,
                           "DELETE FROM PendingChanges"
                           " WHERE target = ? AND cid = ?",
                           -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, target, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, cid, -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
    pthread_mutex_unlock(&cc->lock);
}

/* ------------------------------------------------------------------ */
/* replication service state                                           */

struct zdb_repl {
    zdb_cluster *cluster;
    zdb_config *cfg;
    zdb_engine *cfg_engine;   /* engine behind cfg, for direct calls */

    change_cache cache;

    zdb_repl_apply_fn apply;
    zdb_repl_read_fn read;
    void *ud;

    char self_id[ZDB_NODE_ID_MAX];
    char data_dir[512];
    long long change_seq;

    bool running;
    pthread_t maint_thread;

    /* stage 6c: syncing gate (see zdb_repl_set_syncing) */
    pthread_mutex_t sync_lock;
    bool syncing;

    /* per-peer replay bookkeeping, guarded by replay_lock */
    pthread_mutex_t replay_lock;
    char replaying[ZDB_NODE_ID_MAX];   /* node currently being drained */
};

/* ------------------------------------------------------------------ */
/* dispatcher: answer inbound REPL / QUERY / FLUSH frames              */

/* True when an ACK payload means "delivered" (ok present and true, or
 * the field is absent for older peers that never sent it). */
static bool ack_ok(const char *reply)
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
    zdb_repl *rp = ctx;
    *reply_json = NULL;
    cJSON *req = cJSON_Parse(payload);
    if (!req) {
        return false;
    }

    if (msg_type == ZSTP_REPL && rp && rp->apply) {
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
        *reply_type = ZSTP_ACK;
        *reply_json = json_print(ack);
        cJSON_Delete(ack);
        return *reply_json != NULL;
    }

    if (msg_type == ZSTP_FLUSH && rp) {
        /* stage 6c: a peer asks us to flush our cached changes for it.
         * Drain synchronously and report how many remain. */
        const cJSON *jt =
            cJSON_GetObjectItemCaseSensitive(req, "target");
        char target[ZDB_NODE_ID_MAX] = "";
        if (cJSON_IsString(jt) && jt->valuestring) {
            snprintf(target, sizeof(target), "%.63s", jt->valuestring);
        }
        cJSON_Delete(req);

        size_t remaining = 0;
        if (target[0]) {
            remaining = zdb_repl_drain_peer(rp, target);
        }
        cJSON *ack = cJSON_CreateObject();
        if (!ack) {
            return false;
        }
        cJSON_AddBoolToObject(ack, "ok", remaining == 0);
        cJSON_AddNumberToObject(ack, "pending", (double)remaining);
        *reply_type = ZSTP_ACK;
        *reply_json = json_print(ack);
        cJSON_Delete(ack);
        return *reply_json != NULL;
    }

    if (msg_type == ZSTP_QUERY && rp && rp->read) {
        cJSON *res = rp->read(rp->ud, req);
        cJSON_Delete(req);
        if (!res) {
            res = cJSON_CreateObject();
            if (res) {
                cJSON_AddNullToObject(res, "row");
            }
        }
        *reply_type = ZSTP_RESULT;
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
static bool apply_change_local(zdb_repl *rp, const cJSON *change)
{
    const cJSON *jop =
        cJSON_GetObjectItemCaseSensitive(change, "op");
    const cJSON *jdb =
        cJSON_GetObjectItemCaseSensitive(change, "db");
    const cJSON *jpart =
        cJSON_GetObjectItemCaseSensitive(change, "partition");
    const cJSON *jks =
        cJSON_GetObjectItemCaseSensitive(change, "keyspace");
    const cJSON *jid = cJSON_GetObjectItemCaseSensitive(change, "id");
    const cJSON *jts = cJSON_GetObjectItemCaseSensitive(change, "ts");

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
        char *value_json = json_print(jval);
        if (!value_json) {
            free(filters);
            return false;
        }
        long long ttl_abs = cJSON_IsNumber(jttl)
                                ? (long long)jttl->valuedouble
                                : -1;
        ok = zdb_replica_put(rp->cfg_engine, jpart->valuestring,
                             jks->valuestring, jid->valuestring,
                             value_json, ttl_abs, ts, filters, nfilters);
        free(value_json);
    } else if (strcmp(jop->valuestring, "delete") == 0) {
        ok = zdb_replica_delete(rp->cfg_engine, jpart->valuestring,
                                jks->valuestring, jid->valuestring, ts);
    } else {
        ok = false;
    }
    free(filters);
    return ok;
}

zdb_repl_status zdb_repl_write(zdb_repl *rp, const char *db,
                               const char *change_json)
{
    if (!rp || !change_json) {
        return ZDB_REPL_LOCAL_FAIL;
    }
    cJSON *change = cJSON_Parse(change_json);
    if (!change) {
        return ZDB_REPL_LOCAL_FAIL;
    }

    /* 1. local application first: this node is always one holder */
    if (!apply_change_local(rp, change)) {
        cJSON_Delete(change);
        return ZDB_REPL_LOCAL_FAIL;
    }

    /* unique change id for dedup across retries/replays */
    char cid[96];
    pthread_mutex_lock(&rp->replay_lock);
    long long seq = ++rp->change_seq;
    pthread_mutex_unlock(&rp->replay_lock);
    snprintf(cid, sizeof(cid), "%s-%lld-%lld", rp->self_id, epoch_now(),
             seq);
    cJSON_AddStringToObject(change, "cid", cid);

    char *payload = json_print(change);
    cJSON_Delete(change);
    if (!payload) {
        return ZDB_REPL_LOCAL_FAIL;
    }

    /* 2. fan out to every other online peer */
    zdb_peer_info peers[MAX_PEERS_SNAPSHOT];
    size_t npeers =
        zdb_cluster_peers(rp->cluster, peers, MAX_PEERS_SNAPSHOT);

    int holders = 1;   /* local */
    for (size_t i = 0; i < npeers; i++) {
        if (strcmp(peers[i].id, rp->self_id) == 0) {
            continue;
        }
        if (peers[i].addr[0] == '\0' || peers[i].port <= 0) {
            continue;   /* no dialable address; nothing we can cache */
        }
        if (!peers[i].online) {
            /* 3. offline: persist for replay when it returns */
            cache_append(&rp->cache, peers[i].id, cid, strdup(payload));
            continue;
        }
        char *reply = NULL;
        int t = rpc_once(peers[i].addr, peers[i].port, ZSTP_REPL,
                         payload, ZSTP_ACK, &reply);
        if (t == ZSTP_ACK && ack_ok(reply)) {
            holders++;
            free(reply);
            continue;
        }
        free(reply);
        /* 3. unacknowledged (or refused: syncing peer): persist for
         * later replay */
        cache_append(&rp->cache, peers[i].id, cid, strdup(payload));
    }
    free(payload);

    /* 4. write policy: reject unless rf/2+1 holders are current */
    int rf = 1;
    zdb_database_info info;
    if (rp->cfg && zdb_database_get(rp->cfg, db, &info) &&
        info.replication_factor > 1) {
        rf = info.replication_factor;
    }
    int required = rf / 2 + 1;
    if (holders < required) {
        fprintf(stderr,
                "zdb: quorum lost writing '%s': %d/%d holders reached\n",
                db, holders, required);
        return ZDB_REPL_QUORUM_LOST;
    }
    return ZDB_REPL_OK;
}

/* ------------------------------------------------------------------ */
/* replay                                                              */

/* Sends one cached change right now. Returns true when acknowledged. */
static bool deliver_cached(zdb_repl *rp, const zdb_peer_info *peer,
                           const char *cid, const char *payload)
{
    char *reply = NULL;
    int t = rpc_once(peer->addr, peer->port, ZSTP_REPL, payload, ZSTP_ACK,
                     &reply);
    bool ok = (t == ZSTP_ACK) && ack_ok(reply);
    free(reply);
    if (ok) {
        cache_remove(&rp->cache, peer->id, cid);
        return true;
    }
    return false;
}

typedef struct {
    char target[ZDB_NODE_ID_MAX];
    char cid[96];
    char *payload;   /* malloc'd */
} pending_row;

/* Loads at most `limit` pending rows for target, oldest first.
 * Returns a malloc'd array; caller frees payloads + array. */
static pending_row *cache_load(change_cache *cc, const char *target,
                               size_t limit, size_t *count_out)
{
    *count_out = 0;
    if (!cc->db) {
        return NULL;
    }
    pending_row *rows = malloc(limit * sizeof(*rows));
    if (!rows) {
        return NULL;
    }
    pthread_mutex_lock(&cc->lock);
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(cc->db,
                           "SELECT target, cid, payload FROM"
                           " PendingChanges WHERE target = ?"
                           " ORDER BY seq LIMIT ?",
                           -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, target, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt, 2, (sqlite3_int64)limit);
        while (*count_out < limit &&
               sqlite3_step(stmt) == SQLITE_ROW) {
            pending_row *r = &rows[*count_out];
            memset(r, 0, sizeof(*r));
            const char *tgt = (const char *)sqlite3_column_text(stmt, 0);
            const char *cid = (const char *)sqlite3_column_text(stmt, 1);
            const char *pay = (const char *)sqlite3_column_text(stmt, 2);
            if (tgt) {
                snprintf(r->target, sizeof(r->target), "%s", tgt);
            }
            if (cid) {
                snprintf(r->cid, sizeof(r->cid), "%s", cid);
            }
            r->payload = pay ? strdup(pay) : NULL;
            (*count_out)++;
        }
        sqlite3_finalize(stmt);
    }
    pthread_mutex_unlock(&cc->lock);
    return rows;
}

static void free_rows(pending_row *rows, size_t count)
{
    for (size_t i = 0; i < count; i++) {
        free(rows[i].payload);
    }
    free(rows);
}

/* Resolves a node id to its current dialable peer info. Returns false
 * when unknown or not dialable. */
static bool find_peer(zdb_repl *rp, const char *node_id, zdb_peer_info *out)
{
    zdb_peer_info peers[MAX_PEERS_SNAPSHOT];
    size_t n = zdb_cluster_peers(rp->cluster, peers, MAX_PEERS_SNAPSHOT);
    for (size_t i = 0; i < n; i++) {
        if (strcmp(peers[i].id, node_id) == 0 &&
            peers[i].addr[0] != '\0' && peers[i].port > 0) {
            *out = peers[i];
            return true;
        }
    }
    return false;
}

size_t zdb_repl_pending_for(zdb_repl *rp, const char *node_id)
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

size_t zdb_repl_drain_peer(zdb_repl *rp, const char *node_id)
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
        return zdb_repl_pending_for(rp, node_id);
    }
    snprintf(rp->replaying, sizeof(rp->replaying), "%s", node_id);
    pthread_mutex_unlock(&rp->replay_lock);

    zdb_peer_info peer;
    if (!find_peer(rp, node_id, &peer)) {
        pthread_mutex_lock(&rp->replay_lock);
        rp->replaying[0] = '\0';
        pthread_mutex_unlock(&rp->replay_lock);
        return zdb_repl_pending_for(rp, node_id);
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
    return zdb_repl_pending_for(rp, node_id);
}

/* Drains the queue for one peer. Single-flight per peer via
 * rp->replaying. Runs on the maintenance thread only. */
static void replay_for_peer(zdb_repl *rp, const zdb_peer_info *peer)
{
    pthread_mutex_lock(&rp->replay_lock);
    if (strcmp(rp->replaying, peer->id) == 0) {
        pthread_mutex_unlock(&rp->replay_lock);
        return;
    }
    snprintf(rp->replaying, sizeof(rp->replaying), "%s", peer->id);
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
        printf("zdb: replayed %zu cached changes to %s\n", delivered,
               peer->id);
    }

    pthread_mutex_lock(&rp->replay_lock);
    rp->replaying[0] = '\0';
    pthread_mutex_unlock(&rp->replay_lock);
}

static void *repl_maint_main(void *arg)
{
    zdb_repl *rp = arg;

    zdb_peer_info prev[MAX_PEERS_SNAPSHOT];
    size_t nprev = 0;
    bool first_tick = true;

    while (rp->running) {
        sleep_ms(REPL_TICK_MS);

        zdb_peer_info cur[MAX_PEERS_SNAPSHOT];
        size_t ncur =
            zdb_cluster_peers(rp->cluster, cur, MAX_PEERS_SNAPSHOT);

        for (size_t i = 0; i < ncur; i++) {
            if (!cur[i].online) {
                continue;
            }
            const zdb_peer_info *was = NULL;
            for (size_t j = 0; j < nprev; j++) {
                if (strcmp(prev[j].id, cur[i].id) == 0) {
                    was = &prev[j];
                    break;
                }
            }
            /* newly seen online, or just came back: drain its queue */
            if ((first_tick || !was || !was->online) &&
                cur[i].online) {
                replay_for_peer(rp, &cur[i]);
            }
        }

        memcpy(prev, cur, ncur * sizeof(*cur));
        nprev = ncur;
        first_tick = false;
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/* quorum reads                                                        */

/* How many responding replicas must agree. Responding-based so losing
 * followers degrades availability instead of failing reads. */
static int quorum_required(int responders)
{
    return responders / 2 + 1;
}

/* Sends a QUERY to all other online peers; returns how many replied and
 * fills replies[] with parsed result documents (caller frees). */
#define MAX_REPLIES 16

static size_t query_all(zdb_repl *rp, const cJSON *request,
                        cJSON **replies, size_t cap)
{
    char *payload = json_print(request);
    if (!payload) {
        return 0;
    }
    size_t got = 0;
    zdb_peer_info peers[MAX_PEERS_SNAPSHOT];
    size_t npeers =
        zdb_cluster_peers(rp->cluster, peers, MAX_PEERS_SNAPSHOT);
    for (size_t i = 0; i < npeers && got < cap; i++) {
        if (!peers[i].online || peers[i].addr[0] == '\0' ||
            peers[i].port <= 0) {
            continue;
        }
        char *reply = NULL;
        if (rpc_once(peers[i].addr, peers[i].port, ZSTP_QUERY, payload,
                     ZSTP_RESULT, &reply) == ZSTP_RESULT) {
            cJSON *doc = reply ? cJSON_Parse(reply) : NULL;
            free(reply);
            if (doc) {
                replies[got++] = doc;
            }
        }
    }
    free(payload);
    return got;
}

/* Merges row arrays [{"id","timestamp","value"},..] from several
 * replicas into a plain value array containing only records where at
 * least `required` copies agree verbatim; conflicts resolve LWW. */
static cJSON *merge_agreed_rows(cJSON **row_sets, size_t nsets,
                                int required)
{
    /* collect distinct ids */
    cJSON *out = cJSON_CreateArray();
    if (!out) {
        return NULL;
    }

    typedef struct {
        char id[512];
        cJSON *best_value;      /* borrowed from winner_set */
        long long best_ts;
        int agree;
    } entry;
    entry entries[4096];
    size_t nentries = 0;

    for (size_t s = 0; s < nsets; s++) {
        const cJSON *rows = cJSON_GetObjectItem(row_sets[s], "rows");
        if (!cJSON_IsArray(rows)) {
            continue;
        }
        const cJSON *r = NULL;
        cJSON_ArrayForEach(r, rows) {
            const cJSON *jid = cJSON_GetObjectItemCaseSensitive(r, "id");
            const cJSON *jts =
                cJSON_GetObjectItemCaseSensitive(r, "timestamp");
            const cJSON *jval =
                cJSON_GetObjectItemCaseSensitive(r, "value");
            if (!cJSON_IsString(jid) || !jid->valuestring ||
                !cJSON_IsObject(jval)) {
                continue;
            }
            long long ts = cJSON_IsNumber(jts)
                               ? (long long)jts->valuedouble
                               : 0;

            entry *e = NULL;
            for (size_t k = 0; k < nentries; k++) {
                if (strcmp(entries[k].id, jid->valuestring) == 0) {
                    e = &entries[k];
                    break;
                }
            }
            if (!e) {
                if (nentries >= sizeof(entries) / sizeof(entries[0])) {
                    continue;
                }
                e = &entries[nentries++];
                memset(e, 0, sizeof(*e));
                snprintf(e->id, sizeof(e->id), "%.511s",
                         jid->valuestring);
            }

            /* agreement is decided on the canonical value string */
            char *vs = json_print(jval);
            char best_vs[64] = "";
            if (e->best_value) {
                char *tmp = json_print(e->best_value);
                snprintf(best_vs, sizeof(best_vs), "%.63s",
                         tmp ? tmp : "");
                free(tmp);
            }
            bool same = e->best_value &&
                        strlen(vs) < sizeof(best_vs) &&
                        strcmp(vs, best_vs) == 0;
            if (same) {
                e->agree++;
            }
            if (!e->best_value || ts > e->best_ts) {
                e->best_value = (cJSON *)jval;
                e->best_ts = ts;
                e->agree = same ? e->agree : 1;
            }
            free(vs);
        }
    }

    for (size_t k = 0; k < nentries; k++) {
        if (entries[k].best_value && entries[k].agree >= required) {
            cJSON *copy = cJSON_Duplicate(entries[k].best_value, 1);
            if (copy) {
                cJSON_AddItemToArray(out, copy);
            }
        }
    }
    return out;
}

/* Same merge but emits plain id strings (for the ids operation). */
static cJSON *merge_agreed_ids(char ***id_lists, size_t *counts,
                               size_t nsets, int required)
{
    typedef struct {
        char id[512];
        int seen;
    } entry;
    entry entries[8192];
    size_t nentries = 0;

    for (size_t s = 0; s < nsets; s++) {
        for (size_t i = 0; i < counts[s]; i++) {
            entry *e = NULL;
            for (size_t k = 0; k < nentries; k++) {
                if (strcmp(entries[k].id, id_lists[s][i]) == 0) {
                    e = &entries[k];
                    break;
                }
            }
            if (!e) {
                if (nentries >= sizeof(entries) / sizeof(entries[0])) {
                    continue;
                }
                e = &entries[nentries++];
                memset(e, 0, sizeof(*e));
                snprintf(e->id, sizeof(e->id), "%.511s", id_lists[s][i]);
            }
            e->seen++;
        }
    }

    cJSON *arr = cJSON_CreateArray();
    if (!arr) {
        return NULL;
    }
    for (size_t k = 0; k < nentries; k++) {
        if (entries[k].seen >= required) {
            cJSON_AddItemToArray(arr, cJSON_CreateString(entries[k].id));
        }
    }
    return arr;
}

/* Builds {"q":..,"db":..,...} request skeleton shared by all reads. */
static cJSON *make_request(const char *q, const char *db,
                           const char *partition, const char *keyspace)
{
    cJSON *o = cJSON_CreateObject();
    if (!o) {
        return NULL;
    }
    cJSON_AddStringToObject(o, "q", q);
    cJSON_AddStringToObject(o, "db", db);
    cJSON_AddStringToObject(o, "partition", partition);
    cJSON_AddStringToObject(o, "keyspace", keyspace);
    return o;
}

/* True when quorum reads should engage: clustered, rf > 1. */
static bool quorum_applies(zdb_repl *rp, const char *db)
{
    if (!rp || !rp->cluster || !rp->read) {
        return false;
    }
    zdb_database_info info;
    return zdb_database_get(rp->cfg, db, &info) &&
           info.replication_factor > 1;
}

/* Collects filter/field arrays from JSON string arrays. */
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

cJSON *zdb_repl_read_get(zdb_repl *rp, const char *db, const char *partition,
                         const char *keyspace, const char *id)
{
    /* local copy first */
    long long local_ts = 0;
    cJSON *local = (rp && rp->cfg_engine)
                       ? zdb_get_ts(rp->cfg_engine, partition, keyspace,
                                    id, &local_ts)
                       : NULL;

    if (!quorum_applies(rp, db)) {
        return local;
    }

    cJSON *req = make_request("get", db, partition, keyspace);
    if (!req) {
        return local;
    }
    cJSON_AddStringToObject(req, "id", id);

    cJSON *replies[MAX_REPLIES];
    size_t n = query_all(rp, req, replies, MAX_REPLIES);
    cJSON_Delete(req);

    if (n == 0) {
        return local;   /* no peer responded: serve local view */
    }

    /* pick the value shared by a quorum of responders; fall back to
     * newest when no exact majority exists */
    typedef struct {
        char vs[128];
        long long ts;
        cJSON *sample;
        int votes;
    } group;
    group groups[MAX_REPLIES];
    size_t ng = 0;

    int responders = n + (local ? 1 : 0);
    int required = quorum_required(responders);

    for (size_t i = 0; i < n; i++) {
        const cJSON *row = cJSON_GetObjectItem(replies[i], "row");
        if (!cJSON_IsObject(row)) {
            continue;
        }
        const cJSON *jts =
            cJSON_GetObjectItemCaseSensitive(row, "timestamp");
        const cJSON *jval =
            cJSON_GetObjectItemCaseSensitive(row, "value");
        if (!cJSON_IsObject(jval)) {
            continue;
        }
        long long ts = cJSON_IsNumber(jts)
                           ? (long long)jts->valuedouble
                           : 0;
        char *vs = json_print(jval);
        if (!vs) {
            continue;
        }
        group *g = NULL;
        for (size_t k = 0; k < ng; k++) {
            if (strcmp(groups[k].vs, vs) == 0) {
                g = &groups[k];
                break;
            }
        }
        if (!g && ng < MAX_REPLIES) {
            g = &groups[ng++];
            memset(g, 0, sizeof(*g));
            snprintf(g->vs, sizeof(g->vs), "%s", vs);
            g->sample = (cJSON *)jval;
        }
        if (g) {
            g->votes++;
            if (ts > g->ts) {
                g->ts = ts;
            }
        }
        free(vs);
    }
    if (local) {
        char *lvs = json_print(local);
        for (size_t k = 0; lvs && k < ng; k++) {
            if (strcmp(groups[k].vs, lvs) == 0) {
                groups[k].votes++;
                if (local_ts > groups[k].ts) {
                    groups[k].ts = local_ts;
                }
            }
        }
        free(lvs);
    }

    group *winner = NULL;
    for (size_t k = 0; k < ng; k++) {
        if (groups[k].votes >= required &&
            (!winner || groups[k].ts > winner->ts)) {
            winner = &groups[k];
        }
    }
    if (!winner) {
        for (size_t k = 0; k < ng; k++) {
            if (!winner || groups[k].ts > winner->ts) {
                winner = &groups[k];
            }
        }
    }

    cJSON *result = winner ? cJSON_Duplicate(winner->sample, 1) : NULL;
    for (size_t i = 0; i < n; i++) {
        cJSON_Delete(replies[i]);
    }
    cJSON_Delete(local);
    return result;
}

cJSON *zdb_repl_read_all(zdb_repl *rp, const char *db, const char *partition,
                         const char *keyspace, const char **filters,
                         size_t nfilters)
{
    if (!quorum_applies(rp, db)) {
        return rp ? zdb_all(rp->cfg_engine, partition, keyspace, filters,
                            nfilters)
                  : NULL;
    }

    cJSON *req = make_request("all_ts", db, partition, keyspace);
    if (!req) {
        return NULL;
    }

    cJSON *sets[MAX_REPLIES + 1];
    size_t n = 0;

    /* local set in the same wire shape */
    cJSON *local_rows = zdb_all_ts(rp->cfg_engine, partition, keyspace,
                                   filters, nfilters);
    if (local_rows) {
        cJSON *wrap = cJSON_CreateObject();
        if (wrap) {
            cJSON_AddItemToObject(wrap, "rows", local_rows);
            sets[n++] = wrap;
        } else {
            cJSON_Delete(local_rows);
        }
    }

    if (req) {
        cJSON *farr = cJSON_AddArrayToObject(req, "filters");
        for (size_t i = 0; farr && i < nfilters; i++) {
            cJSON_AddItemToArray(farr, cJSON_CreateString(filters[i]));
        }
        cJSON *replies[MAX_REPLIES];
        size_t got = query_all(rp, req, replies, MAX_REPLIES);
        cJSON_Delete(req);
        for (size_t i = 0; i < got; i++) {
            sets[n++] = replies[i];
        }
    }

    if (n == 0) {
        return cJSON_CreateArray();
    }
    int required = quorum_required((int)n);
    cJSON *merged = merge_agreed_rows(sets, n, required);
    for (size_t i = 0; i < n; i++) {
        cJSON_Delete(sets[i]);
    }
    return merged ? merged : cJSON_CreateArray();
}

char **zdb_repl_read_ids(zdb_repl *rp, const char *db, const char *partition,
                         const char *keyspace, const char **filters,
                         size_t nfilters, size_t *count_out)
{
    *count_out = 0;
    if (!quorum_applies(rp, db)) {
        return rp ? zdb_ids(rp->cfg_engine, partition, keyspace, filters,
                            nfilters, count_out)
                  : NULL;
    }

    cJSON *req = make_request("ids", db, partition, keyspace);
    if (!req) {
        return NULL;
    }
    cJSON *farr = cJSON_AddArrayToObject(req, "filters");
    for (size_t i = 0; farr && i < nfilters; i++) {
        cJSON_AddItemToArray(farr, cJSON_CreateString(filters[i]));
    }

    char **lists[MAX_REPLIES + 1];
    size_t counts[MAX_REPLIES + 1];
    size_t n = 0;

    lists[n] = zdb_ids(rp->cfg_engine, partition, keyspace, filters,
                       nfilters, &counts[n]);
    if (lists[n]) {
        n++;   /* keep even empty: counts[n]==0, list NULL-safe below */
    } else {
        lists[n] = NULL;
        counts[n] = 0;
        n++;
    }

    cJSON *replies[MAX_REPLIES];
    size_t got = query_all(rp, req, replies, MAX_REPLIES);
    cJSON_Delete(req);
    for (size_t i = 0; i < got && n < MAX_REPLIES + 1; i++) {
        const cJSON *ids = cJSON_GetObjectItem(replies[i], "ids");
        lists[n] = strings_from_json(ids, &counts[n]);
        if (!lists[n]) {
            lists[n] = NULL;
            counts[n] = 0;
        }
        n++;
    }
    for (size_t i = 0; i < got; i++) {
        cJSON_Delete(replies[i]);
    }

    int required = quorum_required((int)n);
    cJSON *agreed = merge_agreed_ids(lists, counts, n, required);
    for (size_t i = 0; i < n; i++) {
        zdb_free_strings(lists[i]);
    }
    if (!agreed) {
        return NULL;
    }

    size_t cnt = (size_t)cJSON_GetArraySize(agreed);
    char **out = malloc((cnt + 1) * sizeof(char *));
    if (!out) {
        cJSON_Delete(agreed);
        return NULL;
    }
    size_t w = 0;
    const cJSON *item = NULL;
    cJSON_ArrayForEach(item, agreed) {
        if (cJSON_IsString(item) && item->valuestring) {
            out[w++] = strdup(item->valuestring);
        }
    }
    out[w] = NULL;
    cJSON_Delete(agreed);
    *count_out = w;
    return out;
}

cJSON *zdb_repl_read_query(zdb_repl *rp, const char *db,
                           const char *partition, const char *keyspace,
                           const char **filters, size_t nfilters,
                           const char **fields, size_t nfields)
{
    if (!quorum_applies(rp, db)) {
        return rp ? zdb_query(rp->cfg_engine, partition, keyspace, filters,
                              nfilters, fields, nfields)
                  : NULL;
    }

    cJSON *req = make_request("query_ts", db, partition, keyspace);
    if (!req) {
        return NULL;
    }
    cJSON *farr = cJSON_AddArrayToObject(req, "filters");
    for (size_t i = 0; farr && i < nfilters; i++) {
        cJSON_AddItemToArray(farr, cJSON_CreateString(filters[i]));
    }
    cJSON *garr = cJSON_AddArrayToObject(req, "fields");
    for (size_t i = 0; garr && i < nfields; i++) {
        cJSON_AddItemToArray(garr, cJSON_CreateString(fields[i]));
    }

    cJSON *sets[MAX_REPLIES + 1];
    size_t n = 0;
    cJSON *local_rows = zdb_query_ts(rp->cfg_engine, partition, keyspace,
                                     filters, nfilters, fields, nfields);
    if (local_rows) {
        cJSON *wrap = cJSON_CreateObject();
        if (wrap) {
            cJSON_AddItemToObject(wrap, "rows", local_rows);
            sets[n++] = wrap;
        } else {
            cJSON_Delete(local_rows);
        }
    }

    cJSON *replies[MAX_REPLIES];
    size_t got = query_all(rp, req, replies, MAX_REPLIES);
    cJSON_Delete(req);
    for (size_t i = 0; i < got; i++) {
        sets[n++] = replies[i];
    }

    if (n == 0) {
        return cJSON_CreateArray();
    }
    int required = quorum_required((int)n);
    cJSON *merged = merge_agreed_rows(sets, n, required);
    for (size_t i = 0; i < n; i++) {
        cJSON_Delete(sets[i]);
    }
    return merged ? merged : cJSON_CreateArray();
}

/* ------------------------------------------------------------------ */
/* stage 6c: delta catch-up                                            */

void zdb_repl_set_syncing(zdb_repl *rp, bool syncing)
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
bool zdb_repl_flush(zdb_repl *rp)
{
    long long deadline = mono_ms() + 20000;
    bool all_empty = false;
    for (;;) {
        zdb_peer_info peers[MAX_PEERS_SNAPSHOT];
        size_t n = zdb_cluster_peers(rp->cluster, peers,
                                     MAX_PEERS_SNAPSHOT);
        all_empty = true;
        bool any = false;
        for (size_t i = 0; i < n; i++) {
            if (strcmp(peers[i].id, rp->self_id) == 0) {
                continue;
            }
            if (!peers[i].online || peers[i].addr[0] == '\0' ||
                peers[i].port <= 0) {
                continue;
            }
            any = true;
            char req[96];
            snprintf(req, sizeof(req), "{\"target\":\"%.63s\"}",
                     rp->self_id);
            char *reply = NULL;
            int t = rpc_once(peers[i].addr, peers[i].port, ZSTP_FLUSH,
                             req, ZSTP_ACK, &reply);
            long long pending = 0;
            if (t == ZSTP_ACK && reply) {
                cJSON *doc = cJSON_Parse(reply);
                const cJSON *jp =
                    doc ? cJSON_GetObjectItemCaseSensitive(doc, "pending")
                        : NULL;
                if (cJSON_IsNumber(jp)) {
                    pending = (long long)jp->valuedouble;
                }
                cJSON_Delete(doc);
            }
            free(reply);
            if (pending > 0) {
                all_empty = false;
            }
        }
        if (!any || all_empty) {
            break;
        }
        if (mono_ms() >= deadline) {
            break;
        }
        sleep_ms(100);
    }
    return all_empty;
}

bool zdb_repl_catchup(zdb_repl *rp, const char *owner_addr, int owner_port,
                      const char *partition, const char *keyspace)
{
    if (!rp || !owner_addr || !partition || !keyspace) {
        return false;
    }

    /* refuse writes while we replace the shard file so writers cache
     * deltas instead of applying them to a file the snapshot will
     * overwrite (LWW replays them idempotently afterwards) */
    zdb_repl_set_syncing(rp, true);

    char key[33];
    char path[1024];
    if (!zdb_shard_path(rp->cfg_engine, partition, keyspace, path,
                        sizeof(path), key)) {
        zdb_repl_set_syncing(rp, false);
        return false;
    }
    if (zdb_snap_fetch(owner_addr, owner_port, key, rp->data_dir) != 0) {
        zdb_repl_set_syncing(rp, false);
        return false;
    }
    zdb_shard_invalidate(rp->cfg_engine, partition, keyspace);

    /* reopen writes; cached deltas now land safely after the snapshot */
    zdb_repl_set_syncing(rp, false);

    return zdb_repl_flush(rp);
}

/* ------------------------------------------------------------------ */
/* lifecycle                                                           */

void zdb_repl_set_handlers(zdb_repl *rp, zdb_repl_apply_fn apply,
                           zdb_repl_read_fn read, void *ud)
{
    if (!rp) {
        return;
    }
    rp->apply = apply;
    rp->read = read;
    rp->ud = ud;
}

zdb_repl *zdb_repl_start(zdb_cluster *cluster, zdb_config *cfg,
                         const char *data_dir)
{
    if (!cluster || !cfg || !data_dir) {
        return NULL;
    }
    zdb_repl *rp = calloc(1, sizeof(*rp));
    if (!rp) {
        return NULL;
    }
    rp->cluster = cluster;
    rp->cfg = cfg;
    rp->cfg_engine = zdb_config_engine(cfg);
    snprintf(rp->self_id, sizeof(rp->self_id), "%s",
             zdb_cluster_self_id(cluster));
    snprintf(rp->data_dir, sizeof(rp->data_dir), "%s", data_dir);
    pthread_mutex_init(&rp->sync_lock, NULL);
    pthread_mutex_init(&rp->replay_lock, NULL);
    rp->syncing = false;

    if (!cache_open(&rp->cache, data_dir)) {
        fprintf(stderr, "zdb: change cache unavailable; writes will not"
                        " be cached for offline nodes\n");
    }

    /* install the per-cluster dispatcher before the maintenance thread
     * starts servicing frames */
    zdb_cluster_set_dispatcher(cluster, repl_dispatch, rp);

    if (pthread_create(&rp->maint_thread, NULL, repl_maint_main, rp) != 0) {
        zdb_cluster_set_dispatcher(cluster, NULL, NULL);
        cache_close(&rp->cache);
        free(rp);
        return NULL;
    }
    rp->running = true;

    return rp;
}

void zdb_repl_stop(zdb_repl *rp)
{
    if (!rp) {
        return;
    }
    rp->running = false;
    pthread_join(rp->maint_thread, NULL);
    zdb_cluster_set_dispatcher(rp->cluster, NULL, NULL);
    cache_close(&rp->cache);
    pthread_mutex_destroy(&rp->sync_lock);
    pthread_mutex_destroy(&rp->replay_lock);
    free(rp);
}
