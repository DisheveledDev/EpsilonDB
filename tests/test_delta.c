/* test_delta.c - stage 6c tests: delta catch-up after shard snapshot.
 *
 * Scenario: a,b,c hold replicated data in "main"/"kv". A fourth node d
 * joins (triggering a rebalance wave), snapshots the shard from its live
 * owner, and then catches up the writes that happened while it was
 * syncing. Those writes are cached by the live nodes (d refuses REPL
 * frames while syncing) and replayed in order once d flushes; last-write-
 * wins makes the replay idempotent, so no write is lost and d ends up
 * byte-identical to a quorum.
 *
 * Plain assert-style harness like test_replication. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "../src/engine/zesty_config.h"
#include "../src/socket/zesty_cluster.h"
#include "../src/socket/zesty_repl.h"
#include "../src/socket/zesty_snap.h"

static int g_checks = 0;
static int g_failures = 0;

#define CHECK(cond)                                                          \
    do {                                                                     \
        g_checks++;                                                          \
        if (!(cond)) {                                                       \
            g_failures++;                                                    \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);  \
        }                                                                    \
    } while (0)

typedef struct {
    zdb_engine *engine;
    zdb_config *cfg;
    zdb_cluster *cluster;
    zdb_repl *repl;
    char dir[256];
} node;

static bool test_apply_change(void *ud, const cJSON *change)
{
    node *n = ud;
    const cJSON *jop = cJSON_GetObjectItemCaseSensitive(change, "op");
    const cJSON *jpart =
        cJSON_GetObjectItemCaseSensitive(change, "partition");
    const cJSON *jks =
        cJSON_GetObjectItemCaseSensitive(change, "keyspace");
    const cJSON *jid = cJSON_GetObjectItemCaseSensitive(change, "id");
    const cJSON *jts = cJSON_GetObjectItemCaseSensitive(change, "ts");
    if (!cJSON_IsString(jop) || !cJSON_IsString(jpart) ||
        !cJSON_IsString(jks) || !cJSON_IsString(jid) ||
        !cJSON_IsNumber(jts)) {
        return false;
    }
    long long ts = (long long)jts->valuedouble;

    if (strcmp(jop->valuestring, "put") == 0) {
        const cJSON *jval =
            cJSON_GetObjectItemCaseSensitive(change, "value");
        const cJSON *jttl =
            cJSON_GetObjectItemCaseSensitive(change, "ttl_abs");
        if (!cJSON_IsObject(jval)) {
            return false;
        }
        char *value_json = cJSON_PrintUnformatted(jval);
        if (!value_json) {
            return false;
        }
        long long ttl_abs = cJSON_IsNumber(jttl)
                                ? (long long)jttl->valuedouble
                                : -1;
        bool ok = zdb_replica_put(n->engine, jpart->valuestring,
                                  jks->valuestring, jid->valuestring,
                                  value_json, ttl_abs, ts, NULL, 0);
        free(value_json);
        return ok;
    }
    if (strcmp(jop->valuestring, "delete") == 0) {
        return zdb_replica_delete(n->engine, jpart->valuestring,
                                  jks->valuestring, jid->valuestring, ts);
    }
    return false;
}

static cJSON *test_read_request(void *ud, const cJSON *request)
{
    node *n = ud;
    const cJSON *jq = cJSON_GetObjectItemCaseSensitive(request, "q");
    const cJSON *jpart =
        cJSON_GetObjectItemCaseSensitive(request, "partition");
    const cJSON *jks =
        cJSON_GetObjectItemCaseSensitive(request, "keyspace");
    if (!cJSON_IsString(jq) || !cJSON_IsString(jpart) ||
        !cJSON_IsString(jks)) {
        return NULL;
    }

    cJSON *out = cJSON_CreateObject();
    if (!out) {
        return NULL;
    }
    if (strcmp(jq->valuestring, "get") == 0) {
        const cJSON *jid =
            cJSON_GetObjectItemCaseSensitive(request, "id");
        long long ts = 0;
        cJSON *doc = cJSON_IsString(jid) && jid->valuestring
                         ? zdb_get_ts(n->engine, jpart->valuestring,
                                      jks->valuestring,
                                      jid->valuestring, &ts)
                         : NULL;
        if (doc) {
            cJSON *row = cJSON_CreateObject();
            cJSON_AddStringToObject(
                row, "id",
                cJSON_GetObjectItemCaseSensitive(request, "id")
                    ->valuestring);
            cJSON_AddNumberToObject(row, "timestamp", (double)ts);
            cJSON_AddItemToObject(row, "value", doc);
            cJSON_AddItemToObject(out, "row", row);
        } else {
            cJSON_AddNullToObject(out, "row");
        }
    } else if (strcmp(jq->valuestring, "all_ts") == 0) {
        cJSON *rows = zdb_all_ts(n->engine, jpart->valuestring,
                                 jks->valuestring, NULL, 0);
        cJSON_AddItemToObject(out, "rows", rows ? rows
                                                : cJSON_CreateArray());
    } else {
        cJSON_Delete(out);
        return NULL;
    }
    return out;
}

static void node_start(node *n, const char *dir, int port)
{
    snprintf(n->dir, sizeof(n->dir), "%s", dir);
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s && mkdir -p %s", dir, dir);
    if (system(cmd) != 0) {
        /* best effort */
    }
    n->engine = zdb_engine_open(dir);
    n->cfg = zdb_config_open(n->engine);
    char id[ZDB_NODE_ID_MAX];
    n->cluster = zdb_cluster_start(n->cfg, "127.0.0.1", port, id);
    n->repl = zdb_repl_start(n->cluster, n->cfg, dir);
    zdb_repl_set_handlers(n->repl, test_apply_change, test_read_request,
                          n);
}

static void node_stop(node *n)
{
    zdb_repl_stop(n->repl);
    zdb_cluster_stop(n->cluster);
    zdb_config_close(n->cfg);
    zdb_engine_close(n->engine);
    n->repl = NULL;
    n->cluster = NULL;
    n->cfg = NULL;
    n->engine = NULL;
}

static void build_put(char *out, size_t cap, const char *db,
                      const char *part, const char *ks, const char *id,
                      long long ts, const char *value_json)
{
    snprintf(out, cap,
             "{\"op\":\"put\",\"db\":\"%s\",\"partition\":\"%s\","
             "\"keyspace\":\"%s\",\"id\":\"%s\",\"value\":%s,"
             "\"ttl_abs\":-1,\"ts\":%lld}",
             db, part, ks, id, value_json, ts);
}

static bool wait_for(int seconds, bool (*cond)(void *), void *ctx)
{
    for (int i = 0; i < seconds * 10; i++) {
        if (cond(ctx)) {
            return true;
        }
        usleep(100 * 1000);
    }
    return cond(ctx);
}

static void settle(void)
{
    usleep(3 * 1000 * 1000);
}

typedef struct {
    node *nodes[3];
    size_t want;
} converge_ctx;

static size_t online_peers(zdb_cluster *cl)
{
    zdb_peer_info peers[16];
    size_t n = zdb_cluster_peers(cl, peers, 16);
    size_t online = 0;
    for (size_t i = 0; i < n; i++) {
        if (peers[i].online) {
            online++;
        }
    }
    return online;
}

static bool mesh_converged(void *ctxp)
{
    converge_ctx *c = ctxp;
    for (int i = 0; i < 3; i++) {
        if (c->nodes[i] && online_peers(c->nodes[i]->cluster) < c->want) {
            return false;
        }
    }
    return true;
}

/* Row-by-row equality of two shards via timestamp-tagged reads. */
static bool rows_equal(zdb_engine *a, zdb_engine *b, const char *part,
                       const char *ks)
{
    cJSON *ra = zdb_all_ts(a, part, ks, NULL, 0);
    cJSON *rb = zdb_all_ts(b, part, ks, NULL, 0);
    bool match = false;
    if (ra && rb &&
        cJSON_GetArraySize(ra) == cJSON_GetArraySize(rb)) {
        char *sa = cJSON_PrintUnformatted(ra);
        char *sb = cJSON_PrintUnformatted(rb);
        match = sa && sb && strcmp(sa, sb) == 0;
        free(sa);
        free(sb);
    }
    cJSON_Delete(ra);
    cJSON_Delete(rb);
    return match;
}

/* Completes the pending rebalance wave between two nodes by setting both
 * compliance flags in the leader's settings store and promoting. Mirrors
 * what 6d will drive automatically; here it just lets us reach a stable
 * live table so the next join starts a fresh wave. */
static void complete_wave(node *a, node *b)
{
    const char *lid = zdb_cluster_leader(a->cluster);
    if (!lid) {
        return;
    }
    zdb_cluster *leader = strcmp(lid, zdb_cluster_self_id(a->cluster)) == 0
                              ? a->cluster
                              : b->cluster;
    zdb_config *lcfg = leader == a->cluster ? a->cfg : b->cfg;
    const char *ids[2] = {
        zdb_cluster_self_id(a->cluster),
        zdb_cluster_self_id(b->cluster),
    };
    char val[32];
    snprintf(val, sizeof(val), "%lld",
             zdb_cluster_target_generation(leader));
    for (int i = 0; i < 2; i++) {
        char dn[96];
        snprintf(dn, sizeof(dn), "rebalance.done.%.63s", ids[i]);
        zdb_setting_set(lcfg, dn, val);
    }
    zdb_cluster_promote_target(leader);
    settle();
}

int main(void)
{
    static int port_base = 19401;
    char dir[300];
    node a, b, d;

    snprintf(dir, sizeof(dir), "tests/data/delta/a");
    node_start(&a, dir, port_base);
    CHECK(a.cluster != NULL);
    zdb_database_create(a.cfg, "app", 2);

    snprintf(dir, sizeof(dir), "tests/data/delta/b");
    node_start(&b, dir, port_base + 1);
    zdb_database_create(b.cfg, "app", 2);

    /* stable two-node live table before the join we actually test */
    CHECK(zdb_cluster_join(b.cluster, "127.0.0.1", port_base) == 0);
    converge_ctx cc2 = { { &a, &b, NULL }, 2 };
    CHECK(wait_for(15, mesh_converged, &cc2));
    complete_wave(&a, &b);
    CHECK(zdb_cluster_target_generation(a.cluster) == 0);
    /* the one-shot join connection closing flaps b offline until its
     * persistent connection is up; wait for a stable two-node mesh */
    converge_ctx cc2b = { { &a, &b, NULL }, 2 };
    CHECK(wait_for(15, mesh_converged, &cc2b));
    settle();

    /* --- baseline data on the live shard ----------------------------- */
    for (int i = 0; i < 200; i++) {
        char id[32];
        char value[64];
        char change[512];
        snprintf(id, sizeof(id), "id-%04d", i);
        snprintf(value, sizeof(value), "{\"n\":%d}", i);
        build_put(change, sizeof(change), "app", "main", "kv", id,
                  (long long)time(NULL), value);
        CHECK(zdb_repl_write(a.repl, "app", change) == ZDB_REPL_OK);
    }

    /* --- d joins: leader publishes a 3-slice target ------------------ */
    snprintf(dir, sizeof(dir), "tests/data/delta/d");
    node_start(&d, dir, port_base + 2);
    zdb_database_create(d.cfg, "app", 2);
    CHECK(zdb_cluster_join(d.cluster, "127.0.0.1", port_base) == 0);
    converge_ctx cc3 = { { &a, &b, &d }, 3 };
    CHECK(wait_for(15, mesh_converged, &cc3));
    settle();
    CHECK(zdb_cluster_target_generation(d.cluster) > 0);

    /* --- snapshot the shard while syncing ----------------------------- */
    char key[33];
    char path[1024];
    CHECK(zdb_shard_path(d.engine, "main", "kv", path, sizeof(path),
                         key));

    /* refuse writes so the live nodes cache deltas for us instead */
    zdb_repl_set_syncing(d.repl, true);

    CHECK(zdb_snap_fetch("127.0.0.1", port_base, key, d.dir) == 0);
    zdb_shard_invalidate(d.engine, "main", "kv");

    /* d now holds the base snapshot but not the deltas below */
    cJSON *base = zdb_all_ts(d.engine, "main", "kv", NULL, 0);
    CHECK(base && cJSON_GetArraySize(base) == 200);
    cJSON_Delete(base);

    /* --- writes during sync: cached by a, not applied to d ------------ */
    const char *d_id = zdb_cluster_self_id(d.cluster);
    CHECK(d_id != NULL);
    for (int i = 0; i < 50; i++) {
        char id[32];
        char value[64];
        char change[512];
        snprintf(id, sizeof(id), "delta-%04d", i);
        snprintf(value, sizeof(value), "{\"delta\":%d}", i);
        build_put(change, sizeof(change), "app", "main", "kv", id,
                  (long long)time(NULL), value);
        CHECK(zdb_repl_write(a.repl, "app", change) == ZDB_REPL_OK);
    }

    /* the origin (a) must have queued these for d */
    CHECK(zdb_repl_pending_for(a.repl, d_id) > 0);

    /* d has still not applied them: it refused the writes */
    cJSON *mid = zdb_get(d.engine, "main", "kv", "delta-0000");
    CHECK(mid == NULL);
    cJSON_Delete(mid);

    /* --- reopen writes and flush: cached deltas replay in order ------- */
    zdb_repl_set_syncing(d.repl, false);
    CHECK(zdb_repl_flush(d.repl));

    /* a's queue for d is drained */
    CHECK(zdb_repl_pending_for(a.repl, d_id) == 0);

    /* d ends identical to a quorum (a) with no lost writes */
    bool converged = false;
    for (int i = 0; i < 100 && !converged; i++) {
        converged = rows_equal(a.engine, d.engine, "main", "kv");
        if (!converged) {
            usleep(100 * 1000);
        }
    }
    CHECK(converged);

    cJSON *all = zdb_all_ts(d.engine, "main", "kv", NULL, 0);
    CHECK(all && cJSON_GetArraySize(all) == 250);
    cJSON_Delete(all);

    /* --- integrated catchup path -------------------------------------- */
    for (int i = 0; i < 30; i++) {
        char id[32];
        char value[64];
        char change[512];
        snprintf(id, sizeof(id), "doc-%04d", i);
        snprintf(value, sizeof(value), "{\"doc\":%d}", i);
        build_put(change, sizeof(change), "app", "other", "docs", id,
                  (long long)time(NULL), value);
        CHECK(zdb_repl_write(a.repl, "app", change) == ZDB_REPL_OK);
    }

    CHECK(zdb_repl_catchup(d.repl, "127.0.0.1", port_base, "other",
                           "docs"));
    bool other_converged = false;
    for (int i = 0; i < 100 && !other_converged; i++) {
        other_converged =
            rows_equal(a.engine, d.engine, "other", "docs");
        if (!other_converged) {
            usleep(100 * 1000);
        }
    }
    CHECK(other_converged);

    /* d reports compliance now that its shards are current */
    zdb_cluster_mark_compliant(d.cluster);
    char done[96];
    snprintf(done, sizeof(done), "rebalance.done.%.63s", d_id);
    char *v = zdb_setting_get(d.cfg, done);
    CHECK(v != NULL);
    free(v);

    node_stop(&d);
    node_stop(&b);
    node_stop(&a);

    printf("%d tests, %d failures\n", g_checks, g_failures);
    return g_failures ? 1 : 0;
}
