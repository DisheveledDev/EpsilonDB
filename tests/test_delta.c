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

#include "../src/engine/epsilon_config.h"
#include "../src/socket/epsilon_cluster.h"
#include "../src/socket/epsilon_repl.h"
#include "../src/socket/epsilon_snap.h"
#include "test_sleep.h"

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
    edb_engine *engine;
    edb_config *cfg;
    edb_cluster *cluster;
    edb_repl *repl;
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
        bool ok = edb_replica_put(n->engine, jpart->valuestring,
                                  jks->valuestring, jid->valuestring,
                                  value_json, ttl_abs, ts);
        free(value_json);
        return ok;
    }
    if (strcmp(jop->valuestring, "delete") == 0) {
        return edb_replica_delete(n->engine, jpart->valuestring,
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
                         ? edb_get_ts(n->engine, jpart->valuestring,
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
        cJSON *rows = edb_all_ts(n->engine, jpart->valuestring,
                                 jks->valuestring, NULL);
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
    n->engine = edb_engine_open(dir);
    n->cfg = edb_config_open(n->engine);
    char id[EDB_NODE_ID_MAX];
    n->cluster = edb_cluster_start(n->cfg, "127.0.0.1", port, id);
    n->repl = edb_repl_start(n->cluster, n->cfg, dir);
    edb_repl_set_handlers(n->repl, test_apply_change, test_read_request,
                          n);
}

static void node_stop(node *n)
{
    edb_repl_stop(n->repl);
    edb_cluster_stop(n->cluster);
    edb_config_close(n->cfg);
    edb_engine_close(n->engine);
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
        edb_sleep_us(100 * 1000);
    }
    return cond(ctx);
}

static void settle(void)
{
    edb_sleep_us(3 * 1000 * 1000);
}

typedef struct {
    node *nodes[3];
    size_t want;
} converge_ctx;

static size_t online_peers(edb_cluster *cl)
{
    edb_peer_info peers[16];
    size_t n = edb_cluster_peers(cl, peers, 16);
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
static bool rows_equal(edb_engine *a, edb_engine *b, const char *part,
                       const char *ks)
{
    cJSON *ra = edb_all_ts(a, part, ks, NULL);
    cJSON *rb = edb_all_ts(b, part, ks, NULL);
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
    const char *lid = edb_cluster_leader(a->cluster);
    if (!lid) {
        return;
    }
    edb_cluster *leader = strcmp(lid, edb_cluster_self_id(a->cluster)) == 0
                              ? a->cluster
                              : b->cluster;
    edb_config *lcfg = leader == a->cluster ? a->cfg : b->cfg;
    const char *ids[2] = {
        edb_cluster_self_id(a->cluster),
        edb_cluster_self_id(b->cluster),
    };
    char val[32];
    snprintf(val, sizeof(val), "%lld",
             edb_cluster_target_generation(leader));
    for (int i = 0; i < 2; i++) {
        char dn[96];
        snprintf(dn, sizeof(dn), "rebalance.done.%.63s", ids[i]);
        edb_setting_set(lcfg, dn, val);
    }
    edb_cluster_promote_target(leader);
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
    edb_database_create(a.cfg, "app", 2);

    snprintf(dir, sizeof(dir), "tests/data/delta/b");
    node_start(&b, dir, port_base + 1);
    edb_database_create(b.cfg, "app", 2);

    /* stable two-node live table before the join we actually test */
    CHECK(edb_cluster_join(b.cluster, "127.0.0.1", port_base) == 0);
    converge_ctx cc2 = { { &a, &b, NULL }, 2 };
    CHECK(wait_for(15, mesh_converged, &cc2));
    complete_wave(&a, &b);
    CHECK(edb_cluster_target_generation(a.cluster) == 0);
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
        CHECK(edb_repl_write(a.repl, "app", change) == EDB_REPL_OK);
    }

    /* --- d joins: leader publishes a 3-slice target ------------------ */
    snprintf(dir, sizeof(dir), "tests/data/delta/d");
    node_start(&d, dir, port_base + 2);
    edb_cluster_set_auto_compliant(d.cluster, false);
    edb_database_create(d.cfg, "app", 2);
    CHECK(edb_cluster_join(d.cluster, "127.0.0.1", port_base) == 0);
    converge_ctx cc3 = { { &a, &b, &d }, 3 };
    CHECK(wait_for(15, mesh_converged, &cc3));
    settle();
    CHECK(edb_cluster_target_generation(d.cluster) > 0);

    /* --- snapshot the shard while syncing ----------------------------- */
    char key[33];
    char path[1024];
    CHECK(edb_shard_path(d.engine, "main", "kv", path, sizeof(path),
                         key));

    /* refuse writes so the live nodes cache deltas for us instead */
    edb_repl_set_syncing(d.repl, true);

    CHECK(edb_snap_fetch("127.0.0.1", port_base, key, d.dir) == 0);
    edb_shard_invalidate(d.engine, "main", "kv");

    /* d now holds the base snapshot but not the deltas below */
    cJSON *base = edb_all_ts(d.engine, "main", "kv", NULL);
    CHECK(base && cJSON_GetArraySize(base) == 200);
    cJSON_Delete(base);

    /* --- writes during sync: cached by a, not applied to d ------------ */
    const char *d_id = edb_cluster_self_id(d.cluster);
    CHECK(d_id != NULL);
    for (int i = 0; i < 50; i++) {
        char id[32];
        char value[64];
        char change[512];
        snprintf(id, sizeof(id), "delta-%04d", i);
        snprintf(value, sizeof(value), "{\"delta\":%d}", i);
        build_put(change, sizeof(change), "app", "main", "kv", id,
                  (long long)time(NULL), value);
        CHECK(edb_repl_write(a.repl, "app", change) == EDB_REPL_OK);
    }

    /* the origin (a) must have queued these for d */
    CHECK(edb_repl_pending_for(a.repl, d_id) > 0);

    /* d has still not applied them: it refused the writes */
    cJSON *mid = edb_get(d.engine, "main", "kv", "delta-0000");
    CHECK(mid == NULL);
    cJSON_Delete(mid);

    /* --- reopen writes and flush: cached deltas replay in order ------- */
    edb_repl_set_syncing(d.repl, false);
    CHECK(edb_repl_flush(d.repl));

    /* a's queue for d is drained */
    CHECK(edb_repl_pending_for(a.repl, d_id) == 0);

    /* d ends identical to a quorum (a) with no lost writes */
    bool converged = false;
    for (int i = 0; i < 100 && !converged; i++) {
        converged = rows_equal(a.engine, d.engine, "main", "kv");
        if (!converged) {
            edb_sleep_us(100 * 1000);
        }
    }
    CHECK(converged);

    cJSON *all = edb_all_ts(d.engine, "main", "kv", NULL);
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
        CHECK(edb_repl_write(a.repl, "app", change) == EDB_REPL_OK);
    }

    CHECK(edb_repl_catchup(d.repl, "127.0.0.1", port_base, "other",
                           "docs"));
    bool other_converged = false;
    for (int i = 0; i < 100 && !other_converged; i++) {
        other_converged =
            rows_equal(a.engine, d.engine, "other", "docs");
        if (!other_converged) {
            edb_sleep_us(100 * 1000);
        }
    }
    CHECK(other_converged);

    /* d reports compliance now that its shards are current */
    edb_cluster_mark_compliant(d.cluster);
    char done[96];
    snprintf(done, sizeof(done), "rebalance.done.%.63s", d_id);
    char *v = edb_setting_get(d.cfg, done);
    CHECK(v != NULL);
    free(v);

    node_stop(&d);
    node_stop(&b);
    node_stop(&a);

    printf("%d tests, %d failures\n", g_checks, g_failures);
    return g_failures ? 1 : 0;
}
