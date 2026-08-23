/* test_rebalance.c - stage 6d tests: target promotion + shard GC.
 *
 * Covers, on top of 6a/6c:
 *   - gossip-based compliance: a node marking itself compliant is visible
 *     to the leader, which then promotes the pending target automatically
 *     (no manual zdb_cluster_promote_target call).
 *   - the engine shard GC primitive (zdb_shard_gc) and the cluster-level
 *     zdb_cluster_gc_redundant (removes a shard no longer owned by this
 *     node, never the reserved __system__ config shards).
 *
 * Plain assert-style harness like test_replication. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "../src/engine/zesty_config.h"
#include "../src/socket/zesty_cluster.h"
#include "../src/socket/zesty_repl.h"

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
    if (strcmp(jq->valuestring, "all_ts") == 0) {
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
    node *nodes[4];
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
    for (int i = 0; i < 4; i++) {
        if (c->nodes[i] && online_peers(c->nodes[i]->cluster) < c->want) {
            return false;
        }
    }
    return true;
}

typedef struct {
    node *nodes[4];
    size_t n;
    size_t want_slices;
} promote_view;

/* Completes a pending wave by injecting every node's compliance flag into
 * the leader's settings store and promoting. Used for the intermediate
 * (2- and 3-node) waves so the next join starts from a stable live table;
 * the final wave is promoted automatically via gossiped compliance. */
static void complete_wave(node *nodes[], size_t n)
{
    const char *lid = zdb_cluster_leader(nodes[0]->cluster);
    if (!lid) {
        return;
    }
    node *leader = NULL;
    for (size_t i = 0; i < n; i++) {
        if (strcmp(lid, zdb_cluster_self_id(nodes[i]->cluster)) == 0) {
            leader = nodes[i];
            break;
        }
    }
    if (!leader) {
        return;
    }
    char val[32];
    snprintf(val, sizeof(val), "%lld",
             zdb_cluster_target_generation(leader->cluster));
    for (size_t i = 0; i < n; i++) {
        char dn[96];
        snprintf(dn, sizeof(dn), "rebalance.done.%.63s",
                 zdb_cluster_self_id(nodes[i]->cluster));
        zdb_setting_set(leader->cfg, dn, val);
    }
    zdb_cluster_promote_target(leader->cluster);
    settle();
}

/* Every node sees the same non-zero live generation with want_slices
 * slices and no pending target (wave fully promoted). */
static bool promoted(void *ctxp)
{
    promote_view *p = ctxp;
    if (p->n == 0) {
        return false;
    }
    long long gen = zdb_cluster_generation(p->nodes[0]->cluster);
    if (gen == 0) {
        return false;
    }
    zdb_range_info r[8];
    for (size_t i = 0; i < p->n; i++) {
        if (zdb_cluster_generation(p->nodes[i]->cluster) != gen) {
            return false;
        }
        if (zdb_cluster_target_generation(p->nodes[i]->cluster) != 0) {
            return false;
        }
        if (zdb_cluster_ranges(p->nodes[i]->cluster, r, 8) !=
            p->want_slices) {
            return false;
        }
    }
    return true;
}

int main(void)
{
    static int port_base = 19501;
    char dir[300];

    /* --- unit: engine shard GC primitives ---------------------------- */
    {
        snprintf(dir, sizeof(dir), "tests/data/rebalance/gc");
        char cmd[512];
        snprintf(cmd, sizeof(cmd), "rm -rf %s && mkdir -p %s", dir, dir);
        system(cmd);
        zdb_engine *e = zdb_engine_open(dir);
        CHECK(e != NULL);
        CHECK(zdb_put(e, "main", "kv", "id-0", "{\"n\":0}", -1, NULL, 0));
        CHECK(zdb_put(e, "other", "docs", "d-0", "{\"d\":0}", -1, NULL,
                      0));

        char keys[8][33];
        size_t nk = zdb_engine_shard_keys(e, keys, 8);
        CHECK(nk == 2);

        char key[33];
        char path[1024];
        CHECK(zdb_shard_path(e, "main", "kv", path, sizeof(path), key));
        CHECK(zdb_shard_gc(e, key));
        /* the handle is gone and the file removed */
        CHECK(zdb_shard_is_open(e, "main", "kv") == false);
        struct stat st;
        CHECK(stat(path, &st) != 0);
        /* the other shard is untouched */
        CHECK(zdb_get(e, "other", "docs", "d-0") != NULL);

        zdb_engine_close(e);
    }

    /* --- integration: 3 stable nodes, then a 4th joins ---------------- */
    node a, b, c, d;
    snprintf(dir, sizeof(dir), "tests/data/rebalance/a");
    node_start(&a, dir, port_base);
    zdb_cluster_set_auto_compliant(a.cluster, false);
    zdb_database_create(a.cfg, "app", 2);

    snprintf(dir, sizeof(dir), "tests/data/rebalance/b");
    node_start(&b, dir, port_base + 1);
    zdb_cluster_set_auto_compliant(b.cluster, false);
    zdb_database_create(b.cfg, "app", 2);

    snprintf(dir, sizeof(dir), "tests/data/rebalance/c");
    node_start(&c, dir, port_base + 2);
    zdb_cluster_set_auto_compliant(c.cluster, false);
    zdb_database_create(c.cfg, "app", 2);

    CHECK(zdb_cluster_join(b.cluster, "127.0.0.1", port_base) == 0);
    converge_ctx cc2 = { { &a, &b, NULL, NULL }, 2 };
    CHECK(wait_for(15, mesh_converged, &cc2));
    settle();
    complete_wave((node *[]){ &a, &b }, 2);

    CHECK(zdb_cluster_join(c.cluster, "127.0.0.1", port_base) == 0);
    converge_ctx cc3 = { { &a, &b, &c, NULL }, 3 };
    CHECK(wait_for(15, mesh_converged, &cc3));
    settle();
    complete_wave((node *[]){ &a, &b, &c }, 3);
    /* promotion propagates by gossip; allow a beat for a's pending copy
     * to clear before asserting the wave is done */
    {
        bool cleared = false;
        for (int i = 0; i < 100 && !cleared; i++) {
            cleared = zdb_cluster_target_generation(a.cluster) == 0;
            if (!cleared) {
                usleep(100 * 1000);
            }
        }
        CHECK(cleared);
    }

    /* --- populate several shards across the hash space --------------- */
    for (int i = 0; i < 100; i++) {
        char id[32];
        char value[64];
        char change[512];
        snprintf(id, sizeof(id), "id-%04d", i);
        snprintf(value, sizeof(value), "{\"n\":%d}", i);
        build_put(change, sizeof(change), "app", "main", "kv", id,
                  (long long)time(NULL), value);
        CHECK(zdb_repl_write(a.repl, "app", change) == ZDB_REPL_OK);
    }

    /* --- join a 4th node; it catches up and everyone reports compliant
     * (auto-compliance is disabled on d so the wave stays pending until
     * the test has driven the catch-up explicitly) */
    snprintf(dir, sizeof(dir), "tests/data/rebalance/d");
    node_start(&d, dir, port_base + 3);
    zdb_cluster_set_auto_compliant(d.cluster, false);
    zdb_database_create(d.cfg, "app", 2);
    CHECK(zdb_cluster_join(d.cluster, "127.0.0.1", port_base) == 0);
    converge_ctx cc4 = { { &a, &b, &c, &d }, 4 };
    CHECK(wait_for(15, mesh_converged, &cc4));
    settle();
    {
        bool pending = false;
        for (int i = 0; i < 100 && !pending; i++) {
            pending = zdb_cluster_target_generation(a.cluster) > 0;
            if (!pending) {
                usleep(100 * 1000);
            }
        }
        CHECK(pending);
    }

    /* d catches up the moved shard from its live owner */
    CHECK(zdb_repl_catchup(d.repl, "127.0.0.1", port_base, "main", "kv"));
    zdb_cluster_set_auto_compliant(d.cluster, true);

    /* every node reports compliance (d caught up; a/b/c were current) */
    zdb_cluster_set_auto_compliant(a.cluster, true);
    zdb_cluster_set_auto_compliant(b.cluster, true);
    zdb_cluster_set_auto_compliant(c.cluster, true);
    zdb_cluster_mark_compliant(a.cluster);
    zdb_cluster_mark_compliant(b.cluster);
    zdb_cluster_mark_compliant(c.cluster);
    zdb_cluster_mark_compliant(d.cluster);

    /* --- automatic promotion via gossiped compliance ----------------- */
    promote_view pv = { { &a, &b, &c, &d }, 4, 4 };
    CHECK(wait_for(20, promoted, &pv));
    CHECK(promoted(&pv));

    /* rebalance lock released by promotion (the lock lives in the
     * leader's own settings store) */
    {
        const char *lid = zdb_cluster_leader(a.cluster);
        node *leader = NULL;
        if (lid && strcmp(lid, zdb_cluster_self_id(a.cluster)) == 0) {
            leader = &a;
        } else if (lid &&
                   strcmp(lid, zdb_cluster_self_id(b.cluster)) == 0) {
            leader = &b;
        } else if (lid &&
                   strcmp(lid, zdb_cluster_self_id(c.cluster)) == 0) {
            leader = &c;
        } else if (lid &&
                   strcmp(lid, zdb_cluster_self_id(d.cluster)) == 0) {
            leader = &d;
        }
        CHECK(leader != NULL);
        if (leader) {
            char *lock =
                zdb_setting_get(leader->cfg, "cluster.rebalance_lock");
            CHECK(lock == NULL);
            free(lock);
        }
    }

    /* --- no data loss: the shard still exists on its live owner ------ */
    {
        char key[33];
        char path[1024];
        CHECK(zdb_shard_path(a.engine, "main", "kv", path, sizeof(path),
                             key));
        const char *owner = zdb_cluster_owner(a.cluster, key);
        CHECK(owner != NULL);
        node *holder = NULL;
        if (strcmp(owner, zdb_cluster_self_id(a.cluster)) == 0) {
            holder = &a;
        } else if (strcmp(owner, zdb_cluster_self_id(b.cluster)) == 0) {
            holder = &b;
        } else if (strcmp(owner, zdb_cluster_self_id(c.cluster)) == 0) {
            holder = &c;
        } else {
            holder = &d;
        }
        cJSON *all = zdb_all_ts(holder->engine, "main", "kv", NULL, 0);
        CHECK(all && cJSON_GetArraySize(all) == 100);
        cJSON_Delete(all);
    }

    /* --- GC removes a redundant copy but never system shards --------- */
    {
        char key[33];
        char path[1024];
        CHECK(zdb_shard_path(a.engine, "main", "kv", path, sizeof(path),
                             key));
        const char *owner = zdb_cluster_owner(a.cluster, key);
        CHECK(owner != NULL);

        /* pick a node that does not own the shard, and confirm it holds
         * a (replica) copy to GC */
        node *victim = NULL;
        if (strcmp(owner, zdb_cluster_self_id(a.cluster)) != 0) {
            victim = &a;
        } else {
            victim = &b;
        }
        CHECK(zdb_get(victim->engine, "main", "kv", "id-0000") != NULL);
        CHECK(zdb_cluster_gc_redundant(victim->cluster) >= 1);

        /* the file is gone from the victim */
        char vpath[1024];
        snprintf(vpath, sizeof(vpath), "%s/%s.sqlite", victim->dir, key);
        struct stat st;
        CHECK(stat(vpath, &st) != 0);

        /* data still safe on the owner */
        node *holder = NULL;
        if (strcmp(owner, zdb_cluster_self_id(a.cluster)) == 0) {
            holder = &a;
        } else if (strcmp(owner, zdb_cluster_self_id(b.cluster)) == 0) {
            holder = &b;
        } else if (strcmp(owner, zdb_cluster_self_id(c.cluster)) == 0) {
            holder = &c;
        } else {
            holder = &d;
        }
        cJSON *all = zdb_all_ts(holder->engine, "main", "kv", NULL, 0);
        CHECK(all && cJSON_GetArraySize(all) == 100);
        cJSON_Delete(all);

        /* the victim's own config (system shards) is intact */
        char *ranges = zdb_setting_get(victim->cfg, "cluster.ranges");
        CHECK(ranges != NULL);
        free(ranges);
    }

    node_stop(&d);
    node_stop(&c);
    node_stop(&b);
    node_stop(&a);

    printf("%d tests, %d failures\n", g_checks, g_failures);
    return g_failures ? 1 : 0;
}
