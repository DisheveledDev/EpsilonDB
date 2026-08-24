/* test_replication.c - stage 5 tests: three in-process nodes form a
 * mesh, writes fan out to all peers, quorum reads compare copies,
 * changes made while a peer is down are cached and replayed when it
 * returns, and writes below the replication-factor quorum are rejected.
 * Plain assert-style harness like test_engine. */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

/* --- engine-backed handlers (mirrors what the API layer registers) -- */

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
                                  value_json, ttl_abs, ts);
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
    } else if (strcmp(jq->valuestring, "all_ts") == 0 ||
               strcmp(jq->valuestring, "query_ts") == 0) {
        const cJSON *filters =
            cJSON_GetObjectItemCaseSensitive(request, "filters");
        cJSON *rows = strcmp(jq->valuestring, "all_ts") == 0
                          ? zdb_all_ts(n->engine, jpart->valuestring,
                                       jks->valuestring, filters)
                          : zdb_query_ts(n->engine, jpart->valuestring,
                                         jks->valuestring, filters);
        cJSON_AddItemToObject(out, "rows", rows ? rows
                                                : cJSON_CreateArray());
    } else if (strcmp(jq->valuestring, "ids") == 0) {
        size_t cnt = 0;
        const cJSON *filters =
            cJSON_GetObjectItemCaseSensitive(request, "filters");
        char **ids = zdb_ids(n->engine, jpart->valuestring,
                             jks->valuestring, filters, &cnt);
        cJSON *arr = cJSON_AddArrayToObject(out, "ids");
        for (size_t i = 0; arr && ids && i < cnt; i++) {
            cJSON_AddItemToArray(arr, cJSON_CreateString(ids[i]));
        }
        zdb_free_strings(ids);
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

    /* bind this node's engine to its repl (per-cluster dispatch) */
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

/* Builds a put change document as the API layer would. */
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

/* Lets the mesh reach a stable full-mesh state after joins/restarts;
 * the one-shot join connection closing can briefly flap a peer offline
 * before the persistent connection takes over. */
static void settle(void)
{
    usleep(2 * 1000 * 1000);
}

/* --- condition helpers ---------------------------------------------- */

typedef struct {
    node *a;
    node *b;
    node *c;
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
    return online_peers(c->a->cluster) >= c->want &&
           online_peers(c->b->cluster) >= c->want &&
           online_peers(c->c->cluster) >= c->want;
}

int main(void)
{
    static int port_base = 19301;
    char dir[300];
    node a, b, c;

    /* --- setup: three nodes, one mesh -------------------------------- */

    snprintf(dir, sizeof(dir), "tests/data/repl/a");
    node_start(&a, dir, port_base);
    CHECK(a.cluster != NULL);

    zdb_database_create(a.cfg, "app", 2);
    zdb_database_create(a.cfg, "strict", 3);   /* rf=3: needs 2/2 holders */

    snprintf(dir, sizeof(dir), "tests/data/repl/b");
    node_start(&b, dir, port_base + 1);
    CHECK(b.cluster != NULL);
    zdb_database_create(b.cfg, "app", 2);
    zdb_database_create(b.cfg, "strict", 3);

    snprintf(dir, sizeof(dir), "tests/data/repl/c");
    node_start(&c, dir, port_base + 2);
    CHECK(c.cluster != NULL);
    zdb_database_create(c.cfg, "app", 2);
    zdb_database_create(c.cfg, "strict", 3);

    CHECK(zdb_cluster_join(b.cluster, "127.0.0.1", port_base) == 0);
    CHECK(zdb_cluster_join(c.cluster, "127.0.0.1", port_base) == 0);
    converge_ctx cc = { &a, &b, &c, 3 };
    CHECK(wait_for(15, mesh_converged, &cc));
    settle();

    /* --- fan-out: write on a appears on b and c ---------------------- */
    {
        char change[512];
        build_put(change, sizeof(change), "app", "main", "kv", "greeting",
                  (long long)time(NULL), "{\"hello\":\"world\"}");
        CHECK(zdb_repl_write(a.repl, "app", change) == ZDB_REPL_OK);

        /* poll until both replicas hold the doc */
        for (int i = 0; i < 100; i++) {
            cJSON *vb = zdb_get(b.engine, "main", "kv", "greeting");
            cJSON *vc = zdb_get(c.engine, "main", "kv", "greeting");
            bool both = vb && vc;
            cJSON_Delete(vb);
            cJSON_Delete(vc);
            if (both) {
                break;
            }
            usleep(100 * 1000);
        }

        cJSON *vb = zdb_get(b.engine, "main", "kv", "greeting");
        cJSON *vc = zdb_get(c.engine, "main", "kv", "greeting");
        CHECK(vb != NULL);
        CHECK(vc != NULL);
        if (vb) {
            cJSON *h = cJSON_GetObjectItemCaseSensitive(vb, "hello");
            CHECK(h && cJSON_IsString(h) &&
                  strcmp(h->valuestring, "world") == 0);
        }
        if (vc) {
            cJSON *h = cJSON_GetObjectItemCaseSensitive(vc, "hello");
            CHECK(h && cJSON_IsString(h) &&
                  strcmp(h->valuestring, "world") == 0);
        }
        cJSON_Delete(vb);
        cJSON_Delete(vc);
    }

    /* --- LWW: older timestamp does not overwrite newer --------------- */
    {
        long long now = (long long)time(NULL);
        char newer[512];
        build_put(newer, sizeof(newer), "app", "main", "kv", "lww",
                  now + 10, "{\"v\":2}");
        CHECK(zdb_repl_write(a.repl, "app", newer) == ZDB_REPL_OK);

        char older[512];
        build_put(older, sizeof(older), "app", "main", "kv", "lww",
                  now, "{\"v\":1}");
        CHECK(zdb_repl_write(b.repl, "app", older) == ZDB_REPL_OK);

        cJSON *doc = zdb_get(a.engine, "main", "kv", "lww");
        CHECK(doc != NULL);
        if (doc) {
            cJSON *v = cJSON_GetObjectItemCaseSensitive(doc, "v");
            CHECK(v && v->valueint == 2);
        }
        cJSON_Delete(doc);
    }

    /* --- delete tombstone replicates ---------------------------------- */
    {
        long long now = (long long)time(NULL);
        char change[512];
        snprintf(change, sizeof(change),
                 "{\"op\":\"delete\",\"db\":\"app\",\"partition\":\"main\","
                 "\"keyspace\":\"kv\",\"id\":\"greeting\",\"ts\":%lld}",
                 now);
        CHECK(zdb_repl_write(a.repl, "app", change) == ZDB_REPL_OK);

        cJSON *gone_a = zdb_get(a.engine, "main", "kv", "greeting");
        cJSON *gone_b = zdb_get(b.engine, "main", "kv", "greeting");
        CHECK(gone_a == NULL);
        CHECK(gone_b == NULL);
        cJSON_Delete(gone_a);
        cJSON_Delete(gone_b);
    }

    /* --- offline cache + replay: stop b, write on a, restart b ------- */
    {
        node_stop(&b);

        char change[512];
        build_put(change, sizeof(change), "app", "main", "kv", "offline",
                  (long long)time(NULL), "{\"queued\":true}");
        CHECK(zdb_repl_write(a.repl, "app", change) == ZDB_REPL_OK);

        /* restart b and rejoin */
        snprintf(dir, sizeof(dir), "tests/data/repl/b");
        b.engine = zdb_engine_open(dir);
        b.cfg = zdb_config_open(b.engine);
        b.cluster = zdb_cluster_start(b.cfg, "127.0.0.1", port_base + 1,
                                      NULL);
        b.repl = zdb_repl_start(b.cluster, b.cfg, dir);
        zdb_repl_set_handlers(b.repl, test_apply_change,
                              test_read_request, &b);
        CHECK(b.repl != NULL);
        CHECK(zdb_cluster_join(b.cluster, "127.0.0.1", port_base) == 0);
        converge_ctx again = { &a, &b, &c, 3 };
        CHECK(wait_for(15, mesh_converged, &again));
        settle();

        /* replay happens on the maintenance tick once b is seen back */
        bool replayed = false;
        for (int i = 0; i < 100 && !replayed; i++) {
            cJSON *doc = zdb_get(b.engine, "main", "kv", "offline");
            if (doc) {
                replayed = true;
            }
            cJSON_Delete(doc);
            usleep(100 * 1000);
        }
        CHECK(replayed);
    }

    /* --- quorum reads -------------------------------------------------- */
    {
        char change[512];
        build_put(change, sizeof(change), "app", "main", "users", "u1",
                  (long long)time(NULL),
                  "{\"name\":\"ada\",\"manager\":{\"age\":50}}");
        CHECK(zdb_repl_write(a.repl, "app", change) == ZDB_REPL_OK);
        build_put(change, sizeof(change), "app", "main", "users", "u2",
                  (long long)time(NULL),
                  "{\"name\":\"bob\",\"manager\":{\"age\":30}}");
        CHECK(zdb_repl_write(a.repl, "app", change) == ZDB_REPL_OK);

        /* direct engine read confirms local copy */
        cJSON *local = zdb_get(a.engine, "main", "users", "u1");
        CHECK(local != NULL);
        cJSON_Delete(local);

        /* quorum read returns the merged/agreed doc */
        cJSON *got = zdb_repl_read_get(a.repl, "app", "main", "users",
                                       "u1");
        CHECK(got != NULL);
        if (got) {
            cJSON *name =
                cJSON_GetObjectItemCaseSensitive(got, "name");
            CHECK(name && cJSON_IsString(name) &&
                  strcmp(name->valuestring, "ada") == 0);
        }
        cJSON_Delete(got);

        /* ids/all merges across replicas */
        size_t nids = 0;
        char **ids = zdb_repl_read_ids(a.repl, "app", "main", "users",
                                       NULL, &nids);
        CHECK(ids != NULL && nids == 2);
        zdb_free_strings(ids);

        cJSON *all = zdb_repl_read_all(a.repl, "app", "main", "users",
                                       NULL);
        CHECK(all != NULL && cJSON_GetArraySize(all) == 2);
        cJSON_Delete(all);

        cJSON *filters = cJSON_Parse(
            "{\"key\":\"manager.age\",\"operator\":\"gt\",\"value\":40}");
        cJSON *filtered = zdb_repl_read_query(a.repl, "app", "main", "users",
                                              filters);
        CHECK(filtered && cJSON_GetArraySize(filtered) == 1);
        cJSON_Delete(filtered);
        ids = zdb_repl_read_ids(a.repl, "app", "main", "users", filters,
                                &nids);
        CHECK(ids && nids == 1 && strcmp(ids[0], "u1") == 0);
        zdb_free_strings(ids);
        cJSON_Delete(filters);
    }

    /* --- quorum rejection: rf=3 database with one node down ---------- */
    {
        node_stop(&c);

        /* rf=3 requires ceil majority = 2 holders; only a+b remain:
         * still writable */
        char ok_change[512];
        build_put(ok_change, sizeof(ok_change), "strict", "main", "kv",
                  "writable", (long long)time(NULL), "{\"n\":1}");
        CHECK(zdb_repl_write(a.repl, "strict", ok_change) ==
              ZDB_REPL_OK);

        /* take b down too: only holder left is a -> rejected */
        node_stop(&b);
        char bad_change[512];
        build_put(bad_change, sizeof(bad_change), "strict", "main", "kv",
                  "rejected", (long long)time(NULL), "{\"n\":2}");
        CHECK(zdb_repl_write(a.repl, "strict", bad_change) ==
              ZDB_REPL_QUORUM_LOST);

        /* rf=2 database still writable alone (responders-based read
         * quorum keeps reads available; single-holder write succeeds
         * because 1 >= rf/2+1 is false... verify actual policy: rf=2
         * needs 2 holders, so it must also be rejected now) */
        char app_change[512];
        build_put(app_change, sizeof(app_change), "app", "main", "kv",
                  "alone", (long long)time(NULL), "{\"n\":3}");
        CHECK(zdb_repl_write(a.repl, "app", app_change) ==
              ZDB_REPL_QUORUM_LOST);

        /* bring everyone back; cached strict/app changes must replay */
        snprintf(dir, sizeof(dir), "tests/data/repl/b");
        b.engine = zdb_engine_open(dir);
        b.cfg = zdb_config_open(b.engine);
        b.cluster = zdb_cluster_start(b.cfg, "127.0.0.1", port_base + 1,
                                      NULL);
        b.repl = zdb_repl_start(b.cluster, b.cfg, dir);
        zdb_repl_set_handlers(b.repl, test_apply_change,
                              test_read_request, &b);
        CHECK(zdb_cluster_join(b.cluster, "127.0.0.1", port_base) == 0);

        snprintf(dir, sizeof(dir), "tests/data/repl/c");
        c.engine = zdb_engine_open(dir);
        c.cfg = zdb_config_open(c.engine);
        c.cluster = zdb_cluster_start(c.cfg, "127.0.0.1", port_base + 2,
                                      NULL);
        c.repl = zdb_repl_start(c.cluster, c.cfg, dir);
        zdb_repl_set_handlers(c.repl, test_apply_change,
                              test_read_request, &c);
        CHECK(zdb_cluster_join(c.cluster, "127.0.0.1", port_base) == 0);

        converge_ctx final_cc = { &a, &b, &c, 3 };
        CHECK(wait_for(15, mesh_converged, &final_cc));
        settle();

        bool converged_data = false;
        for (int i = 0; i < 100 && !converged_data; i++) {
            cJSON *doc = zdb_get(c.engine, "main", "kv", "writable");
            if (doc) {
                converged_data = true;
            }
            cJSON_Delete(doc);
            usleep(100 * 1000);
        }
        CHECK(converged_data);
    }

    node_stop(&a);
    node_stop(&b);
    node_stop(&c);

    printf("%d tests, %d failures\n", g_checks, g_failures);
    return g_failures ? 1 : 0;
}
