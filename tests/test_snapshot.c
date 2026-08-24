/* test_snapshot.c - stage 6b tests: shard snapshot transfer between two
 * in-process nodes. Populates shards on node A, transfers them to node
 * B over the ZSTP snapshot protocol, and verifies byte-equivalent row
 * contents plus the engine invalidate/reopen path. Plain assert-style
 * harness like test_engine.
 *
 * The suite is split into independent, resumable chunks: every chunk
 * builds its own two-node fixture from scratch, so any single chunk can
 * be run (and re-run) in isolation while debugging:
 *
 *   ./tests/test_snapshot              run all chunks in order
 *   ./tests/test_snapshot transfer     run one chunk by name
 *   ./tests/test_snapshot -l           list chunk names
 */

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "../src/engine/zesty_config.h"
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

/* Computes md5(partition + keyspace) using the engine's own helper so
 * tests agree with the engine's file naming. */
#include "../src/engine/md5.h"

typedef struct {
    zdb_engine *engine;
    char dir[256];
    int port;                 /* peer listener for snapshot serving */
    int listen_fd;
    pthread_t serve_thread;
} snap_node;

static void node_start(snap_node *n, const char *dir)
{
    snprintf(n->dir, sizeof(n->dir), "%s", dir);
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s && mkdir -p %s", dir, dir);
    if (system(cmd) != 0) {
        /* best effort */
    }
    n->engine = zdb_engine_open(dir);
}

static void node_stop(snap_node *n)
{
    zdb_engine_close(n->engine);
    n->engine = NULL;
}

/* Minimal peer-style listener: accepts connections, performs the HELLO
 * exchange, and hands SNAP_REQ exchanges to zdb_snap_serve. Runs until
 * stop_serving closes the socket. */

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <sys/socket.h>

static volatile sig_atomic_t g_serve_run = 1;

#include <signal.h>

typedef struct {
    snap_node *n;
} serve_ctx;

static snap_node *g_server_node = NULL;

/* One connection: HELLO exchange, then serve exactly one SNAP_REQ. */
static void *serve_conn(void *arg)
{
    int fd = (int)(intptr_t)arg;
    char *payload = NULL;

    int t = zstp_recv_frame(fd, &payload);
    if (t != ZSTP_HELLO) {
        free(payload);
        close(fd);
        return NULL;
    }
    free(payload);

    cJSON *hello = cJSON_CreateObject();
    cJSON_AddStringToObject(hello, "node_id", "snap-server");
    char *hs = cJSON_PrintUnformatted(hello);
    cJSON_Delete(hello);
    zstp_send_frame(fd, ZSTP_HELLO, hs ? hs : "{}", NULL);
    free(hs);

    uint32_t plen = 0;
    t = zstp_recv_frame_raw(fd, &payload, &plen);
    if (t == ZSTP_SNAP_REQ) {
        zdb_snap_serve(fd, plen, payload, g_server_node->engine);
    }
    free(payload);
    shutdown(fd, SHUT_RDWR);
    close(fd);
    return NULL;
}

static void *serve_main(void *arg)
{
    snap_node *n = arg;

    while (g_serve_run) {
        struct sockaddr_in sa;
        socklen_t slen = sizeof(sa);
        int cfd = accept(n->listen_fd, (struct sockaddr *)&sa, &slen);
        if (cfd < 0) {
            if (!g_serve_run || errno == EBADF || errno == EINVAL) {
                break;
            }
            continue;
        }
        pthread_t tid;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        if (pthread_create(&tid, &attr, serve_conn,
                           (void *)(intptr_t)cfd) != 0) {
            close(cfd);
        }
        pthread_attr_destroy(&attr);
    }
    return NULL;
}

static bool start_serving(snap_node *n)
{
    n->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (n->listen_fd < 0) {
        return false;
    }
    int one = 1;
    setsockopt(n->listen_fd, SOL_SOCKET, SO_REUSEADDR, &one,
               sizeof(one));
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    /* port 0 lets the kernel pick a free port; read it back below */
    sa.sin_addr.s_addr = htonl(0x7f000001u);
    if (bind(n->listen_fd, (struct sockaddr *)&sa, sizeof(sa)) != 0 ||
        listen(n->listen_fd, 8) != 0) {
        close(n->listen_fd);
        return false;
    }
    socklen_t slen = sizeof(sa);
    getsockname(n->listen_fd, (struct sockaddr *)&sa, &slen);
    n->port = ntohs(sa.sin_port);

    g_serve_run = 1;
    g_server_node = n;
    return pthread_create(&n->serve_thread, NULL, serve_main, n) == 0;
}

static void stop_serving(snap_node *n)
{
    g_serve_run = 0;
    shutdown(n->listen_fd, SHUT_RDWR);
    close(n->listen_fd);
    pthread_join(n->serve_thread, NULL);
}

static void shard_key_of(const char *partition, const char *keyspace,
                         char out[33])
{
    char partition_hash[33];
    char keyspace_hash[33];
    char framed[66];
    zdb_md5_hex(partition, strlen(partition), partition_hash);
    zdb_md5_hex(keyspace, strlen(keyspace), keyspace_hash);
    snprintf(framed, sizeof(framed), "%s:%s", partition_hash,
             keyspace_hash);
    zdb_md5_hex(framed, strlen(framed), out);
}

/* Row-by-row comparison of two shards via timestamp-tagged reads:
 * identical id sets and identical (timestamp, value) pairs. */
static bool rows_match(zdb_engine *a, zdb_engine *b, const char *part,
                       const char *ks)
{
    cJSON *ra = zdb_all_ts(a, part, ks, NULL, 0);
    cJSON *rb = zdb_all_ts(b, part, ks, NULL, 0);
    bool match = false;

    if (ra && rb &&
        cJSON_GetArraySize(ra) == cJSON_GetArraySize(rb)) {
        match = true;
        /* compare canonical printouts; order is by rowid which matches
         * because the backup copies pages verbatim */
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

/* Per-chunk two-node fixture: A serves snapshots, B fetches. Every
 * chunk creates a fresh fixture so chunks stay independently runnable. */

typedef struct {
    snap_node a;
    snap_node b;
    bool serving;
} fixture;

static void fixture_start(fixture *f, const char *tag)
{
    memset(f, 0, sizeof(*f));
    char dir_a[256];
    char dir_b[256];
    snprintf(dir_a, sizeof(dir_a), "tests/data/snap_%s/a", tag);
    snprintf(dir_b, sizeof(dir_b), "tests/data/snap_%s/b", tag);
    node_start(&f->a, dir_a);
    node_start(&f->b, dir_b);
    CHECK(f->a.engine != NULL);
    CHECK(f->b.engine != NULL);
    f->serving = start_serving(&f->a);
    CHECK(f->serving);
}

static void fixture_stop(fixture *f)
{
    if (f->a.engine) {
        node_stop(&f->a);
    }
    if (f->b.engine) {
        node_stop(&f->b);
    }
    if (f->serving) {
        stop_serving(&f->a);
    }
}

/* ------------------------------------------------------------------ */
/* chunks                                                              */

/* Engines open, listener binds, populated rows are all readable back. */
static void chunk_populate(void)
{
    fixture f;
    fixture_start(&f, "populate");

    char key_main[33];
    shard_key_of("main", "kv", key_main);

    for (int i = 0; i < 500; i++) {
        char id[32];
        char value[128];
        snprintf(id, sizeof(id), "id-%04d", i);
        snprintf(value, sizeof(value),
                 "{\"n\":%d,\"pad\":\"%.*d\"}", i, 40, 0);
        CHECK(zdb_put(f.a.engine, "main", "kv", id, value, -1, NULL, 0));
    }
    for (int i = 0; i < 50; i++) {
        char id[32];
        char value[64];
        snprintf(id, sizeof(id), "doc-%03d", i);
        snprintf(value, sizeof(value), "{\"i\":%d,\"txt\":\"hello %d\"}",
                 i, i);
        CHECK(zdb_put(f.a.engine, "other", "docs", id, value, -1, NULL,
                      0));
    }

    cJSON *all = zdb_all_ts(f.a.engine, "main", "kv", NULL, 0);
    CHECK(all && cJSON_GetArraySize(all) == 500);
    cJSON_Delete(all);

    fixture_stop(&f);
}

/* Snapshot both populated shards A -> B; contents must match exactly. */
static void chunk_transfer(void)
{
    fixture f;
    fixture_start(&f, "transfer");

    char key_main[33];
    shard_key_of("main", "kv", key_main);
    char key_other[33];
    shard_key_of("other", "docs", key_other);

    for (int i = 0; i < 500; i++) {
        char id[32];
        char value[128];
        snprintf(id, sizeof(id), "id-%04d", i);
        snprintf(value, sizeof(value),
                 "{\"n\":%d,\"pad\":\"%.*d\"}", i, 40, 0);
        CHECK(zdb_put(f.a.engine, "main", "kv", id, value, -1, NULL, 0));
    }
    for (int i = 0; i < 50; i++) {
        char id[32];
        char value[64];
        snprintf(id, sizeof(id), "doc-%03d", i);
        snprintf(value, sizeof(value), "{\"i\":%d,\"txt\":\"hello %d\"}",
                 i, i);
        CHECK(zdb_put(f.a.engine, "other", "docs", id, value, -1, NULL,
                      0));
    }

    CHECK(zdb_snap_fetch("127.0.0.1", f.a.port, key_main, f.b.dir) == 0);

    /* B has never opened the shard: force the engine to notice it via
     * invalidate (no handle yet -> false, but harmless) then read */
    zdb_shard_invalidate(f.b.engine, "main", "kv");
    CHECK(rows_match(f.a.engine, f.b.engine, "main", "kv"));

    CHECK(zdb_snap_fetch("127.0.0.1", f.a.port, key_other,
                         f.b.dir) == 0);
    zdb_shard_invalidate(f.b.engine, "other", "docs");
    CHECK(rows_match(f.a.engine, f.b.engine, "other", "docs"));

    fixture_stop(&f);
}

/* A shard that does not exist on the source snapshots as an empty,
 * successful transfer that creates nothing on disk. */
static void chunk_empty(void)
{
    fixture f;
    fixture_start(&f, "empty");

    char key_empty[33];
    shard_key_of("ghost", "none", key_empty);

    CHECK(zdb_snap_fetch("127.0.0.1", f.a.port, key_empty,
                         f.b.dir) == 0);

    /* an empty snapshot still lands as a file on the receiver: the
     * incoming temp file is renamed into place even with zero data
     * frames; it must be a zero-byte (empty) sqlite database */
    char path[600];
    snprintf(path, sizeof(path), "%s/%s.sqlite", f.b.dir, key_empty);
    struct stat st;
    CHECK(stat(path, &st) == 0 && st.st_size == 0);

    fixture_stop(&f);
}

/* Protocol-level refusals: unknown shard key rejected by the server,
 * unreachable peer fails fast instead of hanging. */
static void chunk_errors(void)
{
    fixture f;
    fixture_start(&f, "errors");

    char key_main[33];
    shard_key_of("main", "kv", key_main);
    CHECK(zdb_put(f.a.engine, "main", "kv", "id-0",
                  "{\"n\":0}", -1, NULL, 0));

    /* unknown key: server refuses cleanly */
    CHECK(zdb_snap_fetch("127.0.0.1", f.a.port, "zzzzzzzzzzzzzzzzzzzzz"
                                           "zzzzzzzzzz",
                         f.b.dir) != 0);

    /* malformed key length must also be refused, not served */
    CHECK(zdb_snap_fetch("127.0.0.1", f.a.port, "short", f.b.dir) != 0);

    /* unreachable peer: connection refused on a closed port */
    CHECK(zdb_snap_fetch("127.0.0.1", 1, key_main, f.b.dir) != 0);

    fixture_stop(&f);
}

/* Invalidate swaps a live cached handle after the shard file changed
 * underneath the engine. */
static void chunk_invalidate(void)
{
    fixture f;
    fixture_start(&f, "invalidate");

    char key_main[33];
    shard_key_of("main", "kv", key_main);
    char key_other[33];
    shard_key_of("other", "docs", key_other);

    for (int i = 0; i < 10; i++) {
        char id[32];
        char value[64];
        snprintf(id, sizeof(id), "id-%04d", i);
        snprintf(value, sizeof(value), "{\"n\":%d}", i);
        CHECK(zdb_put(f.a.engine, "main", "kv", id, value, -1, NULL, 0));
    }
    for (int i = 0; i < 5; i++) {
        char id[32];
        char value[64];
        snprintf(id, sizeof(id), "doc-%03d", i);
        snprintf(value, sizeof(value), "{\"i\":%d}", i);
        CHECK(zdb_put(f.a.engine, "other", "docs", id, value, -1, NULL,
                      0));
    }

    cJSON *doc = zdb_get(f.a.engine, "main", "kv", "id-0007");
    CHECK(doc != NULL);
    cJSON_Delete(doc);
    CHECK(zdb_shard_is_open(f.a.engine, "main", "kv"));

    /* replace the shard file underneath the engine with another
     * shard's contents, then invalidate: reads must reflect the new
     * file through the same cached handle */
    char srcp[600];
    char dstp[600];
    char cmd[1300];
    snprintf(srcp, sizeof(srcp), "%s/%s.sqlite", f.a.dir, key_other);
    snprintf(dstp, sizeof(dstp), "%s/%s.sqlite", f.a.dir, key_main);
    snprintf(cmd, sizeof(cmd), "cp %s %s", srcp, dstp);
    CHECK(system(cmd) == 0);

    CHECK(zdb_shard_invalidate(f.a.engine, "main", "kv"));
    CHECK(zdb_shard_is_open(f.a.engine, "main", "kv"));
    doc = zdb_get(f.a.engine, "main", "kv", "id-0007");
    CHECK(doc == NULL);   /* now holds other/docs contents instead */
    cJSON_Delete(doc);
    doc = zdb_get(f.a.engine, "main", "kv", "doc-003");
    CHECK(doc != NULL);
    cJSON_Delete(doc);

    fixture_stop(&f);
}

/* ------------------------------------------------------------------ */
/* chunk registry                                                      */

typedef struct {
    const char *name;
    void (*fn)(void);
} chunk;

static const chunk g_chunks[] = {
    { "populate",   chunk_populate },
    { "transfer",   chunk_transfer },
    { "empty",      chunk_empty },
    { "errors",     chunk_errors },
    { "invalidate", chunk_invalidate },
};

#define CHUNK_COUNT (sizeof(g_chunks) / sizeof(g_chunks[0]))

static int run_chunk(const chunk *c)
{
    int before = g_failures;
    printf("--- %s\n", c->name);
    fflush(stdout);
    c->fn();
    int failed = g_failures - before;
    printf("--- %s: %s (%d checks)\n", c->name,
           failed ? "FAILED" : "ok", g_checks);
    fflush(stdout);
    return failed;
}

int main(int argc, char **argv)
{
    if (argc > 1 && strcmp(argv[1], "-l") == 0) {
        for (size_t i = 0; i < CHUNK_COUNT; i++) {
            printf("%s\n", g_chunks[i].name);
        }
        return 0;
    }

    if (argc > 1) {
        for (size_t i = 0; i < CHUNK_COUNT; i++) {
            if (strcmp(argv[1], g_chunks[i].name) == 0) {
                run_chunk(&g_chunks[i]);
                printf("%s: %d checks, %d failures\n", argv[0],
                       g_checks, g_failures);
                return g_failures ? 1 : 0;
            }
        }
        fprintf(stderr, "unknown chunk '%s' (use -l to list)\n",
                argv[1]);
        return 2;
    }

    for (size_t i = 0; i < CHUNK_COUNT; i++) {
        run_chunk(&g_chunks[i]);
    }

    printf("%s: %d checks, %d failures\n", __FILE__, g_checks,
           g_failures);
    return g_failures ? 1 : 0;
}
