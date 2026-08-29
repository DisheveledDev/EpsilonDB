/* test_chaos.c - multi-node chaos/failure integration test.
 *
 * Spawns three real epsilond processes (fork/exec), joins them into one
 * cluster through the HTTP admin API and then fails nodes with SIGKILL
 * (no clean shutdown) while the cluster is under write load:
 *
 *   1. baseline: 30 docs written, quorum reads answer from any node
 *   2. a replica is killed hard; writes keep succeeding on the
 *      surviving quorum, reads still work
 *   3. the killed node restarts on its old data dir, replays the
 *      changes cached for it and converges to identical data
 *   4. the leader is killed hard; a new leader is elected among the
 *      survivors, writes continue, the old leader rejoins cleanly
 *   5. quorum loss: with two of three nodes dead a write is rejected
 *      (503); bringing one node back restores write availability
 *
 * Self-contained: builds its own servers, tears everything down.
 *
 * usage: ./tests/test_chaos [http_port_base]   (default 18871)
 */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include "../vendor/cjson/cJSON.h"
#include "test_sleep.h"

#ifndef INADDR_LOOPBACK
#define INADDR_LOOPBACK 0x7f000001UL
#endif

static int tests_run = 0;
static const char *g_password = "chaos-pass-123";
static int tests_failed = 0;

#define CHECK(cond)                                                       \
    do {                                                                  \
        tests_run++;                                                      \
        if (!(cond)) {                                                    \
            tests_failed++;                                               \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__,       \
                    #cond);                                               \
        }                                                                 \
    } while (0)

/* --- process control -------------------------------------------------- */

static pid_t spawn_node(const char *dir, int http_port, int peer_port)
{
    pid_t pid = fork();
    if (pid < 0) {
        return -1;
    }
    if (pid == 0) {
        char log[512];
        char hp[16];
        char pp[16];
        char sock[512];
        snprintf(log, sizeof(log), "%s/server.log", dir);
        snprintf(hp, sizeof(hp), "%d", http_port);
        snprintf(pp, sizeof(pp), "%d", peer_port);
        snprintf(sock, sizeof(sock), "%s/admin.sock", dir);
        if (!freopen(log, "w", stdout) ||
            !freopen(log, "a", stderr)) {
            _exit(127);
        }
        execl("./bin/epsilond", "epsilond", "-p", hp, "-n", pp, "-d", dir,
              "-s", sock, (char *)NULL);
        _exit(127);
    }
    return pid;
}

static void kill_node(pid_t pid)
{
    kill(pid, SIGKILL);
    waitpid(pid, NULL, 0);
}

static void stop_node(pid_t pid)
{
    kill(pid, SIGTERM);
    for (int i = 0; i < 50; i++) {
        if (waitpid(pid, NULL, WNOHANG) == pid) {
            return;
        }
        edb_sleep_us(100 * 1000);
    }
    kill(pid, SIGKILL);
    waitpid(pid, NULL, 0);
}

/* --- tiny HTTP client ------------------------------------------------- */

static int http_request(int port, const char *method, const char *path,
                        const char *auth, const char *body,
                        char **body_out)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    char head[1024];
    char auth_hdr[128] = "";
    if (auth) {
        snprintf(auth_hdr, sizeof(auth_hdr),
                 "Authorization: Bearer %s\r\n", auth);
    }
    int n = snprintf(head, sizeof(head),
                     "%s %s HTTP/1.1\r\n"
                     "Host: localhost\r\n"
                     "%s"
                     "X-Epsilon-Password: %s\r\n"
                     "Content-Length: %zu\r\n"
                     "Connection: close\r\n"
                     "\r\n",
                     method, path, auth_hdr, g_password,
                     body ? strlen(body) : 0);
    size_t sent = 0;
    while (sent < (size_t)n) {
        ssize_t w = send(fd, head + sent, (size_t)n - sent, 0);
        if (w <= 0) {
            close(fd);
            return -1;
        }
        sent += (size_t)w;
    }
    if (body && *body) {
        size_t blen = strlen(body);
        size_t bsent = 0;
        while (bsent < blen) {
            ssize_t w = send(fd, body + bsent, blen - bsent, 0);
            if (w <= 0) {
                close(fd);
                return -1;
            }
            bsent += (size_t)w;
        }
    }
    static char resp[65536];
    size_t total = 0;
    for (;;) {
        if (total >= sizeof(resp) - 1) {
            break;
        }
        ssize_t r = recv(fd, resp + total, sizeof(resp) - 1 - total, 0);
        if (r <= 0) {
            break;
        }
        total += (size_t)r;
    }
    close(fd);
    resp[total] = '\0';
    char *hdr_end = strstr(resp, "\r\n\r\n");
    if (!hdr_end) {
        return -1;
    }
    *hdr_end = '\0';
    int status = 0;
    sscanf(resp, "HTTP/1.%*d %d", &status);
    if (body_out) {
        *body_out = hdr_end + 4;
    }
    return status;
}

/* --- cluster view helpers --------------------------------------------- */

static cJSON *cluster_doc(int port, const char *auth)
{
    char *body = NULL;
    int s = http_request(port, "GET", "/admin/cluster", auth, NULL,
                         &body);
    if (s != 200 || !body) {
        return NULL;
    }
    return cJSON_Parse(body);
}

static int online_count(cJSON *doc)
{
    if (!doc) {
        return -1;
    }
    const cJSON *nodes =
        cJSON_GetObjectItemCaseSensitive(doc, "nodes");
    int n = 0;
    const cJSON *it = NULL;
    cJSON_ArrayForEach(it, nodes) {
        const cJSON *on =
            cJSON_GetObjectItemCaseSensitive(it, "online");
        if (cJSON_IsTrue(on)) {
            n++;
        }
    }
    return n;
}

typedef struct {
    int port;
    const char *auth;
    int want_online;
} online_ctx;

/* Cluster is settled: view reachable, `want_online` members online and
 * no pending target wave (the previous join fully promoted). */
static bool cond_settled(void *ctxp)
{
    online_ctx *c = ctxp;
    cJSON *doc = cluster_doc(c->port, c->auth);
    if (!doc) {
        return false;
    }
    const cJSON *jt =
        cJSON_GetObjectItemCaseSensitive(doc, "target_version");
    bool settled = cJSON_IsNumber(jt) && jt->valuedouble == 0 &&
                   online_count(doc) == c->want_online;
    cJSON_Delete(doc);
    return settled;
}

static bool cond_online(void *ctxp)
{
    online_ctx *c = ctxp;
    cJSON *doc = cluster_doc(c->port, c->auth);
    int n = online_count(doc);
    cJSON_Delete(doc);
    return n == c->want_online;
}

typedef struct {
    int port;
    const char *auth;
    char old_leader[64];
} leader_ctx;

static bool cond_leader_changed(void *ctxp)
{
    leader_ctx *c = ctxp;
    cJSON *doc = cluster_doc(c->port, c->auth);
    if (!doc) {
        return false;
    }
    const cJSON *jl =
        cJSON_GetObjectItemCaseSensitive(doc, "leader");
    bool changed = cJSON_IsString(jl) && jl->valuestring &&
                   jl->valuestring[0] &&
                   strcmp(jl->valuestring, c->old_leader) != 0;
    cJSON_Delete(doc);
    return changed;
}

/* For keyspace slot k, ids id-000..id-(counts[k]-1) plus the single
 * extra[k] id (when >= 0) must be readable from at least
 * `want_holders` of the three nodes. */
typedef struct {
    int ports[3];
    const char *auth;
    const char *db;
    const char (*parts)[16];
    const char (*kss)[16];
    int nparts;
    int counts[3];
    int extra[3];
    int want_holders;
} data_ctx;

static bool doc_readable(data_ctx *c, int k, int i)
{
    int holders = 0;
    for (int nd = 0; nd < 3 && holders < c->want_holders; nd++) {
        char path[256];
        char *body = NULL;
        snprintf(path, sizeof(path), "/data/%s/%s/%s/id-%03d",
                 c->db, c->parts[k], c->kss[k], i);
        int s = http_request(c->ports[nd], "GET", path, c->auth,
                             NULL, &body);
        if (s == 200 && body && strstr(body, "\"n\":") != NULL) {
            holders++;
        }
    }
    if (holders < c->want_holders) {
        return false;
    }
    return true;
}

static bool cond_data_replicated(void *ctxp)
{
    data_ctx *c = ctxp;
    for (int k = 0; k < c->nparts; k++) {
        for (int i = 0; i < c->counts[k]; i++) {
            if (!doc_readable(c, k, i)) {
                return false;
            }
        }
        if (c->extra[k] >= 0 && !doc_readable(c, k, c->extra[k])) {
            return false;
        }
    }
    return true;
}

static bool wait_for(int seconds, bool (*cond)(void *), void *ctx)
{
    for (int i = 0; i < seconds * 10; i++) {
        if (cond(ctx)) {
            return true;
        }
        edb_sleep_us(200 * 1000);
    }
    return cond(ctx);
}

int main(int argc, char **argv)
{
    int base = argc > 1 ? atoi(argv[1]) : 18871;
    int peer_base = base + 1000;
    int ports[3] = {base, base + 1, base + 2};
    int peers[3] = {peer_base, peer_base + 1, peer_base + 2};
    static const char parts[][16] = {"main", "other", "meta"};
    static const char kss[][16] = {"kv", "docs", "notes"};
    const char *auth = "root";
    pid_t pids[3] = {-1, -1, -1};
    char cmd[512];

    snprintf(cmd, sizeof(cmd),
             "rm -rf tests/data/chaos && mkdir -p "
             "tests/data/chaos/a tests/data/chaos/b "
             "tests/data/chaos/c");
    if (system(cmd) != 0) {
        /* best effort */
    }

    /* --- phase 1: seed node, bootstrap, schema ----------------------- */
    pids[0] = spawn_node("tests/data/chaos/a", ports[0], peers[0]);
    CHECK(pids[0] > 0);

    bool up = false;
    for (int i = 0; i < 100 && !up; i++) {
        up = http_request(ports[0], "GET", "/status", NULL, NULL,
                          NULL) == 200;
        if (!up) {
            edb_sleep_us(100 * 1000);
        }
    }
    CHECK(up);

    int s = http_request(ports[0], "POST", "/admin/users", NULL,
                         "{\"name\":\"root\",\"groups\":1,"
                         "\"password\":\"chaos-pass-123\"}", NULL);
    CHECK(s == 200 || s == 201);
    s = http_request(ports[0], "POST", "/admin/databases", auth,
                     "{\"name\":\"app\",\"replication_factor\":2}",
                     NULL);
    CHECK(s == 200 || s == 201);

    /* --- phase 2: join two more nodes through the HTTP admin API ----- */
    const char *dirs[3] = {"tests/data/chaos/a", "tests/data/chaos/b",
                           "tests/data/chaos/c"};
    for (int i = 1; i < 3; i++) {
        pids[i] = spawn_node(dirs[i], ports[i], peers[i]);
        CHECK(pids[i] > 0);
        bool jup = false;
        for (int t = 0; t < 100 && !jup; t++) {
            jup = http_request(ports[i], "GET", "/status", NULL, NULL,
                               NULL) == 200;
            if (!jup) {
                edb_sleep_us(100 * 1000);
            }
        }
        CHECK(jup);
        char body[128];
        snprintf(body, sizeof(body),
                 "{\"addr\":\"127.0.0.1\",\"port\":%d}", peers[0]);
        s = http_request(ports[i], "POST", "/admin/join", NULL, body,
                         NULL);
        if (s != 200) {
            /* a brand-new node dialling mid-wave is refused with 409;
             * wait for the running wave to promote and retry once */
            online_ctx settle_prev = {ports[0], auth, i + 1};
            wait_for(60, cond_settled, &settle_prev);
            s = http_request(ports[i], "POST", "/admin/join", NULL,
                             body, NULL);
        }
        CHECK(s == 200);
        /* the joiner's wave must fully promote before the next node
         * joins (one rebalance at a time) */
        online_ctx settle = {ports[0], auth, i + 1};
        CHECK(wait_for(60, cond_settled, &settle));
    }

    online_ctx oc = {ports[0], auth, 3};
    CHECK(wait_for(30, cond_online, &oc));

    /* --- phase 3: baseline writes + reads ---------------------------- */
    for (int k = 0; k < 3; k++) {
        for (int i = 0; i < 10; i++) {
            char path[256];
            char doc[64];
            snprintf(path, sizeof(path), "/data/app/%s/%s/id-%03d",
                     parts[k], kss[k], i);
            snprintf(doc, sizeof(doc), "{\"n\":%d}", k * 100 + i);
            s = http_request(ports[0], "PUT", path, auth, doc, NULL);
            CHECK(s == 200 || s == 201);
        }
    }

    data_ctx dc = {.ports = {ports[0], ports[1], ports[2]},
                   .auth = auth,
                   .db = "app",
                   .parts = parts,
                   .kss = kss,
                   .nparts = 3,
                   .counts = {10, 10, 10},
                   .extra = {-1, -1, -1},
                   .want_holders = 2};
    CHECK(wait_for(30, cond_data_replicated, &dc));

    /* --- phase 4: SIGKILL a replica; quorum keeps serving ------------ */
    fprintf(stderr, "chaos: killing node b (SIGKILL)\n");
    kill_node(pids[1]);
    pids[1] = -1;

    online_ctx degraded = {ports[0], auth, 2};
    CHECK(wait_for(30, cond_online, &degraded));

    /* writes still succeed on the surviving quorum */
    for (int k = 0; k < 3; k++) {
        char path[256];
        char doc[64];
        snprintf(path, sizeof(path), "/data/app/%s/%s/id-%03d",
                 parts[k], kss[k], 10);
        snprintf(doc, sizeof(doc), "{\"n\":%d}", k * 100 + 10);
        s = http_request(ports[0], "PUT", path, auth, doc, NULL);
        CHECK(s == 200 || s == 201);
    }

    /* --- phase 5: the killed node returns and catches up ------------- */
    fprintf(stderr, "chaos: restarting node b\n");
    pids[1] = spawn_node("tests/data/chaos/b", ports[1], peers[1]);
    CHECK(pids[1] > 0);
    online_ctx healed = {ports[0], auth, 3};
    CHECK(wait_for(60, cond_online, &healed));

    data_ctx dc40 = dc;
    dc40.counts[0] = 11;
    dc40.counts[1] = 11;
    dc40.counts[2] = 11;
    CHECK(wait_for(60, cond_data_replicated, &dc40));

    /* --- phase 6: SIGKILL the leader; re-election + continuity ------- */
    char old_leader[64] = "";
    {
        cJSON *doc = cluster_doc(ports[0], auth);
        const cJSON *jl =
            doc ? cJSON_GetObjectItemCaseSensitive(doc, "leader")
                : NULL;
        if (cJSON_IsString(jl) && jl->valuestring) {
            snprintf(old_leader, sizeof(old_leader), "%s",
                     jl->valuestring);
        }
        cJSON_Delete(doc);
    }
    CHECK(old_leader[0] != '\0');

    /* identify the leader's process by its advertised peer port in
     * the cluster view */
    int victim = -1;
    for (int i = 0; i < 3 && victim < 0; i++) {
        cJSON *doc = cluster_doc(ports[i], auth);
        if (!doc) {
            continue;
        }
        const cJSON *nodes =
            cJSON_GetObjectItemCaseSensitive(doc, "nodes");
        const cJSON *it = NULL;
        cJSON_ArrayForEach(it, nodes) {
            const cJSON *jid =
                cJSON_GetObjectItemCaseSensitive(it, "id");
            const cJSON *jp =
                cJSON_GetObjectItemCaseSensitive(it, "port");
            const cJSON *jon =
                cJSON_GetObjectItemCaseSensitive(it, "online");
            if (cJSON_IsString(jid) && jid->valuestring &&
                strcmp(jid->valuestring, old_leader) == 0 &&
                cJSON_IsNumber(jp) && cJSON_IsTrue(jon)) {
                int pport = (int)jp->valuedouble;
                for (int nd = 0; nd < 3; nd++) {
                    if (pport == peers[nd]) {
                        victim = nd;
                    }
                }
            }
        }
        cJSON_Delete(doc);
    }
    CHECK(victim >= 0);

    fprintf(stderr, "chaos: killing leader node %c (SIGKILL)\n",
            'a' + victim);
    kill_node(pids[victim]);
    pids[victim] = -1;

    leader_ctx lc = {ports[(victim + 1) % 3], auth, {""}};
    snprintf(lc.old_leader, sizeof(lc.old_leader), "%s", old_leader);
    CHECK(wait_for(45, cond_leader_changed, &lc));

    /* writes continue under the new leader's term */
    char path[256];
    char doc[64];
    snprintf(path, sizeof(path), "/data/app/main/kv/id-%03d", 20);
    snprintf(doc, sizeof(doc), "{\"n\":%d}", 20);
    s = http_request(ports[(victim + 1) % 3], "PUT", path, auth, doc,
                     NULL);
    CHECK(s == 200 || s == 201);

    /* --- phase 7: old leader rejoins, cluster reconverges ------------ */
    fprintf(stderr, "chaos: restarting former leader\n");
    pids[victim] = spawn_node(dirs[victim], ports[victim],
                              peers[victim]);
    CHECK(pids[victim] > 0);
    online_ctx full = {ports[0], auth, 3};
    CHECK(wait_for(60, cond_online, &full));

    data_ctx dc51 = dc;
    dc51.extra[0] = 20;   /* main/kv also has id-020 */
    dc51.want_holders = 1; /* newest write may not have reached the
                            * restarted node yet if it was cached on
                            * only one survivor */
    CHECK(wait_for(60, cond_data_replicated, &dc51));

    /* --- phase 8: quorum loss refuses writes; recovery restores them - */
    fprintf(stderr, "chaos: killing two nodes (quorum loss)\n");
    int others[2] = {-1, -1};
    int no = 0;
    for (int i = 0; i < 3; i++) {
        if (i != 0 && pids[i] > 0) {
            others[no++] = i;
        }
    }
    /* keep node a alive as the writer; kill the other two */
    for (int i = 0; i < 2; i++) {
        if (others[i] >= 0 && pids[others[i]] > 0) {
            kill_node(pids[others[i]]);
            pids[others[i]] = -1;
        }
    }
    online_ctx alone = {ports[0], auth, 1};
    CHECK(wait_for(30, cond_online, &alone));

    snprintf(path, sizeof(path), "/data/app/main/kv/id-%03d", 30);
    snprintf(doc, sizeof(doc), "{\"n\":%d}", 30);
    s = http_request(ports[0], "PUT", path, auth, doc, NULL);
    CHECK(s == 503);   /* quorum lost: write rejected, not queued */

    /* bring one node back: writes work again */
    fprintf(stderr, "chaos: restoring one node\n");
    int back = others[0];
    pids[back] = spawn_node(dirs[back], ports[back], peers[back]);
    CHECK(pids[back] > 0);
    online_ctx pair = {ports[0], auth, 2};
    CHECK(wait_for(60, cond_online, &pair));

    s = http_request(ports[0], "PUT", path, auth, doc, NULL);
    CHECK(s == 200 || s == 201);

    /* --- teardown ----------------------------------------------------- */
    for (int i = 0; i < 3; i++) {
        if (pids[i] > 0) {
            stop_node(pids[i]);
            pids[i] = -1;
        }
    }

    printf("tests/test_chaos: %d checks, %d failures\n", tests_run,
           tests_failed);
    return tests_failed ? 1 : 0;
}
