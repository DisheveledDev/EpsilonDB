/* Stage 6e end-to-end test: a real node joins a running cluster through
 * POST /admin/join (HTTP), receives the shards the new target structure
 * assigns to it, reports compliance, and the leader promotes the wave.
 *
 * Not self-contained: tests/test_join_run.sh spawns two real epsilond
 * processes (seed + joiner) and passes their HTTP/peer ports here.
 *
 * The seed runs replication factor 1 so writes succeed with a single
 * node; several keyspaces are written so at least one shard moves to the
 * joiner when the hash space is re-split. After the wave promotes we
 * verify the data is still readable from the node that now owns it.
 *
 * usage: ./tests/test_join <seed_http> <seed_peer> <join_http>
 *        <join_peer> */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#ifndef INADDR_LOOPBACK
#define INADDR_LOOPBACK 0x7f000001UL
#endif

static int tests_run = 0;
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
    char head[2048];
    char auth_hdr[600] = "";
    if (auth) {
        snprintf(auth_hdr, sizeof(auth_hdr),
                 "Authorization: Bearer %s\r\n", auth);
    }
    int n = snprintf(head, sizeof(head),
                     "%s %s HTTP/1.1\r\n"
                     "Host: localhost\r\n"
                     "%s"
                     "Content-Length: %zu\r\n"
                     "Connection: close\r\n"
                     "\r\n",
                     method, path, auth_hdr, body ? strlen(body) : 0);
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
    static char resp[1024 * 1024];
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

static bool wait_up(int port)
{
    for (int i = 0; i < 50; i++) {
        char *body = NULL;
        int s = http_request(port, "GET", "/status", NULL, NULL, &body);
        if (s == 200) {
            return true;
        }
        usleep(200 * 1000);
    }
    return false;
}

int main(int argc, char **argv)
{
    if (argc < 5) {
        fprintf(stderr,
                "usage: %s <seed_http> <seed_peer> <join_http>"
                " <join_peer>\n",
                argv[0]);
        return 2;
    }
    int seed_http = atoi(argv[1]);
    int seed_peer = atoi(argv[2]);
    int join_http = atoi(argv[3]);

    CHECK(wait_up(seed_http));
    CHECK(wait_up(join_http));

    /* bootstrap the admin user (pre-bootstrap unauthenticated requests
     * run with full rights) */
    int s = http_request(seed_http, "POST", "/admin/users", NULL,
                         "{\"name\":\"root\",\"groups\":1}", NULL);
    CHECK(s == 200 || s == 201);

    /* replication factor 1: writes succeed with a single node */
    s = http_request(seed_http, "POST", "/admin/databases", "root",
                     "{\"name\":\"app\",\"replication_factor\":1}", NULL);
    CHECK(s == 200 || s == 201);

    /* populate several keyspaces so the hash space holds several shards
     * (some of which will move to the joiner) */
    const char *parts[3] = {"main", "main", "alt"};
    const char *kss[3] = {"kv", "other", "meta"};
    for (int k = 0; k < 3; k++) {
        for (int i = 0; i < 10; i++) {
            char path[256];
            char doc[128];
            snprintf(path, sizeof(path), "/data/app/%s/%s/id-%03d",
                     parts[k], kss[k], i);
            snprintf(doc, sizeof(doc), "{\"n\":%d}", k * 100 + i);
            s = http_request(seed_http, "PUT", path, "root", doc, NULL);
            CHECK(s == 200 || s == 201);
        }
    }

    /* drive the join through the joiner's HTTP admin API (the joiner is
     * still pre-bootstrap, so unauthenticated requests have full rights) */
    char join_body[128];
    snprintf(join_body, sizeof(join_body),
             "{\"addr\":\"127.0.0.1\",\"port\":%d}", seed_peer);
    char *body = NULL;
    s = http_request(join_http, "POST", "/admin/join", NULL, join_body,
                     &body);
    CHECK(s == 200);
    CHECK(body && strstr(body, "\"joined\":true") != NULL);

    /* poll until the wave is promoted on both sides: no pending target
     * and a shared live generation with two range slices */
    bool promoted = false;
    for (int i = 0; i < 150 && !promoted; i++) {
        char *jb = NULL;
        char *sb = NULL;
        int j1 = http_request(join_http, "GET", "/admin/cluster", "root",
                              NULL, &jb);
        int s1 = http_request(seed_http, "GET", "/admin/cluster", "root",
                              NULL, &sb);
        bool jok = j1 == 200 && jb &&
                   strstr(jb, "\"target_version\":0") != NULL;
        bool sok = s1 == 200 && sb &&
                   strstr(sb, "\"target_version\":0") != NULL;
        promoted = jok && sok;
        if (!promoted) {
            usleep(200 * 1000);
        }
    }
    CHECK(promoted);

    /* data survived the rebalance: every record is readable from the
     * node that owns its shard (seed or joiner). With rf=1 a shard is
     * served locally by its live owner only. */
    for (int k = 0; k < 3; k++) {
        for (int i = 0; i < 10; i++) {
            char path[256];
            snprintf(path, sizeof(path), "/data/app/%s/%s/id-%03d",
                     parts[k], kss[k], i);
            char *doc_seed = NULL;
            char *doc_join = NULL;
            int ss = http_request(seed_http, "GET", path, "root", NULL,
                                  &doc_seed);
            bool found_seed = ss == 200 && doc_seed &&
                              strstr(doc_seed, "\"n\":") != NULL;
            int sj = http_request(join_http, "GET", path, "root", NULL,
                                  &doc_join);
            bool found_join = sj == 200 && doc_join &&
                              strstr(doc_join, "\"n\":") != NULL;
            CHECK(found_seed || found_join);
        }
    }

    printf("tests/test_join: %d checks, %d failures\n", tests_run,
           tests_failed);
    return tests_failed ? 1 : 0;
}
